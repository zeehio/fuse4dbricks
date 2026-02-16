"""
Authentication provider for Azure Databricks using Microsoft Entra ID.
Thread-safe implementation using Double-Checked Locking.
"""

import os
import json
import logging
import trio
from enum import Enum
from typing import Any
import time

import msal  # type: ignore[import-untyped]

from fuse4dbricks.fs.utils import join_or_lead_request, notify_followers

logger = logging.getLogger(__name__)

class AuthState(Enum):
    START = "START"
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class EntraIDConfidentialAuthProvider:
    """
    Handles OAuth2 Device Code Flow and silent token refreshment.
    """

    # Static ID for Azure Databricks resource (Standard Azure Public Cloud)
    DATABRICKS_RESOURCE_ID = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"

    def __init__(self, tenant_id, client_id, client_secret, cache_dir):
        """
        :param tenant_id: Azure Tenant ID.
        :param client_id: Azure Application Client ID.
        """
        raise NotImplementedError("Needs proper testing and integration")
        self._refresh_token: dict[int, str] = {}
        "uid -> refresh_token"
        self.scopes = [f"{self.DATABRICKS_RESOURCE_ID}/.default"]
        self.authority = f"https://login.microsoftonline.com/{tenant_id}"

        #self.app = msal.PublicClientApplication(client_id, authority=self.authority)
        self.app = msal.ConfidentialClientApplication(client_id, client_secret, authority=self.authority)

        self._auth_state: dict[int, AuthState] = {}
        self._device_flow: dict[int, Any] = {}
        self._access_token: dict[int, str] = {}
        self._expires_at: dict[int, float] = {}

        self._inflight_refresh: dict[int, trio.Event] = {}
        self._inflight_lock = trio.Lock()
        "uid -> threading.Event for that user's token refresh"

    async def get_access_token(self, ctx_uid: int, force_refresh=False) -> str|None:
        """
        Main entry point to get a valid token. Thread-safe.
        """
        # 1. FAST CHECK (No Lock - Performance)
        # If the token is valid and we are not forcing a refresh, return immediately.
        # This prevents blocking readers 99.9% of the time.
        if self._auth_state.get(ctx_uid) != AuthState.SUCCESS:
            return None
        if not force_refresh and self._is_token_valid(ctx_uid = ctx_uid):
            return self._access_token.get(ctx_uid)

        # 2. Request Coalescing
        wait_event = await join_or_lead_request(
            self._inflight_lock, self._inflight_refresh, ctx_uid
        )

        if wait_event:
            await wait_event.wait()
            # Wake up: Check cache again (Leader should have filled it)
            if self._auth_state.get(ctx_uid) != AuthState.SUCCESS:
                return None
            return self._access_token.get(ctx_uid)

        # 3. Real API Call (Leader Only)
        try:
            # Attempt 1: Silent (Cache / Refresh Token)
            token_res = self._refresh_silently(ctx_uid=ctx_uid)
            if token_res:
                return token_res
            else:
                self._auth_state[ctx_uid] = AuthState.START
        finally:
            await notify_followers(
                self._inflight_lock, self._inflight_refresh, ctx_uid
            )
        return self._access_token.get(ctx_uid)


    def _is_token_valid(self, ctx_uid: int) -> bool:
        """Helper to check validity buffer."""
        # Buffer of 60 seconds before actual expiration
        return self._access_token.get(ctx_uid) is not None and time.time() < (self._expires_at.get(ctx_uid, 0.0) - 60.0)

    def _refresh_silently(self, ctx_uid: int) -> str|None:
        """
        Attempts to refresh the token using the cached account or the Keyring.
        """
        # Check MSAL memory cache first
        accounts = self.app.get_accounts()
        if accounts:
            try:
                result = self.app.acquire_token_silent(self.scopes, account=accounts[0])
                if result and "access_token" in result:
                    self._update_internal_state(result, ctx_uid)
                    return result["access_token"]
            except Exception as e:
                logger.warning(f"Silent refresh failed: {e}")

        # Check Keyring for persistent refresh token
        refresh_token = self._refresh_token
        if refresh_token:
            logger.info("Found refresh token in Keyring. Attempting exchange...")
            try:
                result = self.app.acquire_token_by_refresh_token(
                    refresh_token, scopes=self.scopes
                )
                if "access_token" in result:
                    self._update_internal_state(result, ctx_uid)
                    return result["access_token"]
            except Exception as e:
                logger.error(f"Refresh token exchange failed: {e}")

        return None

    def initiate_device_flow(self, ctx_uid) -> str:
        flow = self.app.initiate_auth_code_flow(scopes=self.scopes)
        #flow = self.app.initiate_device_flow(scopes=self.scopes)
        print(flow)
        #if "message" not in flow:
        #    raise RuntimeError("Failed to initiate Device Code Flow.")
        message = "\n".join([
            "=" * 60,
            flow.get("message", ""),
            "=" * 60,
        ]) + "\n"
        self._device_flow[ctx_uid] = flow
        self._auth_state[ctx_uid] = AuthState.PENDING
        return message

    def poll_device_flow_token(self, ctx_uid, timeout=5) -> str | None:
        state = self._auth_state.get(ctx_uid, AuthState.START)
        if state == AuthState.PENDING:
                flow = self._device_flow.get(ctx_uid)
                if flow is None:
                    self._auth_state[ctx_uid] = AuthState.START
                    return None
                orig_expiration = flow["expires_at"]
                short_expiration = time.time() + timeout
                try:
                    if short_expiration < orig_expiration:
                        flow["expires_at"] = short_expiration
                    result = self.app.acquire_token_by_device_flow(flow)
                finally:
                    flow["expires_at"] = orig_expiration
                if "access_token" in result:
                    self._access_token[ctx_uid] = result["access_token"]
                    return result["access_token"]
                else:
                    error = result.get("error_description", "Unknown error")
                    raise PermissionError(f"Auth failed: {error}")
        return None

    def _run_device_code_flow(self, ctx_uid):
        """
        Executes the interactive authentication flow.
        """
        try:
            flow = self.app.initiate_device_flow(scopes=self.scopes)
            if "message" not in flow:
                raise RuntimeError("Failed to initiate Device Code Flow.")

            print("\n" + "=" * 60)
            print(flow["message"])
            print("=" * 60 + "\n")

            result = self.app.acquire_token_by_device_flow(flow)

            if "access_token" in result:
                self._update_internal_state(result, ctx_uid)
                return result["access_token"]
            else:
                error = result.get("error_description", "Unknown error")
                raise PermissionError(f"Auth failed: {error}")
        except Exception as e:
            logger.error(f"Device flow crashed: {e}")
            raise

    def _update_internal_state(self, msal_result, ctx_uid):
        """
        Updates the in-memory cache and persists the refresh token.
        """
        self._access_token[ctx_uid] = msal_result["access_token"]
        # Buffer of 60 seconds is handled in _is_token_valid,
        # but we store the raw expiration here.
        self._expires_at[ctx_uid] = time.time() + float(msal_result.get("expires_in", 3600.0))

        # Persist the new refresh token
        if "refresh_token" in msal_result:
            self._refresh_token[ctx_uid] = msal_result["refresh_token"]


######################################



class EntraIDPublicAuthProvider:
    def __init__(self, tenant_id, client_id, cache_dir):
        raise NotImplementedError("Needs proper testing and integration")
        self.tenant_id = tenant_id
        self.client_id = client_id
        self._cache_dir = cache_dir
        self.authority = f"https://login.microsoftonline.com/{tenant_id}"
        self.resource_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
        self.scopes = [f"{self.resource_id}/.default"]
        
        self._ensure_dir_permissions(self._cache_dir)
        
        self.app = msal.PublicClientApplication(
            self.client_id, 
            authority=self.authority
        )

    def _ensure_dir_permissions(self, path):
        """Recursively ensures directory exists with 0700 permissions."""
        if not os.path.exists(path):
            os.makedirs(path, mode=0o700)
        else:
            os.chmod(path, 0o700)

    def _get_user_cache_path(self, username: str):
        """Sanitizes username to create a valid, secure file path."""
        # Simple sanitization for file system safety
        safe_name = "".join([c for c in username if c.isalnum() or c in ("@", ".", "_")]).rstrip()
        return os.path.join(self._cache_dir, f"{safe_name}.bin")

    def _load_user_cache(self, username: str) -> msal.SerializableTokenCache:
        cache = msal.SerializableTokenCache()
        path = self._get_user_cache_path(username)
        if os.path.exists(path):
            with open(path, "r") as f:
                cache.deserialize(f.read())
        return cache

    def _save_user_cache(self, username: str, cache: msal.SerializableTokenCache):
        path = self._get_user_cache_path(username)
        if cache.has_state_changed:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, 'w') as f:
                f.write(cache.serialize())

    async def get_silent_token(self, username: str):
        """Attempts to refresh token for a specific user from their specific cache."""
        user_cache = self._load_user_cache(username)
        # We create a temporary app tied to this specific user's cache
        temp_app = msal.PublicClientApplication(
            self.client_id, authority=self.authority, token_cache=user_cache
        )
        
        accounts = temp_app.get_accounts(username=username)
        if accounts:
            result = await trio.to_thread.run_sync(
                temp_app.acquire_token_silent, self.scopes, accounts[0]
            )
            if result:
                self._save_user_cache(username, user_cache)
                return result.get("access_token")
        return None

    async def login_new_user(self, timeout=180):
        """Flow for a new user. Resolves their identity after successful login."""
        flow = self.app.initiate_device_flow(scopes=self.scopes)
        print(f"\n[NEW LOGIN] {flow['message']}\n")

        try:
            with trio.fail_after(timeout):
                result = await trio.to_thread.run_sync(
                    self.app.acquire_token_by_device_flow, flow
                )
            print(result)
            if "access_token" in result:
                # Identify who just logged in
                username = result.get("id_token_claims", {}).get("preferred_username")
                print(f"User '{username}' logged in successfully.")
                if not username:
                    raise Exception("Could not determine username from token.")
                
                # Save into a user-specific cache
                user_cache = msal.SerializableTokenCache()
                # Re-run a dummy app to populate the cache object from the result
                temp_app = msal.PublicClientApplication(
                    self.client_id, authority=self.authority, token_cache=user_cache
                )
                # This internal call populates the cache with the 'result' we just got
                user_cache.deserialize(json.dumps(user_cache.serialize())) # Helper to trigger sync
                
                # Actually, the easiest way is to let MSAL handle the result into a cache:
                # We initialize a new cache and manually add the result.
                # But even better: acquire_token_by_device_flow already updated self.app.token_cache
                # if we had one. Since we didn't, we extract and save:
                
                # To keep it simple: we save the whole result as the initial cache state for that user
                # but we must use a cache object to do it correctly.
                final_cache = msal.SerializableTokenCache()
                temp_app = msal.PublicClientApplication(self.client_id, token_cache=final_cache)
                # This is a trick to 'ingest' the result into a specific cache
                await trio.to_thread.run_sync(temp_app.acquire_token_by_device_flow, flow)
                
                self._save_user_cache(username, final_cache)
                return username, result["access_token"]
            
        except trio.TooSlowError:
            raise TimeoutError("Device flow expired.")

##############################

async def manage_user(auth, username):
    token = await auth.get_silent_token(username)
    if token:
        print(f"User {username} is already logged in.")
    else:
        print(f"User {username} needs re-authentication.")
