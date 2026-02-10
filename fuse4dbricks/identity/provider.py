"""
Authentication provider for Azure Databricks using Microsoft Entra ID.
Thread-safe implementation using Double-Checked Locking.
"""

import logging
import threading
import time
from abc import ABC, abstractmethod

import msal  # type: ignore[import-untyped]
import pyfuse3
import errno


logger = logging.getLogger(__name__)

class AuthProvider(ABC):
    @abstractmethod
    def get_access_token(self, ctx_uid: int, force_refresh: bool = False):
        """
        Abstract method to retrieve an access token.
        :param force_refresh: If True, forces a token refresh.
        """
        pass

    @abstractmethod
    def get_principal(self, ctx_uid: int) -> str:
        # get the principal for a uid
        pass

    @abstractmethod
    def set_credentials(self, ctx_uid: int, *, principal: str|None = None, access_token: str|None=None):
        pass

class AccessTokenAuthProvider(AuthProvider):
    def __init__(self):
        self._uid_to_principal: dict[int, str] = {}
        "uid -> principal"
        self._uid_to_access_token: dict[int, str] = {}
        "uid -> access_token"

    def get_access_token(self, ctx_uid: int, force_refresh: bool = False) -> str:
        try:
            return self._uid_to_access_token[ctx_uid]
        except KeyError as exc:
            raise pyfuse3.FUSEError(errno.EACCES) from exc

    def get_principal(self, ctx_uid: int) -> str:
        try:
            return self._uid_to_principal[ctx_uid]
        except KeyError as exc:
            raise pyfuse3.FUSEError(errno.EACCES) from exc

    def set_credentials(self, ctx_uid: int, *, principal: str|None = None, access_token: str|None = None):
        if principal is not None:
            self._uid_to_principal[ctx_uid] = principal
        if access_token is not None:
            self._uid_to_access_token[ctx_uid] = access_token

class EntraIDAuthProvider(AuthProvider):
    """
    Handles OAuth2 Device Code Flow and silent token refreshment.
    """

    # Static ID for Azure Databricks resource (Standard Azure Public Cloud)
    DATABRICKS_RESOURCE_ID = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"

    def __init__(self, tenant_id, client_id, keyring_manager):
        """
        :param tenant_id: Azure Tenant ID.
        :param client_id: Azure Application Client ID.
        :param keyring_manager: Instance of LinuxKeyringManager.
        """
        self.keyring = keyring_manager
        self.scopes = [f"{self.DATABRICKS_RESOURCE_ID}/.default"]
        self.authority = f"https://login.microsoftonline.com/{tenant_id}"

        self.app = msal.PublicClientApplication(client_id, authority=self.authority)

        self._access_token = None
        self._expires_at = 0.0

        # Lock to prevent race conditions during token refresh
        self._lock = threading.Lock()

    def get_access_token(self, ctx_uid: int, force_refresh=False):
        """
        Main entry point to get a valid token. Thread-safe.
        """
        # 1. FAST CHECK (No Lock - Performance)
        # If the token is valid and we are not forcing a refresh, return immediately.
        # This prevents blocking readers 99.9% of the time.
        if not force_refresh and self._is_token_valid():
            return self._access_token

        # 2. CRITICAL SECTION (With Lock - Safety)
        # If we reach here, the token has expired (or is about to).
        # We acquire the lock to ensure ONLY ONE thread performs the refresh.
        with self._lock:

            # 3. DOUBLE-CHECK
            # Why? Because while we were waiting for the lock, another thread
            # might have already entered and refreshed the token.
            # If it is valid now, great! We use it and save a network call.
            if not force_refresh and self._is_token_valid():
                return self._access_token

            # 4. ACTUAL REFRESH
            # We are now certain we are the chosen one to perform the refresh.

            # Attempt 1: Silent (Cache / Refresh Token)
            token_res = self._refresh_silently()
            if token_res:
                return token_res

            # Attempt 2: Interactive (Device Code)
            # Note: This will block all threads until the user logs in.
            # This is desired behavior (we don't want 50 simultaneous login prompts).
            return self._run_device_code_flow()

    def _is_token_valid(self):
        """Helper to check validity buffer."""
        # Buffer of 60 seconds before actual expiration
        return self._access_token and time.time() < (self._expires_at - 60.0)

    def _refresh_silently(self):
        """
        Attempts to refresh the token using the cached account or the Keyring.
        """
        # Check MSAL memory cache first
        accounts = self.app.get_accounts()
        if accounts:
            try:
                result = self.app.acquire_token_silent(self.scopes, account=accounts[0])
                if result and "access_token" in result:
                    self._update_internal_state(result)
                    return result["access_token"]
            except Exception as e:
                logger.warning(f"Silent refresh failed: {e}")

        # Check Keyring for persistent refresh token
        refresh_token = self.keyring.get_refresh_token()
        if refresh_token:
            logger.info("Found refresh token in Keyring. Attempting exchange...")
            try:
                result = self.app.acquire_token_by_refresh_token(
                    refresh_token, scopes=self.scopes
                )
                if "access_token" in result:
                    self._update_internal_state(result)
                    return result["access_token"]
            except Exception as e:
                logger.error(f"Refresh token exchange failed: {e}")

        return None

    def _run_device_code_flow(self):
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
                self._update_internal_state(result)
                return result["access_token"]
            else:
                error = result.get("error_description", "Unknown error")
                raise PermissionError(f"Auth failed: {error}")
        except Exception as e:
            logger.error(f"Device flow crashed: {e}")
            raise

    def _update_internal_state(self, msal_result):
        """
        Updates the in-memory cache and persists the refresh token.
        """
        self._access_token = msal_result["access_token"]
        # Buffer of 60 seconds is handled in _is_token_valid,
        # but we store the raw expiration here.
        self._expires_at = time.time() + float(msal_result.get("expires_in", 3600.0))

        # Persist the new refresh token in the keyring
        if "refresh_token" in msal_result:
            self.keyring.save_refresh_token(msal_result["refresh_token"])
