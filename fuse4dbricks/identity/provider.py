"""
Authentication provider for Azure Databricks using Microsoft Entra ID.
"""
import time
import msal
import logging

logger = logging.getLogger(__name__)

class EntraIDAuthProvider:
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
        
        self.app = msal.PublicClientApplication(
            client_id, 
            authority=self.authority
        )
        
        self._access_token = None
        self._expires_at = 0

    def get_access_token(self, force_refresh=False):
        """
        Main entry point to get a valid token.
        """
        # 1. Use in-memory token if valid
        if self._access_token and time.time() < self._expires_at - 60 and not force_refresh:
            return self._access_token

        # 2. Try silent refresh (MSAL cache + Keyring)
        token_res = self._refresh_silently()
        if token_res:
            return token_res

        # 3. Fallback to Device Code Flow
        return self._run_device_code_flow()

    def _refresh_silently(self):
        """
        Attempts to refresh the token using the cached account or the Keyring.
        """
        accounts = self.app.get_accounts()
        if accounts:
            result = self.app.acquire_token_silent(self.scopes, account=accounts[0])
            if result and "access_token" in result:
                self._update_internal_state(result)
                return result["access_token"]
        
        # If MSAL cache is empty (e.g. script restart), check Keyring
        refresh_token = self.keyring.get_refresh_token()
        if refresh_token:
            logger.info("Found refresh token in Keyring. Attempting exchange...")
            # We use acquire_token_by_refresh_token provided by MSAL
            result = self.app.acquire_token_by_refresh_token(refresh_token, scopes=self.scopes)
            if "access_token" in result:
                self._update_internal_state(result)
                return result["access_token"]
        
        return None

    def _run_device_code_flow(self):
        """
        Executes the interactive authentication flow.
        """
        flow = self.app.initiate_device_flow(scopes=self.scopes)
        if "message" not in flow:
            raise RuntimeError("Failed to initiate Device Code Flow.")

        print("\n" + "="*60)
        print(flow["message"])
        print("="*60 + "\n")

        result = self.app.acquire_token_by_device_flow(flow)
        
        if "access_token" in result:
            self._update_internal_state(result)
            return result["access_token"]
        else:
            raise PermissionError(f"Auth failed: {result.get('error_description')}")

    def _update_internal_state(self, msal_result):
        """
        Updates the in-memory cache and persists the refresh token.
        """
        self._access_token = msal_result["access_token"]
        # Buffer of 60 seconds for safety
        self._expires_at = time.time() + int(msal_result.get("expires_in", 3600))
        
        # Persist the new refresh token in the kernel
        if "refresh_token" in msal_result:
            self.keyring.save_refresh_token(msal_result["refresh_token"])
