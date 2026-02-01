"""
Async client for Databricks Unity Catalog & Files API (v2.0).
Handles pagination, robust authentication, and consistency checks for FUSE.
"""
import httpx
import logging
import urllib.parse
from email.utils import formatdate, parsedate_to_datetime

logger = logging.getLogger(__name__)

class UnityCatalogClient:
    def __init__(self, workspace_url, auth_provider):
        self.base_url = workspace_url.rstrip("/")
        self.auth_provider = auth_provider

        # Connect timeout is short to fail fast, read timeout longer for large chunks
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=30.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
            follow_redirects=True 
        )

    async def close(self):
        await self.client.aclose()

    async def _get_headers(self):
        token = self.auth_provider.get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json, application/octet-stream"
        }

    async def _request(self, method, url, params=None, headers=None, stream=False):
        """
        Internal wrapper to handle Authentication and Token Refreshing automatically.
        Raises exception on http status error, consider catching 412 if the If-Unmodified-Since header is given.
        """
        if headers is None:
            headers = {}
        
        # Inject dynamic auth token
        base_headers = await self._get_headers()
        headers.update(base_headers)

        request = self.client.build_request(method, url, params=params, headers=headers)
        
        try:
            response = await self.client.send(request, stream=stream)
        except httpx.ConnectError as e:
            logger.error(f"Connection error to {url}: {e}")
            raise

        # 401 Retry Logic (Token Expiration Race Condition)
        if response.status_code == 401:
            logger.warning("Token expired (401). Refreshing and retrying...")
            # Force refresh in the provider
            self.auth_provider.get_access_token(force_refresh=True)
            
            # Re-build request with new token
            headers.update(await self._get_headers())
            request = self.client.build_request(method, url, params=params, headers=headers)
            response = await self.client.send(request, stream=stream)

        if not stream:
            if response.status_code == 404:
                return None
            response.raise_for_status()

            if method == "HEAD":
                return response
            return response.json()
        
        # For streams, we return the response object so caller can .aread() or check status
        if response.status_code >= 400:
             response.raise_for_status()
        return response

    # --- Discovery Layer (Pagination Support) ---

    async def _fetch_all_pages(self, endpoint, key, params=None):
        if params is None:
            params = {}
        results = []
        next_token = None

        while True:
            if next_token:
                params['page_token'] = next_token

            data = await self._request("GET", endpoint, params=params)
            if not data:
                break

            results.extend(data.get(key, []))
            next_token = data.get('next_page_token')
            if not next_token:
                break

        return results

    async def list_catalogs(self):
        return await self._fetch_all_pages("/api/2.1/unity-catalog/catalogs", "catalogs")

    async def list_schemas(self, catalog_name):
        return await self._fetch_all_pages(
            "/api/2.1/unity-catalog/schemas", "schemas", 
            params={"catalog_name": catalog_name}
        )

    async def list_volumes(self, catalog_name, schema_name):
        return await self._fetch_all_pages(
            "/api/2.1/unity-catalog/volumes", "volumes", 
            params={"catalog_name": catalog_name, "schema_name": schema_name}
        )

    # --- File System Layer (Files API 2.0) ---
    
    def _quote_path(self, path):
        if not path.startswith("/"):
            path = "/" + path
        return urllib.parse.quote(path)

    async def get_file_metadata(self, path):
        """
        HEAD request for size/mtime.
        Uses robust RFC 7231 date parsing.
        Returns the size in bytes and the mtime as a timestamp
        """
        encoded_path = self._quote_path(path)
        endpoint = f"/api/2.0/fs/files{encoded_path}"
        
        response = await self._request("HEAD", endpoint)
        if response is None:
            return None

        size = int(response.headers.get("Content-Length", 0))
        last_modified = response.headers.get("Last-Modified")
        
        mtime = 0.0
        if last_modified:
            try:
                dt = parsedate_to_datetime(last_modified)
                mtime = dt.timestamp()
            except Exception:
                logger.warning(f"Failed to parse Last-Modified: {last_modified}")
                pass

        return {"size": size, "mtime": mtime, "is_dir": False}

    async def list_directory_contents(self, path):
        """Lists directory contents with pagination."""
        encoded_path = self._quote_path(path)
        endpoint = f"/api/2.0/fs/directories{encoded_path}"
        
        results = []
        next_token = None
        
        while True:
            params = {}
            if next_token:
                params['page_token'] = next_token
                
            data = await self._request("GET", endpoint, params=params)
            if data is None:
                break # Handle 404 as empty dir
    
            results.extend(data.get("contents", []))
            next_token = data.get("next_page_token")
            if not next_token:
                break
    
        return results

    async def download_chunk(self, path, offset, length, if_unmodified_since=None):
        """
        Downloads binary chunk using Range header.
        Supports conditional read (If-Unmodified-Since) to ensure contents haven't changed by another user while reading them.
        """
        encoded_path = self._quote_path(path)
        endpoint = f"/api/2.0/fs/files{encoded_path}"

        headers = {"Range": f"bytes={offset}-{offset + length - 1}"}

        if if_unmodified_since:
            headers["If-Unmodified-Since"] = formatdate(if_unmodified_since, usegmt=True)
    
        response = await self._request("GET", endpoint, headers=headers, stream=True)
        return await response.aread()
