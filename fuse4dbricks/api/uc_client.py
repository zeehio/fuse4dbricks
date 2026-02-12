"""
Async client for Databricks Unity Catalog & Files API (v2.0).
Handles pagination, robust authentication, and asynchronous streaming for FUSE.
"""

import logging
import os
import urllib.parse
from dataclasses import dataclass
from email.utils import formatdate, parsedate_to_datetime
from enum import Enum
from typing import AsyncGenerator

import httpx

from fuse4dbricks.identity.provider import AuthProvider

logger = logging.getLogger(__name__)


class UcNodeType(Enum):
    """Unity Catalog node types, with the root node"""

    ROOT = 0
    CATALOG = 1
    SCHEMA = 2
    VOLUME = 3
    DIRECTORY = 4
    FILE = 5


@dataclass
class UnityCatalogEntry:
    name: str
    uc_path: str
    "The databricks path /Volumes/catalog/schema..."
    entry_type: UcNodeType
    size: int = 0
    ctime: float | None = None
    mtime: float | None = None

    def is_dir(self):
        UC_NODE_IS_DIR = {
            UcNodeType.ROOT: True,
            UcNodeType.CATALOG: True,
            UcNodeType.SCHEMA: True,
            UcNodeType.VOLUME: True,
            UcNodeType.DIRECTORY: True,
            UcNodeType.FILE: False,
        }
        return UC_NODE_IS_DIR[self.entry_type]


class UnityCatalogClient:
    def __init__(self, workspace_url: str, auth_provider: AuthProvider):
        self.base_url = workspace_url.rstrip("/")
        self.auth_provider = auth_provider

        # Connect timeout is short to fail fast, read timeout longer for large chunks
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=30.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
            follow_redirects=True,
        )

    async def close(self):
        await self.client.aclose()

    async def _get_headers(self, ctx_uid):
        token = self.auth_provider.get_access_token(ctx_uid=ctx_uid)
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json, application/octet-stream",
        }

    async def _request(self, method, url, *, ctx_uid, params=None, headers=None, stream=False):
        """
        Internal wrapper to handle Authentication and Token Refreshing automatically.
        """
        if headers is None:
            headers = {}

        base_headers = await self._get_headers(ctx_uid=ctx_uid)
        headers.update(base_headers)

        request = self.client.build_request(method, url, params=params, headers=headers)

        try:
            response = await self.client.send(request, stream=stream)
        except httpx.ConnectError as e:
            logger.error(f"Connection error to {url}: {e}")
            raise

        # 401 Retry Logic (Token Expiration)
        if response.status_code == 401:
            logger.warning("Token expired (401). Refreshing and retrying...")
            self.auth_provider.get_access_token(ctx_uid=ctx_uid, force_refresh=True)

            headers.update(await self._get_headers(ctx_uid=ctx_uid))
            # For stream=True, we must ensure the first response is closed before retrying
            if stream:
                await response.aclose()

            request = self.client.build_request(
                method, url, params=params, headers=headers
            )
            response = await self.client.send(request, stream=stream)

        if not stream:
            if response.status_code == 404:
                return None
            response.raise_for_status()

            if method == "HEAD":
                return response
            return response.json()

        # For streams, we return the response object to be used in a context manager
        if response.status_code >= 400:
            await response.aclose()
            response.raise_for_status()
        return response

    # --- Discovery Layer (Pagination Support) ---

    async def _fetch_all_pages(self, endpoint, key, *, ctx_uid, params=None):
        if params is None:
            params = {}
        results = []
        next_token = None

        while True:
            if next_token:
                params["page_token"] = next_token

            data = await self._request("GET", endpoint, ctx_uid=ctx_uid, params=params)
            if not data:
                break

            results.extend(data.get(key, []))
            next_token = data.get("next_page_token")
            if not next_token:
                break
        return results

    async def _get_catalogs(self, ctx_uid: int) -> list[UnityCatalogEntry]:
        catalogs = await self._fetch_all_pages(
            "/api/2.1/unity-catalog/catalogs", "catalogs", ctx_uid=ctx_uid,
        )
        return [
            UnityCatalogEntry(
                name=x["name"],
                uc_path="/Volumes/" + x["name"],
                entry_type=UcNodeType.CATALOG,
                ctime=float(x["created_at"]) / 1000.0,
                mtime=float(x["updated_at"]) / 1000.0,
            )
            for x in catalogs
        ]

    async def get_current_user_info(self, ctx_uid: int) -> str:
        endpoint = "/api/2.0/preview/scim/v2/Me"
        resp = await self._request("GET", endpoint, ctx_uid=ctx_uid)
        return resp["userName"]

    async def check_permissions(self, securable: str, securable_type: str,
                                privileges: list[str], ctx_uid: int) -> bool:
        endpoint = f"/api/2.1/unity-catalog/effective-permissions/{securable_type}/{securable}"
        principal = self.auth_provider.get_principal(ctx_uid)
        priv_assignments = await self._fetch_all_pages(
            endpoint, "privilege_assignments", ctx_uid=ctx_uid,
            params={"principal": principal},
        )
        all_privs = set()
        for priv_assign in priv_assignments:
            for privilege in priv_assign["privileges"]:
                all_privs.add(privilege["privilege"])
        return set(privileges).issubset(all_privs)


    async def _get_catalog(self, catalog_name: str, ctx_uid: int) -> UnityCatalogEntry | None:
        endpoint = f"/api/2.1/unity-catalog/catalogs/{catalog_name}"
        catalog = await self._request("GET", endpoint, ctx_uid=ctx_uid)
        if not catalog:
            return None
        return UnityCatalogEntry(
            name=catalog["name"],
            uc_path="/Volumes/" + catalog["name"],
            entry_type=UcNodeType.CATALOG,
            ctime=float(catalog["created_at"]) / 1000.0,
            mtime=float(catalog["updated_at"]) / 1000.0,
        )

    async def _get_schemas(self, catalog_name: str, ctx_uid) -> list[UnityCatalogEntry]:
        schemas = await self._fetch_all_pages(
            "/api/2.1/unity-catalog/schemas",
            "schemas",
            ctx_uid=ctx_uid,
            params={"catalog_name": catalog_name},
        )
        return [
            UnityCatalogEntry(
                name=x["name"],
                uc_path="/Volumes/" + x["catalog_name"] + "/" + x["name"],
                entry_type=UcNodeType.SCHEMA,
                ctime=float(x["created_at"]) / 1000.0,
                mtime=float(x["updated_at"]) / 1000.0,
            )
            for x in schemas
        ]

    async def _get_schema(self, catalog_name, schema_name, ctx_uid: int) -> UnityCatalogEntry | None:
        endpoint = f"/api/2.1/unity-catalog/schemas/{catalog_name}.{schema_name}"
        schema = await self._request("GET", endpoint, ctx_uid=ctx_uid)
        if not schema:
            return None
        return UnityCatalogEntry(
            name=schema["name"],
            uc_path="/Volumes/" + schema["catalog_name"] + "/" + schema["name"],
            entry_type=UcNodeType.SCHEMA,
            ctime=float(schema["created_at"]) / 1000.0,
            mtime=float(schema["updated_at"]) / 1000.0,
        )

    async def _get_volumes(self, catalog_name, schema_name, ctx_uid: int):
        volumes = await self._fetch_all_pages(
            "/api/2.1/unity-catalog/volumes",
            "volumes",
            ctx_uid=ctx_uid,
            params={"catalog_name": catalog_name, "schema_name": schema_name},
        )
        return [
            UnityCatalogEntry(
                name=x["name"],
                uc_path="/Volumes/"
                + x["catalog_name"]
                + "/"
                + x["schema_name"]
                + "/"
                + x["name"],
                entry_type=UcNodeType.VOLUME,
                ctime=float(x["created_at"]) / 1000.0,
                mtime=float(x["updated_at"]) / 1000.0,
            )
            for x in volumes
        ]

    async def _get_volume(
        self, catalog_name, schema_name, volume_name,
        ctx_uid: int,
    ) -> UnityCatalogEntry | None:
        endpoint = (
            f"/api/2.1/unity-catalog/volumes/{catalog_name}.{schema_name}.{volume_name}"
        )
        volume = await self._request("GET", endpoint, ctx_uid=ctx_uid)
        if not volume:
            return None
        return UnityCatalogEntry(
            name=volume["name"],
            uc_path="/Volumes/"
            + volume["catalog_name"]
            + "/"
            + volume["schema_name"]
            + "/"
            + volume["name"],
            entry_type=UcNodeType.VOLUME,
            ctime=float(volume["created_at"]) / 1000.0,
            mtime=float(volume["updated_at"]) / 1000.0,
        )

    # --- File System Layer (Files API 2.0) ---

    def _quote_path(self, path):
        if not path.startswith("/"):
            path = "/" + path
        return urllib.parse.quote(path)

    async def _get_file_metadata(self, path: str, ctx_uid) -> UnityCatalogEntry | None:
        """HEAD request for size/mtime."""
        encoded_path = self._quote_path(path)
        endpoint = f"/api/2.0/fs/files{encoded_path}"

        response = await self._request("HEAD", endpoint, ctx_uid=ctx_uid)
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
        return UnityCatalogEntry(
            name=os.path.basename(path),
            uc_path=path,
            entry_type=UcNodeType.FILE,
            size=size,
            mtime=mtime,
        )

    async def _get_directory_metadata(self, path: str, ctx_uid) -> UnityCatalogEntry | None:
        """HEAD request, checks for existance."""
        encoded_path = self._quote_path(path)
        endpoint = f"/api/2.0/fs/directories{encoded_path}"

        response = await self._request("HEAD", endpoint, ctx_uid=ctx_uid)
        if response is None:
            return None
        return UnityCatalogEntry(
            name=os.path.basename(path),
            uc_path=path,
            entry_type=UcNodeType.DIRECTORY,
        )

    async def get_path_metadata(
        self, uc_path: str, *, ctx_uid: int, expected_type: UcNodeType | None = None
    ) -> UnityCatalogEntry | None:
        """
        Gets the catalog entry. None if not found. expected_type is optional

        :param uc_path: The /Volumes/catalog/Schema/volume/path/to/file.txt
        :param expected_type: You may set this to UcNodeType.DIRECTORY if you expect the path to be a
           folder. You save one request if you do it. defaults to None.
        :raises ValueError: When the path is not valid
        :return: A UnityCatalogEntry holding path metadata
        """
        logger.debug(f"uc_client.get_path_metadata for {uc_path}")
        if not uc_path.startswith("/"):
            raise ValueError("uc_path should start with /Volumes")
        parts = uc_path.split("/")
        if len(parts) < 2 or parts[1] != "Volumes":
            raise ValueError("uc_path should start with /Volumes")
        if len(parts) == 2:
            raise ValueError("'/Volumes' is not a unity catalog entry.")
        catalog = parts[2]
        if len(parts) == 3:
            return await self._get_catalog(catalog, ctx_uid=ctx_uid)
        schema = parts[3]
        if len(parts) == 4:
            return await self._get_schema(catalog, schema, ctx_uid=ctx_uid)
        volume = parts[4]
        if len(parts) == 5:
            return await self._get_volume(catalog, schema, volume, ctx_uid=ctx_uid)
        if expected_type == UcNodeType.DIRECTORY:
            entry = await self._get_directory_metadata(uc_path, ctx_uid=ctx_uid)
            if entry is None:
                # File not found, check if that path is now a directory (because
                # someone created a folder with that same name)
                entry = await self._get_file_metadata(uc_path, ctx_uid=ctx_uid)
                # Returns either the folder or None if not found
                return entry
        entry = await self._get_file_metadata(uc_path, ctx_uid=ctx_uid)
        if entry is None:
            entry = await self._get_directory_metadata(uc_path, ctx_uid=ctx_uid)
        return entry

    async def _list_directory_contents(
        self, path, *, ctx_uid: int, limit=None
    ) -> list[UnityCatalogEntry] | None:
        """Lists directory contents, or None if not a directory"""
        encoded_path = self._quote_path(path)
        endpoint = f"/api/2.0/fs/directories{encoded_path}"

        results = []
        next_token = None
        first_page = True
        while True:
            params: dict[str, str] = {}
            if next_token:
                params["page_token"] = next_token

            data = await self._request("GET", endpoint, params=params, ctx_uid=ctx_uid)
            if first_page and data is None:
                return None  # Possibly not a directory
            if data is None:
                break  # Possibly an empty directory (or no new files)
            first_page = False
            contents = data.get("contents", [])
            for x in contents:
                results.append(
                    UnityCatalogEntry(
                        name=x["name"],
                        uc_path=x["path"],
                        entry_type=(
                            UcNodeType.DIRECTORY
                            if x["is_directory"]
                            else UcNodeType.FILE
                        ),
                        size=x.get("file_size", 0),
                        mtime=float(x.get("last_modified", 0.0)) / 1000.0,
                    )
                )
            next_token = data.get("next_page_token")
            if not next_token:
                break
            if limit is not None and len(results) >= limit:
                break
        return results

    async def get_path_contents(self, uc_path: str, ctx_uid: int) -> list[UnityCatalogEntry] | None:
        """
        Lists the path contents, or returns None if not a catalog/schema/volume/directory

        :param uc_path: The unity catalog path /Volumes/catalog/schema/...
        :raises ValueError: path is malformed
        :return: A list of unity catalog entries with the metadata of the path contents
        """
        logger.debug(f"uc_client.get_path_contents for {uc_path}")
        if not uc_path.startswith("/"):
            raise ValueError("path should start with /Volumes")
        parts = uc_path.split("/")
        if len(parts) < 2 or parts[1] != "Volumes":
            raise ValueError("path should start with /Volumes")
        if len(parts) == 2:
            return await self._get_catalogs(ctx_uid=ctx_uid)
        catalog = parts[2]
        if len(parts) == 3:
            return await self._get_schemas(catalog_name=catalog, ctx_uid=ctx_uid)
        schema = parts[3]
        if len(parts) == 4:
            return await self._get_volumes(catalog, schema, ctx_uid=ctx_uid)
        return await self._list_directory_contents(uc_path, ctx_uid=ctx_uid)

    async def download_chunk_stream(
        self,
        path: str,
        offset: int,
        length: int,
        *,
        ctx_uid: int,
        if_unmodified_since: float | None = None,
    ) -> AsyncGenerator[bytes, None]:
        """
        Asynchronously streams binary data from Unity Catalog.
        Yields bytes in 64KB chunks to keep memory usage constant.
        """
        logger.debug(f"uc_client.download_chunk_stream for {path}")
        encoded_path = self._quote_path(path)
        endpoint = f"/api/2.0/fs/files{encoded_path}"

        headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
        if if_unmodified_since:
            headers["If-Unmodified-Since"] = formatdate(
                if_unmodified_since, usegmt=True
            )

        # We use _request with stream=True
        response = await self._request("GET", endpoint, ctx_uid=ctx_uid, headers=headers, stream=True)

        try:
            # Yield data in increments of 64KB
            async for chunk in response.aiter_bytes(chunk_size=65536):
                yield chunk
        finally:
            # Crucial: close the stream to release the connection back to the pool
            await response.aclose()
