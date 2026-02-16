"""
Metadata Manager for FUSE4Databricks.
Handles attribute caching, directory listings, and request coalescing to minimize Unity Catalog API calls.
"""

import errno
import logging
import stat
import time
from collections import OrderedDict
from typing import Tuple, TypeVar

import trio

try:
    import pyfuse3
except ImportError:
    import fuse4dbricks.mock.pyfuse3 as pyfuse3  # type: ignore[no-redef]

from fuse4dbricks.api.uc_client import (UcNodeType, UnityCatalogClient,
                                        UnityCatalogEntry)
from fuse4dbricks.fs.inode_manager import InodeEntry, InodeEntryAttr
from fuse4dbricks.fs.utils import (fs_to_uc_path,  fs_to_securable,
                                   join_or_lead_request,
                                   notify_followers, uc_to_fs_path)

logger = logging.getLogger(__name__)


def uc_node_type_from_entry(entry: InodeEntry) -> UcNodeType:
    fs_path = entry.fs_path
    is_dir = entry.is_dir
    if not fs_path.startswith("/"):
        raise ValueError("Expected absolute path")
    parts = fs_path.split("/")
    # parts has at least length 2
    catalog = parts[1]
    if len(parts) == 2:
        if catalog == "":
            return UcNodeType.ROOT
        else:
            return UcNodeType.CATALOG
    if len(parts) == 3:
        return UcNodeType.SCHEMA
    if len(parts) == 4:
        return UcNodeType.VOLUME
    if is_dir:
        return UcNodeType.DIRECTORY
    else:
        return UcNodeType.FILE


class MetadataManager:
    def __init__(
        self,
        uc_client: UnityCatalogClient,
        ttl: float=30.0,
        max_entries=20000,
        ttl_catalog=600.0,
    ):
        """
        :param uc_client: The Unity Catalog API client.
        :param ttl: Time-to-live in seconds for cached attributes.
        :param max_entries: Maximum number of metadata entries to keep in RAM.
        """
        self.uc_client = uc_client
        self._ttl: float = ttl
        self._ttl_catalog = ttl_catalog
        self.max_entries = max_entries

        # --- Caches ---
        self._attr_cache: OrderedDict[
            Tuple[str, bool], Tuple[float, InodeEntryAttr]
        ] = OrderedDict()
        """(fs_path, is_dir) -> (expires_at, InodeEntryAttr)
        (fs_path, is_dir) uniquely identifies an inode, even if the inode does not exist yet
        """

        # { fs_path: (expiration_ts, list_of_children_names) }
        self._dir_cache: OrderedDict[str, Tuple[float, OrderedDict[str, InodeEntryAttr]]] = (
            OrderedDict()
        )
        """
        fs_path -> (expires_at, OrderedDict[name, InodeEntryAttr])
        fs_path uniquely identifies a unity catalog directory.
        """

        # Lock to protect the cache dictionaries (Fast in-memory lock)
        self._cache_lock = trio.Lock()

        # --- Request Coalescing (Thundering Herd Protection) ---
        # Stores active requests: { full_path: trio.Event }
        self._inflight_attr: dict[str, trio.Event] = {}
        self._inflight_dir: dict[str, trio.Event] = {}
        self._inflight_lock = trio.Lock()

        self._permissions_lock = trio.Lock()
        self._inflight_permissions: dict[Tuple[int, str], trio.Event] = {}
        self._permissions_inflight_lock = trio.Lock()
        self._permissions_cache: OrderedDict[
            Tuple[int, str], Tuple[float, bool]
        ] = OrderedDict()

        self._principal_cache: OrderedDict[int, str] = OrderedDict()
        self._principal_cache_lock = trio.Lock()
        self._principal_inflight: dict[int, trio.Event] = {}
        self._principal_inflight_lock = trio.Lock()

        """(uid, securable) -> (expires_at, has_permission)
        Caches permission checks to avoid redundant API calls for the same user and securable.
        """

    def get_ttl(self, uc_type: UcNodeType | None):
        if uc_type in (UcNodeType.FILE, UcNodeType.DIRECTORY):
            return self._ttl
        else:
            return self._ttl_catalog

    # =========================================================================
    # Public API
    # =========================================================================

    async def _get_principal(self, ctx) -> str|None:
        async with self._principal_cache_lock:
            principal = self._principal_cache.get(ctx.uid)
            if principal is not None:
                return principal
        # 2. Request Coalescing
        wait_event = await join_or_lead_request(
            self._principal_inflight_lock, self._principal_inflight, ctx.uid
        )

        if wait_event:
            await wait_event.wait()
            # Wake up: Check cache again (Leader should have filled it)
            async with self._principal_cache_lock:
                principal = self._principal_cache.get(ctx.uid)
                return principal

        # 3. Real API Call (Leader Only)
        try:
            principal = await self.uc_client.get_current_user_info(ctx.uid)
            async with self._principal_cache_lock:
                self._principal_cache[ctx.uid] = principal
                if len(self._principal_cache) > self.max_entries:
                    self._principal_cache.popitem(last=False)
        finally:
            await notify_followers(
                self._principal_inflight_lock, self._principal_inflight, ctx.uid
            )
        return principal

    async def check_access(self, entry: InodeEntry, mode: int, ctx) -> bool:
        #R_OK = 4
        W_OK = 2
        if (mode & W_OK):
            raise pyfuse3.FUSEError(errno.EACCES)
        (securable, securable_type) = fs_to_securable(entry.fs_path)
        if securable == "":
            return True
        if securable_type == "catalog":
            req_privileges = ["USE_CATALOG"]
        elif securable_type == "schema":
            req_privileges = ["USE_SCHEMA"]
        elif securable_type == "volume":
            req_privileges = ["READ_VOLUME"]
        else:
            logger.error(f"Unexpected securable type for path {entry.fs_path}")
            raise pyfuse3.FUSEError(errno.EACCES)
        # 1. Permission Check with Caching
        async with self._permissions_lock:
            cached = self._get_valid_cache(self._permissions_cache, (ctx.uid, securable))
            # TODO: We should be able to tell difference between no cache entry and negative cache entry (e.g. path does not exist)
            if cached is not None:
                return cached
        # 2. Request Coalescing
        wait_event = await join_or_lead_request(
            self._permissions_inflight_lock, self._inflight_permissions, (ctx.uid, securable)
        )

        if wait_event:
            await wait_event.wait()
            # Wake up: Check cache again (Leader should have filled it)
            async with self._cache_lock:
                permissions_fullfilled = self._get_valid_cache(
                    self._permissions_cache, (ctx.uid, securable)
                )
                if permissions_fullfilled is None:
                    # Cache miss after waiting means the leader did not find the securable or an error occurred. Deny access.
                    return False
                return permissions_fullfilled
        # 3. Real API Call (Leader Only)
        try:
            principal = await self._get_principal(ctx)
            if principal is None:
                raise pyfuse3.FUSEError(errno.EACCES)
            permissions_fullfilled = await self.uc_client.check_permissions(securable, securable_type, privileges=req_privileges, principal=principal, ctx_uid=ctx.uid)
            # Cache the result
            async with self._permissions_lock:
                self._permissions_cache[(ctx.uid, securable)] = (time.time() + self._ttl, permissions_fullfilled)
                if len(self._permissions_cache) > self.max_entries:
                    self._permissions_cache.popitem(last=False)
        finally:
            await notify_followers(
                self._permissions_inflight_lock, self._inflight_permissions, (ctx.uid, securable)
            )
        return permissions_fullfilled

    async def get_attributes(self, entry: InodeEntry, ctx) -> InodeEntryAttr | None:
        """
        Retrieves attributes for an entry.
        Flow: RAM Cache -> Coalescing Wait -> API Call (Type-Specific Validation).
        Returns None if the object no longer exists or changed type.
        """
        # 1. Fast Check (Memory)
        async with self._cache_lock:
            cached = self._get_valid_cache(
                self._attr_cache, (entry.fs_path, entry.is_dir)
            )
            # TODO: We should be able to tell difference between no cache entry and negative cache entry
            if cached:
                return cached

        # 2. Request Coalescing
        wait_event = await join_or_lead_request(
            self._inflight_lock, self._inflight_attr, entry.fs_path
        )

        if wait_event:
            await wait_event.wait()
            # Wake up: Check cache again (Leader should have filled it)
            async with self._cache_lock:
                attr = self._get_valid_cache(
                    self._attr_cache, (entry.fs_path, entry.is_dir)
                )
                return attr

        # 3. Real API Call (Leader Only)
        try:
            attr = await self._refresh_entry_metadata(entry, ctx)
        finally:
            await notify_followers(
                self._inflight_lock, self._inflight_attr, entry.fs_path
            )
        return attr

    async def lookup_child(
        self, parent_entry: InodeEntry, name: str, ctx
    ) -> InodeEntryAttr | None:
        """
        Efficiently checks if a specific child exists without listing the whole parent.
        Returns: attributes_dict
        """
        # Construct path
        if parent_entry.fs_path == "/":
            child_fs_path = f"/{name}"
        else:
            child_fs_path = f"{parent_entry.fs_path}/{name}"

        # 1. Fast Check
        async with self._cache_lock:
            for is_dir in (False, True):
                cached = self._get_valid_cache(
                    self._attr_cache, (child_fs_path, is_dir)
                )
                if cached:
                    return cached

        # 2. Coalescing (Reuse attr inflight tracker)
        wait_event = await join_or_lead_request(
            self._inflight_lock, self._inflight_attr, child_fs_path
        )

        if wait_event:
            await wait_event.wait()
            async with self._cache_lock:
                for is_dir in (False, True):
                    cached = self._get_valid_cache(
                        self._attr_cache, (child_fs_path, is_dir)
                    )
                    if cached:
                        return cached

        # 3. Real API Lookup
        try:
            uc_path = fs_to_uc_path(child_fs_path)
            uc_entry = await self.uc_client.get_path_metadata(uc_path, ctx_uid=ctx.uid)
            if uc_entry is None:
                # Not found (negative cache could be implemented here if desired)
                return None
            is_dir = uc_entry.is_dir()
            attr = self._uc_to_inode_entry_attr(uc_entry, ctx)
            await self._update_attr_cache(
                child_fs_path, attr, is_dir, ttl=self.get_ttl(uc_entry.entry_type)
            )
            return attr
        finally:
            await notify_followers(
                self._inflight_lock, self._inflight_attr, child_fs_path
            )

    async def list_directory(
        self, entry: InodeEntry, ctx: pyfuse3.RequestContext
    ) -> OrderedDict[str, InodeEntryAttr] | None:
        """
        Returns a list of children for a given entry or None if given entry is not a directory
        """
        logger.debug(f"metadata_manager.list_directory for {entry.fs_path}")
        # 1. Fast Check
        async with self._cache_lock:
            cached = self._get_valid_cache(self._dir_cache, entry.fs_path)
            if cached is not None:
                # TODO: Implement negative caching
                return cached

        # 2. Coalescing
        wait_event = await join_or_lead_request(
            self._inflight_lock, self._inflight_dir, entry.fs_path
        )

        if wait_event:
            await wait_event.wait()
            async with self._cache_lock:
                cached = self._get_valid_cache(self._dir_cache, entry.fs_path)
                # if cache is None it is because was just refilled and not found
                return cached

        # 3. Real API Call
        try:
            # Fetch raw metadata (size, mtime, etc.) along with names
            uc_path = fs_to_uc_path(entry.fs_path)
            uc_entries = await self.uc_client.get_path_contents(uc_path, ctx_uid=ctx.uid)
            if (
                uc_entries is None
            ):  # May this happen because of any other reason as not a directory?
                raise pyfuse3.FUSEError(errno.ENOTDIR)

            results = OrderedDict()
            now = time.time()
            for uc_entry in uc_entries:
                attr = self._uc_to_inode_entry_attr(uc_entry, ctx)
                results[uc_entry.name] = attr
                # read ahead: Cache attributes of children to speed up subsequent getattr and lookups
                await self._update_attr_cache(
                    fs_path=uc_to_fs_path(uc_entry.uc_path),
                    attr=attr,
                    is_dir=uc_entry.is_dir(),
                    ttl=self.get_ttl(uc_entry.entry_type),
                )

            # Store Directory Listing
            async with self._cache_lock:
                self._dir_cache[entry.fs_path] = (now + self._ttl, results)
            return results
        except Exception as e:
            # Many errors may happen. An access denied error should be raised.
            if isinstance(e, pyfuse3.FUSEError) and e.errno == errno.EACCES:
                raise
            # Other errors should be logged and we should return None?
            import traceback
            logger.error(f"List directory failed for {entry.fs_path}: {e}")
            print(traceback.format_exc())
            return None
        finally:
            await notify_followers(
                self._inflight_lock, self._inflight_dir, entry.fs_path
            )

    def invalidate(self, fs_path: str, is_dir: bool):
        """
        Force-evict a path from caches. Crucial after a delete or write operation.
        """
        try:
            attr_cache_key = (fs_path, is_dir)
            if attr_cache_key in self._attr_cache:
                del self._attr_cache[attr_cache_key]

            parent_path = "/".join(fs_path.rstrip("/").split("/")[:-1]) or "/"
            if parent_path in self._dir_cache:
                del self._dir_cache[parent_path]
        except Exception:
            pass

    # =========================================================================
    # Internal Logic & Helpers
    # =========================================================================
    _K = TypeVar("_K")
    _V = TypeVar("_V")

    def _get_valid_cache(
        self, cache_dict: OrderedDict[_K, tuple[float, _V]], key: _K
    ) -> _V | None:
        """Validates TTL and updates LRU position."""
        if key in cache_dict:
            expires_at, data = cache_dict[key]
            if time.time() < expires_at:
                cache_dict.move_to_end(key)
                return data
            else:
                del cache_dict[key]  # Expired
        return None

    @staticmethod
    def _uc_to_inode_entry_attr(
        uc_entry: UnityCatalogEntry, ctx: pyfuse3.RequestContext
    ) -> InodeEntryAttr:
        is_dir = uc_entry.is_dir()
        mode = (stat.S_IFDIR | 0o755) if is_dir else (stat.S_IFREG | 0o644)
        ctime = uc_entry.ctime if uc_entry.ctime is not None else 0
        mtime = uc_entry.mtime if uc_entry.mtime is not None else 0
        attr = InodeEntryAttr(
            st_mode=mode,
            st_nlink=2 if is_dir else 1,
            st_size=uc_entry.size,
            st_ctime=ctime,
            st_mtime=mtime,
            st_atime=0,
            st_uid=ctx.uid,
            st_gid=ctx.gid,
        )
        return attr

    async def _update_attr_cache(
        self, fs_path: str, attr: InodeEntryAttr, is_dir: bool, ttl=None
    ):
        if ttl is None:
            ttl = self._ttl
        async with self._cache_lock:
            self._attr_cache[(fs_path, is_dir)] = (time.time() + ttl, attr)
            if (fs_path, not is_dir) in self._attr_cache:
                del self._attr_cache[(fs_path, not is_dir)]
            if len(self._attr_cache) > self.max_entries:
                self._attr_cache.popitem(last=False)

    # --- API Interaction & Validation ---

    async def _refresh_entry_metadata(
        self, entry: InodeEntry, ctx
    ) -> InodeEntryAttr | None:
        """
        Refreshes metadata via API, checking for Type Consistency.
        """
        node_type = uc_node_type_from_entry(entry)
        # Root node has no unity catalog entry
        if node_type == UcNodeType.ROOT:
            return entry.attr
        # Refresh
        uc_path = fs_to_uc_path(entry.fs_path)
        uc_entry = await self.uc_client.get_path_metadata(
            uc_path,
            ctx_uid=ctx.uid,
            expected_type=node_type
        )
        if uc_entry is None:
            # Not found, was probably deleted. Invalidate cache to trigger ENOENT on next access.
            self.invalidate(entry.fs_path, entry.is_dir)
            return None
        # Convert to inode attributes
        attr = self._uc_to_inode_entry_attr(uc_entry=uc_entry, ctx=ctx)
        # was file, now is dir or viceversa:
        if node_type != uc_entry.entry_type:
            self.invalidate(entry.fs_path, entry.is_dir)
            # Keep cache for a future request
            await self._update_attr_cache(
                entry.fs_path,
                attr,
                uc_entry.is_dir(),
                ttl=self.get_ttl(uc_entry.entry_type),
            )
            return None
        # All good, cache and return
        await self._update_attr_cache(
            entry.fs_path, attr, entry.is_dir, ttl=self.get_ttl(uc_entry.entry_type)
        )
        return attr
