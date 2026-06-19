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
import pyfuse3

from fuse4dbricks.api.uc_client import (UcNodeType, UnityCatalogClient,
                                        UnityCatalogEntry)
from fuse4dbricks.fs.inode_manager import InodeEntry, InodeEntryAttr
from fuse4dbricks.fs.utils import (fs_to_uc_path,  fs_to_securable,
                                   InflightCoalescer,
                                   uc_to_fs_path)

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
        ttl_negative: float=5.0,
    ):
        """
        :param uc_client: The Unity Catalog API client.
        :param ttl: Time-to-live in seconds for cached attributes.
        :param max_entries: Maximum number of metadata entries to keep in RAM.
        :param ttl_negative: Time-to-live in seconds for negative (not-found)
            cache entries. Kept short because it hides newly-created paths.
        """
        self.uc_client = uc_client
        self._ttl: float = ttl
        self._ttl_catalog = ttl_catalog
        self._ttl_negative = ttl_negative
        self.max_entries = max_entries

        # --- Caches ---
        self._attr_cache: OrderedDict[
            Tuple[str, bool], Tuple[float, InodeEntryAttr]
        ] = OrderedDict()
        """(fs_path, is_dir) -> (expires_at, InodeEntryAttr)
        (fs_path, is_dir) uniquely identifies an inode, even if the inode does not exist yet
        """

        # Negative (not-found) cache, keyed by PRINCIPAL so one principal's
        # 404 (which may be a disguised permission failure) never becomes a
        # false ENOENT for another principal. Positive metadata stays global
        # (the documented shared-metadata cache); a negative is identity-
        # relative, so the two are asymmetric on purpose. A principal's own
        # negative takes precedence over any global positive (see the read
        # paths). Short TTL because it hides newly-created paths.
        self._negative_cache: OrderedDict[Tuple[str, str], float] = OrderedDict()
        """(principal, fs_path) -> expires_at"""

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
        self._attr_coalescer: InflightCoalescer[str] = InflightCoalescer()
        self._dir_coalescer: InflightCoalescer[str] = InflightCoalescer()

        self._permissions_cache: OrderedDict[
            Tuple[str, str], Tuple[float, bool]
        ] = OrderedDict()
        self._permissions_lock = trio.Lock()
        self._permissions_coalescer: InflightCoalescer[Tuple[str, str]] = InflightCoalescer()

        self._principal_cache: OrderedDict[int, str] = OrderedDict()
        self._principal_cache_lock = trio.Lock()
        self._principal_coalescer: InflightCoalescer[int] = InflightCoalescer()

        """(principal, securable) -> (expires_at, has_permission)
        Caches permission checks to avoid redundant API calls for the same
        principal and securable. Keyed by principal (not uid) so a uid that
        switches tokens to a different principal cannot read the previous
        principal's cached grants.
        """

    def get_ttl(self, uc_type: UcNodeType | None):
        if uc_type in (UcNodeType.FILE, UcNodeType.DIRECTORY):
            return self._ttl
        else:
            return self._ttl_catalog

    # =========================================================================
    # Public API
    # =========================================================================

    async def reauthorize(self, ctx: pyfuse3.RequestContext) -> None:
        """Re-evaluate a uid's identity after its access token changed.

        Refreshes the ``uid -> principal`` mapping using the (now updated)
        token. The permission cache is keyed by *principal*, so once this
        mapping is refreshed, requests under the new token naturally address
        that principal's own cache entries while the previous principal's
        grants are simply never looked up again (and expire by TTL). No
        explicit permission-cache purge is required.
        """
        async with self._principal_cache_lock:
            self._principal_cache.pop(ctx.uid, None)

        try:
            new_principal = await self.uc_client.get_current_user_info(ctx)
        except Exception:
            # Token invalid/expired or transient failure: we can't confirm the
            # identity. Leave it unresolved so the next request re-derives it.
            new_principal = None

        if new_principal is not None:
            async with self._principal_cache_lock:
                self._principal_cache[ctx.uid] = new_principal

    def forget_principal(self, uid: int) -> None:
        """Drop the cached principal for a uid after its token is invalidated
        (e.g. on a 401); the next request re-resolves it. Synchronous so it can
        be called from the (sync) token-invalidation callback; the pop is atomic
        under trio's cooperative scheduler, so no lock is required.
        """
        self._principal_cache.pop(uid, None)

    def _peek_principal(self, uid: int) -> str | None:
        """Cache-only principal lookup (no I/O). Used by the read paths so a
        metadata read never gains an API dependency just to consult the negative
        cache; if the principal isn't known yet, negative caching is skipped."""
        return self._principal_cache.get(uid)

    def _negative_hit(self, principal: str | None, fs_path: str) -> bool:
        """Whether ``fs_path`` is known-absent for ``principal``. Caller must
        hold ``self._cache_lock`` (mirrors ``_get_valid_cache``)."""
        if principal is None:
            return False
        key = (principal, fs_path)
        expires_at = self._negative_cache.get(key)
        if expires_at is None:
            return False
        if time.time() < expires_at:
            self._negative_cache.move_to_end(key)
            return True
        del self._negative_cache[key]
        return False

    async def _record_negative(self, principal: str | None, fs_path: str) -> None:
        """Record that ``fs_path`` is absent for ``principal`` (true 404 only).
        No-op when the principal is unknown, so we never store a negative under
        an unverified identity. The positive cache is left untouched: a global
        positive populated by a different principal stays valid for that other
        identity, and the read paths check this principal's negative first."""
        if principal is None:
            return
        async with self._cache_lock:
            self._negative_cache[(principal, fs_path)] = time.time() + self._ttl_negative
            if len(self._negative_cache) > self.max_entries:
                self._negative_cache.popitem(last=False)

    async def _get_principal(self, ctx: pyfuse3.RequestContext) -> str|None:
        async with self._principal_cache_lock:
            principal = self._principal_cache.get(ctx.uid)
            if principal is not None:
                return principal
        # 2. Request Coalescing
        (wait_event, leader) = await self._principal_coalescer.join_or_lead(ctx.uid)

        if not leader:
            await wait_event.wait()
            # Wake up: Check cache again (Leader should have filled it)
            async with self._principal_cache_lock:
                principal = self._principal_cache.get(ctx.uid)
                return principal

        # 3. Real API Call (Leader Only)
        try:
            principal = await self.uc_client.get_current_user_info(ctx)
            async with self._principal_cache_lock:
                self._principal_cache[ctx.uid] = principal
                if len(self._principal_cache) > self.max_entries:
                    self._principal_cache.popitem(last=False)
        finally:
            await self._principal_coalescer.notify_done(ctx.uid)
        return principal

    async def check_access(self, entry: InodeEntry, mode: int, ctx) -> bool:
        #R_OK = 4
        W_OK = 2
        (securable, securable_type) = fs_to_securable(entry.fs_path)
        if securable == "":
            return True
        if securable_type == "catalog":
            if (mode & W_OK):
                raise pyfuse3.FUSEError(errno.EACCES)
            req_privileges = ["USE_CATALOG"]
        elif securable_type == "schema":
            if (mode & W_OK):
                raise pyfuse3.FUSEError(errno.EACCES)
            req_privileges = ["USE_SCHEMA"]
        elif securable_type == "volume":
            if (mode & W_OK):
                req_privileges = ["READ_VOLUME", "WRITE_VOLUME"]
            else:
                req_privileges = ["READ_VOLUME"]
        else:
            logger.error(f"Unexpected securable type for path {entry.fs_path}")
            raise pyfuse3.FUSEError(errno.EACCES)
        # Resolve identity first: the permission cache is keyed by principal, so
        # a uid that switches tokens to a different principal can never read the
        # previous principal's cached grants.
        principal = await self._get_principal(ctx)
        if principal is None:
            # Identity could not be resolved: the leader's get_current_user_info
            # failed and a coalescing follower woke to no cached principal. That
            # is a retryable condition, not a denial, so surface EAGAIN instead
            # of laundering a transient failure into a permanent EACCES.
            logger.error(f"Unable to get principal for {ctx=}")
            raise pyfuse3.FUSEError(errno.EAGAIN)
        cache_key = (principal, securable)
        # 1. Permission Check with Caching
        async with self._permissions_lock:
            cached = self._get_valid_cache(self._permissions_cache, cache_key)
            # TODO: We should be able to tell difference between no cache entry and negative cache entry (e.g. path does not exist)
            if cached is not None:
                return cached
        # 2. Request Coalescing
        (wait_event, leader) = await self._permissions_coalescer.join_or_lead(cache_key)

        if not leader:
            await wait_event.wait()
            # Wake up: Check cache again (Leader should have filled it)
            async with self._permissions_lock:
                permissions_fullfilled = self._get_valid_cache(
                    self._permissions_cache, cache_key
                )
            if permissions_fullfilled is None:
                # A genuine denial is cached as False, so a missing entry means
                # the leader's request failed (e.g. rate limit/outage after the
                # HTTP-layer retries). Surface a retryable error instead of
                # fabricating a permanent "permission denied".
                raise pyfuse3.FUSEError(errno.EAGAIN)
            return permissions_fullfilled
        # 3. Real API Call (Leader Only)
        try:
            permissions_fullfilled = await self.uc_client.check_permissions(securable, securable_type, privileges=req_privileges, principal=principal, ctx=ctx)
            # Cache the result. Catalog/schema grants change rarely and use the
            # longer catalog TTL (matching get_ttl); volume grants gate file
            # contents, so they use the shorter default TTL for faster revocation.
            perm_ttl = self._ttl_catalog if securable_type in ("catalog", "schema") else self._ttl
            async with self._permissions_lock:
                self._permissions_cache[cache_key] = (time.time() + perm_ttl, permissions_fullfilled)
                if len(self._permissions_cache) > self.max_entries:
                    self._permissions_cache.popitem(last=False)
        finally:
            await self._permissions_coalescer.notify_done(cache_key)
        return permissions_fullfilled

    async def get_attributes(self, entry: InodeEntry, ctx) -> InodeEntryAttr | None:
        """
        Retrieves attributes for an entry.
        Flow: RAM Cache -> Coalescing Wait -> API Call (Type-Specific Validation).
        Returns None if the object no longer exists or changed type.
        """
        principal = self._peek_principal(ctx.uid)
        # 1. Fast Check (Memory): this principal's own negative wins over any
        # global positive, then the global positive.
        async with self._cache_lock:
            if self._negative_hit(principal, entry.fs_path):
                return None
            cached = self._get_valid_cache(
                self._attr_cache, (entry.fs_path, entry.is_dir)
            )
            if cached:
                return cached

        # 2. Request Coalescing
        (wait_event, leader) = await self._attr_coalescer.join_or_lead(entry.fs_path)

        if not leader:
            await wait_event.wait()
            # Wake up: Check cache again (Leader should have filled it)
            async with self._cache_lock:
                if self._negative_hit(principal, entry.fs_path):
                    return None  # leader recorded "absent" -> ENOENT
                attr = self._get_valid_cache(
                    self._attr_cache, (entry.fs_path, entry.is_dir)
                )
            if attr is not None:
                return attr
            # Leader recorded neither a positive nor our negative: it failed.
            # Surface a retryable error instead of a false ENOENT.
            raise pyfuse3.FUSEError(errno.EAGAIN)

        # 3. Real API Call (Leader Only)
        try:
            attr = await self._refresh_entry_metadata(entry, ctx, principal=principal)
        finally:
            await self._attr_coalescer.notify_done(entry.fs_path)
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

        principal = self._peek_principal(ctx.uid)
        # 1. Fast Check: own negative first, then global positive.
        async with self._cache_lock:
            if self._negative_hit(principal, child_fs_path):
                return None
            for is_dir in (False, True):
                cached = self._get_valid_cache(
                    self._attr_cache, (child_fs_path, is_dir)
                )
                if cached:
                    return cached

        # 2. Coalescing (Reuse attr inflight tracker)
        (wait_event, leader) = await self._attr_coalescer.join_or_lead(child_fs_path)

        if not leader:
            await wait_event.wait()
            async with self._cache_lock:
                if self._negative_hit(principal, child_fs_path):
                    return None
                for is_dir in (False, True):
                    cached = self._get_valid_cache(
                        self._attr_cache, (child_fs_path, is_dir)
                    )
                    if cached:
                        return cached
            # Leader recorded neither a positive nor our negative: it failed.
            # Surface a retryable error rather than running our own lookup —
            # the leader already owns this coalescer key, so a follower calling
            # notify_done() could pop a *subsequent* leader's entry and wake its
            # followers into a thundering herd. Mirrors get_attributes().
            raise pyfuse3.FUSEError(errno.EAGAIN)

        # 3. Real API Lookup (leader only)
        try:
            uc_path = fs_to_uc_path(child_fs_path)
            uc_entry = await self.uc_client.get_path_metadata(uc_path, ctx=ctx)
            if uc_entry is None:
                # True 404: record a negative for this principal so a repeated
                # lookup is answered from cache instead of re-hitting the API.
                # (The read paths check this principal's negative before any
                # global positive, so no positive eviction is needed here.)
                await self._record_negative(principal, child_fs_path)
                return None
            is_dir = uc_entry.is_dir()
            attr = self._uc_to_inode_entry_attr(uc_entry, ctx)
            await self._update_attr_cache(
                child_fs_path, attr, is_dir, ttl=self.get_ttl(uc_entry.entry_type),
                principal=principal,
            )
            return attr
        finally:
            await self._attr_coalescer.notify_done(child_fs_path)

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
        (wait_event, leader) = await self._dir_coalescer.join_or_lead(entry.fs_path)

        if not leader:
            await wait_event.wait()
            async with self._cache_lock:
                cached = self._get_valid_cache(self._dir_cache, entry.fs_path)
                # if cache is None it is because was just refilled and not found
                return cached

        # 3. Real API Call
        try:
            # Fetch raw metadata (size, mtime, etc.) along with names
            uc_path = fs_to_uc_path(entry.fs_path)
            uc_entries = await self.uc_client.get_path_contents(uc_path, ctx=ctx)
            if (
                uc_entries is None
            ):  # May this happen because of any other reason as not a directory?
                raise pyfuse3.FUSEError(errno.ENOTDIR)

            results = OrderedDict()
            now = time.time()
            principal = self._peek_principal(ctx.uid)
            for uc_entry in uc_entries:
                attr = self._uc_to_inode_entry_attr(uc_entry, ctx)
                results[uc_entry.name] = attr
                # read ahead: Cache attributes of children to speed up subsequent
                # getattr and lookups. Pass principal so a listed child clears
                # this principal's stale negative entry; otherwise `ls` could show
                # a file that a subsequent stat reports as ENOENT until the
                # negative TTL expires.
                await self._update_attr_cache(
                    fs_path=uc_to_fs_path(uc_entry.uc_path),
                    attr=attr,
                    is_dir=uc_entry.is_dir(),
                    ttl=self.get_ttl(uc_entry.entry_type),
                    principal=principal,
                )

            # Store Directory Listing
            async with self._cache_lock:
                self._dir_cache[entry.fs_path] = (now + self._ttl, results)
            return results
        except Exception as e:
            # Many errors may happen. An access denied error should be raised.
            if isinstance(e, pyfuse3.FUSEError) and e.errno == errno.EACCES:
                raise
            # Other errors are logged and surfaced as "no listing" (None).
            logger.exception("List directory failed for %s: %s", entry.fs_path, e)
            return None
        finally:
            await self._dir_coalescer.notify_done(entry.fs_path)

    def invalidate(self, fs_path: str, is_dir: bool):
        """
        Force-evict a path from caches. Crucial after a delete or write operation.
        """
        try:
            attr_cache_key = (fs_path, is_dir)
            if attr_cache_key in self._attr_cache:
                del self._attr_cache[attr_cache_key]

            # A mutating op (create/write/mkdir/rename/truncate/delete) just
            # changed this path's presence, so any cached negative is now stale.
            # Drop it for every principal: invalidate() is synchronous and has
            # no principal in hand, and a just-created path must be immediately
            # visible on this mount rather than hidden until the negative TTL
            # expires. Build the key list first, then delete -- there is no
            # await between, so this stays atomic under trio (mirrors the
            # lock-free contract of the rest of this method).
            stale_negatives = [k for k in self._negative_cache if k[1] == fs_path]
            for key in stale_negatives:
                del self._negative_cache[key]

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
            # st_uid/st_gid are meaningless: the attr is cached globally, so
            # these end up reflecting whoever populated the shared cache first,
            # not the real owner. Access is decided by per-user Unity Catalog
            # permission checks, never from POSIX ownership. See README's
            # "Known limitations".
            st_uid=ctx.uid,
            st_gid=ctx.gid,
        )
        return attr

    async def _update_attr_cache(
        self, fs_path: str, attr: InodeEntryAttr, is_dir: bool, ttl=None,
        principal: str | None = None,
    ):
        if ttl is None:
            ttl = self._ttl
        async with self._cache_lock:
            self._attr_cache[(fs_path, is_dir)] = (time.time() + ttl, attr)
            if (fs_path, not is_dir) in self._attr_cache:
                del self._attr_cache[(fs_path, not is_dir)]
            # The path now exists for this principal, so clear its negative
            # entry (if any). Other principals' negatives are left alone.
            if principal is not None:
                self._negative_cache.pop((principal, fs_path), None)
            if len(self._attr_cache) > self.max_entries:
                self._attr_cache.popitem(last=False)

    # --- API Interaction & Validation ---

    async def _refresh_entry_metadata(
        self, entry: InodeEntry, ctx, principal: str | None = None
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
            ctx=ctx,
            expected_type=node_type
        )
        if uc_entry is None:
            # Not found, was probably deleted. Invalidate cache to trigger ENOENT
            # on next access, and record a negative for this principal so the
            # next getattr is answered from cache instead of re-hitting the API.
            self.invalidate(entry.fs_path, entry.is_dir)
            await self._record_negative(principal, entry.fs_path)
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
                principal=principal,
            )
            return None
        # All good, cache and return
        await self._update_attr_cache(
            entry.fs_path, attr, entry.is_dir, ttl=self.get_ttl(uc_entry.entry_type),
            principal=principal,
        )
        return attr
