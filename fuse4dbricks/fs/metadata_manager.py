"""
Metadata Manager for FUSE4Databricks.
Handles attribute caching, directory listings, hierarchical resolution, 
and request coalescing to minimize Unity Catalog API calls.
"""
import time
import trio
import logging
import os
import stat
from collections import OrderedDict
from fs.inode_manager import (
    TYPE_ROOT, TYPE_CATALOG, TYPE_SCHEMA, TYPE_VOLUME, TYPE_DIRECTORY, TYPE_FILE
)

logger = logging.getLogger(__name__)

class MetadataManager:
    def __init__(self, uc_client, ttl=30, max_entries=20000):
        """
        :param uc_client: The Unity Catalog API client.
        :param ttl: Time-to-live in seconds for cached attributes.
        :param max_entries: Maximum number of metadata entries to keep in RAM.
        """
        self.uc_client = uc_client
        self.ttl = ttl
        self.max_entries = max_entries
        
        # --- Caches ---
        # { full_path: (expiration_ts, attr_dict) }
        self._attr_cache = OrderedDict()
        # { full_path: (expiration_ts, list_of_children_names) }
        self._dir_cache = OrderedDict()
        
        # Lock to protect the cache dictionaries (Fast in-memory lock)
        self._cache_lock = trio.Lock()
        
        # --- Request Coalescing (Thundering Herd Protection) ---
        # Stores active requests: { full_path: trio.Event }
        self._inflight_attr = {}
        self._inflight_dir = {}
        self._inflight_lock = trio.Lock()

    # =========================================================================
    # Public API
    # =========================================================================

    async def get_attributes(self, entry):
        """
        Retrieves attributes for an entry. 
        Flow: RAM Cache -> Coalescing Wait -> API Call.
        """
        # 1. Fast Check (Memory)
        async with self._cache_lock:
            cached = self._get_valid_cache(self._attr_cache, entry.full_path)
            if cached:
                return cached

        # 2. Request Coalescing
        wait_event = await self._join_or_lead_request(self._inflight_attr, entry.full_path)
        
        if wait_event:
            await wait_event.wait()
            # Wake up: Check cache again (Leader should have filled it)
            async with self._cache_lock:
                return self._get_valid_cache(self._attr_cache, entry.full_path) or entry.attr

        # 3. Real API Call (Leader Only)
        try:
            attr = await self._fetch_entry_metadata(entry)
            
            if attr:
                await self._update_attr_cache(entry.full_path, attr)
            return attr or entry.attr

        finally:
            await self._notify_followers(self._inflight_attr, entry.full_path)

    async def lookup_child(self, parent_entry, name):
        """
        Efficiently checks if a specific child exists without listing the whole parent.
        Returns: (attributes_dict, entry_type) or (None, None).
        """
        # Construct path
        if parent_entry.full_path == "/":
            child_path = f"/{name}"
        else:
            child_path = f"{parent_entry.full_path}/{name}"

        # 1. Fast Check
        async with self._cache_lock:
            cached = self._get_valid_cache(self._attr_cache, child_path)
            if cached:
                return cached, self._determine_type(cached)

        # 2. Coalescing (Reuse attr inflight tracker)
        wait_event = await self._join_or_lead_request(self._inflight_attr, child_path)

        if wait_event:
            await wait_event.wait()
            async with self._cache_lock:
                cached = self._get_valid_cache(self._attr_cache, child_path)
                return (cached, self._determine_type(cached)) if cached else (None, None)

        # 3. Real API Lookup
        try:
            attr, entry_type = await self._fetch_single_entity(parent_entry, name, child_path)
            
            if attr:
                await self._update_attr_cache(child_path, attr)
            return attr, entry_type

        finally:
            await self._notify_followers(self._inflight_attr, child_path)

    async def list_directory(self, entry):
        """
        Returns a list of children for a given entry.
        IMPROVEMENT: Performs 'Read-Ahead' by priming the attribute cache 
        for all children found, speeding up subsequent 'lookup' calls.
        """
        # 1. Fast Check
        async with self._cache_lock:
            cached = self._get_valid_cache(self._dir_cache, entry.full_path)
            if cached:
                return cached

        # 2. Coalescing
        wait_event = await self._join_or_lead_request(self._inflight_dir, entry.full_path)

        if wait_event:
            await wait_event.wait()
            async with self._cache_lock:
                return self._get_valid_cache(self._dir_cache, entry.full_path) or []

        # 3. Real API Call
        try:
            # Fetch raw metadata (size, mtime, etc.) along with names
            raw_items = await self._fetch_raw_children(entry)
            
            children_results = []
            now = time.time()
            
            async with self._cache_lock:
                # --- BULK CACHE PRIMING (READ-AHEAD) ---
                for item in raw_items:
                    # Construct child path
                    child_path = f"{entry.full_path}/{item['name']}"
                    if entry.full_path == "/": child_path = f"/{item['name']}"

                    # Generate full FUSE attributes
                    attr = self._gen_physical_attr(item) if entry.entry_type in [TYPE_VOLUME, TYPE_DIRECTORY] else self._gen_virtual_attr(is_dir=True)
                    
                    # Store in Attribute Cache (Populating it proactively)
                    self._attr_cache[child_path] = (now + self.ttl, attr)
                    
                    # Prepare simple list for the directory cache
                    is_dir = item.get('is_dir', False)
                    children_results.append({
                        'name': item['name'],
                        'type': TYPE_DIRECTORY if is_dir else TYPE_FILE
                    })
                
                # Store Directory Listing
                self._dir_cache[entry.full_path] = (now + self.ttl, children_results)
                
                # Maintenance
                if len(self._attr_cache) > self.max_entries:
                    self._attr_cache.popitem(last=False)

            return children_results

        except Exception as e:
            logger.error(f"List directory failed for {entry.full_path}: {e}")
            # Return empty list on failure to avoid crashing FUSE
            return []
        finally:
            await self._notify_followers(self._inflight_dir, entry.full_path)

    def invalidate(self, path):
        """
        Force-evict a path. Crucial after a delete or write operation.
        Also invalidates the parent directory listing.
        """
        # We don't need async lock for simple dict operations if not awaiting
        # But for consistency with trio, we can just do a best-effort removal 
        # or use the lock if calling from async context.
        # Here we assume this is called from an async context.
        try:
            if path in self._attr_cache:
                del self._attr_cache[path]
            
            parent_path = "/".join(path.rstrip('/').split('/')[:-1]) or "/"
            if parent_path in self._dir_cache:
                del self._dir_cache[parent_path]
        except Exception:
            pass

    # =========================================================================
    # Internal Logic & Helpers
    # =========================================================================

    def _get_valid_cache(self, cache_dict, key):
        """Validates TTL and updates LRU position."""
        if key in cache_dict:
            expires_at, data = cache_dict[key]
            if time.time() < expires_at:
                cache_dict.move_to_end(key)
                return data
            else:
                del cache_dict[key] # Expired
        return None

    async def _update_attr_cache(self, path, attr):
        async with self._cache_lock:
            self._attr_cache[path] = (time.time() + self.ttl, attr)
            if len(self._attr_cache) > self.max_entries:
                self._attr_cache.popitem(last=False)

    async def _join_or_lead_request(self, inflight_dict, key):
        """
        Helper for Request Coalescing.
        Returns trio.Event if we need to wait, or None if we are the leader.
        """
        async with self._inflight_lock:
            if key in inflight_dict:
                return inflight_dict[key]
            else:
                inflight_dict[key] = trio.Event()
                return None

    async def _notify_followers(self, inflight_dict, key):
        """Wake up waiting threads and cleanup."""
        async with self._inflight_lock:
            if key in inflight_dict:
                inflight_dict[key].set()
                del inflight_dict[key]

    def _determine_type(self, attr):
        if attr['st_nlink'] == 2:
            return TYPE_DIRECTORY
        return TYPE_FILE

    # --- API Interaction ---

    async def _fetch_entry_metadata(self, entry):
        """Fetches metadata for an existing entry to refresh attributes."""
        # Virtual nodes (Root, Catalogs, etc.) have static attributes
        if entry.entry_type in [TYPE_ROOT, TYPE_CATALOG, TYPE_SCHEMA, TYPE_VOLUME]:
            return entry.attr

        api_path = self._resolve_api_path(entry.full_path)
        # api_path is None for root, catalog and schema.
        if not api_path:
            return entry.attr

        try:
            meta = await self.uc_client.get_file_metadata(api_path)
            if not meta:
                return None 
            return self._gen_physical_attr(meta)
        except Exception:
            return entry.attr

    async def _fetch_single_entity(self, parent_entry, name, full_path):
        """Specific lookup logic per hierarchy level."""
        p_type = parent_entry.entry_type
        
        try:
            if p_type == TYPE_ROOT:
                if await self.uc_client.get_catalog(name):
                    return self._gen_virtual_attr(is_dir=True), TYPE_CATALOG

            elif p_type == TYPE_CATALOG:
                if await self.uc_client.get_schema(parent_entry.name, name):
                    return self._gen_virtual_attr(is_dir=True), TYPE_SCHEMA

            elif p_type == TYPE_SCHEMA:
                parts = parent_entry.full_path.strip('/').split('/')
                if await self.uc_client.get_volume(parts[0], parts[1], name):
                    return self._gen_virtual_attr(is_dir=True), TYPE_VOLUME

            elif p_type in [TYPE_VOLUME, TYPE_DIRECTORY]:
                api_path = self._resolve_api_path(full_path)
                meta = await self.uc_client.get_file_metadata(api_path)
                if meta:
                    attr = self._gen_physical_attr(meta)
                    t_type = TYPE_DIRECTORY if meta.get('is_dir') else TYPE_FILE
                    return attr, t_type
        except Exception:
            return None, None
            
        return None, None

    async def _fetch_raw_children(self, entry):
        """Fetches raw list from API to populate read-ahead cache."""
        p_type = entry.entry_type
        raw_results = []

        if p_type == TYPE_ROOT:
            items = await self.uc_client.list_catalogs()
            # Virtual nodes, we mock size/mtime
            for i in items:
                raw_results.append({'name': i['name'], 'is_dir': True})

        elif p_type == TYPE_CATALOG:
            items = await self.uc_client.list_schemas(entry.name)
            for i in items:
                raw_results.append({'name': i['name'], 'is_dir': True})

        elif p_type == TYPE_SCHEMA:
            parts = entry.full_path.strip('/').split('/')
            items = await self.uc_client.list_volumes(parts[0], parts[1])
            for i in items:
                raw_results.append({'name': i['name'], 'is_dir': True})

        elif p_type in [TYPE_VOLUME, TYPE_DIRECTORY]:
            api_path = self._resolve_api_path(entry.full_path)
            items = await self.uc_client.list_directory_contents(api_path)
            # Normalize API response
            for i in items:
                raw_results.append({
                    'name': i['name'],
                    'is_dir': i.get('is_directory', False) or i.get('is_dir', False),
                    'size': i.get('file_size', 0),
                    'mtime': i.get('modification_time', 0)
                })
        
        return raw_results

    # --- Attribute Generation ---

    def _gen_virtual_attr(self, is_dir=True):
        now = time.time()
        return {
            "st_mode": (stat.S_IFDIR | 0o755),
            "st_nlink": 2,
            "st_size": 4096,
            "st_mtime": now,
            "st_ctime": now,
            "st_atime": now,
            "st_uid": os.getuid(),
            "st_gid": os.getgid(),
        }

    def _gen_physical_attr(self, meta):
        is_dir = meta.get('is_dir', False)
        # Handle mtime being None or in milliseconds
        mtime = meta.get('mtime') or meta.get('modification_time', 0)
        # Heuristic: if mtime > 20000000000, it's likely milliseconds
        if mtime > 20000000000: 
            mtime = mtime / 1000.0
        
        return {
            "st_mode": (stat.S_IFDIR | 0o755) if is_dir else (stat.S_IFREG | 0o644),
            "st_nlink": 2 if is_dir else 1,
            "st_size": meta.get('size', 0) or meta.get('file_size', 0),
            "st_mtime": mtime,
            "st_ctime": mtime,
            "st_atime": time.time(),
            "st_uid": os.getuid(),
            "st_gid": os.getgid(),
        }

    def _resolve_api_path(self, fs_path):
        """
        Translates FUSE path /cat/sch/vol/path to UC /Volumes/cat/sch/vol/path.
        """
        parts = fs_path.strip('/').split('/')
        if len(parts) < 3:
            return None
        
        vol_prefix = f"/Volumes/{parts[0]}/{parts[1]}/{parts[2]}"
        if len(parts) == 3:
            return vol_prefix
        
        rest = "/".join(parts[3:])
        return f"{vol_prefix}/{rest}"
