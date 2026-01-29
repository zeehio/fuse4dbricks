"""
Manages caching for Metadata, Discovery Lists, and Data Chunks.
Implements 'Read-Through': Operations -> Cache -> API.
"""
import logging
import time
import trio
from fs.inode_manager import (
    TYPE_ROOT, TYPE_CATALOG, TYPE_SCHEMA, TYPE_VOLUME, TYPE_DIRECTORY, TYPE_FILE
)

logger = logging.getLogger(__name__)

class CacheManager:
    def __init__(self, uc_client, persistence, metadata_ttl=60):
        self.uc_client = uc_client
        self.disk = persistence
        self.ttl = metadata_ttl
        self.chunk_size = 8 * 1024 * 1024  # 8 MB

    # --- Metadata & Discovery ---

    async def get_attr(self, entry):
        """Returns attributes. Only refreshes physical FILES via API."""
        # Optimization: Directories/Virtual nodes don't change size/mtime often enough.
        if entry.entry_type in [TYPE_ROOT, TYPE_CATALOG, TYPE_SCHEMA, TYPE_VOLUME, TYPE_DIRECTORY]:
            return entry.attr

        now = time.time()
        last_validated = entry.attr.get('_last_validated', 0)

        if (now - last_validated) < self.ttl:
            return entry.attr

        # Only refresh physical files
        api_path = self._resolve_api_path(entry.full_path)
        if not api_path: return entry.attr

        meta = await self.uc_client.get_file_metadata(api_path)
        if meta:
            entry.attr['st_size'] = meta['size']
            entry.attr['st_mtime'] = meta['mtime']
            entry.attr['st_ctime'] = meta['mtime']
            entry.attr['_last_validated'] = now
        
        return entry.attr

    async def lookup_name(self, parent_entry, name):
        """Resolves child name based on parent type."""
        p_type = parent_entry.entry_type
        
        try:
            if p_type == TYPE_ROOT:
                items = await self.uc_client.list_catalogs()
                for i in items:
                    if i['name'] == name: return {'type': TYPE_CATALOG}

            elif p_type == TYPE_CATALOG:
                items = await self.uc_client.list_schemas(parent_entry.name)
                for i in items:
                    if i['name'] == name: return {'type': TYPE_SCHEMA}

            elif p_type == TYPE_SCHEMA:
                parts = parent_entry.full_path.split('/') # ['', Cat, Sch]
                items = await self.uc_client.list_volumes(parts[1], parts[2])
                for i in items:
                    if i['name'] == name: return {'type': TYPE_VOLUME}

            elif p_type in [TYPE_VOLUME, TYPE_DIRECTORY]:
                # Physical Lookup
                child_path = f"{parent_entry.full_path}/{name}"
                api_path = self._resolve_api_path(child_path)
                meta = await self.uc_client.get_file_metadata(api_path)
                if meta:
                    return {
                        'type': TYPE_DIRECTORY if meta['is_dir'] else TYPE_FILE,
                        'size': meta['size'],
                        'mtime': meta['mtime']
                    }
        except Exception:
            return None
        return None

    async def list_children(self, parent_entry):
        results = []
        p_type = parent_entry.entry_type

        try:
            if p_type == TYPE_ROOT:
                items = await self.uc_client.list_catalogs()
                results = [{'name': i['name'], 'type': TYPE_CATALOG} for i in items]

            elif p_type == TYPE_CATALOG:
                items = await self.uc_client.list_schemas(parent_entry.name)
                results = [{'name': i['name'], 'type': TYPE_SCHEMA} for i in items]

            elif p_type == TYPE_SCHEMA:
                parts = parent_entry.full_path.split('/')
                items = await self.uc_client.list_volumes(parts[1], parts[2])
                results = [{'name': i['name'], 'type': TYPE_VOLUME} for i in items]

            elif p_type in [TYPE_VOLUME, TYPE_DIRECTORY]:
                api_path = self._resolve_api_path(parent_entry.full_path)
                files = await self.uc_client.list_directory_contents(api_path)
                for f in files:
                    is_dir = f.get('is_directory', False) or f.get('is_dir', False)
                    results.append({
                        'name': f['name'],
                        'type': TYPE_DIRECTORY if is_dir else TYPE_FILE,
                        'size': f.get('file_size', 0),
                        'mtime': f.get('modification_time', 0) / 1000.0
                    })
        except Exception as e:
            logger.error(f"Discovery failed: {e}")
            raise

        return results

    # --- Data Chunk Methods ---

    async def read_file(self, full_fs_path, offset, length, mtime):
        api_path = self._resolve_api_path(full_fs_path)
        file_id = self.disk.generate_file_id(api_path, mtime)
        
        start_chunk = offset // self.chunk_size
        end_chunk = (offset + length - 1) // self.chunk_size
        
        chunks_map = {}

        async with trio.open_nursery() as nursery:
            for i in range(start_chunk, end_chunk + 1):
                nursery.start_soon(self._fetch_chunk_task, file_id, api_path, i, chunks_map)

        # Assemble
        output = bytearray()
        current_pos = offset
        remaining = length
        
        for i in range(start_chunk, end_chunk + 1):
            if i not in chunks_map: raise IOError(f"Chunk {i} missing")
            
            chunk_data = chunks_map[i]
            chunk_start = i * self.chunk_size
            start_in_chunk = max(0, current_pos - chunk_start)
            take = min(len(chunk_data) - start_in_chunk, remaining)
            
            output.extend(chunk_data[start_in_chunk : start_in_chunk + take])
            current_pos += take
            remaining -= take
            if remaining <= 0: break
                
        return bytes(output)

    async def _fetch_chunk_task(self, file_id, api_path, chunk_idx, out_map):
        # 1. Disk
        data = await trio.to_thread.run_sync(self.disk.retrieve_chunk, file_id, chunk_idx)
        if data:
            out_map[chunk_idx] = data
            return

        # 2. Network
        data = await self.uc_client.download_chunk(api_path, chunk_idx * self.chunk_size, self.chunk_size)
        if not data: data = b""

        # 3. Save
        await trio.to_thread.run_sync(self.disk.store_chunk, file_id, chunk_idx, data)
        out_map[chunk_idx] = data

    def _resolve_api_path(self, fs_path):
        """Translates /Cat/Sch/Vol/file -> /Volumes/Cat/Sch/Vol/file"""
        parts = fs_path.split('/')
        if len(parts) < 4: return None
        vol_root = f"/Volumes/{parts[1]}/{parts[2]}/{parts[3]}"
        rel_path = "/" + "/".join(parts[4:])
        return f"{vol_root}{rel_path}"
