"""
Disk Persistence Layer using atomic writes and LRU eviction.
Manages 8MB chunks stored in a sharded directory structure.
"""
import os
import time
import shutil
import hashlib
import logging
from heapq import heappush, heappop

logger = logging.getLogger(__name__)

class DiskPersistence:
    def __init__(self, cache_dir, max_size_gb=10):
        self.cache_dir = cache_dir
        self.max_size_bytes = max_size_gb * 1024 * 1024 * 1024
        self.current_size = 0
        self.access_log = []  # Heap: (access_time, file_path, size)
        
        self._init_cache()

    def _init_cache(self):
        """Scans existing cache to rebuild LRU heap and size."""
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
            return

        logger.info(f"Scanning cache directory {self.cache_dir}...")
        for root, _, files in os.walk(self.cache_dir):
            for f in files:
                if f.endswith(".tmp"): # Cleanup partial writes
                    os.remove(os.path.join(root, f))
                    continue
                    
                path = os.path.join(root, f)
                try:
                    stat = os.stat(path)
                    self.current_size += stat.st_size
                    heappush(self.access_log, (stat.st_atime, path, stat.st_size))
                except OSError:
                    pass
        logger.info(f"Cache Initialized: {self.current_size / (1024**3):.2f} GB used.")

    def generate_file_id(self, full_path, mtime):
        """
        Creates a deterministic unique ID based on path AND mtime.
        This ensures we invalidate cache if the remote file changes.
        """
        raw_str = f"{full_path}:{mtime}"
        return hashlib.sha256(raw_str.encode('utf-8')).hexdigest()

    def _get_chunk_path(self, file_id, chunk_index):
        # Sharding: use first 2 chars of hash to spread files across subdirs
        shard = file_id[:2]
        shard_dir = os.path.join(self.cache_dir, shard)
        os.makedirs(shard_dir, exist_ok=True)
        return os.path.join(shard_dir, f"{file_id}_{chunk_index}.bin")

    def retrieve_chunk(self, file_id, chunk_index):
        """Returns bytes if exists, else None."""
        path = self._get_chunk_path(file_id, chunk_index)
        try:
            # Update access time (touch) for LRU
            os.utime(path, None)
            with open(path, "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def store_chunk(self, file_id, chunk_index, data):
        """Atomic write of chunk data."""
        size = len(data)
        self.evict(size)

        final_path = self._get_chunk_path(file_id, chunk_index)
        temp_path = final_path + ".tmp"
        
        try:
            with open(temp_path, "wb") as f:
                f.write(data)
            os.rename(temp_path, final_path)
            
            self.current_size += size
            # Push to heap: (now, path, size)
            heappush(self.access_log, (time.time(), final_path, size))
            
        except OSError as e:
            logger.error(f"Failed to write chunk to disk: {e}")
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def evict(self, required_space):
        """Removes oldest files until we have space."""
        while self.current_size + required_space > self.max_size_bytes:
            if not self.access_log:
                # Cache is empty but still no space? (Edge case: tiny quota)
                break
                
            _, path, size = heappop(self.access_log)
            
            # Check if file still exists (might have been deleted already)
            if os.path.exists(path):
                try:
                    os.remove(path)
                    self.current_size -= size
                except OSError:
                    pass
