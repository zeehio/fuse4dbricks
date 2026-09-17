"""
Disk Persistence Layer for FUSE.
Implements atomic writes, single-level sharding, and lazy LRU eviction.
"""

import hashlib
import itertools
import logging
import os
import shutil
import time
from heapq import heappop, heappush
from typing import Tuple

import trio

logger = logging.getLogger(__name__)


def clear_cache(cache_dir: str):
    """Removes *.tmp and *.bin files from cache_dir recursively"""
    logging.info("Clearing cache... %s", cache_dir)
    for root, _, files in os.walk(cache_dir):
        for f in files:
            path = os.path.join(root, f)
            if path.endswith(".tmp") or path.endswith(".bin"):
                os.unlink(path)


class DiskPersistence:
    def __init__(self, cache_dir: str, max_size_gb: int = 2048, max_age_days: int = 7):
        self.cache_dir = cache_dir
        self.max_size_bytes = float(max_size_gb) * 1024**3
        self.max_age_seconds = max_age_days * 86400
        self.start_time = time.time()

        self.current_size = 0
        self.lock = trio.Lock()
        self.access_log: list[Tuple[float, str, int]] = []  # Heap: (now, cache_path, bytes_written)
        self.access_map: dict[str, float] = {}  # {cache_path: latest_use_timestamp}
        # Makes each .tmp name unique within the process, so two writes of the
        # same chunk can never scribble over each other's partial file.
        self._temp_seq = itertools.count()

    def run_services(self, nursery):
        """Starts background maintenance and discovery."""
        nursery.start_soon(self._graceful_init)
        nursery.start_soon(self._background_maintenance)

    def _get_chunk_path(self, fs_path: str, chunk_index: int, mtime: float, gen: int = 0) -> str:
        """Compute the cache path for a chunk (256-shard distribution).

        Pure: does NOT touch the filesystem. ``retrieve_chunk`` runs on every
        read (cache hits included), so creating the shard dir here meant a
        blocking ``os.makedirs`` syscall in the async hot path plus empty dirs
        for chunks that are never written. Directory creation now lives on the
        write path only (see ``store_chunk`` / ``_ensure_parent_dir``); a read
        of a missing chunk simply hits FileNotFoundError and returns None.
        """
        sha256_hash = hashlib.sha256(fs_path.encode("utf-8")).hexdigest()
        shard1 = sha256_hash[:2]
        shard2 = f"{(chunk_index // 1000):07d}"  # Secondary shard to prevent too many files in one dir
        shard_dir = os.path.join(self.cache_dir, shard1, shard2)
        mtime_ms = int(mtime * 1000)
        # gen is a local cache epoch (see DataManager.invalidate_path); it keeps
        # a same-second overwrite from reusing a stale chunk file on disk.
        return os.path.join(
            shard_dir, f"{sha256_hash}_{mtime_ms}_g{gen}_{chunk_index:07d}.bin"
        )

    @staticmethod
    def _ensure_parent_dir(path: str) -> None:
        """Create the parent directory of ``path`` (idempotent). Runs off the
        event loop via ``trio.to_thread`` from the write path."""
        os.makedirs(os.path.dirname(path), exist_ok=True)

    async def _graceful_init(self):
        """Non-blocking cache discovery."""
        logger.info("Starting graceful cache discovery...")
        await trio.to_thread.run_sync(self._sync_scan)
        logger.info(f"Discovery complete. Initial size: {self.current_size/1e9:.2f} GB")

    def _sync_scan(self):
        """Synchronous recursive walk to rebuild state."""
        for root, _, files in os.walk(self.cache_dir):
            for f in files:
                cache_path = os.path.join(root, f)
                try:
                    stat = os.stat(cache_path)

                    if f.endswith(".tmp"):
                        if stat.st_mtime < self.start_time:
                            os.remove(cache_path)
                        continue

                    self.current_size += stat.st_size
                    self.access_map[cache_path] = stat.st_atime
                    heappush(self.access_log, (stat.st_atime, cache_path, stat.st_size))
                except OSError:
                    continue

    async def retrieve_chunk(
        self, fs_path: str, chunk_index: int, mtime: float, gen: int = 0
    ) -> bytes | None:
        """
        Retrieves a chunk from disk.
        Updates LRU access time BEFORE reading to prevent concurrent eviction.
        """
        cache_path = self._get_chunk_path(fs_path, chunk_index, mtime, gen)

        # 1. OPTIMISTIC PROMOTION (Pinning)
        # Mark as accessed so GC doesn't delete it while we read.
        # Double-check pattern is not strictly needed here for safety,
        # but the lock is needed to update the map safely.
        async with self.lock:
            if cache_path in self.access_map:
                self.access_map[cache_path] = time.time()

        try:
            # 2. READ (Safe now)
            data = await trio.to_thread.run_sync(self._read_file, cache_path)

            # 3. SELF-HEALING
            # If file exists but wasn't in map (race condition or init miss)
            if cache_path not in self.access_map:
                async with self.lock:
                    if cache_path not in self.access_map:
                        self.current_size += len(data)
                        now = time.time()
                        self.access_map[cache_path] = now
                        heappush(self.access_log, (now, cache_path, len(data)))

            return data

        except FileNotFoundError:
            return None

    def _read_file(self, path: str) -> bytes:
        with open(path, "rb") as f:
            return f.read()

    async def chunk_exists(
        self, fs_path: str, chunk_index: int, mtime: float, gen: int = 0
    ) -> bool:
        """Whether a chunk is already cached on disk, without reading its
        content. For a prefetch, the caller only needs to know whether a
        download is required; reading the full chunk back (as
        ``retrieve_chunk`` does) would cost as much I/O as the read it is
        trying to save.
        """
        cache_path = self._get_chunk_path(fs_path, chunk_index, mtime, gen)
        async with self.lock:
            if cache_path in self.access_map:
                self.access_map[cache_path] = time.time()
        return await trio.to_thread.run_sync(os.path.exists, cache_path)

    async def store_chunk(
        self,
        fs_path: str,
        chunk_index: int,
        mtime: float,
        data: bytes,
        gen: int = 0,
    ) -> None:
        """Writes an already-downloaded chunk to the cache.

        Atomic (write to ``.tmp``, then rename) and LRU-accounted, as before.
        What changed is *when* this runs: the downloader hands the bytes back to
        the waiting reader first and only then queues this write (see
        ``DataManager._process_request``), so a slow local disk never adds
        latency to a read. Because the write now outlives the read that produced
        it, it must clean up after itself when cancelled at shutdown.
        """
        cache_path = self._get_chunk_path(fs_path, chunk_index, mtime, gen)
        temp_path = f"{cache_path}.{os.getpid()}.{next(self._temp_seq)}.tmp"
        bytes_written = len(data)
        try:
            # The shard dir is created lazily here (off-thread), only when we
            # actually write a chunk — not on every read in _get_chunk_path.
            await trio.to_thread.run_sync(self._ensure_parent_dir, cache_path)

            # Write to .tmp (No lock needed)
            async with await trio.open_file(temp_path, "wb") as f:
                await f.write(data)

            # Size of the chunk we are about to replace, if any, so the
            # bookkeeping below can discount it (0 when there is nothing there).
            replaced_bytes = await trio.to_thread.run_sync(self._size_or_zero, cache_path)

            # Atomic Rename (No lock needed)
            await trio.to_thread.run_sync(os.rename, temp_path, cache_path)

            # 1. EVICT (Manages Lock Internally)
            await self.evict(bytes_written)

            # 2. UPDATE METADATA (Acquire Lock)
            async with self.lock:
                if cache_path in self.access_map:
                    # Overwriting a chunk we already account for. Without this,
                    # current_size keeps the old chunk's bytes forever: the
                    # stale heap entry is dropped without a refund when it is
                    # popped (its path is no longer the one in access_map), so
                    # the cache would believe it is fuller than it is and evict
                    # too eagerly.
                    self.current_size -= replaced_bytes
                self.current_size += bytes_written
                now = time.time()
                self.access_map[cache_path] = now
                heappush(self.access_log, (now, cache_path, bytes_written))
        except BaseException:
            # BaseException, not Exception: this runs as a background task now,
            # so trio.Cancelled at shutdown is an expected way to get here and
            # must not leave a partial .tmp behind. The cleanup is shielded so
            # it still runs inside the cancelled scope.
            with trio.CancelScope(shield=True):
                await trio.to_thread.run_sync(self._remove_quietly, temp_path)
            raise

    @staticmethod
    def _size_or_zero(path: str) -> int:
        """Size of ``path``, or 0 if it is not there."""
        try:
            return os.stat(path).st_size
        except OSError:
            return 0

    @staticmethod
    def _remove_quietly(path: str) -> None:
        """Delete ``path`` if it is there, ignoring the case where it is not."""
        try:
            os.remove(path)
        except OSError:
            pass

    async def evict(self, required_space: int):
        """
        Free up space removing old items.
        THREAD-SAFE: Uses lock for state, releases for I/O.
        """
        while True:
            # 1. DECISION PHASE (Lock Held)
            async with self.lock:
                if self.current_size + required_space <= self.max_size_bytes:
                    return

                if not self.access_log:
                    return

                ts_log, path, size = heappop(self.access_log)

                if path not in self.access_map:
                    continue

                # Lazy check: if map has newer timestamp, repush
                if self.access_map[path] > ts_log:
                    heappush(self.access_log, (self.access_map[path], path, size))
                    continue

                # Remove from index immediately
                del self.access_map[path]
                self.current_size -= size

            # 2. I/O PHASE (Lock Released)
            try:
                await trio.to_thread.run_sync(os.remove, path)
                logger.debug(f"Evicted: {path}")
            except OSError:
                pass

    async def _background_maintenance(self):
        """
        Hourly cleanup. Uses snapshot approach to avoid blocking.
        """
        while True:
            await trio.sleep(3600)
            now = time.time()

            # 1. Fast Snapshot (Lock Held)
            async with self.lock:
                try:
                    usage = await trio.to_thread.run_sync(
                        shutil.disk_usage, self.cache_dir
                    )
                    disk_critical = (usage.free / usage.total) < 0.05
                except OSError:
                    disk_critical = False  # Disk might be unmounted or weird error

                expired_candidates = [
                    path
                    for path, ts in self.access_map.items()
                    if (now - ts > self.max_age_seconds) or disk_critical
                ]

            # 2. Slow Deletion (Lock Released)
            for path in expired_candidates:
                await trio.sleep(0)  # Cooperative yield

                try:
                    stat = await trio.to_thread.run_sync(os.stat, path)
                    await trio.to_thread.run_sync(os.remove, path)

                    # 3. State Update (Lock Held)
                    async with self.lock:
                        if path not in self.access_map:
                            continue

                        del self.access_map[path]
                        self.current_size -= stat.st_size
                except OSError:
                    pass
