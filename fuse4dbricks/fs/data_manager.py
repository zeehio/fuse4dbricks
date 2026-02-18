from dataclasses import dataclass
import errno
from typing import Tuple
from collections import OrderedDict
import trio

try:
    import pyfuse3
except ImportError:
    import fuse4dbricks.mock.pyfuse3 as pyfuse3  # type: ignore[no-redef]

from fuse4dbricks.fs.utils import fs_to_uc_path, InflightCoalescer

from fuse4dbricks.api.uc_client import UnityCatalogClient
from fuse4dbricks.storage.persistence import DiskPersistence

@dataclass
class _ChunkRequest:
    fs_path: str
    "path to download"
    chunk_id: int
    "piece to download"
    mtime: float
    "last modified, to ensure chunks are not altered between reads"
    chunk_size: int
    "size of this chunk. It may be smaller than the typical chunk_size on the last chunk of o a file"
    ctx_uid: int
    "who requested the chunk"


class DataManager:
    def __init__(self, uc_client: UnityCatalogClient, persistence: DiskPersistence, ram_cache_mb = 512, num_workers=10):
        """
        uc_client: client to download chunks from UC
        persistence: where to store downloaded chunks on disk
        ram_cache_mb: how much RAM to use for caching chunks in memory (LRU eviction)
        num_workers: how many concurrent downloads to allow
        """
        self.uc_client = uc_client
        self.persistence = persistence
        self.chunk_size = 8 * 1024 * 1024
        self._inflight_coalescer: InflightCoalescer[Tuple[str, int, float]] = InflightCoalescer()
        "Inflight coalescer for downloads identified by (file_id, chunk_id, mtime)"
        

        self._chunk_cache: OrderedDict[Tuple[str, int, float], bytes] = OrderedDict()
        """(fs_path, chunk_id, mtime) -> chunk_data"""
        self._chunk_cache_lock = trio.Lock()
        self._chunk_cache_max_size = int(ram_cache_mb*1024*1024 / self.chunk_size)  # max number of chunks to cache in memory

        self._requests_lock = trio.Lock()
        self._requests_priority: OrderedDict[Tuple[str, int, float], _ChunkRequest] = OrderedDict()
        self._requests_regular: OrderedDict[Tuple[str, int, float], _ChunkRequest] = OrderedDict()
        self._requests_num_workers = num_workers

    def run_services(self, nursery):
        """Starts download workers"""
        for i in range(self._requests_num_workers):
            nursery.start_soon(self._downloader)

    async def _downloader(self):
        while True:
            async with self._requests_lock:
                cache_key = None
                chunk_request = None
                priority = "regular"
                try:
                    (cache_key, chunk_request) = self._requests_priority.popitem()
                    priority = "high"
                except KeyError:
                    pass
                if chunk_request is None:
                    try:
                        (cache_key, chunk_request) = self._requests_regular.popitem()
                        priority = "regular"
                    except KeyError:
                        pass
            if cache_key is None or chunk_request is None:
                await trio.sleep(0.2)
                continue
            # We have a chunk_request to download
            # check if we already downloaded it:
            chunk = await self.persistence.retrieve_chunk(fs_path=chunk_request.fs_path, chunk_index=chunk_request.chunk_id, mtime=chunk_request.mtime)
            if chunk is None:
                # download it.
                chunk = await self._fetch_chunk(chunk_request)
            if priority == "high":
                await self._save_chunk_to_ram(fs_path=chunk_request.fs_path, chunk_id=chunk_request.chunk_id, mtime=chunk_request.mtime, chunk=chunk)
            await self._inflight_coalescer.notify_done(cache_key)

    async def _save_chunk_to_ram(self, fs_path, chunk_id, mtime, chunk):
        cache_key = (fs_path, chunk_id, mtime)
        async with self._chunk_cache_lock:
            self._chunk_cache[cache_key] = chunk
            # Move to end to mark as recently used
            self._chunk_cache.move_to_end(cache_key)
            # Evict least recently used if cache exceeds max size
            if len(self._chunk_cache) > self._chunk_cache_max_size:
                self._chunk_cache.popitem(last=False)
        return

    async def _fetch_chunk(self, chunk_request: _ChunkRequest):
        uc_path = fs_to_uc_path(chunk_request.fs_path)
        offset = self.chunk_size * chunk_request.chunk_id
        length = chunk_request.chunk_size
        stream = self.uc_client.download_chunk_stream(
            path=uc_path,
            offset=offset,
            length=length,
            ctx_uid=chunk_request.ctx_uid,
            if_unmodified_since=chunk_request.mtime,
        )
        chunk = await self.persistence.store_chunk_from_stream(
            fs_path=chunk_request.fs_path,
            chunk_index=chunk_request.chunk_id,
            mtime=chunk_request.mtime,
            stream=stream,
        )
        return chunk

    async def _enqueue_fetch(self, chunk_request: _ChunkRequest, high_priority):
        cache_key = (chunk_request.fs_path, chunk_request.chunk_id, chunk_request.mtime)
        async with self._requests_lock:
            if high_priority:
                # If request for that chunk was present as a regular request, promote it:
                if cache_key in self._requests_regular:
                    self._requests_regular.pop(cache_key)
                self._requests_priority[cache_key] = chunk_request
            else:
                # If request for that chunk was already a priority request, ignore it, otherwise add it
                if cache_key not in self._requests_priority:
                    self._requests_regular[cache_key] = chunk_request

    async def _request_fetch_ahead_chunks(self, fs_path: str, chunks_to_prefetch: list[Tuple[int, int]], mtime: float, ctx_uid: int):
        for (chunk_id, chunk_size) in chunks_to_prefetch:
            cache_key = (fs_path, chunk_id, mtime)
            (wait_event, leader) = await self._inflight_coalescer.join_or_lead(cache_key)
            if not leader:
                continue  # already being fetched, we don't care about it
            chunk_request = _ChunkRequest(fs_path=fs_path, chunk_id=chunk_id, mtime=mtime, chunk_size=chunk_size, ctx_uid=ctx_uid)
            await self._enqueue_fetch(chunk_request, high_priority=False)
        return None

    async def _get_chunk_from_cache_or_disk(self, fs_path: str, chunk_id: int, mtime: float) -> bytes | None:
        cache_key = (fs_path, chunk_id, mtime)
        # get chunk from ram
        async with self._chunk_cache_lock:
            if cache_key in self._chunk_cache:
                cached = self._chunk_cache[cache_key]
                # Move to end to mark as recently used
                self._chunk_cache.move_to_end(cache_key)
                return cached
        # get chunk from disk
        chunk = await self.persistence.retrieve_chunk(fs_path, chunk_id, mtime)
        if chunk is not None:
            # promote it to RAM before returning it
            async with self._chunk_cache_lock:
                self._chunk_cache[cache_key] = chunk
                # Move to end to mark as recently used
                self._chunk_cache.move_to_end(cache_key)
                # Evict least recently used if cache exceeds max size
                if len(self._chunk_cache) > self._chunk_cache_max_size:
                    self._chunk_cache.popitem(last=False)
        return chunk


    async def _read_chunk(
        self, fs_path: str, chunk_id: int, mtime: float, chunk_size: int, 
        ctx_uid: int, out_dict
    ):
        cache_key = (fs_path, chunk_id, mtime)
        chunk = await self._get_chunk_from_cache_or_disk(fs_path=fs_path, chunk_id=chunk_id, mtime=mtime)
        if chunk is not None:
            out_dict[chunk_id] = chunk
            return

        # 2. Coalescing
        (wait_event, leader) = await self._inflight_coalescer.join_or_lead(cache_key)

        if leader:
            # get chunk from network
            chunk_request = _ChunkRequest(fs_path=fs_path, chunk_id=chunk_id, mtime=mtime, chunk_size=chunk_size, ctx_uid=ctx_uid)
            await self._enqueue_fetch(chunk_request, high_priority=True)

        # Now we wait for the download to complete and fetch it from RAM or Disk
        await wait_event.wait()
        chunk = await self._get_chunk_from_cache_or_disk(fs_path=fs_path, chunk_id=chunk_id, mtime=mtime)
        # here if chunk is none, we will return None and let the caller decide what to do (probably return EIO)
        out_dict[chunk_id] = chunk
        return

    async def read(
        self, fs_path: str, offset: int, length: int, mtime: float, file_size: int,
        ctx_uid: int
    ) -> bytes:
        """
        Reads file contents
        Raises on error raise pyfuse3.FUSEError(errno.EIO)
        """
        # Determine path and chunks that need to be read
        if offset >= file_size:
            return bytes()
        last_chunk_size = file_size % self.chunk_size
        num_chunks_in_file = file_size // self.chunk_size
        if last_chunk_size > 0:
            num_chunks_in_file += 1
        else:
            last_chunk_size = self.chunk_size
        start_chunk = offset // self.chunk_size
        end_chunk = (offset + length - 1) // self.chunk_size
        if end_chunk >= num_chunks_in_file:
            end_chunk = num_chunks_in_file - 1
        chunks_to_read = list(range(start_chunk, end_chunk + 1))
        # Download all required chunks
        chunks: dict[int, bytes] = {}
        # dictionary: chunk_id -> bytes
        async with trio.open_nursery() as nursery:
            for chunk_id in chunks_to_read:
                if chunk_id == num_chunks_in_file - 1:
                    chunk_size = last_chunk_size
                else:
                    chunk_size = self.chunk_size
                nursery.start_soon(
                    self._read_chunk, fs_path, chunk_id, mtime, chunk_size, ctx_uid, chunks
                )
        # fetch ahead next 10 chunks once we are beyond chunk 0 (avoid prefetching too much on `head *` command)
        num_chunks_to_prefetch = 10 if end_chunk > 0 else 1
        chunks_to_prefetch = []
        for i in range(num_chunks_to_prefetch):
            chunk_to_prefetch = end_chunk + 1 + i
            if chunk_to_prefetch >= num_chunks_in_file:
                # chunk_to_prefetch beyond EOF, stop
                break
            # chunk_size: Last chunk in file may be shorter
            if chunk_to_prefetch == num_chunks_in_file -1:
                chunk_size = last_chunk_size
            else:
                chunk_size = self.chunk_size
            # Add to list to fetch:
            chunks_to_prefetch.append((chunk_to_prefetch, chunk_size))
        await self._request_fetch_ahead_chunks(fs_path, chunks_to_prefetch, mtime, ctx_uid)
        # Assemble chunks
        result = bytearray()
        for chunk_id in chunks_to_read:
            chunk_data = chunks.get(chunk_id)
            if chunk_data is None:
                raise pyfuse3.FUSEError(errno.EIO)
            # Calculate the start and end within the chunk
            chunk_start = max(0, offset - chunk_id * self.chunk_size)
            chunk_end = min(
                len(chunk_data), offset + length - chunk_id * self.chunk_size
            )
            result.extend(chunk_data[chunk_start:chunk_end])
        return bytes(result)

    async def write(self, fs_path: str, offset: int, buffer: bytes, ctx_uid: int) -> int:
        raise pyfuse3.FUSEError(errno.EACCES)
