import errno
import hashlib
import trio
from typing import Tuple

try:
    import pyfuse3
except ImportError:
    import fuse4dbricks.mock.pyfuse3 as pyfuse3

from fuse4dbricks.api.uc_client import UnityCatalogClient

class DataManager:
    def __init__(self, uc_client: UnityCatalogClient, persistence: DiskPersistence):
        self.uc_client = uc_client
        self.persistence = persistence
        self.chunk_size = 8 * 1024 * 1024
        self._inflight_downloads: dict[Tuple[str, int, float], trio.Event] = {}
        "(file_id, chunk_id, mtime) -> trio.Event"
        self._inflight_lock = trio.Lock()

    async def _read_chunk(self, fs_path: str, chunk_id: int, mtime: float, out_dict):
        # get chunk from disk
        chunk = await self.persistence.retrieve_chunk(fs_path, chunk_id, mtime)
        
        if chunk is not None:
            out_dict[chunk_id] = chunk
            return
        # inflight strategy.
        # get chunk from network
        # save chunk in out_dict
        out_dict[chunk_id] = chunk
        pass

    async def read(self, fs_path: str, offset: int, length: int, mtime: float) -> bytes:
        """
        Reads file contents
        Raises on error raise pyfuse3.FUSEError(errno.EIO)
        """
        # Determine path and chunks that need to be read
        start_chunk = offset // self.chunk_size
        end_chunk = (offset + length - 1) // self.chunk_size
        chunks_to_read = list(range(start_chunk, end_chunk + 1))
        # Download all required chunks
        chunks: dict[int, bytes] = {}
        # dictionary: chunk_id -> bytes
        async with trio.open_nursery() as nursery:
            for chunk_id in chunks_to_read:
                nursery.start_soon(self._read_chunk, fs_path, chunk_id, mtime, chunks)
        # Assemble chunks
        result = bytearray()
        for chunk_id in chunks_to_read:
            chunk_data = chunks.get(chunk_id)
            if chunk_data is None:
                raise pyfuse3.FUSEError(errno.EIO)
            # Calculate the start and end within the chunk
            chunk_start = max(0, offset - chunk_id * self.chunk_size)
            chunk_end = min(len(chunk_data), offset + length - chunk_id * self.chunk_size)
            result.extend(chunk_data[chunk_start:chunk_end])
        return bytes(result)
