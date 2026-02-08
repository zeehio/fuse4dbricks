from dataclasses import dataclass
import hashlib
import trio
from typing import Tuple

# TODO: move some methods from cache_manager here
# FIXME: Not implemented

@dataclass


class DataManager:
    def __init__(self, uc_client, persistence):
        self.uc_client = uc_client
        self.persistence = persistence
        self.chunk_size = 8 * 1024 * 1024
        self._inflight_downloads: dict[Tuple[str, int], trio.Event] = {}
        "(file_id, chunk_id) -> trio.Event"
        self._inflight_lock = trio.Lock()



    def generate_file_id(self, full_path, mtime):
        """
        Creates a deterministic unique ID based on path AND mtime.
        This ensures we invalidate cache if the remote file changes.
        """
        raw_str = f"{full_path}:{mtime}"
        return hashlib.sha256(raw_str.encode('utf-8')).hexdigest()

    def _read_chunk(self, fs_path: str, chunk_id: int, mtime: float) -> bytes:
        pass

    async def read_file(self, fs_path: str, offset: int, length: int, mtime: float) -> bytes:
        """
        Reads file contents
        Raises on error raise pyfuse3.FUSEError(errno.EIO)
        """
        # Determine path and chunks that need to be read
        start_chunk = offset // self.chunk_size
        end_chunk = (offset + length - 1) // self.chunk_size
        chunks_to_read = list(range(start_chunk, end_chunk + 1))
        # Download all required chunks
        chunks: dict[int, bytes] = {} # dictionary: chunk_id -> bytes
        # Assemble chunks
        result = bytearray()
        for chunk_id in chunks_to_read:
            chunk_data = chunks.get(chunk_id)
            if chunk_data is None:
                raise RuntimeError(f"Chunk {chunk_id} not found in downloaded chunks")
            # Calculate the start and end within the chunk
            chunk_start = max(0, offset - chunk_id * self.chunk_size)
            chunk_end = min(len(chunk_data), offset + length - chunk_id * self.chunk_size)
            result.extend(chunk_data[chunk_start:chunk_end])
        return bytes(result)
