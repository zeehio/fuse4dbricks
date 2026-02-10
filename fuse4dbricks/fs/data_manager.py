import errno
from typing import Tuple

import trio

try:
    import pyfuse3
except ImportError:
    import fuse4dbricks.mock.pyfuse3 as pyfuse3  # type: ignore[no-redef]

from fuse4dbricks.fs.utils import (fs_to_uc_path, join_or_lead_request,
                                   notify_followers)

from fuse4dbricks.api.uc_client import UnityCatalogClient
from fuse4dbricks.storage.persistence import DiskPersistence


class DataManager:
    def __init__(self, uc_client: UnityCatalogClient, persistence: DiskPersistence):
        self.uc_client = uc_client
        self.persistence = persistence
        self.chunk_size = 8 * 1024 * 1024
        self._inflight_downloads: dict[Tuple[str, int, float], trio.Event] = {}
        "(file_id, chunk_id, mtime) -> trio.Event"
        self._inflight_lock = trio.Lock()

    async def _read_chunk(
        self, fs_path: str, chunk_id: int, mtime: float, chunk_size: int, 
        ctx_uid: int, out_dict
    ):
        # get chunk from ram? no need, the kernel file cache does that already :)
        # get chunk from disk
        chunk = await self.persistence.retrieve_chunk(fs_path, chunk_id, mtime)
        if chunk is not None:
            out_dict[chunk_id] = chunk
            return

        # 2. Coalescing
        wait_event = await join_or_lead_request(
            self._inflight_lock, self._inflight_downloads, (fs_path, chunk_id, mtime)
        )

        if wait_event:
            await wait_event.wait()
            chunk = await self.persistence.retrieve_chunk(fs_path, chunk_id, mtime)
            out_dict[chunk_id] = chunk
            return
        # get chunk from network
        try:
            uc_path = fs_to_uc_path(fs_path)
            offset = self.chunk_size * chunk_id
            length = chunk_size
            stream = self.uc_client.download_chunk_stream(
                path=uc_path,
                offset=offset,
                length=length,
                ctx_uid=ctx_uid,
                if_unmodified_since=mtime,
            )
            chunk = await self.persistence.store_chunk_from_stream(
                fs_path=fs_path,
                chunk_index=chunk_id,
                mtime=mtime,
                stream=stream,
            )
        finally:
            await notify_followers(
                self._inflight_lock,
                self._inflight_downloads,
                (fs_path, chunk_id, mtime),
            )
        # save chunk in out_dict
        out_dict[chunk_id] = chunk

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
