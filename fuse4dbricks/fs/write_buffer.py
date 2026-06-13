"""
Tempfile-backed write buffer for a single open writable file handle.
"""

import os
import tempfile


class WriteBuffer:
    """
    Tempfile-backed write buffer for a single open writable file handle.

    Uses a named tempfile in {cache_dir}/writes/ so memory usage stays flat
    regardless of file size — a 50 GB CRAM costs the same RAM as a 1 KB file.
    The buffer also supports read() so O_RDWR handles can serve reads from the
    local copy (which reflects in-progress writes) rather than from DataManager.
    """

    def __init__(self, writes_dir: str, initial_data: bytes = b""):
        self._tmp = tempfile.NamedTemporaryFile(
            delete=False, prefix="fuse4dbricks_write_", dir=writes_dir
        )
        self._size: int = 0
        if initial_data:
            self._tmp.write(initial_data)
            self._size = len(initial_data)

    def write(self, offset: int, data: bytes) -> int:
        self._tmp.seek(offset)
        self._tmp.write(data)
        end = offset + len(data)
        if end > self._size:
            self._size = end
        return len(data)

    def read(self, offset: int, length: int) -> bytes:
        self._tmp.seek(offset)
        return self._tmp.read(length)

    def size(self) -> int:
        return self._size

    @property
    def path(self) -> str:
        return self._tmp.name

    def close(self) -> None:
        """Close and delete the tempfile. Always call this, even on error."""
        try:
            self._tmp.close()
        finally:
            try:
                os.unlink(self._tmp.name)
            except OSError:
                pass
