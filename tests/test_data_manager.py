"""
Tests for fuse4dbricks.fs.data_manager.DataManager.read.

Strategy: mock _read_chunk so that it always populates out_dict with
synthetic data, bypassing the network / disk / scheduler entirely.
This lets us unit-test the chunk-selection and byte-assembly math in
DataManager.read without spinning up the full download infrastructure.
"""

import errno
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pyfuse3

from fuse4dbricks.fs.data_manager import DataManager


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def ctx():
    return SimpleNamespace(uid=1000, pid=5678, gid=1000)


def _make_manager() -> DataManager:
    uc_client = MagicMock()
    persistence = MagicMock()
    persistence.retrieve_chunk = AsyncMock(return_value=None)
    persistence.store_chunk_from_stream = AsyncMock(return_value=b"")
    dm = DataManager(uc_client=uc_client, persistence=persistence, ram_cache_mb=1, num_workers=1)
    return dm


@pytest.fixture
def manager():
    return _make_manager()


# ---------------------------------------------------------------------------
# Helper: fake _read_chunk that fills out_dict with 'x' * chunk_size
# ---------------------------------------------------------------------------


def _make_fake_read_chunk(chunk_size: int, fill_byte: bytes = b"x"):
    """Returns a coroutine that writes `chunk_size` bytes of fill_byte to out_dict[chunk_id]."""
    async def _fake(fs_path, chunk_id, mtime, gen, cs, ctx, out_dict):
        out_dict[chunk_id] = fill_byte * cs

    return _fake


def _make_fake_read_chunk_none():
    """Returns a coroutine that sets out_dict[chunk_id] = None (simulates download failure)."""
    async def _fake(fs_path, chunk_id, mtime, gen, chunk_size, ctx, out_dict):
        out_dict[chunk_id] = None

    return _fake


# ---------------------------------------------------------------------------
# Tests: edge / trivial cases
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_read_offset_at_or_beyond_file_size_returns_empty(manager, ctx):
    result = await manager.read(
        "/cat/sch/vol/file.txt", offset=100, length=10, mtime=0.0, file_size=100, ctx=ctx
    )
    assert result == b""


@pytest.mark.trio
async def test_read_offset_beyond_file_size_returns_empty(manager, ctx):
    result = await manager.read(
        "/cat/sch/vol/file.txt", offset=200, length=10, mtime=0.0, file_size=100, ctx=ctx
    )
    assert result == b""


# ---------------------------------------------------------------------------
# Tests: single-chunk files
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_read_single_chunk_full_file(manager, ctx):
    """Read the entirety of a file smaller than one chunk."""
    file_size = 100
    with patch.object(manager, "_read_chunk", side_effect=_make_fake_read_chunk(file_size)):
        with patch.object(manager, "_request_fetch_ahead_chunks", new_callable=AsyncMock):
            result = await manager.read(
                "/f", offset=0, length=file_size, mtime=0.0, file_size=file_size, ctx=ctx
            )
    assert len(result) == file_size
    assert result == b"x" * file_size


@pytest.mark.trio
async def test_read_single_chunk_partial_from_middle(manager, ctx):
    """Read bytes [10:20] from a 100-byte file."""
    file_size = 100
    with patch.object(manager, "_read_chunk", side_effect=_make_fake_read_chunk(file_size)):
        with patch.object(manager, "_request_fetch_ahead_chunks", new_callable=AsyncMock):
            result = await manager.read(
                "/f", offset=10, length=10, mtime=0.0, file_size=file_size, ctx=ctx
            )
    assert len(result) == 10
    assert result == b"x" * 10


# ---------------------------------------------------------------------------
# Tests: multi-chunk files (chunk_size = 8 MiB)
# ---------------------------------------------------------------------------

CHUNK = 8 * 1024 * 1024  # 8 MiB


@pytest.mark.trio
async def test_read_exact_two_chunks(manager, ctx):
    """File is exactly 2 chunks; read across the boundary."""
    file_size = 2 * CHUNK
    with patch.object(manager, "_read_chunk", side_effect=_make_fake_read_chunk(CHUNK)):
        with patch.object(manager, "_request_fetch_ahead_chunks", new_callable=AsyncMock):
            result = await manager.read(
                "/f", offset=0, length=file_size, mtime=0.0, file_size=file_size, ctx=ctx
            )
    assert len(result) == file_size


@pytest.mark.trio
async def test_read_spanning_chunk_boundary(manager, ctx):
    """Read a range that starts in chunk 0 and ends in chunk 1."""
    file_size = 2 * CHUNK
    offset = CHUNK - 5
    length = 10  # 5 bytes from chunk 0, 5 bytes from chunk 1

    with patch.object(manager, "_read_chunk", side_effect=_make_fake_read_chunk(CHUNK)):
        with patch.object(manager, "_request_fetch_ahead_chunks", new_callable=AsyncMock):
            result = await manager.read(
                "/f", offset=offset, length=length, mtime=0.0, file_size=file_size, ctx=ctx
            )
    assert len(result) == length


@pytest.mark.trio
async def test_read_last_chunk_smaller_than_chunk_size(manager, ctx):
    """
    File size is not a multiple of chunk_size.
    The last chunk is smaller; the returned data must match that smaller size.
    """
    last_chunk_size = 500
    file_size = CHUNK + last_chunk_size  # 1 full chunk + 500 bytes

    call_log: list[int] = []

    async def fake_read_chunk(fs_path, chunk_id, mtime, gen, cs, ctx, out_dict):
        call_log.append((chunk_id, cs))
        out_dict[chunk_id] = b"x" * cs

    with patch.object(manager, "_read_chunk", side_effect=fake_read_chunk):
        with patch.object(manager, "_request_fetch_ahead_chunks", new_callable=AsyncMock):
            result = await manager.read(
                "/f", offset=0, length=file_size, mtime=0.0, file_size=file_size, ctx=ctx
            )

    assert len(result) == file_size
    # Verify that chunk 1 was requested with the correct smaller size
    chunk1_calls = [cs for cid, cs in call_log if cid == 1]
    assert chunk1_calls == [last_chunk_size]


@pytest.mark.trio
async def test_read_file_size_exact_multiple_of_chunk_size(manager, ctx):
    """
    When file_size % chunk_size == 0, the last chunk should be chunk_size, not 0.
    """
    file_size = 2 * CHUNK

    call_log: list[tuple[int, int]] = []

    async def fake_read_chunk(fs_path, chunk_id, mtime, gen, cs, ctx, out_dict):
        call_log.append((chunk_id, cs))
        out_dict[chunk_id] = b"x" * cs

    with patch.object(manager, "_read_chunk", side_effect=fake_read_chunk):
        with patch.object(manager, "_request_fetch_ahead_chunks", new_callable=AsyncMock):
            await manager.read(
                "/f", offset=0, length=file_size, mtime=0.0, file_size=file_size, ctx=ctx
            )

    for chunk_id, cs in call_log:
        assert cs == CHUNK, f"chunk {chunk_id} had size {cs}, expected {CHUNK}"


# ---------------------------------------------------------------------------
# Tests: error handling
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_read_raises_eio_when_chunk_is_none(manager, ctx):
    """If a chunk cannot be downloaded, read must raise FUSEError(EIO)."""
    file_size = 100
    with patch.object(manager, "_read_chunk", side_effect=_make_fake_read_chunk_none()):
        with patch.object(manager, "_request_fetch_ahead_chunks", new_callable=AsyncMock):
            with pytest.raises(pyfuse3.FUSEError) as exc_info:
                await manager.read(
                    "/f", offset=0, length=file_size, mtime=0.0, file_size=file_size, ctx=ctx
                )
    assert exc_info.value.errno == errno.EIO


# ---------------------------------------------------------------------------
# Tests: prefetch behaviour
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_read_prefetch_not_triggered_at_start_of_first_chunk(manager, ctx):
    """
    When reading from offset 0 within chunk 0 (end_chunk == 0), prefetch should
    be limited to 1 chunk ahead (not 10), per the implementation comment.
    """
    file_size = 12 * CHUNK
    prefetch_calls: list = []

    async def fake_prefetch(fs_path, chunks, mtime, gen, ctx):
        prefetch_calls.append(chunks)

    with patch.object(manager, "_read_chunk", side_effect=_make_fake_read_chunk(CHUNK)):
        with patch.object(manager, "_request_fetch_ahead_chunks", side_effect=fake_prefetch):
            await manager.read(
                "/f", offset=0, length=CHUNK, mtime=0.0, file_size=file_size, ctx=ctx
            )

    # When end_chunk == 0, num_chunks_to_prefetch == 1
    assert len(prefetch_calls) == 1
    assert len(prefetch_calls[0]) <= 1


@pytest.mark.trio
async def test_read_prefetch_triggered_after_first_chunk(manager, ctx):
    """Reading chunk 1 should trigger prefetch of up to 10 subsequent chunks."""
    file_size = 20 * CHUNK
    prefetch_calls: list = []

    async def fake_prefetch(fs_path, chunks, mtime, gen, ctx):
        prefetch_calls.append(chunks)

    with patch.object(manager, "_read_chunk", side_effect=_make_fake_read_chunk(CHUNK)):
        with patch.object(manager, "_request_fetch_ahead_chunks", side_effect=fake_prefetch):
            await manager.read(
                "/f", offset=CHUNK, length=CHUNK, mtime=0.0, file_size=file_size, ctx=ctx
            )

    assert len(prefetch_calls) == 1
    assert len(prefetch_calls[0]) == 10  # 10 chunks prefetched


@pytest.mark.trio
async def test_read_prefetch_stops_at_eof(manager, ctx):
    """Prefetch must not request chunks beyond the last chunk in the file."""
    file_size = 3 * CHUNK  # 3 chunks total (0, 1, 2)
    prefetch_calls: list = []

    async def fake_prefetch(fs_path, chunks, mtime, gen, ctx):
        prefetch_calls.append(chunks)

    with patch.object(manager, "_read_chunk", side_effect=_make_fake_read_chunk(CHUNK)):
        with patch.object(manager, "_request_fetch_ahead_chunks", side_effect=fake_prefetch):
            # Read chunk 1; only chunk 2 can be prefetched
            await manager.read(
                "/f", offset=CHUNK, length=CHUNK, mtime=0.0, file_size=file_size, ctx=ctx
            )

    assert len(prefetch_calls) == 1
    prefetched_chunk_ids = [cid for cid, _ in prefetch_calls[0]]
    assert all(cid < 3 for cid in prefetched_chunk_ids)
    assert 3 not in prefetched_chunk_ids  # chunk 3 doesn't exist


# ---------------------------------------------------------------------------
# Tests: cache generation (invalidate_path)
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_invalidate_path_bumps_generation(manager):
    fs = "/cat/sch/vol/f.txt"
    assert manager._generation(fs) == 0
    await manager.invalidate_path(fs)
    assert manager._generation(fs) == 1
    await manager.invalidate_path(fs)
    assert manager._generation(fs) == 2
    # Other paths are unaffected.
    assert manager._generation("/cat/sch/vol/other.txt") == 0


@pytest.mark.trio
async def test_invalidate_path_changes_the_chunk_cache_key(manager):
    """After a write bumps the generation, the next lookup keys the chunk under
    the new generation -- so a stale RAM/disk chunk from before the write (same
    fs_path, chunk and mtime, but old gen) is bypassed. This is what stops a
    same-second overwrite (mtime has 1s resolution) from serving stale bytes."""
    fs = "/cat/sch/vol/f.txt"
    mtime = 1000.0

    await manager._get_chunk_from_cache_or_disk(fs, 0, mtime, manager._generation(fs))
    manager.persistence.retrieve_chunk.assert_awaited_with(fs, 0, mtime, 0)

    await manager.invalidate_path(fs)

    await manager._get_chunk_from_cache_or_disk(fs, 0, mtime, manager._generation(fs))
    manager.persistence.retrieve_chunk.assert_awaited_with(fs, 0, mtime, 1)
