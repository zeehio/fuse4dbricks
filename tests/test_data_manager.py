"""
Tests for fuse4dbricks.fs.data_manager.DataManager.

Two strategies, depending on what is under test:

- For the chunk-selection and byte-assembly math in read(), _read_chunk is
  mocked so that it always populates out_dict with synthetic data, bypassing
  the network / disk / scheduler entirely.
- For the download-and-cache behaviour, the real path runs with the network
  client and the disk persistence layer mocked, and the manager's background
  services started (see the `running` helper).
"""

import errno
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pyfuse3
import trio
import trio.testing

from fuse4dbricks.fs.data_manager import DataManager, _ChunkRequest
from fuse4dbricks.fs.ram_cache import RamCache


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def ctx():
    return SimpleNamespace(uid=1000, pid=5678, gid=1000)


def _download_stream(*parts: bytes):
    """A stand-in for UnityCatalogClient.download_chunk_stream()."""
    async def _stream():
        for part in parts:
            await trio.sleep(0)  # simulate network latency between packets
            yield part

    # side_effect, not return_value: every call must get a fresh generator.
    return MagicMock(side_effect=lambda *args, **kwargs: _stream())


def _make_manager(num_workers: int = 1) -> DataManager:
    uc_client = MagicMock()
    uc_client.download_chunk_stream = _download_stream(b"downloaded")
    persistence = MagicMock()
    persistence.retrieve_chunk = AsyncMock(return_value=None)
    persistence.chunk_exists = AsyncMock(return_value=False)
    persistence.store_chunk = AsyncMock(return_value=None)
    dm = DataManager(
        uc_client=uc_client, persistence=persistence, ram_cache_mb=1, num_workers=num_workers
    )
    return dm


@pytest.fixture
def manager():
    return _make_manager()


@asynccontextmanager
async def running(dm: DataManager):
    """Runs a DataManager's background services (download workers and disk-cache
    writers) for the duration of the block, then shuts them down cleanly so any
    queued disk write has landed before the assertions run."""
    async with trio.open_nursery() as nursery:
        dm.run_services(nursery)
        try:
            yield dm
        finally:
            dm.close()


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
# Tests: _process_request (on-demand reads vs. prefetch downloads)
#
# On-demand ("high" priority) reads need the actual bytes now, so they read
# the chunk (from disk, falling back to network) and cache it in RAM.
# Prefetch ("regular" priority) only needs the chunk to end up on disk --
# reading it back would cost as much disk I/O as the read it's trying to
# save, for a chunk that may never be read, so it uses a cheap existence
# check and never populates the RAM cache.
# ---------------------------------------------------------------------------


def _chunk_request(manager, chunk_id=0, mtime=100.0, gen=0):
    return _ChunkRequest(
        fs_path="/c/s/v/f", chunk_id=chunk_id, mtime=mtime, gen=gen,
        chunk_size=manager.chunk_size, ctx=SimpleNamespace(uid=1000, pid=1, gid=1000),
    )


@pytest.mark.trio
async def test_process_request_high_priority_disk_hit_caches_in_ram(manager):
    # The `manager` fixture's ram_cache_mb=1 rounds down to 0-entry capacity
    # (1 MB < one 8 MB chunk), which would no-op every put/get below.
    manager._ram_cache = RamCache(max_entries=8)
    manager.persistence.retrieve_chunk = AsyncMock(return_value=b"cached-bytes")
    request = _chunk_request(manager)

    async with running(manager):
        await manager._process_request(request, priority="high")

    manager.persistence.store_chunk.assert_not_awaited()
    cached = await manager._ram_cache.get(("/c/s/v/f", 0, 100.0, 0))
    assert cached == b"cached-bytes"


@pytest.mark.trio
async def test_process_request_high_priority_miss_downloads_and_caches_in_ram(manager):
    manager._ram_cache = RamCache(max_entries=8)
    manager.persistence.retrieve_chunk = AsyncMock(return_value=None)
    request = _chunk_request(manager)

    async with running(manager):
        await manager._process_request(request, priority="high")

    cached = await manager._ram_cache.get(("/c/s/v/f", 0, 100.0, 0))
    assert cached == b"downloaded"
    manager.persistence.store_chunk.assert_awaited_once_with(
        fs_path="/c/s/v/f", chunk_index=0, mtime=100.0, gen=0, data=b"downloaded"
    )


@pytest.mark.trio
async def test_process_request_prefetch_skips_download_when_already_on_disk(manager):
    manager.persistence.chunk_exists = AsyncMock(return_value=True)
    request = _chunk_request(manager, chunk_id=5)

    async with running(manager):
        await manager._process_request(request, priority="regular")

    manager.persistence.chunk_exists.assert_awaited_once_with(
        fs_path="/c/s/v/f", chunk_index=5, mtime=100.0, gen=0
    )
    # No download, and — crucially — no read of the chunk's content either
    # (that would cost the same disk I/O the existence check is avoiding).
    manager.persistence.retrieve_chunk.assert_not_awaited()
    manager.persistence.store_chunk.assert_not_awaited()
    assert await manager._ram_cache.get(("/c/s/v/f", 5, 100.0, 0)) is None


@pytest.mark.trio
async def test_process_request_prefetch_downloads_when_missing_but_not_cached_in_ram(manager):
    manager.persistence.chunk_exists = AsyncMock(return_value=False)
    request = _chunk_request(manager, chunk_id=5)

    async with running(manager):
        await manager._process_request(request, priority="regular")

    manager.persistence.retrieve_chunk.assert_not_awaited()
    manager.persistence.store_chunk.assert_awaited_once()
    # Prefetched content is deliberately not promoted into the RAM cache.
    assert await manager._ram_cache.get(("/c/s/v/f", 5, 100.0, 0)) is None


# ---------------------------------------------------------------------------
# Tests: the disk-cache write is off the read path
#
# A downloaded chunk is handed to the waiting reader as soon as the network
# transfer finishes; persisting it to the disk cache happens concurrently. A
# read therefore costs max(network, disk) rather than network + disk, and a
# slow, busy or broken local disk cannot hold a read up.
# ---------------------------------------------------------------------------


FILE_SIZE = 10  # b"downloaded"


@pytest.mark.trio
async def test_read_does_not_wait_for_the_disk_cache_write(manager, ctx):
    """The read must complete while the disk write is still pending."""
    write_started = trio.Event()
    release_write = trio.Event()

    async def hanging_store_chunk(**kwargs):
        write_started.set()
        await release_write.wait()

    manager.persistence.store_chunk = AsyncMock(side_effect=hanging_store_chunk)

    async with running(manager):
        with trio.fail_after(5):
            result = await manager.read(
                "/c/s/v/f", offset=0, length=FILE_SIZE, mtime=100.0,
                file_size=FILE_SIZE, ctx=ctx,
            )
            assert result == b"downloaded"
            # The read returned; the write it triggered is still in flight.
            await write_started.wait()
        assert not release_write.is_set()
        release_write.set()

    manager.persistence.store_chunk.assert_awaited_once()


@pytest.mark.trio
async def test_read_returns_bytes_when_the_disk_cache_write_fails(manager, ctx):
    """A failed cache write costs a future disk-cache miss, nothing more: the
    reader already has valid bytes straight from the network."""
    manager.persistence.store_chunk = AsyncMock(side_effect=OSError("disk full"))

    async with running(manager):
        with trio.fail_after(5):
            result = await manager.read(
                "/c/s/v/f", offset=0, length=FILE_SIZE, mtime=100.0,
                file_size=FILE_SIZE, ctx=ctx,
            )

    assert result == b"downloaded"
    manager.persistence.store_chunk.assert_awaited_once()


@pytest.mark.trio
async def test_prefetch_does_not_wait_for_the_disk_cache_write(manager, ctx):
    """Story 6: the prefetch path must not block on the disk write either."""
    release_write = trio.Event()
    writes_started = 0

    async def hanging_store_chunk(**kwargs):
        nonlocal writes_started
        writes_started += 1
        await release_write.wait()

    # Two workers, so the read's own download is not stuck behind the prefetch.
    dm = _make_manager(num_workers=2)
    dm.persistence.store_chunk = AsyncMock(side_effect=hanging_store_chunk)

    async with running(dm):
        with trio.fail_after(5):
            # A 2-chunk file read from chunk 0: chunk 1 gets prefetched.
            await dm.read(
                "/c/s/v/f", offset=0, length=FILE_SIZE, mtime=100.0,
                file_size=dm.chunk_size + FILE_SIZE, ctx=ctx,
            )
            # Both the read's and the prefetch's writes are pending, yet the
            # read already returned and the prefetch worker moved on.
            while writes_started < 2:
                await trio.sleep(0)
        release_write.set()

    assert dm.persistence.store_chunk.await_count == 2


@pytest.mark.trio
async def test_second_read_of_the_same_chunk_is_served_from_cache(manager, ctx):
    """Story 4: once the background write has landed, a repeat read is served
    from cache instead of hitting the network again."""
    stored: dict[tuple, bytes] = {}

    async def store_chunk(*, fs_path, chunk_index, mtime, gen, data):
        stored[(fs_path, chunk_index, mtime, gen)] = data

    async def retrieve_chunk(fs_path, chunk_index, mtime, gen=0):
        return stored.get((fs_path, chunk_index, mtime, gen))

    manager.persistence.store_chunk = AsyncMock(side_effect=store_chunk)
    manager.persistence.retrieve_chunk = AsyncMock(side_effect=retrieve_chunk)

    async with running(manager):
        with trio.fail_after(5):
            first = await manager.read(
                "/c/s/v/f", offset=0, length=FILE_SIZE, mtime=100.0,
                file_size=FILE_SIZE, ctx=ctx,
            )
            # Let the background write land before reading again.
            while not stored:
                await trio.sleep(0)
            second = await manager.read(
                "/c/s/v/f", offset=0, length=FILE_SIZE, mtime=100.0,
                file_size=FILE_SIZE, ctx=ctx,
            )

    assert first == second == b"downloaded"
    # One network fetch only: the second read came from the disk cache.
    assert manager.uc_client.download_chunk_stream.call_count == 1


@pytest.mark.trio
async def test_read_succeeds_with_a_ram_cache_too_small_for_a_chunk(manager, ctx):
    """The `manager` fixture's 1 MB RAM cache holds no 8 MB chunk at all. The
    reader is handed the downloaded bytes directly, so it must not depend on
    the RAM cache — nor on the disk write, which has not landed yet."""
    assert (await manager._ram_cache.stats())[1] == 0

    async with running(manager):
        with trio.fail_after(5):
            result = await manager.read(
                "/c/s/v/f", offset=0, length=FILE_SIZE, mtime=100.0,
                file_size=FILE_SIZE, ctx=ctx,
            )

    assert result == b"downloaded"


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
    the new generation — so a stale RAM/disk chunk from before the write (same
    fs_path, chunk and mtime, but old gen) is bypassed. This is what stops a
    same-second overwrite (mtime has 1s resolution) from serving stale bytes."""
    fs = "/cat/sch/vol/f.txt"
    mtime = 1000.0

    await manager._get_chunk_from_cache_or_disk(fs, 0, mtime, manager._generation(fs))
    manager.persistence.retrieve_chunk.assert_awaited_with(fs, 0, mtime, 0)

    await manager.invalidate_path(fs)

    await manager._get_chunk_from_cache_or_disk(fs, 0, mtime, manager._generation(fs))
    manager.persistence.retrieve_chunk.assert_awaited_with(fs, 0, mtime, 1)


@pytest.mark.trio
async def test_shutdown_with_a_pending_disk_write_is_clean(ctx):
    """Story 5: shutting down while a chunk is still queued for the disk cache
    must not raise, and must not strand the writers. A clean close drains what
    is already queued; anything still unwritten costs a future disk-cache miss
    and nothing else, since the bytes reached their reader long ago."""
    release_write = trio.Event()
    writes_started = 0

    async def hanging_store_chunk(**kwargs):
        nonlocal writes_started
        writes_started += 1
        await release_write.wait()

    # One download worker and one writer: the writer is busy on the read's own
    # chunk, so the prefetched chunk is still queued behind it.
    dm = _make_manager(num_workers=1)
    dm.persistence.store_chunk = AsyncMock(side_effect=hanging_store_chunk)

    async with trio.open_nursery() as nursery:
        dm.run_services(nursery)
        with trio.fail_after(5):
            await dm.read(
                "/c/s/v/f", offset=0, length=FILE_SIZE, mtime=100.0,
                file_size=dm.chunk_size + FILE_SIZE, ctx=ctx,
            )
            while writes_started < 1:
                await trio.sleep(0)
            await trio.testing.wait_all_tasks_blocked()
            dm.close()
            release_write.set()

    # The in-progress write and the queued one both completed; the nursery
    # exited on its own rather than being cancelled or deadlocked.
    assert dm.persistence.store_chunk.await_count == 2


@pytest.mark.trio
async def test_read_fails_with_eio_when_the_download_fails(manager, ctx):
    """A chunk that never arrives is still an EIO, and nothing is cached."""

    async def failing_stream(*args, **kwargs):
        raise ConnectionError("Network Reset")
        yield b""  # pragma: no cover - makes this an async generator

    manager.uc_client.download_chunk_stream = MagicMock(side_effect=failing_stream)

    async with running(manager):
        with trio.fail_after(5):
            with pytest.raises(pyfuse3.FUSEError) as exc_info:
                await manager.read(
                    "/c/s/v/f", offset=0, length=FILE_SIZE, mtime=100.0,
                    file_size=FILE_SIZE, ctx=ctx,
                )

    assert exc_info.value.errno == errno.EIO
    manager.persistence.store_chunk.assert_not_awaited()


# ---------------------------------------------------------------------------
# Tests: no race between requests for a chunk whose disk write is still pending
#
# Returning the bytes early opens a window in which the chunk is in neither
# cache but a write for it is in flight. The coalescer key stays reserved
# across that window, so a request arriving in it is served the bytes that were
# already downloaded instead of leading a second download -- which would both
# waste the transfer and race the pending write for the same cache file.
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_request_during_the_write_window_does_not_redownload(ctx):
    """A second read of the same chunk while its write is still pending must
    not hit the network again, nor queue a second write of the same file."""
    release_write = trio.Event()
    writes = []

    async def hanging_store_chunk(**kwargs):
        writes.append(kwargs)
        await release_write.wait()

    dm = _make_manager(num_workers=2)
    dm.persistence.store_chunk = AsyncMock(side_effect=hanging_store_chunk)
    # Nothing is on disk for the whole window: the chunk is still being written.
    dm.persistence.retrieve_chunk = AsyncMock(return_value=None)

    async with running(dm):
        with trio.fail_after(5):
            first = await dm.read(
                "/c/s/v/f", offset=0, length=FILE_SIZE, mtime=100.0,
                file_size=FILE_SIZE, ctx=ctx,
            )
            while not writes:
                await trio.sleep(0)
            # The write is in flight; ask for the very same chunk again.
            second = await dm.read(
                "/c/s/v/f", offset=0, length=FILE_SIZE, mtime=100.0,
                file_size=FILE_SIZE, ctx=ctx,
            )
        release_write.set()

    assert first == second == b"downloaded"
    assert dm.uc_client.download_chunk_stream.call_count == 1
    assert len(writes) == 1


@pytest.mark.trio
async def test_chunk_can_be_downloaded_again_once_its_write_finished(manager, ctx):
    """The reservation is only held for the duration of the write: afterwards a
    fresh request may lead a new download (here the chunk is gone from disk)."""
    async with running(manager):
        with trio.fail_after(5):
            for _ in range(2):
                assert await manager.read(
                    "/c/s/v/f", offset=0, length=FILE_SIZE, mtime=100.0,
                    file_size=FILE_SIZE, ctx=ctx,
                ) == b"downloaded"
                while not manager.persistence.store_chunk.await_count:
                    await trio.sleep(0)

    assert manager.uc_client.download_chunk_stream.call_count == 2


@pytest.mark.trio
async def test_failed_download_does_not_wedge_the_chunk(manager, ctx):
    """A leader that fails must free the key, or every later read of that chunk
    would join a dead reservation and fail forever."""
    calls = 0

    async def flaky_stream(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("Network Reset")
        yield b"downloaded"

    manager.uc_client.download_chunk_stream = MagicMock(side_effect=flaky_stream)

    async with running(manager):
        with trio.fail_after(5):
            with pytest.raises(pyfuse3.FUSEError):
                await manager.read(
                    "/c/s/v/f", offset=0, length=FILE_SIZE, mtime=100.0,
                    file_size=FILE_SIZE, ctx=ctx,
                )
            # The retry must be allowed to lead a fresh download.
            result = await manager.read(
                "/c/s/v/f", offset=0, length=FILE_SIZE, mtime=100.0,
                file_size=FILE_SIZE, ctx=ctx,
            )

    assert result == b"downloaded"
    assert calls == 2


@pytest.mark.trio
async def test_a_real_read_is_not_queued_behind_a_prefetch_disk_write(ctx):
    """Handing a chunk to the writers must never block the download worker. If
    it did, a worker parked waiting for a writer would stop serving the
    priority queue, and a read someone is blocked on would wait for some
    unrelated prefetch's disk write -- the latency this whole indirection
    exists to remove."""
    release_write = trio.Event()
    writes_started = 0

    async def hanging_store_chunk(**kwargs):
        nonlocal writes_started
        writes_started += 1
        await release_write.wait()

    # A single download worker and a single writer, so the writer saturates
    # immediately and the worker must not be the one that waits for it.
    dm = _make_manager(num_workers=1)
    dm.persistence.store_chunk = AsyncMock(side_effect=hanging_store_chunk)
    file_size = 2 * dm.chunk_size + FILE_SIZE  # 3 chunks

    async with running(dm):
        # Read chunk 0: its write occupies the writer, and the prefetch of
        # chunk 1 then fills the queue behind it.
        await dm.read("/c/s/v/f", offset=0, length=FILE_SIZE, mtime=100.0,
                      file_size=file_size, ctx=ctx)
        while not writes_started:
            await trio.sleep(0)
        await trio.testing.wait_all_tasks_blocked()

        with trio.fail_after(5):
            # A real read, of an unrelated chunk, with the disk fully backed up.
            result = await dm.read(
                "/c/s/v/f", offset=2 * dm.chunk_size, length=FILE_SIZE,
                mtime=100.0, file_size=file_size, ctx=ctx,
            )
        release_write.set()

    assert result == b"downloaded"
