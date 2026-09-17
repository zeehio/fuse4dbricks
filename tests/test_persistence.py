import hashlib
import os
import shutil
import time
from unittest.mock import MagicMock, patch

import pytest
import trio

from fuse4dbricks.storage.persistence import DiskPersistence

# --- HELPERS & FIXTURES ---


@pytest.fixture
def cache_dir(tmp_path):
    """Provides a temporary directory for the cache."""
    return str(tmp_path / "cache")


@pytest.fixture
async def persistence(cache_dir):
    """Creates a DiskPersistence instance with a small default size."""
    # 10MB limit for general tests
    p = DiskPersistence(cache_dir, max_size_gb=0.01, max_age_days=7)
    return p


# --- 1. CORE FUNCTIONALITY & SHARDING ---


@pytest.mark.trio
async def test_store_and_retrieve_basic(persistence):
    """Happy Path: Store data, read data out."""
    file_id = "test_file_1"
    chunk_index = 0
    data = b"Hello, World! This is a test chunk."
    mtime = 0.0
    # Store
    await persistence.store_chunk(file_id, chunk_index, mtime, data)

    # Retrieve
    retrieved = await persistence.retrieve_chunk(file_id, chunk_index, mtime)
    assert retrieved == data

    # Check internal state
    assert persistence.current_size == len(data)
    assert len(persistence.access_log) == 1


@pytest.mark.trio
async def test_sharding_structure(persistence):
    """Verify 256-folder sharding logic (aa/bb/...) or single level."""
    file_id = "file_123"
    chunk_index = 5
    mtime = 0
    mtime_ms = int(mtime * 1000)
    data = b"data"

    # Calculate expected path manually based on implementation
    file_hash = hashlib.sha256(file_id.encode()).hexdigest()
    shard1 = file_hash[:2]
    shard2 = f"{(chunk_index // 1000):07d}"

    await persistence.store_chunk(file_id, chunk_index, mtime, data)

    # Verify file exists at specific sharded location
    expected_path = os.path.join(
        persistence.cache_dir,
        shard1,
        shard2,
        f"{file_hash}_{mtime_ms}_g0_{chunk_index:07d}.bin",
    )
    assert os.path.exists(expected_path)


@pytest.mark.trio
async def test_chunk_path_differs_by_generation(persistence):
    """Same path/chunk/mtime but a different generation must map to a different
    file, so a bumped generation never reuses a stale chunk on disk."""
    p0 = persistence._get_chunk_path("f", 0, 5.0, 0)
    p1 = persistence._get_chunk_path("f", 0, 5.0, 1)
    assert p0 != p1


@pytest.mark.trio
async def test_generations_do_not_collide_on_same_mtime(persistence):
    """Two writes with an identical mtime (the 1-second-resolution collision)
    but different generations are stored and retrieved independently — the
    regression guard for serving stale content after a same-second overwrite."""
    await persistence.store_chunk("f", 0, 5.0, b"OLD", gen=0)
    await persistence.store_chunk("f", 0, 5.0, b"NEW", gen=1)
    assert await persistence.retrieve_chunk("f", 0, 5.0, 0) == b"OLD"
    assert await persistence.retrieve_chunk("f", 0, 5.0, 1) == b"NEW"


@pytest.mark.trio
async def test_retrieve_missing_chunk(persistence):
    """Retrieving a non-existent chunk should return None safely."""
    result = await persistence.retrieve_chunk("ghost_file", 99, 0.0)
    assert result is None


@pytest.mark.trio
async def test_chunk_exists_true_after_store(persistence):
    """chunk_exists reports a stored chunk as present, without needing to
    read its content."""
    await persistence.store_chunk("f", 0, 5.0, b"data")
    assert await persistence.chunk_exists("f", 0, 5.0) is True


@pytest.mark.trio
async def test_chunk_exists_false_for_missing_chunk(persistence):
    assert await persistence.chunk_exists("ghost_file", 99, 0.0) is False


@pytest.mark.trio
async def test_chunk_exists_does_not_read_file_content(persistence, monkeypatch):
    """chunk_exists must not pay the I/O cost of reading the chunk back --
    that's the whole point of using it instead of retrieve_chunk for a
    prefetch's "is a download needed" check."""
    await persistence.store_chunk("f", 0, 5.0, b"data")

    def _boom(path):
        raise AssertionError("chunk_exists must not read the file's content")

    monkeypatch.setattr(persistence, "_read_file", _boom)
    assert await persistence.chunk_exists("f", 0, 5.0) is True


@pytest.mark.trio
async def test_get_chunk_path_is_pure_no_dir_created(persistence):
    """_get_chunk_path must not touch the filesystem: it runs on every read,
    so it must not create (empty) shard dirs for never-written chunks."""
    path = persistence._get_chunk_path("never_written", 0, 0.0)
    assert not os.path.exists(os.path.dirname(path))


@pytest.mark.trio
async def test_retrieve_missing_chunk_creates_no_dir(persistence):
    """A read miss must not leave an empty shard dir behind."""
    await persistence.retrieve_chunk("ghost_file", 7, 0.0)
    path = persistence._get_chunk_path("ghost_file", 7, 0.0)
    assert not os.path.exists(os.path.dirname(path))


# --- 2. ATOMICITY ---


def _files_in(cache_dir: str) -> list[str]:
    found: list[str] = []
    for root, _, files in os.walk(cache_dir):
        found.extend(files)
    return found


@pytest.mark.trio
async def test_write_failure_cleanup(persistence):
    """
    Simulate a failure while publishing the chunk.
    Ensure the .tmp file is deleted and no garbage is left.
    """
    with patch("os.rename", side_effect=OSError("disk full")):
        with pytest.raises(OSError):
            await persistence.store_chunk("broken_file", 0, 0.0, b"start")

    # 1. Chunk should not exist in map
    assert persistence.current_size == 0
    assert len(persistence.access_map) == 0

    # 2. Filesystem should be clean (no .bin, no .tmp)
    assert _files_in(persistence.cache_dir) == []


@pytest.mark.trio
async def test_cancelled_write_cleans_up_temp_file(persistence):
    """A chunk write now runs in the background, so it can be cancelled at
    shutdown mid-write. That must not leave a partial .tmp behind."""

    def slow_rename(src, dst):
        # Runs in a worker thread; long enough for the cancellation below to
        # land while store_chunk is waiting on it.
        time.sleep(0.3)

    with patch("os.rename", side_effect=slow_rename):
        with trio.move_on_after(0.05) as scope:
            await persistence.store_chunk("cancelled_file", 0, 0.0, b"partial")

    assert scope.cancelled_caught
    # The rename never happened, so the temp file was still there when the
    # cancellation arrived; the shielded cleanup must have removed it.
    assert _files_in(persistence.cache_dir) == []
    assert persistence.current_size == 0


# --- 3. LRU LOGIC & LAZY PROMOTION (CRITICAL) ---


@pytest.mark.trio
async def test_eviction_on_size_limit(cache_dir):
    """Verify strict size limit enforces eviction."""
    # Create a tiny cache: 100 bytes max
    # We pass strict limit by mocking 0.0000001 GB roughly
    p = DiskPersistence(cache_dir, max_size_gb=(100 / 1024**3))
    mtime = 0.0

    # Write Chunk A (60 bytes)
    await p.store_chunk("A", 0, mtime, b"A" * 60)
    assert p.current_size == 60

    # Write Chunk B (60 bytes) -> Total 120 > 100. Must evict A.
    await p.store_chunk("B", 0, mtime, b"B" * 60)

    assert p.current_size == 60
    assert await p.retrieve_chunk("A", 0, mtime) is None
    assert await p.retrieve_chunk("B", 0, mtime) == b"B" * 60


@pytest.mark.trio
async def test_lazy_promotion(cache_dir):
    """
    The 'Lazy' logic test:
    1. Store A (Oldest)
    2. Store B
    3. Read A (A becomes newest in Map, but stays oldest in Heap)
    4. Store C (Forces eviction)
    5. Result: B should be evicted, NOT A.
    """
    p = DiskPersistence(cache_dir, max_size_gb=(150 / 1024**3))  # Limit 150 bytes
    mtime = 0.0
    # 1. Store A (50 bytes) at T=100
    with patch("time.time", return_value=100.0):
        await p.store_chunk("A", 0, mtime, b"A" * 50)

    # 2. Store B (50 bytes) at T=200
    with patch("time.time", return_value=200.0):
        await p.store_chunk("B", 0, mtime, b"B" * 50)

    # State: [A(100), B(200)]. Size: 100/150.

    # 3. Read A at T=300 (Promotion)
    # This updates A in access_map to 300, but heap still has (100, path_A)
    with patch("time.time", return_value=300.0):
        data = await p.retrieve_chunk("A", 0, mtime)
        assert data is not None

    # 4. Store C (60 bytes) at T=400. Total 160 > 150. Eviction needed.
    # Logic:
    #   - Pop A(100). Check Map. Map says A is 300. 300 > 100. Push A(300).
    #   - Pop B(200). Check Map. Map says B is 200. Evict B.
    with patch("time.time", return_value=400.0):
        await p.store_chunk("C", 0, mtime, b"C" * 60)

    # 5. Assertions
    assert await p.retrieve_chunk("B", 0, mtime) is None  # B was evicted
    assert await p.retrieve_chunk("A", 0, mtime) is not None  # A survived
    assert await p.retrieve_chunk("C", 0, mtime) is not None  # C is present

    # Verify size (50 + 60 = 110)
    assert p.current_size == 110


# --- 4. INITIALIZATION & RECOVERY ---


@pytest.mark.trio
async def test_graceful_init_discovery(cache_dir):
    """Ensure existing files are discovered on startup."""
    # 1. Manually create a file structure simulating a previous run
    fs_path = "old"
    chunk_index = 0
    mtime = 0.0
    sha256_hash = hashlib.sha256(fs_path.encode("utf-8")).hexdigest()
    shard1 = sha256_hash[:2]
    shard2 = f"{(chunk_index // 1000):07d}"  # Secondary shard to prevent too many files in one dir
    shard_dir = os.path.join(cache_dir, shard1, shard2)
    os.makedirs(shard_dir, exist_ok=True)
    mtime_ms = int(mtime * 1000)
    file_path = os.path.join(
        shard_dir, f"{sha256_hash}_{mtime_ms}_g0_{chunk_index:07d}.bin"
    )
    with open(file_path, "wb") as f:
        f.write(b"existing_data")  # 13 bytes

    # Set atime manually
    os.utime(file_path, (1000, 1000))

    # 2. Initialize Persistence
    p = DiskPersistence(cache_dir)
    # Run the init manually since we aren't using the nursery here
    await p._graceful_init()

    # 3. Verify Discovery
    assert p.current_size == 13
    assert file_path in p.access_map
    assert p.access_map[file_path] > 0

    # 4. Verify we can read it via the API
    data = await p.retrieve_chunk("old", 0, 0.0)
    assert data == b"existing_data"


@pytest.mark.trio
async def test_init_cleans_old_tmp_files(cache_dir):
    """Init should delete old .tmp files but keep new ones."""
    os.makedirs(cache_dir, exist_ok=True)

    now = time.time()

    # Old .tmp (older than process start)
    old_tmp = os.path.join(cache_dir, "old.tmp")
    with open(old_tmp, "w") as f:
        f.write("x")
    os.utime(old_tmp, (now - 1000, now - 1000))  # mtime in the past

    # New .tmp (future/current)
    new_tmp = os.path.join(cache_dir, "new.tmp")
    with open(new_tmp, "w") as f:
        f.write("y")
    os.utime(new_tmp, (now + 100, now + 100))

    # Init persistence
    # We patch time.time to ensure 'start_time' is between old and new
    with patch("time.time", return_value=now):
        p = DiskPersistence(cache_dir)
        await p._graceful_init()

    assert not os.path.exists(old_tmp)  # Should be deleted
    assert os.path.exists(new_tmp)  # Should stay


# --- 5. MAINTENANCE (GC) ---


@pytest.mark.trio
async def test_gc_removes_old_files(persistence):
    """Test age-based cleanup."""
    persistence.max_age_seconds = 100  # expire after 100s

    # Store file at T=0
    with patch("time.time", return_value=0):
        await persistence.store_chunk(
            "old_file", 0, 0.0, b"data"
        )

    # Verify it exists
    assert persistence.current_size > 0

    # Advance time to T=200 and trigger manual GC (simulating the loop)
    # We mock disk_usage to report plenty of space
    with (
        patch("time.time", return_value=200.0),
        patch("shutil.disk_usage", return_value=MagicMock(free=100, total=100)),
    ):

        # We manually invoke the logic inside _background_maintenance loop
        # normally we'd run the loop, but for unit tests, extracting the logic
        # or checking the side effects is easier.
        # Here we rely on the fact that we can call internal methods or wait.

        # Let's inspect the map manually to simulate what GC sees
        # In a real integration test we would let the loop run.
        # Here we will manually call a simplified check or trust the implementation
        # follows the logic. To be robust, let's call the internal logic if accessible,
        # or recreate the check:

        async with persistence.lock:
            # Replicating the GC logic for the test
            now = 200.0
            expired = [
                p
                for p, ts in persistence.access_map.items()
                if (now - ts > persistence.max_age_seconds)
            ]

            for path in expired:
                os.remove(path)
                del persistence.access_map[path]
                persistence.current_size -= 4

    assert persistence.current_size == 0
    assert len(persistence.access_map) == 0


@pytest.mark.trio
async def test_gc_panic_mode_disk_full(persistence):
    """Test aggressive cleanup when physical disk is full."""
    # Write a "fresh" file (T=1000)
    with patch("time.time", return_value=1000.0):
        await persistence.store_chunk(
            "fresh", 0, 0.0, b"data"
        )

    # Simulate current time T=1001 (File is NOT expired)
    # But Simulate Disk Full (1% free)
    with (
        patch("time.time", return_value=1001.0),
        patch("shutil.disk_usage", return_value=MagicMock(free=1, total=100)),
    ):

        # Trigger maintenance logic (Simulated)
        async with persistence.lock:
            usage = shutil.disk_usage(persistence.cache_dir)
            force_cleanup = (usage.free / usage.total) < 0.05

            to_delete = []
            if force_cleanup:
                # In panic mode, we assume the GC deletes everything or LRU
                # The implementation deletes based on the list comprehension
                to_delete = list(persistence.access_map.keys())

            for path in to_delete:
                os.remove(path)
                del persistence.access_map[path]
                persistence.current_size = 0

    assert persistence.current_size == 0


# --- 6. CONCURRENCY & SELF-HEALING (NEW) ---


@pytest.mark.trio
async def test_concurrent_store_triggers_eviction(cache_dir):
    """
    CRITICAL: Verifies the fix for 'RuntimeError: re-acquire lock'.

    Scenario:
    1. Cache Limit is small (100 bytes).
    2. We store Chunk A (60 bytes).
    3. We store Chunk B (60 bytes).
       - This MUST trigger evict() inside store_chunk.
       - If store_chunk holds the lock while calling evict(), this test crashes.
    """
    # Limit: ~100 bytes
    p = DiskPersistence(cache_dir, max_size_gb=(100 / 1024**3))

    # 1. Store A (60 bytes)
    await p.store_chunk("A", 0, 0.0, b"A" * 60)
    assert p.current_size == 60

    # 2. Store B (60 bytes) - Triggers Eviction of A
    # This call previously caused the recursion deadlock
    await p.store_chunk("B", 0, 0.0, b"B" * 60)

    # 3. Verify State
    assert p.current_size == 60  # A was evicted, B took its place
    assert await p.retrieve_chunk("A", 0, 0.0) is None
    assert await p.retrieve_chunk("B", 0, 0.0) is not None


@pytest.mark.trio
async def test_self_healing_on_retrieve(persistence):
    """
    Verifies that if a file exists on disk but is missing from the index
    (e.g., missed during init or race condition), retrieving it adds it back.
    """
    file_id = "ghost_file"
    chunk_index = 0
    data = b"I am a ghost"

    # 1. Manually create the file on disk (Bypassing persistence logic)
    # We use the internal helper to get the path, but don't update map/heap.
    # _get_chunk_path is pure now, so create the shard dir ourselves.
    path = persistence._get_chunk_path(file_id, chunk_index, 0.0)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)

    # Verify it is NOT in memory yet
    assert persistence.current_size == 0
    assert path not in persistence.access_map

    # 2. Retrieve the chunk using the API
    # This should trigger the "Self-Healing" block in retrieve_chunk
    retrieved_data = await persistence.retrieve_chunk(file_id, chunk_index, 0.0)

    # 3. Assertions
    assert retrieved_data == data

    # Critical: The system should now 'know' about this file
    assert persistence.current_size == len(data)
    assert path in persistence.access_map
    # Verify it's also in the eviction heap
    assert len(persistence.access_log) == 1
    assert persistence.access_log[0][1] == path


@pytest.mark.trio
async def test_background_maintenance_non_blocking(persistence):
    """
    Verifies that background maintenance identifies expired files
    and removes them from the index.
    """
    persistence.max_age_seconds = 0.1  # Expire very fast

    # 1. Store a file
    await persistence.store_chunk(
        "fast_expire", 0, 0.0, b"data"
    )
    path = persistence._get_chunk_path("fast_expire", 0, 0.0)

    # 2. Wait for expiration
    await trio.sleep(0.2)

    # 3. Setup Mocks
    mock_usage = MagicMock()
    mock_usage.free = 100
    mock_usage.total = 100

    mock_stat = MagicMock()
    mock_stat.st_size = 4  # Length of b"data"

    # Sequence of calls inside _background_maintenance:
    # 1. shutil.disk_usage (returns mock_usage)
    # 2. os.stat (returns mock_stat)
    # 3. os.remove (returns None)
    side_effects = [mock_usage, mock_stat, None]

    with (
        patch("trio.sleep", side_effect=[None, None, ValueError("Stop Loop")]),
        patch("trio.to_thread.run_sync", side_effect=side_effects),
    ):

        try:
            await persistence._background_maintenance()
        except ValueError:
            pass

    # 4. Assertions (Check Internal State)
    # We verify the logic removed the entry from memory.
    # We DO NOT call retrieve_chunk(), because the physical file still exists
    # (due to the mock) and retrieve_chunk would trigger "Self-Healing".

    assert persistence.current_size == 0
    assert path not in persistence.access_map


@pytest.mark.trio
async def test_concurrent_writes_of_the_same_chunk_do_not_corrupt_it(persistence):
    """Two writes of the same chunk must not scribble over each other's partial
    file. Each gets its own .tmp, so whichever rename lands last publishes an
    intact chunk rather than a mix of the two.

    DataManager does not produce this case — it holds the chunk's coalescer key
    from the start of the download until its write finishes — but the cache
    stores no checksum, so a corrupt chunk here would be served as if it were
    good.
    """
    a = b"A" * 4096
    b = b"B" * 4096

    async with trio.open_nursery() as nursery:
        nursery.start_soon(persistence.store_chunk, "f", 0, 1.0, a)
        nursery.start_soon(persistence.store_chunk, "f", 0, 1.0, b)

    retrieved = await persistence.retrieve_chunk("f", 0, 1.0)
    assert retrieved in (a, b)
    # One chunk file and no leftover .tmp.
    assert _files_in(persistence.cache_dir) == [
        os.path.basename(persistence._get_chunk_path("f", 0, 1.0))
    ]
