"""
Tests for fuse4dbricks.fs.metadata_manager.MetadataManager.

Covers:
  - uc_node_type_from_entry   (path-depth → UcNodeType)
  - get_ttl                   (file vs catalog TTL)
  - _uc_to_inode_entry_attr   (UnityCatalogEntry → InodeEntryAttr)
  - _get_valid_cache          (TTL expiry + LRU move)
  - check_access              (write denied; root granted; cached + API-backed)
  - lookup_child              (cache hit; API miss; not-found → None)
  - get_attributes            (root short-circuit; cache hit; API refresh)
  - list_directory            (cache hit; API call; ENOTDIR when uc_entries is None)
  - invalidate                (evicts attr and parent dir caches)
"""

import errno
import stat
import time
from collections import OrderedDict
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pyfuse3
import trio

from fuse4dbricks.api.uc_client import UcNodeType, UnityCatalogEntry
from fuse4dbricks.fs.inode_manager import InodeEntry, InodeEntryAttr
from fuse4dbricks.fs.metadata_manager import MetadataManager, uc_node_type_from_entry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_attr(is_dir: bool, size: int = 0) -> InodeEntryAttr:
    mode = (stat.S_IFDIR | 0o755) if is_dir else (stat.S_IFREG | 0o644)
    return InodeEntryAttr(
        st_mode=mode,
        st_nlink=2 if is_dir else 1,
        st_size=size,
        st_ctime=0.0,
        st_mtime=0.0,
        st_atime=0.0,
        st_uid=0,
        st_gid=0,
    )


def _make_entry(fs_path: str, is_dir: bool, inode: int = 10) -> InodeEntry:
    return InodeEntry(
        inode=inode,
        parent_inode=pyfuse3.ROOT_INODE,
        name=fs_path.split("/")[-1] or "/",
        fs_path=fs_path,
        attr=_make_attr(is_dir),
    )


def _uc_entry(name: str, uc_path: str, entry_type: UcNodeType, size: int = 0) -> UnityCatalogEntry:
    return UnityCatalogEntry(name=name, uc_path=uc_path, entry_type=entry_type, size=size, mtime=1000.0, ctime=900.0)


@pytest.fixture
def mock_uc_client():
    client = MagicMock()
    client.get_current_user_info = AsyncMock(return_value="user@example.com")
    client.check_permissions = AsyncMock(return_value=True)
    client.get_path_metadata = AsyncMock(return_value=None)
    client.get_path_contents = AsyncMock(return_value=None)
    return client


@pytest.fixture
def manager(mock_uc_client):
    return MetadataManager(uc_client=mock_uc_client, ttl=30.0, max_entries=1000, ttl_catalog=600.0)


@pytest.fixture
def ctx():
    # pyfuse3.RequestContext ignores constructor kwargs; use SimpleNamespace.
    return SimpleNamespace(uid=1000, pid=5678, gid=1000)


# ---------------------------------------------------------------------------
# uc_node_type_from_entry
# ---------------------------------------------------------------------------


def test_uc_node_type_root():
    entry = _make_entry("/", is_dir=True)
    assert uc_node_type_from_entry(entry) == UcNodeType.ROOT


def test_uc_node_type_catalog():
    entry = _make_entry("/my_catalog", is_dir=True)
    assert uc_node_type_from_entry(entry) == UcNodeType.CATALOG


def test_uc_node_type_schema():
    entry = _make_entry("/cat/sch", is_dir=True)
    assert uc_node_type_from_entry(entry) == UcNodeType.SCHEMA


def test_uc_node_type_volume():
    entry = _make_entry("/cat/sch/vol", is_dir=True)
    assert uc_node_type_from_entry(entry) == UcNodeType.VOLUME


def test_uc_node_type_directory_inside_volume():
    entry = _make_entry("/cat/sch/vol/folder", is_dir=True)
    assert uc_node_type_from_entry(entry) == UcNodeType.DIRECTORY


def test_uc_node_type_file_inside_volume():
    entry = _make_entry("/cat/sch/vol/file.txt", is_dir=False)
    assert uc_node_type_from_entry(entry) == UcNodeType.FILE


# ---------------------------------------------------------------------------
# get_ttl
# ---------------------------------------------------------------------------


def test_get_ttl_file(manager):
    assert manager.get_ttl(UcNodeType.FILE) == 30.0


def test_get_ttl_directory(manager):
    assert manager.get_ttl(UcNodeType.DIRECTORY) == 30.0


def test_get_ttl_volume(manager):
    assert manager.get_ttl(UcNodeType.VOLUME) == 600.0


def test_get_ttl_catalog(manager):
    assert manager.get_ttl(UcNodeType.CATALOG) == 600.0


def test_get_ttl_schema(manager):
    assert manager.get_ttl(UcNodeType.SCHEMA) == 600.0


def test_get_ttl_root(manager):
    assert manager.get_ttl(UcNodeType.ROOT) == 600.0


def test_get_ttl_none(manager):
    assert manager.get_ttl(None) == 600.0


# ---------------------------------------------------------------------------
# _uc_to_inode_entry_attr
# ---------------------------------------------------------------------------


def test_uc_to_inode_entry_attr_file(ctx):
    uc_entry = _uc_entry("file.txt", "/Volumes/c/s/v/file.txt", UcNodeType.FILE, size=1024)
    attr = MetadataManager._uc_to_inode_entry_attr(uc_entry, ctx)
    assert bool(attr.st_mode & stat.S_IFREG)
    assert attr.st_size == 1024
    assert attr.st_mtime == 1000.0
    assert attr.st_ctime == 900.0
    assert attr.st_uid == 1000
    assert attr.st_gid == 1000
    assert attr.st_nlink == 1


def test_uc_to_inode_entry_attr_directory(ctx):
    uc_entry = _uc_entry("folder", "/Volumes/c/s/v/folder", UcNodeType.DIRECTORY)
    attr = MetadataManager._uc_to_inode_entry_attr(uc_entry, ctx)
    assert bool(attr.st_mode & stat.S_IFDIR)
    assert attr.st_nlink == 2


def test_uc_to_inode_entry_attr_none_times(ctx):
    uc_entry = UnityCatalogEntry(
        name="x", uc_path="/Volumes/c/s/v/x", entry_type=UcNodeType.FILE, size=0, mtime=None, ctime=None
    )
    attr = MetadataManager._uc_to_inode_entry_attr(uc_entry, ctx)
    assert attr.st_mtime == 0
    assert attr.st_ctime == 0


# ---------------------------------------------------------------------------
# _get_valid_cache
# ---------------------------------------------------------------------------


def test_get_valid_cache_hit(manager):
    future = time.time() + 100
    cache: OrderedDict = OrderedDict()
    cache["key"] = (future, "value")
    result = manager._get_valid_cache(cache, "key")
    assert result == "value"


def test_get_valid_cache_expired(manager):
    past = time.time() - 1
    cache: OrderedDict = OrderedDict()
    cache["key"] = (past, "old_value")
    result = manager._get_valid_cache(cache, "key")
    assert result is None
    assert "key" not in cache  # expired entry removed


def test_get_valid_cache_missing(manager):
    cache: OrderedDict = OrderedDict()
    assert manager._get_valid_cache(cache, "missing") is None


# ---------------------------------------------------------------------------
# check_access
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_check_access_write_raises_eacces(manager, ctx):
    entry = _make_entry("/cat", is_dir=True)
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await manager.check_access(entry, mode=2, ctx=ctx)  # W_OK
    assert exc_info.value.errno == errno.EACCES


@pytest.mark.trio
async def test_check_access_root_always_granted(manager, ctx):
    entry = _make_entry("/", is_dir=True)
    result = await manager.check_access(entry, mode=4, ctx=ctx)
    assert result is True


@pytest.mark.trio
async def test_check_access_catalog_calls_api(manager, mock_uc_client, ctx):
    mock_uc_client.check_permissions.return_value = True
    entry = _make_entry("/my_catalog", is_dir=True)
    result = await manager.check_access(entry, mode=4, ctx=ctx)
    assert result is True
    mock_uc_client.get_current_user_info.assert_awaited_once()
    mock_uc_client.check_permissions.assert_awaited_once()


@pytest.mark.trio
async def test_check_access_permission_denied_by_api(manager, mock_uc_client, ctx):
    mock_uc_client.check_permissions.return_value = False
    entry = _make_entry("/my_catalog", is_dir=True)
    result = await manager.check_access(entry, mode=4, ctx=ctx)
    assert result is False


@pytest.mark.trio
async def test_check_access_result_cached(manager, mock_uc_client, ctx):
    """Second check_access for the same (uid, securable) must not call API again."""
    entry = _make_entry("/cat", is_dir=True)
    await manager.check_access(entry, mode=4, ctx=ctx)
    await manager.check_access(entry, mode=4, ctx=ctx)
    assert mock_uc_client.check_permissions.await_count == 1


@pytest.mark.trio
async def test_check_access_follower_miss_raises_eagain(manager, mock_uc_client, ctx):
    """A coalescing follower that wakes to no cached decision must surface a
    retryable EAGAIN, not a fabricated deny: the leader's request failed (it
    would have cached False for a real denial)."""
    entry = _make_entry("/my_catalog", is_dir=True)
    securable = "my_catalog"
    # Simulate a leader already in flight that finished without caching a
    # decision (i.e. it errored): pre-arm the coalescer with an already-set
    # event so our call takes the follower path and wakes immediately.
    done = trio.Event()
    done.set()
    manager._permissions_coalescer._inflight[(ctx.uid, securable)] = done

    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await manager.check_access(entry, mode=4, ctx=ctx)
    assert exc_info.value.errno == errno.EAGAIN
    # It must not have fabricated the answer via its own API call.
    mock_uc_client.check_permissions.assert_not_awaited()


@pytest.mark.trio
async def test_check_access_volume_uses_read_volume_privilege(manager, mock_uc_client, ctx):
    entry = _make_entry("/cat/sch/vol", is_dir=True)
    await manager.check_access(entry, mode=4, ctx=ctx)
    call_kwargs = mock_uc_client.check_permissions.call_args
    assert "READ_VOLUME" in call_kwargs.kwargs.get("privileges", call_kwargs.args[2] if len(call_kwargs.args) > 2 else [])


# ---------------------------------------------------------------------------
# lookup_child
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_lookup_child_api_returns_file(manager, mock_uc_client, ctx):
    uc_e = _uc_entry("file.txt", "/Volumes/c/s/v/file.txt", UcNodeType.FILE, size=512)
    mock_uc_client.get_path_metadata.return_value = uc_e
    parent = _make_entry("/cat/sch/vol", is_dir=True)
    attr = await manager.lookup_child(parent, "file.txt", ctx)
    assert attr is not None
    assert attr.st_size == 512
    assert bool(attr.st_mode & stat.S_IFREG)


@pytest.mark.trio
async def test_lookup_child_api_returns_none_for_missing(manager, mock_uc_client, ctx):
    mock_uc_client.get_path_metadata.return_value = None
    parent = _make_entry("/cat/sch/vol", is_dir=True)
    attr = await manager.lookup_child(parent, "ghost.txt", ctx)
    assert attr is None


@pytest.mark.trio
async def test_lookup_child_cache_hit_skips_api(manager, mock_uc_client, ctx):
    """After a successful lookup, a second call must return cached result without API call."""
    uc_e = _uc_entry("data.csv", "/Volumes/c/s/v/data.csv", UcNodeType.FILE, size=100)
    mock_uc_client.get_path_metadata.return_value = uc_e
    parent = _make_entry("/cat/sch/vol", is_dir=True)

    first = await manager.lookup_child(parent, "data.csv", ctx)
    second = await manager.lookup_child(parent, "data.csv", ctx)

    assert first is not None
    assert second is not None
    assert mock_uc_client.get_path_metadata.await_count == 1  # Only called once


@pytest.mark.trio
async def test_lookup_child_directory(manager, mock_uc_client, ctx):
    uc_e = _uc_entry("subdir", "/Volumes/c/s/v/subdir", UcNodeType.DIRECTORY)
    mock_uc_client.get_path_metadata.return_value = uc_e
    parent = _make_entry("/cat/sch/vol", is_dir=True)
    attr = await manager.lookup_child(parent, "subdir", ctx)
    assert attr is not None
    assert bool(attr.st_mode & stat.S_IFDIR)


# ---------------------------------------------------------------------------
# get_attributes
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_get_attributes_root_returns_entry_attr_without_api(manager, mock_uc_client, ctx):
    root = _make_entry("/", is_dir=True)
    attr = await manager.get_attributes(root, ctx)
    assert attr is root.attr  # Short-circuit: root node returns stored attr
    mock_uc_client.get_path_metadata.assert_not_awaited()


@pytest.mark.trio
async def test_get_attributes_cache_hit_skips_api(manager, mock_uc_client, ctx):
    uc_e = _uc_entry("vol", "/Volumes/cat/sch/vol", UcNodeType.VOLUME)
    mock_uc_client.get_path_metadata.return_value = uc_e
    entry = _make_entry("/cat/sch/vol", is_dir=True)

    await manager.get_attributes(entry, ctx)  # Populates cache
    await manager.get_attributes(entry, ctx)  # Should hit cache

    assert mock_uc_client.get_path_metadata.await_count == 1


@pytest.mark.trio
async def test_get_attributes_api_returns_none_when_deleted(manager, mock_uc_client, ctx):
    mock_uc_client.get_path_metadata.return_value = None
    entry = _make_entry("/cat/sch/vol/gone.txt", is_dir=False)
    attr = await manager.get_attributes(entry, ctx)
    assert attr is None


# ---------------------------------------------------------------------------
# list_directory
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_list_directory_returns_children(manager, mock_uc_client, ctx):
    entries = [
        _uc_entry("file_a.txt", "/Volumes/c/s/v/file_a.txt", UcNodeType.FILE, size=10),
        _uc_entry("subdir", "/Volumes/c/s/v/subdir", UcNodeType.DIRECTORY),
    ]
    mock_uc_client.get_path_contents.return_value = entries
    entry = _make_entry("/cat/sch/vol", is_dir=True)

    items = await manager.list_directory(entry, ctx)
    assert items is not None
    assert "file_a.txt" in items
    assert "subdir" in items


@pytest.mark.trio
async def test_list_directory_cache_hit_skips_api(manager, mock_uc_client, ctx):
    mock_uc_client.get_path_contents.return_value = []
    entry = _make_entry("/cat/sch/vol", is_dir=True)

    await manager.list_directory(entry, ctx)
    await manager.list_directory(entry, ctx)

    assert mock_uc_client.get_path_contents.await_count == 1


@pytest.mark.trio
async def test_list_directory_raises_enotdir_when_api_returns_none(manager, mock_uc_client, ctx):
    mock_uc_client.get_path_contents.return_value = None
    entry = _make_entry("/cat/sch/vol/file.txt", is_dir=False)

    result = await manager.list_directory(entry, ctx)
    # Implementation swallows ENOTDIR and returns None (see list_directory except block)
    assert result is None


# ---------------------------------------------------------------------------
# invalidate
# ---------------------------------------------------------------------------


def test_invalidate_removes_attr_cache(manager, ctx):
    future = time.time() + 100
    manager._attr_cache[("/cat/sch/vol/file.txt", False)] = (future, _make_attr(False))
    manager.invalidate("/cat/sch/vol/file.txt", is_dir=False)
    assert ("/cat/sch/vol/file.txt", False) not in manager._attr_cache


def test_invalidate_removes_parent_dir_cache(manager, ctx):
    future = time.time() + 100
    manager._dir_cache["/cat/sch/vol"] = (future, OrderedDict())
    manager.invalidate("/cat/sch/vol/file.txt", is_dir=False)
    assert "/cat/sch/vol" not in manager._dir_cache


def test_invalidate_nonexistent_path_does_not_raise(manager):
    manager.invalidate("/nonexistent/path/file.txt", is_dir=False)  # Must not raise


# ---------------------------------------------------------------------------
# reauthorize  (token change -> refresh principal, drop grants iff principal changed)
# ---------------------------------------------------------------------------


def _seed_permission(manager, uid: int, securable: str, granted: bool = True) -> None:
    manager._permissions_cache[(uid, securable)] = (time.time() + 100, granted)


@pytest.mark.trio
async def test_reauthorize_same_principal_keeps_permission_cache(manager, mock_uc_client, ctx):
    """A token that maps to the same principal must not invalidate cached grants."""
    manager._principal_cache[ctx.uid] = "user@example.com"
    mock_uc_client.get_current_user_info = AsyncMock(return_value="user@example.com")
    _seed_permission(manager, ctx.uid, "cat")
    _seed_permission(manager, ctx.uid, "cat.sch")

    await manager.reauthorize(ctx)

    assert (ctx.uid, "cat") in manager._permissions_cache
    assert (ctx.uid, "cat.sch") in manager._permissions_cache
    assert manager._principal_cache[ctx.uid] == "user@example.com"


@pytest.mark.trio
async def test_reauthorize_changed_principal_drops_only_that_uid(manager, mock_uc_client, ctx):
    """A token mapping to a different principal drops that uid's grants, not others'."""
    other_uid = ctx.uid + 1
    manager._principal_cache[ctx.uid] = "old@example.com"
    mock_uc_client.get_current_user_info = AsyncMock(return_value="new@example.com")
    _seed_permission(manager, ctx.uid, "cat")
    _seed_permission(manager, ctx.uid, "cat.sch")
    _seed_permission(manager, other_uid, "cat")

    await manager.reauthorize(ctx)

    assert (ctx.uid, "cat") not in manager._permissions_cache
    assert (ctx.uid, "cat.sch") not in manager._permissions_cache
    assert (other_uid, "cat") in manager._permissions_cache
    assert manager._principal_cache[ctx.uid] == "new@example.com"


@pytest.mark.trio
async def test_reauthorize_fetch_failure_drops_grants(manager, mock_uc_client, ctx):
    """If the new token's principal can't be resolved, no stale grant may survive."""
    manager._principal_cache[ctx.uid] = "old@example.com"
    mock_uc_client.get_current_user_info = AsyncMock(side_effect=RuntimeError("401"))
    _seed_permission(manager, ctx.uid, "cat")

    await manager.reauthorize(ctx)

    assert (ctx.uid, "cat") not in manager._permissions_cache
    # Unresolved principal must not stay cached.
    assert ctx.uid not in manager._principal_cache
