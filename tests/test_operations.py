"""
Tests for fuse4dbricks.fs.operations.UnityCatalogFS.

Covers:
  - _dispatch              (path → "auth" | "unity_catalog")
  - _raise_fuse_error      (UcError subtype → correct errno)
  - _entry_to_fuse_attr    (InodeEntry → pyfuse3.EntryAttributes)
  - getattr                (ENOENT when inode missing; delegates to managers)
  - open                   (permission flags; EISDIR for directories)
  - release                (state cleanup; delegates auth release)
  - read                   (routes to auth or data manager)
  - write                  (auth files writable; non-auth → EIO via bug, noted inline)
  - forget                 (delegates to inode_manager)
"""

import errno
import stat
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pyfuse3

from fuse4dbricks.api.errors import (
    UcBadRequest,
    UcConflict,
    UcError,
    UcNotFound,
    UcPermissionDenied,
    UcRateLimited,
    UcUnavailable,
)
from fuse4dbricks.fs.inode_manager import InodeEntry, InodeEntryAttr, InodeManager
from fuse4dbricks.fs.operations import UnityCatalogFS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_attr(is_dir: bool, size: int = 512) -> InodeEntryAttr:
    mode = (stat.S_IFDIR | 0o755) if is_dir else (stat.S_IFREG | 0o644)
    return InodeEntryAttr(
        st_mode=mode,
        st_nlink=2 if is_dir else 1,
        st_size=size,
        st_ctime=1000.0,
        st_mtime=2000.0,
        st_atime=3000.0,
        st_uid=1000,
        st_gid=1000,
    )


def _make_entry(inode: int, parent: int, name: str, fs_path: str, is_dir: bool, size: int = 512) -> InodeEntry:
    return InodeEntry(
        inode=inode,
        parent_inode=parent,
        name=name,
        fs_path=fs_path,
        attr=_make_attr(is_dir, size=size),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def ctx():
    return SimpleNamespace(uid=1000, pid=5678, gid=1000)


@pytest.fixture
def inode_manager():
    return InodeManager()


@pytest.fixture
def metadata_manager():
    mgr = MagicMock()
    mgr.check_access = AsyncMock(return_value=True)
    mgr.get_attributes = AsyncMock(return_value=_make_attr(False))
    mgr.lookup_child = AsyncMock(return_value=_make_attr(False))
    mgr.list_directory = AsyncMock(return_value={})
    return mgr


@pytest.fixture
def data_manager():
    mgr = MagicMock()
    mgr.read = AsyncMock(return_value=b"hello")
    return mgr


@pytest.fixture
def auth_manager():
    mgr = MagicMock()
    mgr.check_access = AsyncMock(return_value=True)
    mgr.get_attributes = AsyncMock(return_value=_make_attr(False))
    mgr.lookup_child = AsyncMock(return_value=_make_attr(False))
    mgr.list_directory = AsyncMock(return_value={})
    mgr.read = AsyncMock(return_value=b"README")
    mgr.write = AsyncMock(return_value=5)
    mgr.release = AsyncMock()
    return mgr


@pytest.fixture
def fs(inode_manager, metadata_manager, data_manager, auth_manager):
    return UnityCatalogFS(
        inode_manager=inode_manager,
        metadata_manager=metadata_manager,
        data_manager=data_manager,
        auth_manager=auth_manager,
    )


# ---------------------------------------------------------------------------
# _dispatch
# ---------------------------------------------------------------------------


def test_dispatch_auth_dir(fs):
    assert fs._dispatch("/.auth") == "auth"


def test_dispatch_auth_subpath(fs):
    assert fs._dispatch("/.auth/personal_access_token") == "auth"


def test_dispatch_readme(fs):
    assert fs._dispatch("/README.txt") == "auth"


def test_dispatch_catalog(fs):
    assert fs._dispatch("/my_catalog") == "unity_catalog"


def test_dispatch_deep_path(fs):
    assert fs._dispatch("/cat/sch/vol/folder/file.txt") == "unity_catalog"


def test_dispatch_root(fs):
    assert fs._dispatch("/") == "unity_catalog"


# ---------------------------------------------------------------------------
# _raise_fuse_error
# ---------------------------------------------------------------------------


def test_raise_fuse_error_permission_denied(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(UcPermissionDenied("denied"))
    assert exc_info.value.errno == errno.EACCES


def test_raise_fuse_error_not_found(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(UcNotFound("not found"))
    assert exc_info.value.errno == errno.ENOENT


def test_raise_fuse_error_bad_request(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(UcBadRequest("bad"))
    assert exc_info.value.errno == errno.EINVAL


def test_raise_fuse_error_conflict(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(UcConflict("conflict"))
    assert exc_info.value.errno == errno.EEXIST


def test_raise_fuse_error_rate_limited(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(UcRateLimited("rate limited"))
    assert exc_info.value.errno == errno.EAGAIN


def test_raise_fuse_error_unavailable(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(UcUnavailable("unavailable"))
    assert exc_info.value.errno == errno.EAGAIN


def test_raise_fuse_error_generic_uc_error(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(UcError("generic", status_code=500))
    assert exc_info.value.errno == errno.EIO


def test_raise_fuse_error_unknown_exception(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(RuntimeError("unexpected"))
    assert exc_info.value.errno == errno.EIO


# ---------------------------------------------------------------------------
# _entry_to_fuse_attr
# ---------------------------------------------------------------------------


def test_entry_to_fuse_attr_file(fs):
    entry = _make_entry(10, pyfuse3.ROOT_INODE, "file.txt", "/cat/sch/vol/file.txt", is_dir=False, size=1024)
    attr = fs._entry_to_fuse_attr(entry)
    assert attr.st_ino == 10
    assert attr.st_size == 1024
    assert attr.st_atime_ns == int(3000.0 * 1e9)
    assert attr.st_mtime_ns == int(2000.0 * 1e9)
    assert attr.st_ctime_ns == int(1000.0 * 1e9)
    assert attr.st_uid == 1000
    assert attr.st_gid == 1000
    assert attr.st_blksize == 4096
    assert attr.st_blocks == (1024 + 511) // 512


def test_entry_to_fuse_attr_directory(fs):
    entry = _make_entry(11, pyfuse3.ROOT_INODE, "subdir", "/cat/sch/vol/subdir", is_dir=True)
    attr = fs._entry_to_fuse_attr(entry)
    assert bool(attr.st_mode & stat.S_IFDIR)


# ---------------------------------------------------------------------------
# getattr
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_getattr_missing_inode_raises_enoent(fs, ctx):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.getattr(9999, ctx)
    assert exc_info.value.errno == errno.ENOENT


@pytest.mark.trio
async def test_getattr_unity_catalog_file(fs, inode_manager, metadata_manager, ctx):
    # Create an inode under a realistic UC path
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    f = inode_manager.add_entry(vol.inode, "data.txt", attr=_make_attr(False, size=42))

    metadata_manager.get_attributes.return_value = _make_attr(False, size=42)

    attr = await fs.getattr(f.inode, ctx)
    assert attr.st_size == 42


@pytest.mark.trio
async def test_getattr_auth_file_delegates_to_auth_manager(fs, inode_manager, auth_manager, ctx):
    readme = inode_manager.add_entry(pyfuse3.ROOT_INODE, "README.txt", attr=_make_attr(False))
    auth_manager.get_attributes.return_value = _make_attr(False, size=200)

    attr = await fs.getattr(readme.inode, ctx)
    auth_manager.get_attributes.assert_awaited_once()
    assert attr is not None


@pytest.mark.trio
async def test_getattr_raises_enoent_when_manager_returns_none(fs, inode_manager, metadata_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    metadata_manager.get_attributes.return_value = None

    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.getattr(cat.inode, ctx)
    assert exc_info.value.errno in (errno.ENOENT, errno.EIO)


# ---------------------------------------------------------------------------
# open / release
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_open_read_only_creates_file_handle(fs, inode_manager, metadata_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    f = inode_manager.add_entry(vol.inode, "data.txt", attr=_make_attr(False))

    O_RDONLY = 0
    file_info = await fs.open(f.inode, O_RDONLY, ctx)
    assert file_info is not None
    fh = int(file_info.fh)
    assert fh in fs._open_state


@pytest.mark.trio
async def test_open_directory_raises_eisdir(fs, inode_manager, metadata_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))

    O_RDONLY = 0
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.open(cat.inode, O_RDONLY, ctx)
    assert exc_info.value.errno == errno.EISDIR


@pytest.mark.trio
async def test_open_missing_inode_raises_enoent(fs, ctx, metadata_manager):
    metadata_manager.check_access.return_value = False
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.open(9999, 0, ctx)
    assert exc_info.value.errno in (errno.EACCES, errno.ENOENT)


@pytest.mark.trio
async def test_release_removes_file_handle_state(fs, inode_manager, metadata_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    f = inode_manager.add_entry(vol.inode, "f.txt", attr=_make_attr(False))

    file_info = await fs.open(f.inode, 0, ctx)
    fh = int(file_info.fh)
    assert fh in fs._open_state

    await fs.release(pyfuse3.FileHandleT(fh))
    assert fh not in fs._open_state


@pytest.mark.trio
async def test_release_unknown_fh_is_noop(fs):
    await fs.release(pyfuse3.FileHandleT(9999))  # Must not raise


@pytest.mark.trio
async def test_release_auth_file_calls_auth_manager_release(fs, inode_manager, auth_manager, ctx):
    readme = inode_manager.add_entry(pyfuse3.ROOT_INODE, "README.txt", attr=_make_attr(False))
    file_info = await fs.open(readme.inode, 0, ctx)
    fh = int(file_info.fh)
    await fs.release(pyfuse3.FileHandleT(fh))
    auth_manager.release.assert_awaited_once()


# ---------------------------------------------------------------------------
# read
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_read_uc_file_delegates_to_data_manager(fs, inode_manager, data_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    f = inode_manager.add_entry(vol.inode, "data.txt", attr=_make_attr(False, size=100))

    file_info = await fs.open(f.inode, 0, ctx)
    fh = int(file_info.fh)

    data_manager.read.return_value = b"hello world"
    result = await fs.read(fh, offset=0, length=100)
    assert result == b"hello world"
    data_manager.read.assert_awaited_once()


@pytest.mark.trio
async def test_read_auth_file_delegates_to_auth_manager(fs, inode_manager, auth_manager, ctx):
    readme = inode_manager.add_entry(pyfuse3.ROOT_INODE, "README.txt", attr=_make_attr(False, size=500))
    file_info = await fs.open(readme.inode, 0, ctx)
    fh = int(file_info.fh)

    auth_manager.read.return_value = b"README content"
    result = await fs.read(fh, offset=0, length=500)
    assert result == b"README content"
    auth_manager.read.assert_awaited_once()


@pytest.mark.trio
async def test_read_unknown_fh_raises_eio(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.read(9999, offset=0, length=100)
    assert exc_info.value.errno == errno.EIO


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_write_auth_file_delegates_to_auth_manager(fs, inode_manager, auth_manager, ctx):
    readme = inode_manager.add_entry(pyfuse3.ROOT_INODE, "README.txt", attr=_make_attr(False))
    file_info = await fs.open(readme.inode, 0, ctx)
    fh = int(file_info.fh)

    auth_manager.write.return_value = 5
    result = await fs.write(fh, offset=0, buffer=b"hello")
    assert result == 5
    auth_manager.write.assert_awaited_once()


@pytest.mark.trio
async def test_write_uc_file_raises_eio(fs, inode_manager, metadata_manager, ctx):
    """
    Writing to a non-auth file raises pyfuse3.FUSEError(EACCES) inside the try block,
    which is caught by `except Exception` and re-raised as EIO.
    This is a known bug in operations.py; this test documents the current behaviour.
    """
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    f = inode_manager.add_entry(vol.inode, "data.txt", attr=_make_attr(False))

    file_info = await fs.open(f.inode, 0, ctx)
    fh = int(file_info.fh)

    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.write(fh, offset=0, buffer=b"data")
    # EACCES is swallowed by generic except→EIO; document actual behaviour:
    assert exc_info.value.errno == errno.EIO


@pytest.mark.trio
async def test_write_unknown_fh_raises_eio(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.write(9999, offset=0, buffer=b"x")
    assert exc_info.value.errno == errno.EIO


# ---------------------------------------------------------------------------
# forget
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_forget_decrements_ref_count(fs, inode_manager):
    entry = inode_manager.add_entry(pyfuse3.ROOT_INODE, "tmp", attr=_make_attr(False))
    inode_manager.increment_lookup_count(entry.inode)  # ref = 1
    assert inode_manager.get_entry(entry.inode) is not None

    await fs.forget([(entry.inode, 1)])

    # ref = 0 → inode should be cleaned up
    assert inode_manager.get_entry(entry.inode) is None
