"""
Tests for fuse4dbricks.fs.auth_manager.AuthManager.

Covers:
  - check_access   (permission bit matching)
  - lookup_child   (virtual file discovery)
  - list_directory (directory listing)
  - read           (README content, access-token read denied)
  - write          (token buffering, append, non-auth file rejected)
  - release        (commits buffered token to auth_provider)
"""

import errno
import stat
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pyfuse3

from fuse4dbricks.fs.auth_manager import AuthManager
from fuse4dbricks.fs.inode_manager import InodeEntry, InodeEntryAttr


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _make_attr(mode: int, size: int = 0) -> InodeEntryAttr:
    return InodeEntryAttr(
        st_mode=mode,
        st_nlink=2 if (mode & stat.S_IFDIR) else 1,
        st_size=size,
        st_ctime=0,
        st_mtime=0,
        st_atime=0,
        st_uid=0,
        st_gid=0,
    )


def _make_entry(inode: int, parent: int, name: str, fs_path: str, is_dir: bool) -> InodeEntry:
    mode = (stat.S_IFDIR | 0o755) if is_dir else (stat.S_IFREG | 0o644)
    return InodeEntry(inode=inode, parent_inode=parent, name=name, fs_path=fs_path, attr=_make_attr(mode))


@pytest.fixture
def auth_provider():
    return MagicMock()


@pytest.fixture
def manager(auth_provider):
    return AuthManager(
        uc_client=MagicMock(),
        auth_provider=auth_provider,
        metadata_manager=AsyncMock(),
        workspace="https://workspace.example.com",
    )


@pytest.fixture
def ctx():
    # pyfuse3.RequestContext ignores constructor kwargs; use SimpleNamespace
    # so tests can control uid/gid/pid freely.
    return SimpleNamespace(uid=1000, pid=5678, gid=1000)


@pytest.fixture
def root_entry():
    return _make_entry(pyfuse3.ROOT_INODE, pyfuse3.ROOT_INODE, "/", "/", is_dir=True)


@pytest.fixture
def auth_dir_entry():
    return _make_entry(2, pyfuse3.ROOT_INODE, ".auth", "/.auth", is_dir=True)


@pytest.fixture
def token_entry():
    return _make_entry(3, 2, "personal_access_token", "/.auth/personal_access_token", is_dir=False)


@pytest.fixture
def readme_root_entry():
    return _make_entry(4, pyfuse3.ROOT_INODE, "README.txt", "/README.txt", is_dir=False)


# ---------------------------------------------------------------------------
# check_access
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_check_access_auth_dir_r_ok(manager, auth_dir_entry, ctx):
    # /.auth has mode 0o555 (r-xr-xr-x) → R_OK must be granted
    assert await manager.check_access(auth_dir_entry, mode=4, ctx=ctx) is True


@pytest.mark.trio
async def test_check_access_token_file_r_ok_denied(manager, token_entry, ctx):
    # personal_access_token has mode 0o222 (write-only) → R_OK must be denied
    assert await manager.check_access(token_entry, mode=4, ctx=ctx) is False


@pytest.mark.trio
async def test_check_access_token_file_w_ok(manager, token_entry, ctx):
    # personal_access_token has mode 0o222 → W_OK must be granted
    assert await manager.check_access(token_entry, mode=2, ctx=ctx) is True


@pytest.mark.trio
async def test_check_access_readme_r_ok(manager, readme_root_entry, ctx):
    # /README.txt has mode 0o444 (read-only) → R_OK must be granted
    assert await manager.check_access(readme_root_entry, mode=4, ctx=ctx) is True


@pytest.mark.trio
async def test_check_access_readme_w_ok_denied(manager, readme_root_entry, ctx):
    # README.txt is read-only → W_OK must be denied
    assert await manager.check_access(readme_root_entry, mode=2, ctx=ctx) is False


@pytest.mark.trio
async def test_check_access_nonexistent_returns_false(manager, ctx):
    ghost = _make_entry(99, pyfuse3.ROOT_INODE, "ghost", "/ghost", is_dir=False)
    assert await manager.check_access(ghost, mode=4, ctx=ctx) is False


# ---------------------------------------------------------------------------
# lookup_child
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_lookup_child_auth_dir_from_root(manager, root_entry, ctx):
    attr = await manager.lookup_child(root_entry, ".auth", ctx)
    assert attr is not None
    assert bool(attr.st_mode & stat.S_IFDIR)


@pytest.mark.trio
async def test_lookup_child_readme_from_root(manager, root_entry, ctx):
    attr = await manager.lookup_child(root_entry, "README.txt", ctx)
    assert attr is not None
    assert bool(attr.st_mode & stat.S_IFREG)


@pytest.mark.trio
async def test_lookup_child_token_from_auth_dir(manager, auth_dir_entry, ctx):
    attr = await manager.lookup_child(auth_dir_entry, "personal_access_token", ctx)
    assert attr is not None
    assert bool(attr.st_mode & stat.S_IFREG)


@pytest.mark.trio
async def test_lookup_child_readme_from_auth_dir(manager, auth_dir_entry, ctx):
    attr = await manager.lookup_child(auth_dir_entry, "README.txt", ctx)
    assert attr is not None


@pytest.mark.trio
async def test_lookup_child_nonexistent_returns_none(manager, root_entry, ctx):
    attr = await manager.lookup_child(root_entry, "does_not_exist", ctx)
    assert attr is None


# ---------------------------------------------------------------------------
# list_directory
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_list_directory_auth_dir_contains_expected_entries(manager, auth_dir_entry, ctx):
    items = await manager.list_directory(auth_dir_entry, ctx)
    assert items is not None
    assert "README.txt" in items
    assert "personal_access_token" in items


@pytest.mark.trio
async def test_list_directory_nonexistent_returns_none(manager, ctx):
    ghost = _make_entry(99, pyfuse3.ROOT_INODE, "ghost", "/ghost", is_dir=True)
    items = await manager.list_directory(ghost, ctx)
    assert items is None


@pytest.mark.trio
async def test_list_directory_root_returns_none(manager, root_entry, ctx):
    # The root overlay is handled by operations.readdir via root_overlay(),
    # not by list_directory; asking this manager to list "/" yields nothing.
    assert await manager.list_directory(root_entry, ctx) is None


# ---------------------------------------------------------------------------
# root_overlay
# ---------------------------------------------------------------------------


def test_root_overlay_contains_auth_and_readme(manager):
    overlay = manager.root_overlay()
    # AuthManager is the single source of truth for the names overlaid at root.
    assert list(overlay.keys()) == [".auth", "README.txt"]


def test_root_overlay_entry_types(manager):
    overlay = manager.root_overlay()
    assert bool(overlay[".auth"].st_mode & stat.S_IFDIR)
    assert bool(overlay["README.txt"].st_mode & stat.S_IFREG)


# ---------------------------------------------------------------------------
# read
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_read_readme_returns_non_empty_bytes(manager, ctx):
    data = await manager.read("/README.txt", offset=0, length=10_000, mtime=0, file_size=0, ctx=ctx)
    assert len(data) > 0
    assert b"README" in data


@pytest.mark.trio
async def test_read_readme_with_offset(manager, ctx):
    full = await manager.read("/README.txt", offset=0, length=10_000, mtime=0, file_size=0, ctx=ctx)
    partial = await manager.read("/README.txt", offset=5, length=20, mtime=0, file_size=0, ctx=ctx)
    assert partial == full[5:25]


@pytest.mark.trio
async def test_read_readme_past_end_returns_empty(manager, ctx):
    data = await manager.read("/README.txt", offset=10_000_000, length=100, mtime=0, file_size=0, ctx=ctx)
    assert data == b""


@pytest.mark.trio
async def test_read_access_token_raises_eacces(manager, ctx):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await manager.read("/.auth/personal_access_token", offset=0, length=100, mtime=0, file_size=0, ctx=ctx)
    assert exc_info.value.errno == errno.EACCES


@pytest.mark.trio
async def test_read_nonexistent_path_raises_eio(manager, ctx):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await manager.read("/nonexistent", offset=0, length=100, mtime=0, file_size=0, ctx=ctx)
    assert exc_info.value.errno == errno.EIO


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_write_token_returns_byte_count(manager, ctx):
    data = b"dapi0000000000000000000-2"
    n = await manager.write("/.auth/personal_access_token", 0, data, ctx=ctx)
    assert n == len(data)


@pytest.mark.trio
async def test_write_nonexistent_path_raises_eio(manager, ctx):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await manager.write("/nonexistent", 0, b"data", ctx=ctx)
    assert exc_info.value.errno == errno.EIO


@pytest.mark.trio
async def test_write_readme_raises_eacces(manager, ctx):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await manager.write("/README.txt", 0, b"data", ctx=ctx)
    assert exc_info.value.errno == errno.EACCES


@pytest.mark.trio
async def test_write_with_nonzero_offset_without_prior_write_raises_eio(manager, ctx):
    # offset=5 but buffer is empty → offset beyond buffer end → EIO
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await manager.write("/.auth/personal_access_token", 5, b"data", ctx=ctx)
    assert exc_info.value.errno == errno.EIO


# ---------------------------------------------------------------------------
# release
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_release_token_commits_to_auth_provider(manager, auth_provider, ctx):
    token = b"dapi0000000000000000000-2"
    await manager.write("/.auth/personal_access_token", 0, token, ctx=ctx)
    await manager.release("/.auth/personal_access_token", ctx=ctx)
    auth_provider.set_access_token.assert_called_once_with(
        ctx_uid=1000, access_token="dapi0000000000000000000-2"
    )


@pytest.mark.trio
async def test_release_token_strips_trailing_whitespace(manager, auth_provider, ctx):
    await manager.write("/.auth/personal_access_token", 0, b"mytoken\n", ctx=ctx)
    await manager.release("/.auth/personal_access_token", ctx=ctx)
    auth_provider.set_access_token.assert_called_once_with(ctx_uid=1000, access_token="mytoken")


@pytest.mark.trio
async def test_release_without_prior_write_does_not_call_set_access_token(manager, auth_provider, ctx):
    # No write → no buffer → release should be a no-op for the auth provider
    await manager.release("/.auth/personal_access_token", ctx=ctx)
    auth_provider.set_access_token.assert_not_called()


@pytest.mark.trio
async def test_release_readme_does_not_call_set_access_token(manager, auth_provider, ctx):
    await manager.release("/README.txt", ctx=ctx)
    auth_provider.set_access_token.assert_not_called()


@pytest.mark.trio
async def test_release_nonexistent_path_raises_eio(manager, ctx):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await manager.release("/nonexistent", ctx=ctx)
    assert exc_info.value.errno == errno.EIO


@pytest.mark.trio
async def test_write_buffers_are_isolated_per_uid(manager, auth_provider):
    ctx_1000 = SimpleNamespace(uid=1000, pid=1, gid=1000)
    ctx_2000 = SimpleNamespace(uid=2000, pid=2, gid=2000)

    await manager.write("/.auth/personal_access_token", 0, b"token_for_1000", ctx=ctx_1000)
    await manager.write("/.auth/personal_access_token", 0, b"token_for_2000", ctx=ctx_2000)

    await manager.release("/.auth/personal_access_token", ctx=ctx_1000)
    await manager.release("/.auth/personal_access_token", ctx=ctx_2000)

    calls = {call.kwargs["ctx_uid"]: call.kwargs["access_token"] for call in auth_provider.set_access_token.call_args_list}
    assert calls[1000] == "token_for_1000"
    assert calls[2000] == "token_for_2000"
