"""
Live smoke test for UnityCatalogClient against a real Databricks workspace.

This hits the network but does NOT mount anything, so when it fails you know the
problem is the API/auth/base-URL layer rather than the FUSE layer (see
test_e2e_mount.py for the full mounted stack).

Skipped unless DATABRICKS_HOST and DATABRICKS_TOKEN are set (see .env.example):

    set -a; source .env; set +a
    pytest tests/test_uc_client_live.py -v

Write-path and API-edge-case tests also require FUSE4DBRICKS_TEST_VOLUME
(a writable volume path like /Volumes/catalog/schema/volume).
"""

import os
import uuid
from types import SimpleNamespace

import pytest

from fuse4dbricks.api.errors import UcBadRequest, UcConflict, UcNotFound
from fuse4dbricks.api.uc_client import UcNodeType, UnityCatalogClient

HOST = os.environ.get("DATABRICKS_HOST")
TOKEN = os.environ.get("DATABRICKS_TOKEN")
VOLUME = os.environ.get("FUSE4DBRICKS_TEST_VOLUME")  # e.g. /Volumes/cat/sch/vol

requires_databricks = pytest.mark.skipif(
    not (HOST and TOKEN),
    reason="Requires DATABRICKS_HOST and DATABRICKS_TOKEN",
)

requires_databricks_write = pytest.mark.skipif(
    not (HOST and TOKEN and VOLUME),
    reason="Requires DATABRICKS_HOST, DATABRICKS_TOKEN and FUSE4DBRICKS_TEST_VOLUME",
)


class _EnvAuth:
    """Minimal AuthProvider that hands out the env token regardless of ctx."""

    async def get_access_token(self, ctx):
        return TOKEN

    def invalidate_access_token(self, ctx):
        pass


def _ctx():
    return SimpleNamespace(uid=os.getuid(), gid=os.getgid(), pid=os.getpid())


def _test_path(suffix: str) -> str:
    """Return a unique path inside FUSE4DBRICKS_TEST_VOLUME for a live test."""
    run_id = uuid.uuid4().hex[:8]
    return f"{VOLUME.rstrip('/')}/pytest_{run_id}_{suffix}"


@requires_databricks
@pytest.mark.trio
async def test_live_list_catalogs_smoke():
    """Listing catalogs validates auth, the base URL, pagination and parsing."""
    client = UnityCatalogClient(HOST, _EnvAuth())
    try:
        catalogs = await client._get_catalogs(ctx=_ctx())
        assert isinstance(catalogs, list)
        for entry in catalogs:
            assert entry.entry_type == UcNodeType.CATALOG
            assert entry.uc_path.startswith("/Volumes/")
            assert entry.name
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Write-path happy-path tests
# ---------------------------------------------------------------------------


@requires_databricks_write
@pytest.mark.trio
async def test_live_upload_and_delete_file(tmp_path):
    """Upload a small file, verify it exists, then delete it."""
    local = tmp_path / "hello.txt"
    local.write_bytes(b"hello from fuse4dbricks live test\n")
    uc_path = _test_path("upload.txt")

    client = UnityCatalogClient(HOST, _EnvAuth())
    ctx = _ctx()
    try:
        await client.upload_file(uc_path, str(local), ctx=ctx)
        meta = await client.get_path_metadata(uc_path, ctx=ctx)
        assert meta is not None
        assert meta.size == local.stat().st_size

        await client.delete_file(uc_path, ctx=ctx)
        meta_after = await client.get_path_metadata(uc_path, ctx=ctx)
        assert meta_after is None
    finally:
        # best-effort cleanup in case test failed mid-way
        try:
            await client.delete_file(uc_path, ctx=ctx)
        except Exception:
            pass
        await client.close()


@requires_databricks_write
@pytest.mark.trio
async def test_live_create_and_delete_directory():
    """Create a directory, verify it appears, then delete it."""
    uc_path = _test_path("emptydir")

    client = UnityCatalogClient(HOST, _EnvAuth())
    ctx = _ctx()
    try:
        await client.create_directory(uc_path, ctx=ctx)
        meta = await client.get_path_metadata(uc_path, ctx=ctx)
        assert meta is not None
        assert meta.is_dir()

        await client.delete_directory(uc_path, ctx=ctx)
        meta_after = await client.get_path_metadata(uc_path, ctx=ctx)
        assert meta_after is None
    finally:
        try:
            await client.delete_directory(uc_path, ctx=ctx)
        except Exception:
            pass
        await client.close()


# ---------------------------------------------------------------------------
# API edge-case tests — these verify actual Databricks API behavior so that
# any future API change that alters these semantics is caught immediately.
# ---------------------------------------------------------------------------


@requires_databricks_write
@pytest.mark.trio
async def test_live_delete_directory_non_empty_raises_ucbadrequest(tmp_path):
    """DELETE /directories/{path} on a non-empty directory → 400 → UcBadRequest.

    The Databricks API refuses to delete a non-empty directory. rmdir() catches
    UcBadRequest and maps it to ENOTEMPTY.
    """
    local = tmp_path / "content.txt"
    local.write_bytes(b"content")
    uc_dir = _test_path("nonempty_dir")
    uc_file = f"{uc_dir}/content.txt"

    client = UnityCatalogClient(HOST, _EnvAuth())
    ctx = _ctx()
    try:
        await client.create_directory(uc_dir, ctx=ctx)
        await client.upload_file(uc_file, str(local), ctx=ctx)

        with pytest.raises(UcBadRequest):
            await client.delete_directory(uc_dir, ctx=ctx)
    finally:
        try:
            await client.delete_file(uc_file, ctx=ctx)
        except Exception:
            pass
        try:
            await client.delete_directory(uc_dir, ctx=ctx)
        except Exception:
            pass
        await client.close()


@requires_databricks_write
@pytest.mark.trio
async def test_live_delete_directory_file_path_raises_ucnotfound(tmp_path):
    """DELETE /directories/{path} where path is a file → UcNotFound (404).

    The /directories/ endpoint returns 404 for a path that exists as a file.
    This means rmdir() raises ENOENT for this case instead of the POSIX-correct
    ENOTDIR. This test documents real API behavior; update it if Databricks
    changes the status code.
    """
    local = tmp_path / "file.txt"
    local.write_bytes(b"data")
    uc_path = _test_path("file_not_dir.txt")

    client = UnityCatalogClient(HOST, _EnvAuth())
    ctx = _ctx()
    try:
        await client.upload_file(uc_path, str(local), ctx=ctx)

        with pytest.raises(UcNotFound):
            await client.delete_directory(uc_path, ctx=ctx)
    finally:
        try:
            await client.delete_file(uc_path, ctx=ctx)
        except Exception:
            pass
        await client.close()


@requires_databricks_write
@pytest.mark.trio
async def test_live_create_directory_path_is_file_raises_ucconflict(tmp_path):
    """PUT /directories/{path} where path is an existing file → UcConflict (409).

    Databricks returns 409 when asked to create a directory at a path that is
    already occupied by a file. mkdir() currently raises EINVAL for this case;
    POSIX would prefer EEXIST. This test documents the real API behavior.
    """
    local = tmp_path / "file.txt"
    local.write_bytes(b"data")
    uc_path = _test_path("already_a_file.txt")

    client = UnityCatalogClient(HOST, _EnvAuth())
    ctx = _ctx()
    try:
        await client.upload_file(uc_path, str(local), ctx=ctx)

        with pytest.raises(UcConflict):
            await client.create_directory(uc_path, ctx=ctx)
    finally:
        try:
            await client.delete_file(uc_path, ctx=ctx)
        except Exception:
            pass
        await client.close()


@requires_databricks_write
@pytest.mark.trio
async def test_live_create_directory_parent_is_file_raises(tmp_path):
    """PUT /directories/{path/subdir} where path is a file → UcBadRequest, UcConflict or UcNotFound.

    When a parent path component is a regular file, Databricks rejects the
    request. The exact status code (400, 409 or 404) is what this test verifies —
    it must be one of those errors, and NOT a successful creation.
    Update the assertion if Databricks changes this behavior.
    """
    local = tmp_path / "file.txt"
    local.write_bytes(b"data")
    uc_parent = _test_path("parent_file.txt")
    uc_child = f"{uc_parent}/subdir"

    client = UnityCatalogClient(HOST, _EnvAuth())
    ctx = _ctx()
    try:
        await client.upload_file(uc_parent, str(local), ctx=ctx)

        with pytest.raises((UcBadRequest, UcConflict, UcNotFound)):
            await client.create_directory(uc_child, ctx=ctx)
    finally:
        try:
            await client.delete_file(uc_parent, ctx=ctx)
        except Exception:
            pass
        await client.close()
