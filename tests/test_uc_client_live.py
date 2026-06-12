"""
Live smoke test for UnityCatalogClient against a real Databricks workspace.

This hits the network but does NOT mount anything, so when it fails you know the
problem is the API/auth/base-URL layer rather than the FUSE layer (see
test_e2e_mount.py for the full mounted stack).

Skipped unless DATABRICKS_HOST and DATABRICKS_TOKEN are set (see .env.example):

    set -a; source .env; set +a
    pytest tests/test_uc_client_live.py -v
"""

import os
from types import SimpleNamespace

import pytest

from fuse4dbricks.api.uc_client import UcNodeType, UnityCatalogClient

HOST = os.environ.get("DATABRICKS_HOST")
TOKEN = os.environ.get("DATABRICKS_TOKEN")

requires_databricks = pytest.mark.skipif(
    not (HOST and TOKEN),
    reason="Requires DATABRICKS_HOST and DATABRICKS_TOKEN",
)


class _EnvAuth:
    """Minimal AuthProvider that hands out the env token regardless of ctx."""

    async def get_access_token(self, ctx):
        return TOKEN

    def invalidate_access_token(self, ctx):
        pass


def _ctx():
    return SimpleNamespace(uid=os.getuid(), gid=os.getgid(), pid=os.getpid())


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
