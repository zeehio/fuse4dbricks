"""
Tests for fuse4dbricks.auth.provider.

Covers the multi-user trust boundary:
  - _subuid_owner / _home_for_uid   (uid -> home, including subuid ranges)
  - _get_env_for_pid                (binary /proc/<pid>/environ parsing)
  - _read_token_from_path           (atomic open+fstat ownership check, then parse)
  - DatabricksUnifiedAuthProvider.get_access_token  (end-to-end resolution)
  - AuthProvider                    (local-token precedence, invalidation, EACCES)
"""

import errno
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, mock_open

import pytest
import pyfuse3

from fuse4dbricks.auth import provider as provider_mod
from fuse4dbricks.auth.provider import AuthProvider, DatabricksUnifiedAuthProvider


def _ctx(uid=1000, pid=5678, gid=1000):
    return SimpleNamespace(uid=uid, pid=pid, gid=gid)


@pytest.fixture
def unified():
    return DatabricksUnifiedAuthProvider()


# ---------------------------------------------------------------------------
# _subuid_owner
# ---------------------------------------------------------------------------


def test_subuid_owner_found_in_range(unified, tmp_path):
    f = tmp_path / "subuid"
    f.write_text("alice:100000:65536\nbob:200000:65536\n")
    assert unified._subuid_owner(150000, subuid_file=str(f)) == "alice"
    assert unified._subuid_owner(200000, subuid_file=str(f)) == "bob"


def test_subuid_owner_not_in_any_range(unified, tmp_path):
    f = tmp_path / "subuid"
    f.write_text("alice:100000:10\n")
    assert unified._subuid_owner(999999, subuid_file=str(f)) is None


def test_subuid_owner_missing_file(unified, tmp_path):
    assert unified._subuid_owner(150000, subuid_file=str(tmp_path / "nope")) is None


def test_subuid_owner_skips_comments_and_malformed(unified, tmp_path):
    f = tmp_path / "subuid"
    f.write_text("# comment\nbadline\nalice:notanumber:10\nbob:200000:5\n")
    assert unified._subuid_owner(200001, subuid_file=str(f)) == "bob"


# ---------------------------------------------------------------------------
# _home_for_uid
# ---------------------------------------------------------------------------


def test_home_for_uid_negative_raises(unified):
    with pytest.raises(ValueError):
        unified._home_for_uid(-1)


def test_home_for_uid_real_user(unified, monkeypatch):
    monkeypatch.setattr(provider_mod.pwd, "getpwuid", lambda uid: SimpleNamespace(pw_dir="/home/real"))
    assert unified._home_for_uid(1000) == "/home/real"


def test_home_for_uid_subuid_owner(unified, monkeypatch, tmp_path):
    f = tmp_path / "subuid"
    f.write_text("owner:500000:65536\n")

    def _getpwuid(uid):
        raise KeyError(uid)

    monkeypatch.setattr(provider_mod.pwd, "getpwuid", _getpwuid)
    monkeypatch.setattr(provider_mod.pwd, "getpwnam", lambda name: SimpleNamespace(pw_dir="/home/owner"))
    assert unified._home_for_uid(500100, subuid_file=str(f)) == "/home/owner"


def test_home_for_uid_unresolvable_raises(unified, monkeypatch, tmp_path):
    def _getpwuid(uid):
        raise KeyError(uid)

    monkeypatch.setattr(provider_mod.pwd, "getpwuid", _getpwuid)
    with pytest.raises(KeyError):
        unified._home_for_uid(500100, subuid_file=str(tmp_path / "nope"))


# ---------------------------------------------------------------------------
# _get_env_for_pid  (binary parsing + non-UTF-8 tolerance)
# ---------------------------------------------------------------------------


def test_get_env_for_pid_missing_file(unified, monkeypatch):
    monkeypatch.setattr(provider_mod.os.path, "exists", lambda p: False)
    assert unified._get_env_for_pid(1234) is None


def test_get_env_for_pid_parses_and_skips_non_utf8(unified, monkeypatch):
    # NUL-separated, no '=' entries skipped, undecodable entry skipped.
    raw = b"A=1\x00DATABRICKS_TOKEN=dapi-xyz\x00NOEQUALS\x00BAD=\xff\xfe\x00B=2\x00"
    monkeypatch.setattr(provider_mod.os.path, "exists", lambda p: True)
    monkeypatch.setattr("builtins.open", mock_open(read_data=raw))
    env = unified._get_env_for_pid(1234)
    assert env == {"A": "1", "DATABRICKS_TOKEN": "dapi-xyz", "B": "2"}


# ---------------------------------------------------------------------------
# _read_token_from_path  (atomic open+fstat ownership check, then parse)
# ---------------------------------------------------------------------------


def test_read_token_from_path_owned_by_uid(unified, tmp_path):
    cfg = tmp_path / "cfg"
    cfg.write_text("[PROD]\ntoken = dapi-prod\n")
    found, token = unified._read_token_from_path(str(cfg), os.getuid(), "PROD")
    assert (found, token) == (True, "dapi-prod")


def test_read_token_from_path_not_owned_is_rejected(unified, tmp_path):
    # Trust-boundary regression: under root + allow_other, a config file (or
    # a symlink swapped in to point at another user's file) not owned by the
    # requesting uid must be rejected — root must not cache the victim's
    # token under the attacker's uid. Ownership is checked with fstat() on
    # the same fd that is then read, so there is no separate check-then-open
    # step for a swapped path to land in between.
    cfg = tmp_path / "cfg"
    cfg.write_text("[DEFAULT]\ntoken = victim-secret\n")
    found, token = unified._read_token_from_path(str(cfg), os.getuid() + 424242, "DEFAULT")
    assert (found, token) == (False, None)


def test_read_token_from_path_not_regular_is_rejected(unified, tmp_path):
    d = tmp_path / "adir"
    d.mkdir()
    found, token = unified._read_token_from_path(str(d), os.getuid(), "DEFAULT")
    assert (found, token) == (False, None)


def test_read_token_from_path_missing_file_is_not_logged_as_error(unified, tmp_path, caplog):
    """A missing config file is the normal state for a uid without a
    Databricks setup (e.g. root probing the mount). It must not be logged at
    error level, or a repeatedly-probing uid floods the journal with one line
    per request."""
    with caplog.at_level("DEBUG", logger="fuse4dbricks.auth.provider"):
        found, token = unified._read_token_from_path(str(tmp_path / "nope"), os.getuid(), "DEFAULT")
    assert (found, token) == (False, None)
    assert not [r for r in caplog.records if r.levelname in ("ERROR", "WARNING")]
    assert any(r.levelname == "DEBUG" for r in caplog.records)


def test_read_token_from_path_open_failure_is_logged_as_warning(unified, tmp_path, monkeypatch, caplog):
    """An open() failure that is NOT 'file missing' (e.g. permission denied)
    is unexpected and stays visible at warning level."""
    def _boom(path, flags):
        raise PermissionError("denied")
    monkeypatch.setattr(provider_mod.os, "open", _boom)
    with caplog.at_level("DEBUG", logger="fuse4dbricks.auth.provider"):
        found, token = unified._read_token_from_path(str(tmp_path / "cfg"), os.getuid(), "DEFAULT")
    assert (found, token) == (False, None)
    assert any(r.levelname == "WARNING" for r in caplog.records)


def test_read_token_from_path_reads_token(unified, tmp_path):
    cfg = tmp_path / "cfg"
    cfg.write_text("[DEFAULT]\ntoken = dapi-default\n\n[PROD]\ntoken = dapi-prod\n")
    assert unified._read_token_from_path(str(cfg), os.getuid(), "PROD") == (True, "dapi-prod")


def test_read_token_from_path_missing_profile(unified, tmp_path):
    cfg = tmp_path / "cfg"
    cfg.write_text("[DEFAULT]\ntoken = x\n")
    assert unified._read_token_from_path(str(cfg), os.getuid(), "NOPE") == (True, None)


def test_read_token_from_path_missing_token_key(unified, tmp_path):
    cfg = tmp_path / "cfg"
    cfg.write_text("[DEFAULT]\nhost = https://x\n")
    assert unified._read_token_from_path(str(cfg), os.getuid(), "DEFAULT") == (True, None)


# ---------------------------------------------------------------------------
# DatabricksUnifiedAuthProvider.get_access_token  (end-to-end)
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_unified_get_token_prefers_env_token(unified, monkeypatch):
    monkeypatch.setattr(unified, "_get_env_for_pid", lambda pid: {"DATABRICKS_TOKEN": "dapi-env"})
    assert await unified.get_access_token(_ctx()) == "dapi-env"


@pytest.mark.trio
async def test_unified_get_token_uses_env_config_file(unified, tmp_path, monkeypatch):
    cfg = tmp_path / "cfg"
    cfg.write_text("[DEFAULT]\ntoken = dapi-cfg\n")
    monkeypatch.setattr(unified, "_get_env_for_pid", lambda pid: {"DATABRICKS_CONFIG_FILE": str(cfg)})
    assert await unified.get_access_token(_ctx(uid=os.getuid())) == "dapi-cfg"


@pytest.mark.trio
async def test_unified_get_token_missing_env_file_falls_back_to_default(unified, tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    (home / ".databrickscfg").write_text("[DEFAULT]\ntoken = dapi-home\n")
    monkeypatch.setattr(
        unified, "_get_env_for_pid",
        lambda pid: {"DATABRICKS_CONFIG_FILE": str(tmp_path / "does-not-exist")},
    )
    monkeypatch.setattr(unified, "_home_for_uid", lambda uid: str(home))
    assert await unified.get_access_token(_ctx(uid=os.getuid())) == "dapi-home"


@pytest.mark.trio
async def test_unified_get_token_env_file_not_owned_is_ignored(unified, tmp_path, monkeypatch):
    cfg = tmp_path / "cfg"
    cfg.write_text("[DEFAULT]\ntoken = x\n")
    monkeypatch.setattr(unified, "_get_env_for_pid", lambda pid: {"DATABRICKS_CONFIG_FILE": str(cfg)})
    monkeypatch.setattr(unified, "_home_for_uid", lambda uid: str(tmp_path / "emptyhome"))
    # A uid that does not own cfg -> the env config file is rejected; the
    # default home has no config file either -> nothing resolved.
    assert await unified.get_access_token(_ctx(uid=os.getuid() + 424242)) is None


@pytest.mark.trio
async def test_unified_get_token_none_when_unresolvable(unified, tmp_path, monkeypatch):
    monkeypatch.setattr(unified, "_get_env_for_pid", lambda pid: None)
    monkeypatch.setattr(unified, "_home_for_uid", lambda uid: str(tmp_path / "empty"))
    assert await unified.get_access_token(_ctx(uid=os.getuid())) is None


# ---------------------------------------------------------------------------
# AuthProvider
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_authprovider_local_token_takes_precedence(monkeypatch):
    prov = AuthProvider(unified_auth=True)
    # Even with unified auth available, a stored local token wins and unified
    # is never consulted.
    prov._unified_auth = SimpleNamespace(get_access_token=AsyncMock(return_value="unified"))
    prov.set_access_token(1000, "local")
    assert await prov.get_access_token(_ctx(uid=1000)) == "local"
    prov._unified_auth.get_access_token.assert_not_awaited()


@pytest.mark.trio
async def test_authprovider_no_token_no_unified_raises_eacces():
    prov = AuthProvider(unified_auth=False)
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await prov.get_access_token(_ctx(uid=1000))
    assert exc_info.value.errno == errno.EACCES


@pytest.mark.trio
async def test_authprovider_unified_token_is_cached():
    prov = AuthProvider(unified_auth=True)
    prov._unified_auth = SimpleNamespace(get_access_token=AsyncMock(return_value="dapi-u"))
    assert await prov.get_access_token(_ctx(uid=1000)) == "dapi-u"
    # Second call serves from the per-uid cache without re-consulting unified.
    assert await prov.get_access_token(_ctx(uid=1000)) == "dapi-u"
    assert prov._unified_auth.get_access_token.await_count == 1


@pytest.mark.trio
async def test_authprovider_unified_returns_none_raises_eacces():
    prov = AuthProvider(unified_auth=True)
    prov._unified_auth = SimpleNamespace(get_access_token=AsyncMock(return_value=None))
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await prov.get_access_token(_ctx(uid=1000))
    assert exc_info.value.errno == errno.EACCES


def test_authprovider_invalidate_removes_local_token():
    prov = AuthProvider(unified_auth=False)
    prov.set_access_token(1000, "local")
    prov.invalidate_access_token(_ctx(uid=1000))
    assert 1000 not in prov._uid_to_access_token
    # Invalidating an absent uid is a no-op (must not raise).
    prov.invalidate_access_token(_ctx(uid=4242))


def test_authprovider_invalidate_notifies_callback():
    prov = AuthProvider(unified_auth=False)
    seen = []
    prov.set_token_invalidation_callback(seen.append)
    prov.set_access_token(1000, "local")
    prov.invalidate_access_token(_ctx(uid=1000))
    # The callback fires with the uid even when no token was cached, so the
    # 401 path always drops principal-derived caches.
    prov.invalidate_access_token(_ctx(uid=4242))
    assert seen == [1000, 4242]


# ---------------------------------------------------------------------------
# AuthProvider single-principal mode
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_single_principal_shares_token_across_uids():
    prov = AuthProvider(unified_auth=True, single_principal=True)
    prov._unified_auth = SimpleNamespace(
        get_access_token=AsyncMock(return_value="dapi-shared")
    )
    # First request resolves the token...
    assert await prov.get_access_token(_ctx(uid=1000)) == "dapi-shared"
    # ...a different uid (e.g. Windows Explorer's synthetic uid) reuses it from
    # the shared slot without re-resolving.
    assert await prov.get_access_token(_ctx(uid=4294967295)) == "dapi-shared"
    assert prov._unified_auth.get_access_token.await_count == 1


@pytest.mark.trio
async def test_single_principal_resolves_from_server_identity():
    prov = AuthProvider(unified_auth=True, single_principal=True)
    captured = {}

    async def fake(ctx):
        captured["uid"] = ctx.uid
        captured["pid"] = ctx.pid
        return "dapi-x"

    prov._unified_auth = SimpleNamespace(get_access_token=AsyncMock(side_effect=fake))
    # Request comes from some other uid/pid, but resolution uses THIS process.
    await prov.get_access_token(_ctx(uid=1000, pid=5678))
    assert captured["uid"] == os.getuid()
    assert captured["pid"] == os.getpid()


@pytest.mark.trio
async def test_single_principal_written_token_serves_all_uids():
    # No unified auth: token comes from a .auth write under one uid and must be
    # usable by every other uid.
    prov = AuthProvider(unified_auth=False, single_principal=True)
    prov.set_access_token(1000, "dapi-written")
    assert prov._uid_to_access_token == {AuthProvider._SHARED_TOKEN_KEY: "dapi-written"}
    assert await prov.get_access_token(_ctx(uid=2000)) == "dapi-written"


def test_single_principal_invalidate_clears_shared_slot_for_any_uid():
    prov = AuthProvider(unified_auth=False, single_principal=True)
    prov.set_access_token(1000, "dapi-written")
    # Invalidating from a different uid still clears the one shared token.
    prov.invalidate_access_token(_ctx(uid=9999))
    assert prov._uid_to_access_token == {}


def test_default_mode_keys_token_per_uid():
    # Regression: without single_principal, tokens remain per-uid.
    prov = AuthProvider(unified_auth=False)
    prov.set_access_token(1000, "a")
    prov.set_access_token(2000, "b")
    assert prov._uid_to_access_token == {1000: "a", 2000: "b"}
