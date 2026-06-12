"""
Tests for fuse4dbricks.auth.provider.

Covers the multi-user trust boundary:
  - _subuid_owner / _home_for_uid   (uid -> home, including subuid ranges)
  - _get_env_for_pid                (binary /proc/<pid>/environ parsing)
  - _get_profile_and_config_file_name  (ownership/readability checks)
  - _get_token_from_config          (config parsing)
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
# _get_profile_and_config_file_name  (ownership / readability)
# ---------------------------------------------------------------------------


def test_profile_config_from_env_owned_by_uid(unified, tmp_path):
    cfg = tmp_path / "cfg"
    cfg.write_text("[DEFAULT]\ntoken=x\n")
    uid = os.getuid()
    env = {"DATABRICKS_CONFIG_PROFILE": "PROD", "DATABRICKS_CONFIG_FILE": str(cfg)}
    profile, config_file = unified._get_profile_and_config_file_name(env, uid)
    assert profile == "PROD"
    assert config_file == str(cfg)


def test_profile_config_env_file_not_owned_is_ignored(unified, tmp_path, monkeypatch):
    cfg = tmp_path / "cfg"
    cfg.write_text("[DEFAULT]\ntoken=x\n")
    # A uid that does not own the temp file -> the env config file is rejected
    # and we fall back to the home default (which we point at an empty dir).
    monkeypatch.setattr(unified, "_home_for_uid", lambda uid: str(tmp_path / "emptyhome"))
    env = {"DATABRICKS_CONFIG_FILE": str(cfg)}
    profile, config_file = unified._get_profile_and_config_file_name(env, os.getuid() + 424242)
    assert (profile, config_file) == (None, None)


def test_profile_config_env_file_not_regular_is_ignored(unified, tmp_path, monkeypatch):
    d = tmp_path / "adir"
    d.mkdir()
    monkeypatch.setattr(unified, "_home_for_uid", lambda uid: str(tmp_path / "emptyhome"))
    env = {"DATABRICKS_CONFIG_FILE": str(d)}
    profile, config_file = unified._get_profile_and_config_file_name(env, os.getuid())
    assert (profile, config_file) == (None, None)


def test_profile_config_default_home_file(unified, tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    (home / ".databrickscfg").write_text("[DEFAULT]\ntoken=x\n")
    monkeypatch.setattr(unified, "_home_for_uid", lambda uid: str(home))
    profile, config_file = unified._get_profile_and_config_file_name(env=None, uid=1000)
    assert profile == "DEFAULT"
    assert config_file == str(home / ".databrickscfg")


def test_profile_config_nothing_found(unified, tmp_path, monkeypatch):
    monkeypatch.setattr(unified, "_home_for_uid", lambda uid: str(tmp_path / "empty"))
    profile, config_file = unified._get_profile_and_config_file_name(env=None, uid=1000)
    assert (profile, config_file) == (None, None)


# ---------------------------------------------------------------------------
# _get_token_from_config
# ---------------------------------------------------------------------------


def test_get_token_from_config_reads_token(unified, tmp_path):
    cfg = tmp_path / "cfg"
    cfg.write_text("[DEFAULT]\ntoken = dapi-default\n\n[PROD]\ntoken = dapi-prod\n")
    assert unified._get_token_from_config("PROD", str(cfg)) == "dapi-prod"


def test_get_token_from_config_missing_profile(unified, tmp_path):
    cfg = tmp_path / "cfg"
    cfg.write_text("[DEFAULT]\ntoken = x\n")
    assert unified._get_token_from_config("NOPE", str(cfg)) is None


def test_get_token_from_config_missing_token_key(unified, tmp_path):
    cfg = tmp_path / "cfg"
    cfg.write_text("[DEFAULT]\nhost = https://x\n")
    assert unified._get_token_from_config("DEFAULT", str(cfg)) is None


# ---------------------------------------------------------------------------
# DatabricksUnifiedAuthProvider.get_access_token  (end-to-end)
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_unified_get_token_prefers_env_token(unified, monkeypatch):
    monkeypatch.setattr(unified, "_get_env_for_pid", lambda pid: {"DATABRICKS_TOKEN": "dapi-env"})
    assert await unified.get_access_token(_ctx()) == "dapi-env"


@pytest.mark.trio
async def test_unified_get_token_falls_back_to_config(unified, tmp_path, monkeypatch):
    cfg = tmp_path / "cfg"
    cfg.write_text("[DEFAULT]\ntoken = dapi-cfg\n")
    monkeypatch.setattr(unified, "_get_env_for_pid", lambda pid: {})
    monkeypatch.setattr(
        unified, "_get_profile_and_config_file_name", lambda env, uid: ("DEFAULT", str(cfg))
    )
    assert await unified.get_access_token(_ctx()) == "dapi-cfg"


@pytest.mark.trio
async def test_unified_get_token_none_when_unresolvable(unified, monkeypatch):
    monkeypatch.setattr(unified, "_get_env_for_pid", lambda pid: None)
    monkeypatch.setattr(unified, "_get_profile_and_config_file_name", lambda env, uid: (None, None))
    assert await unified.get_access_token(_ctx()) is None


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
