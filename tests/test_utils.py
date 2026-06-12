"""
Tests for fuse4dbricks.fs.utils:
  - fs_to_uc_path   (FUSE path → Unity Catalog path)
  - uc_to_fs_path   (Unity Catalog path → FUSE path)
  - fs_to_securable (FUSE path → (securable_name, securable_type))
  - InflightCoalescer
"""

import pytest
import trio
import trio.testing

from fuse4dbricks.fs.utils import (
    InflightCoalescer,
    fs_to_securable,
    fs_to_uc_path,
    uc_to_fs_path,
)


# ---------------------------------------------------------------------------
# fs_to_uc_path
# ---------------------------------------------------------------------------


def test_fs_to_uc_path_root():
    assert fs_to_uc_path("/") == "/Volumes"


def test_fs_to_uc_path_catalog():
    assert fs_to_uc_path("/my_catalog") == "/Volumes/my_catalog"


def test_fs_to_uc_path_schema():
    assert fs_to_uc_path("/cat/sch") == "/Volumes/cat/sch"


def test_fs_to_uc_path_volume():
    assert fs_to_uc_path("/cat/sch/vol") == "/Volumes/cat/sch/vol"


def test_fs_to_uc_path_file_in_volume():
    assert fs_to_uc_path("/cat/sch/vol/file.txt") == "/Volumes/cat/sch/vol/file.txt"


def test_fs_to_uc_path_nested_file():
    assert (
        fs_to_uc_path("/cat/sch/vol/a/b/c.csv") == "/Volumes/cat/sch/vol/a/b/c.csv"
    )


# ---------------------------------------------------------------------------
# uc_to_fs_path
# ---------------------------------------------------------------------------


def test_uc_to_fs_path_volumes_root():
    assert uc_to_fs_path("/Volumes") == "/"


def test_uc_to_fs_path_catalog():
    assert uc_to_fs_path("/Volumes/my_catalog") == "/my_catalog"


def test_uc_to_fs_path_schema():
    assert uc_to_fs_path("/Volumes/cat/sch") == "/cat/sch"


def test_uc_to_fs_path_volume():
    assert uc_to_fs_path("/Volumes/cat/sch/vol") == "/cat/sch/vol"


def test_uc_to_fs_path_file():
    assert uc_to_fs_path("/Volumes/cat/sch/vol/file.txt") == "/cat/sch/vol/file.txt"


def test_uc_to_fs_path_invalid_prefix_raises():
    with pytest.raises(ValueError):
        uc_to_fs_path("/NotVolumes/path")


def test_fs_uc_roundtrip_file():
    """fs → UC → fs should be identity."""
    original = "/cat/sch/vol/folder/data.parquet"
    assert uc_to_fs_path(fs_to_uc_path(original)) == original


def test_fs_uc_roundtrip_root():
    assert uc_to_fs_path(fs_to_uc_path("/")) == "/"


# ---------------------------------------------------------------------------
# fs_to_securable
# ---------------------------------------------------------------------------


def test_fs_to_securable_root():
    sec, typ = fs_to_securable("/")
    assert sec == ""
    assert typ == "root"


def test_fs_to_securable_catalog():
    sec, typ = fs_to_securable("/my_catalog")
    assert sec == "my_catalog"
    assert typ == "catalog"


def test_fs_to_securable_schema():
    sec, typ = fs_to_securable("/cat/sch")
    assert sec == "cat.sch"
    assert typ == "schema"


def test_fs_to_securable_volume():
    sec, typ = fs_to_securable("/cat/sch/vol")
    assert sec == "cat.sch.vol"
    assert typ == "volume"


def test_fs_to_securable_deep_path_returns_volume_securable():
    """Paths deeper than 3 segments (inside the volume) still return the volume."""
    sec, typ = fs_to_securable("/cat/sch/vol/folder/file.txt")
    assert sec == "cat.sch.vol"
    assert typ == "volume"


# ---------------------------------------------------------------------------
# InflightCoalescer
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_coalescer_first_caller_is_leader():
    coalescer: InflightCoalescer[str] = InflightCoalescer()
    _event, is_leader = await coalescer.join_or_lead("k")
    assert is_leader is True


@pytest.mark.trio
async def test_coalescer_second_caller_on_same_key_is_follower():
    coalescer: InflightCoalescer[str] = InflightCoalescer()
    event1, leader1 = await coalescer.join_or_lead("k")
    event2, leader2 = await coalescer.join_or_lead("k")
    assert leader1 is True
    assert leader2 is False
    assert event1 is event2  # Same event object


@pytest.mark.trio
async def test_coalescer_different_keys_are_independent_leaders():
    coalescer: InflightCoalescer[str] = InflightCoalescer()
    _, leader1 = await coalescer.join_or_lead("key_a")
    _, leader2 = await coalescer.join_or_lead("key_b")
    assert leader1 is True
    assert leader2 is True


@pytest.mark.trio
async def test_coalescer_notify_done_sets_event():
    coalescer: InflightCoalescer[str] = InflightCoalescer()
    event, _ = await coalescer.join_or_lead("k")
    assert not event.is_set()
    await coalescer.notify_done("k")
    assert event.is_set()


@pytest.mark.trio
async def test_coalescer_notify_done_removes_key_so_next_caller_is_leader():
    coalescer: InflightCoalescer[str] = InflightCoalescer()
    await coalescer.join_or_lead("k")
    await coalescer.notify_done("k")
    _, is_leader = await coalescer.join_or_lead("k")
    assert is_leader is True


@pytest.mark.trio
async def test_coalescer_follower_unblocked_by_notify_done():
    """Follower task waiting on event.wait() must be woken when leader calls notify_done."""
    coalescer: InflightCoalescer[str] = InflightCoalescer()
    follower_woken = []

    # Become leader first (before follower task starts)
    leader_event, is_leader = await coalescer.join_or_lead("work")
    assert is_leader is True

    async def follower():
        event, leader = await coalescer.join_or_lead("work")
        assert leader is False
        await event.wait()
        follower_woken.append(True)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(follower)
        # Let follower reach event.wait()
        await trio.testing.wait_all_tasks_blocked()
        await coalescer.notify_done("work")

    assert follower_woken == [True]


@pytest.mark.trio
async def test_coalescer_notify_done_noop_for_unknown_key():
    """notify_done on an unknown key must not raise."""
    coalescer: InflightCoalescer[str] = InflightCoalescer()
    await coalescer.notify_done("never_registered")  # should not raise
