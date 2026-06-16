"""Tests for fuse4dbricks.fs.securable_filter.SecurableFilter."""

from fuse4dbricks.fs.securable_filter import SecurableFilter


def test_empty_filter_allows_everything():
    f = SecurableFilter()
    assert not f.is_active
    assert f.is_path_allowed("/")
    assert f.is_path_allowed("/cat")
    assert f.is_path_allowed("/cat/sch/vol/deep/file.bin")


# ---------------------------------------------------------------------------
# denylist
# ---------------------------------------------------------------------------


def test_denylist_blocks_securable_and_subtree():
    f = SecurableFilter(denylist=["cat.sch.vol"])
    assert f.is_active
    # The denied securable and everything under it is blocked.
    assert not f.is_path_allowed("/cat/sch/vol")
    assert not f.is_path_allowed("/cat/sch/vol/sub/file.bin")
    # Ancestors and siblings remain accessible.
    assert f.is_path_allowed("/")
    assert f.is_path_allowed("/cat")
    assert f.is_path_allowed("/cat/sch")
    assert f.is_path_allowed("/cat/sch/other_vol")
    assert f.is_path_allowed("/cat/other_sch")


def test_denylist_schema_level():
    f = SecurableFilter(denylist=["cat.sch"])
    assert not f.is_path_allowed("/cat/sch")
    assert not f.is_path_allowed("/cat/sch/vol/file")
    assert f.is_path_allowed("/cat/other")


def test_denylist_does_not_block_prefix_lookalike():
    # "/cat/sch/volume" must not be blocked by a "cat.sch.vol" deny rule.
    f = SecurableFilter(denylist=["cat.sch.vol"])
    assert f.is_path_allowed("/cat/sch/volume")
    assert f.is_path_allowed("/cat/sch/vol2")


# ---------------------------------------------------------------------------
# allowlist
# ---------------------------------------------------------------------------


def test_allowlist_permits_securable_subtree_and_ancestors():
    f = SecurableFilter(allowlist=["cat.sch.vol"])
    # Ancestors are navigable so you can reach the securable.
    assert f.is_path_allowed("/")
    assert f.is_path_allowed("/cat")
    assert f.is_path_allowed("/cat/sch")
    # The securable and its whole subtree are accessible.
    assert f.is_path_allowed("/cat/sch/vol")
    assert f.is_path_allowed("/cat/sch/vol/a/b/c.bin")


def test_allowlist_blocks_siblings_off_the_chain():
    f = SecurableFilter(allowlist=["cat.sch.vol"])
    assert not f.is_path_allowed("/cat/sch/other_vol")
    assert not f.is_path_allowed("/cat/other_sch")
    assert not f.is_path_allowed("/other_cat")


def test_allowlist_multiple_entries():
    f = SecurableFilter(allowlist=["cat.sch1.vol1", "cat.sch2"])
    assert f.is_path_allowed("/cat/sch1/vol1/x")
    assert f.is_path_allowed("/cat/sch2/anything/deep")
    assert f.is_path_allowed("/cat/sch1")  # ancestor of vol1
    assert not f.is_path_allowed("/cat/sch1/vol2")
    assert not f.is_path_allowed("/cat/sch3")


def test_catalog_level_allowlist():
    f = SecurableFilter(allowlist=["cat"])
    assert f.is_path_allowed("/cat")
    assert f.is_path_allowed("/cat/sch/vol/file")
    assert not f.is_path_allowed("/other_cat")


# ---------------------------------------------------------------------------
# allow + deny combined (deny wins)
# ---------------------------------------------------------------------------


def test_deny_wins_over_allow():
    f = SecurableFilter(allowlist=["cat.sch"], denylist=["cat.sch.secret"])
    assert f.is_path_allowed("/cat/sch/public/file")
    assert f.is_path_allowed("/cat/sch")
    # Denied subtree is blocked even though it's inside the allowed schema.
    assert not f.is_path_allowed("/cat/sch/secret")
    assert not f.is_path_allowed("/cat/sch/secret/inner")
    # Still nothing outside the allowlist.
    assert not f.is_path_allowed("/other_cat")


# ---------------------------------------------------------------------------
# parsing / normalization
# ---------------------------------------------------------------------------


def test_case_insensitive_matching():
    f = SecurableFilter(denylist=["Cat.Sch.Vol"])
    assert not f.is_path_allowed("/cat/sch/vol")
    assert not f.is_path_allowed("/CAT/SCH/VOL/file")

    g = SecurableFilter(allowlist=["cat.sch.vol"])
    assert g.is_path_allowed("/Cat/Sch/Vol/File.BIN")


def test_accepts_slash_form_and_strips_whitespace():
    f = SecurableFilter(allowlist=["  /cat/sch/vol  "])
    assert f.is_path_allowed("/cat/sch/vol/x")
    assert not f.is_path_allowed("/cat/other")


def test_blank_entries_are_ignored():
    # An allowlist of only blanks is treated as "no allowlist" (all allowed).
    f = SecurableFilter(allowlist=["", "   "])
    assert not f.is_active
    assert f.is_path_allowed("/anything/at/all")
