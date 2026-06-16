"""
Allow/deny filtering of Unity Catalog securables by path prefix.

A *securable* is written dotted, like ``cat.schema.vol``, and maps to the
in-mount path ``/cat/schema/vol`` (the same convention as
``fuse4dbricks.fs.utils.fs_to_securable``). This module turns the
``--securable-allowlist`` / ``--securable-denylist`` CLI options into a single
predicate the FUSE layer consults before listing or accessing a path.
"""

from __future__ import annotations


class SecurableFilter:
    """Decides whether a Unity Catalog fs path may be listed/accessed.

    Rules:
      * **Deny wins:** a path at or below any denied securable is forbidden.
      * With a non-empty allowlist, a path is permitted only if it lies on an
        *allowed chain* — i.e. it is an allowlisted securable, an ancestor of
        one (so it stays navigable), or a descendant of one (full access below
        it).
      * An empty allowlist means "everything is allowed" (still subject to the
        denylist).

    Matching is case-insensitive: Unity Catalog catalog/schema/volume names are
    themselves case-insensitive, and an allow/deny entry is a securable (at most
    catalog.schema.volume), so it never constrains case-sensitive file names
    deeper in the tree.
    """

    def __init__(
        self,
        allowlist: list[str] | None = None,
        denylist: list[str] | None = None,
    ):
        self._allow = [self._parse(s) for s in (allowlist or [])]
        self._allow = [a for a in self._allow if a]  # drop empties
        self._deny = [self._parse(s) for s in (denylist or [])]
        self._deny = [d for d in self._deny if d]

    @staticmethod
    def _parse(securable: str) -> list[str]:
        """``"cat.schema.vol"`` (or ``"/cat/schema/vol"``) -> ``["cat","schema","vol"]``."""
        s = securable.strip().strip("/")
        return [p.casefold() for p in s.replace("/", ".").split(".") if p]

    @staticmethod
    def _components(fs_path: str) -> list[str]:
        return [c.casefold() for c in fs_path.split("/") if c]

    @staticmethod
    def _is_prefix(prefix: list[str], parts: list[str]) -> bool:
        return len(prefix) <= len(parts) and parts[: len(prefix)] == prefix

    def is_path_allowed(self, fs_path: str) -> bool:
        """Whether ``fs_path`` (an in-mount path like ``/cat/schema/vol/f``) may
        be listed or accessed."""
        parts = self._components(fs_path)
        # Deny wins: at or below any denied securable.
        if any(self._is_prefix(d, parts) for d in self._deny):
            return False
        if not self._allow:
            return True
        # On an allowed chain: one of parts/allowed is a prefix of the other
        # (ancestor, equal, or descendant of an allowlisted securable).
        return any(
            self._is_prefix(a, parts) or self._is_prefix(parts, a)
            for a in self._allow
        )

    @property
    def is_active(self) -> bool:
        """True if any allow/deny rule is configured (otherwise a no-op)."""
        return bool(self._allow or self._deny)
