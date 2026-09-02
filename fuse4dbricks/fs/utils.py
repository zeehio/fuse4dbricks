import trio
from typing import TypeVar, Tuple, Generic

def fs_to_securable(fs_path: str) -> Tuple[str, str]:
    
    parts = fs_path.strip("/").split("/")
    catalog = parts[0]
    if len(parts) == 1:
        if catalog == "":
            return (catalog, "root")
        return (catalog, "catalog")
    schema = parts[1]
    if len(parts) == 2:
        return (f"{catalog}.{schema}", "schema")
    volume = parts[2]
    return (f"{catalog}.{schema}.{volume}", "volume")

def fs_to_uc_path(fs_path: str):
    """
    Translates FUSE path /cat/sch/vol/path to UC /Volumes/cat/sch/vol/path.
    """
    parts = fs_path.strip("/").split("/")
    catalog = parts[0]
    if len(parts) == 1:
        if catalog == "":
            return "/Volumes"
        else:
            return f"/Volumes/{catalog}"
    schema = parts[1]
    if len(parts) == 2:
        return f"/Volumes/{catalog}/{schema}"
    volume = parts[2]
    vol_prefix = f"/Volumes/{catalog}/{schema}/{volume}"
    if len(parts) == 3:
        return vol_prefix
    rest = "/".join(parts[3:])
    return f"{vol_prefix}/{rest}"


def uc_to_fs_path(uc_path: str) -> str:
    """
    Translates UC path /Volumes/cat/sch/vol/path to FUSE /cat/sch/vol/path.
    """
    if not uc_path.startswith("/Volumes"):
        raise ValueError("Unexpected UC path format")
    if uc_path == "/Volumes":
        return "/"
    parts = uc_path[len("/Volumes/"):].split("/")
    return "/" + "/".join(parts)

_InflightKey = TypeVar("_InflightKey")
_InflightResult = TypeVar("_InflightResult")


class InflightEntry(Generic[_InflightResult]):
    """Waitable slot for one in-flight operation.

    Behaves like the ``trio.Event`` it wraps (``wait()`` / ``is_set()``) and
    additionally carries the value the leader produced, so a follower can be
    handed the result directly instead of having to look it up somewhere else.
    """

    __slots__ = ("_event", "result")

    def __init__(self) -> None:
        self._event = trio.Event()
        self.result: _InflightResult | None = None

    def is_set(self) -> bool:
        return self._event.is_set()

    async def wait(self) -> None:
        await self._event.wait()

    def _set(self) -> None:
        self._event.set()


class InflightCoalescer(Generic[_InflightKey, _InflightResult]):
    """
    Request coalescing helper.

    Tracks in-flight work keyed by an arbitrary key.
    - join_or_lead(key) returns (entry, is_leader)
    - notify_done(key, result) wakes followers and removes the key

    ``result`` is optional: callers that publish their outcome elsewhere (a
    cache, a shared dict) can ignore it and leave it ``None``.
    """

    def __init__(self) -> None:
        self._lock = trio.Lock()
        self._inflight: dict[_InflightKey, InflightEntry[_InflightResult]] = {}

    async def join_or_lead(self, key: _InflightKey) -> Tuple[InflightEntry[_InflightResult], bool]:
        """
        If no request is running for key, caller becomes leader and must perform the work.
        Followers should await the returned entry and then read its ``result``.
        """
        async with self._lock:
            leader = key not in self._inflight
            if leader:
                self._inflight[key] = InflightEntry()
            return self._inflight[key], leader

    async def notify_done(self, key: _InflightKey, result: _InflightResult | None = None) -> None:
        """Publish ``result``, wake up any followers waiting on key and cleanup.

        The entry is dropped from the map here, so the result lives exactly as
        long as the waiters that still hold a reference to it.
        """
        async with self._lock:
            entry = self._inflight.pop(key, None)
            if entry is not None:
                entry.result = result
                entry._set()
