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
    - notify_done(key, result) wakes followers and frees the key

    ``result`` is optional: callers that publish their outcome elsewhere (a
    cache, a shared dict) can ignore it and leave it ``None``.

    Waking followers and freeing the key can also be done separately, with
    publish() and release(). A leader whose work is only *usable* at one point
    but only *finished* later (it still has to commit the result somewhere)
    publishes at the first and releases at the second: followers arriving in
    between are served the published result immediately, rather than becoming
    leaders themselves and redoing work that is already committing.
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
        await self.publish(key, result)
        await self.release(key)

    async def publish(self, key: _InflightKey, result: _InflightResult | None = None) -> None:
        """Publish ``result`` and wake up any followers waiting on key.

        The key stays reserved, so no one else can become its leader until
        release() is called. Followers joining after this point do not wait:
        the entry is already set and carries the result.
        """
        async with self._lock:
            entry = self._inflight.get(key)
            if entry is not None:
                entry.result = result
                entry._set()

    async def release(self, key: _InflightKey) -> None:
        """Free the key so the next caller can lead a fresh attempt.

        Also wakes followers if publish() was never called, so a leader that
        dies without publishing cannot leave them waiting forever.
        """
        async with self._lock:
            entry = self._inflight.pop(key, None)
            if entry is not None:
                entry._set()
