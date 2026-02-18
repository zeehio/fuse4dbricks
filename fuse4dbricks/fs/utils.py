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

async def join_or_lead_request(
    lock: trio.Lock,
    inflight_dict: dict[_InflightKey, trio.Event],
    key: _InflightKey,
) -> Tuple[trio.Event, bool]:
    """
    Helper for Request Coalescing.
    Returns trio.Event and whether or not we are leaders
    """
    async with lock:
        leader = key not in inflight_dict
        if leader:
            inflight_dict[key] = trio.Event()
        return (inflight_dict[key], leader)

async def notify_followers(
    lock: trio.Lock, inflight_dict: dict[_InflightKey, trio.Event], key: _InflightKey
):
    """Wake up waiting threads and cleanup."""
    async with lock:
        if key in inflight_dict:
            inflight_dict[key].set()
            del inflight_dict[key]

class InflightRequestCoalescer(Generic[_InflightKey]):
    """
    Request coalescing helper.

    Tracks in-flight work keyed by an arbitrary key.
    - join_or_lead(key) returns (event, is_leader)
    - notify_done(key) wakes followers and removes the key
    """

    def __init__(self) -> None:
        self._lock = trio.Lock()
        self._inflight: dict[_InflightKey, trio.Event] = {}

    async def join_or_lead(self, key: _InflightKey) -> Tuple[trio.Event, bool]:
        """
        If no request is running for key, caller becomes leader and must perform the work.
        Followers should await the returned event.
        """
        async with self._lock:
            leader = key not in self._inflight
            if leader:
                self._inflight[key] = trio.Event()
            return self._inflight[key], leader

    async def notify_done(self, key: _InflightKey) -> None:
        """Wake up any followers waiting on key and cleanup."""
        async with self._lock:
            ev = self._inflight.pop(key, None)
            if ev is not None:
                ev.set()
