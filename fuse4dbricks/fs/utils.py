import trio
from typing import TypeVar, Tuple

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
