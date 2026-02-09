import trio


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
    parts = uc_path[len("/Volumes/") :].split("/")
    return "/" + "/".join(parts)


async def join_or_lead_request(
    lock: trio.Lock,
    inflight_dict: dict[str, trio.Event],
    key: str,
) -> trio.Event | None:
    """
    Helper for Request Coalescing.
    Returns trio.Event if we need to wait, or None if we are the leader.
    """
    async with lock:
        if key in inflight_dict:
            return inflight_dict[key]
        else:
            inflight_dict[key] = trio.Event()
            return None


async def notify_followers(
    lock: trio.Lock, inflight_dict: dict[str, trio.Event], key: str
):
    """Wake up waiting threads and cleanup."""
    async with lock:
        if key in inflight_dict:
            inflight_dict[key].set()
            del inflight_dict[key]
