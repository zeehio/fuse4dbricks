"""
End-to-end tests: mount the real filesystem against a live Unity Catalog volume
and exercise it through the kernel. This is the only layer that covers real
``readdir`` (the auth overlay), ``getattr``/``ENOENT``, chunk streaming on
``read`` and read-only ``write`` enforcement — none of which the mocked unit
tests can reach.

Doubly gated, so it never breaks vanilla CI:
  - skipped unless DATABRICKS_HOST, DATABRICKS_TOKEN and FUSE4DBRICKS_TEST_VOLUME
    are set (see .env.example), and
  - skipped unless the host can mount FUSE (/dev/fuse present, fusermount[3]
    available).

The token is read from the *requesting* process environment (this pytest
process), so it must be present in the shell that runs pytest:

    set -a; source .env; set +a
    pytest tests/test_e2e_mount.py -v
"""

import errno
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MOUNTPOINT = REPO_ROOT / ".e2e_mount"

HOST = os.environ.get("DATABRICKS_HOST")
TOKEN = os.environ.get("DATABRICKS_TOKEN")
VOLUME = os.environ.get("FUSE4DBRICKS_TEST_VOLUME")  # /Volumes/<cat>/<sch>/<vol>
TEST_FILE_REL = os.environ.get("FUSE4DBRICKS_TEST_FILE")  # relative to VOLUME

MOUNT_TIMEOUT_S = 30.0


def _fuse_available() -> bool:
    return os.path.exists("/dev/fuse") and bool(
        shutil.which("fusermount3") or shutil.which("fusermount")
    )


requires_live_mount = pytest.mark.skipif(
    not (HOST and TOKEN and VOLUME and _fuse_available()),
    reason=(
        "Requires DATABRICKS_HOST, DATABRICKS_TOKEN, FUSE4DBRICKS_TEST_VOLUME "
        "and FUSE support (/dev/fuse + fusermount)"
    ),
)


def _volume_relpath() -> str:
    """'/Volumes/cat/sch/vol' -> 'cat/sch/vol' (the in-mount path)."""
    assert VOLUME and VOLUME.startswith("/Volumes/"), (
        f"FUSE4DBRICKS_TEST_VOLUME must look like /Volumes/cat/sch/vol, got {VOLUME!r}"
    )
    return VOLUME[len("/Volumes/"):].rstrip("/")


def _unmount(mnt: Path) -> None:
    cmd = "fusermount3" if shutil.which("fusermount3") else "fusermount"
    subprocess.run(
        [cmd, "-u", str(mnt)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def _wait_until_mounted(mnt: Path, proc: subprocess.Popen, log: tempfile.TemporaryFile) -> None:
    deadline = time.monotonic() + MOUNT_TIMEOUT_S
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            log.seek(0)
            raise RuntimeError(
                f"fuse4dbricks exited early (code {proc.returncode}):\n{log.read()}"
            )
        if os.path.ismount(mnt):
            return
        time.sleep(0.25)
    raise TimeoutError(f"Mount did not become ready within {MOUNT_TIMEOUT_S}s at {mnt}")


def _resolve_mountpoint(suffix: str = "") -> Path:
    raw = os.environ.get("FUSE4DBRICKS_TEST_MOUNTPOINT")
    mnt = Path(raw) if raw else DEFAULT_MOUNTPOINT
    if not mnt.is_absolute():
        mnt = (REPO_ROOT / mnt).resolve()
    if suffix:
        mnt = mnt.parent / (mnt.name + suffix)
    return mnt


@contextmanager
def _mounted(extra_args=(), suffix: str = ""):
    """Mount fuse4dbricks at a (possibly suffixed) mountpoint, yield it, unmount."""
    mnt = _resolve_mountpoint(suffix)
    mnt.mkdir(parents=True, exist_ok=True)
    _unmount(mnt)  # in case a previous run left it mounted

    log = tempfile.TemporaryFile(mode="w+")
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "fuse4dbricks.main",
            "--workspace",
            HOST,
            *extra_args,
            str(mnt),
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
        env={**os.environ},
    )
    try:
        _wait_until_mounted(mnt, proc, log)
        yield mnt
    finally:
        _unmount(mnt)
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
        log.close()
        try:
            # Only remove dirs we created under the default scheme, never a
            # user-supplied FUSE4DBRICKS_TEST_MOUNTPOINT.
            if mnt.name.startswith(DEFAULT_MOUNTPOINT.name):
                mnt.rmdir()  # only if empty
        except OSError:
            pass


@pytest.fixture(scope="module")
def mountpoint():
    with _mounted() as mnt:
        yield mnt


@pytest.fixture(scope="module")
def readonly_mountpoint():
    with _mounted(extra_args=("--read-only",), suffix="_ro") as mnt:
        yield mnt


def _read_back(path: str, expected: bytes, timeout_s: float = 20.0) -> bytes:
    """Read a file through the mount until its content equals ``expected``.

    Retries on both OSError (a freshly-created path is hidden by the negative
    cache for its short TTL, and a just-uploaded object has a brief
    read-after-write window) and on a not-yet-consistent body (e.g. a transient
    zero-length read), so the test does not race the live API.
    """
    deadline = time.monotonic() + timeout_s
    last: object = "no attempt"
    while time.monotonic() < deadline:
        try:
            with open(path, "rb") as f:
                data = f.read()
            if data == expected:
                return data
            last = f"content not settled (len={len(data)}, want {len(expected)})"
        except OSError as exc:
            last = exc
        time.sleep(0.5)
    raise AssertionError(f"read-back of {path} did not settle within {timeout_s}s: {last}")


# ---------------------------------------------------------------------------
# readdir: the root overlay is merged with the live catalog listing
# ---------------------------------------------------------------------------


@requires_live_mount
def test_root_lists_auth_overlay_and_catalog(mountpoint):
    entries = set(os.listdir(mountpoint))
    # Virtual overlay entries (root_overlay) ...
    assert ".auth" in entries
    assert "README.txt" in entries
    # ... merged with the real Unity Catalog listing.
    catalog = _volume_relpath().split("/")[0]
    assert catalog in entries, f"catalog {catalog!r} not in root listing {sorted(entries)}"


@requires_live_mount
def test_auth_dir_contents(mountpoint):
    auth_dir = os.path.join(mountpoint, ".auth")
    assert os.path.isdir(auth_dir)
    assert {"README.txt", "personal_access_token"} <= set(os.listdir(auth_dir))


@requires_live_mount
def test_readme_is_readable(mountpoint):
    with open(os.path.join(mountpoint, "README.txt"), "rb") as f:
        data = f.read()
    assert len(data) > 0


# ---------------------------------------------------------------------------
# getattr / read against the live volume
# ---------------------------------------------------------------------------


@requires_live_mount
def test_volume_is_a_directory(mountpoint):
    vol = os.path.join(mountpoint, _volume_relpath())
    assert os.path.isdir(vol)


@requires_live_mount
def test_stat_nonexistent_raises_enoent(mountpoint):
    # Exercises getattr/lookup: a missing file must surface ENOENT, not EIO.
    ghost = os.path.join(mountpoint, _volume_relpath(), "fuse4dbricks_e2e_ghost__.nope")
    with pytest.raises(FileNotFoundError):
        os.stat(ghost)


@requires_live_mount
@pytest.mark.skipif(not TEST_FILE_REL, reason="FUSE4DBRICKS_TEST_FILE not set")
def test_read_known_file_matches_size(mountpoint):
    # Exercises read -> data_manager -> uc_client.download_chunk_stream end to end.
    path = os.path.join(mountpoint, _volume_relpath(), TEST_FILE_REL)
    st = os.stat(path)
    with open(path, "rb") as f:
        data = f.read()
    assert len(data) == st.st_size


@requires_live_mount
@pytest.mark.skipif(not TEST_FILE_REL, reason="FUSE4DBRICKS_TEST_FILE not set")
def test_write_is_rejected_read_only(readonly_mountpoint):
    # Mounted with --read-only: opening a Unity Catalog file for write must
    # fail with EROFS (a read-only filesystem), surfaced as OSError.
    path = os.path.join(readonly_mountpoint, _volume_relpath(), TEST_FILE_REL)
    with pytest.raises(OSError) as excinfo:
        with open(path, "wb"):
            pass
    assert excinfo.value.errno == errno.EROFS


@requires_live_mount
def test_partial_wronly_write_preserves_tail(mountpoint):
    # Regression for silent truncation through the real upload path: opening an
    # existing file O_WRONLY *without* O_TRUNC and rewriting only the leading
    # bytes must preserve the rest of the file on the volume. Two bugs used to
    # break this: (1) the write buffer started empty for O_WRONLY, and (2) small
    # buffered writes were never flushed, so the upload read back zero bytes.
    name = f"fuse4dbricks_e2e_partial_{uuid.uuid4().hex}.bin"
    path = os.path.join(mountpoint, _volume_relpath(), name)
    original = bytes(range(256)) * 8  # 2048 distinctive bytes
    try:
        with open(path, "wb") as fobj:
            fobj.write(original)
        # Whole file must be present (waits out the negative-cache TTL and the
        # read-after-write window). This also warms the cache so the O_WRONLY
        # reopen below pre-loads from cache rather than racing the API.
        _read_back(path, original)

        # Reopen O_WRONLY (no O_TRUNC) and overwrite only the first 3 bytes.
        fd = os.open(path, os.O_WRONLY)
        try:
            assert os.pwrite(fd, b"AAA", 0) == 3
        finally:
            os.close(fd)

        # The leading bytes change; the untouched tail must survive the upload.
        expected = b"AAA" + original[3:]
        _read_back(path, expected)
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
