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


def _read_back(path: str, expected: bytes) -> bytes:
    """Read a file through the mount and assert its content equals ``expected``.

    Reads exactly once, with no retry: flush() uploads synchronously on close()
    and the Files API is immediately read-after-write consistent (see
    test_uc_client_live.py), so a write is durable and readable the instant the
    writing handle is closed -- there is nothing to wait out.
    """
    with open(path, "rb") as f:
        data = f.read()
    assert data == expected, (
        f"read-back of {path}: got {len(data)} bytes, want {len(expected)}"
    )
    return data


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
def test_statfs_and_df(mountpoint):
    # Exercises statfs: without it, statvfs/df fail with ENOSYS. We report a
    # synthetic capacity, so a writable mount shows space available.
    st = os.statvfs(mountpoint)
    assert st.f_bsize > 0
    assert st.f_blocks > 0
    assert st.f_bavail > 0  # writable mount -> space available
    # The `df` tool itself must succeed against the mount.
    result = subprocess.run(
        ["df", str(mountpoint)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@requires_live_mount
def test_single_principal_lists_catalog():
    # In single-principal mode the token is resolved from the fuse4dbricks
    # process's own environment (which carries DATABRICKS_TOKEN here), not from
    # the requesting process, so the catalog must still be listable.
    catalog = _volume_relpath().split("/")[0]
    with _mounted(extra_args=("--single-principal",), suffix="_sp") as mnt:
        entries = set(os.listdir(mnt))
        assert ".auth" in entries
        assert catalog in entries


@requires_live_mount
def test_securable_denylist_hides_catalog():
    # Mounting with the test catalog on the denylist must hide it from the root
    # listing and make the volume under it unreachable; the auth overlay is
    # unaffected.
    catalog = _volume_relpath().split("/")[0]
    with _mounted(extra_args=("--securable-denylist", catalog), suffix="_deny") as mnt:
        entries = set(os.listdir(mnt))
        assert ".auth" in entries  # overlay is never filtered
        assert catalog not in entries  # whole catalog hidden
        with pytest.raises(FileNotFoundError):
            os.stat(os.path.join(mnt, _volume_relpath()))


@requires_live_mount
def test_securable_allowlist_restricts_root_listing():
    # Allowlisting only a bogus securable hides the real catalog (it is off the
    # allowed chain), proving the allowlist restricts visibility. The auth
    # overlay still shows.
    catalog = _volume_relpath().split("/")[0]
    bogus = "fuse4dbricks_nonexistent_catalog_zzz.sch.vol"
    with _mounted(extra_args=("--securable-allowlist", bogus), suffix="_allow") as mnt:
        entries = set(os.listdir(mnt))
        assert ".auth" in entries
        assert catalog not in entries


@requires_live_mount
def test_securable_allowlist_keeps_listed_volume_reachable():
    # Allowlisting the test volume keeps its ancestor catalog navigable and the
    # volume itself reachable.
    securable = _volume_relpath().replace("/", ".")  # cat/sch/vol -> cat.sch.vol
    catalog = _volume_relpath().split("/")[0]
    with _mounted(extra_args=("--securable-allowlist", securable), suffix="_allow2") as mnt:
        entries = set(os.listdir(mnt))
        assert catalog in entries  # ancestor stays navigable
        assert os.path.isdir(os.path.join(mnt, _volume_relpath()))


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
        # Whole file must be present immediately after close (flush() uploaded
        # synchronously). This also primes the O_WRONLY reopen below.
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


@requires_live_mount
def test_write_is_durable_and_visible_on_close(mountpoint):
    # close() must make a write durable and immediately visible -- no waiting.
    #
    # The upload runs in flush(), which handles the close() syscall: the kernel
    # blocks on it, so by the time close() returns the bytes are on the backend
    # and the in-process + kernel caches have been invalidated. Combined with the
    # Files API being immediately read-after-write consistent (proven in
    # test_uc_client_live.py), the file resolves, stats with the right size and
    # reads back its content on the very first attempt after close -- with zero
    # sleeps or retries.
    #
    # Before the fix the upload ran only in release(), which the kernel does not
    # wait for, so close() returned before the data was uploaded and the file was
    # invisible for the duration of the (asynchronous) upload.
    name = f"fuse4dbricks_e2e_visible_{uuid.uuid4().hex}.bin"
    path = os.path.join(mountpoint, _volume_relpath(), name)
    payload = b"durable-on-close\n" * 8
    try:
        # The path must not exist yet (no positive entry cached anywhere).
        assert not os.path.exists(path)

        with open(path, "wb") as fobj:
            fobj.write(payload)

        # Immediately after close -- no sleep, no retry -- the file is fully there.
        assert os.path.exists(path)
        assert os.stat(path).st_size == len(payload)
        _read_back(path, payload)
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


@requires_live_mount
def test_rename_moves_file_content(mountpoint):
    # Exercises rename through the real copy(download+upload)+delete path: the
    # content must appear at the new name and the old name must be gone.
    base = _volume_relpath()
    suffix = uuid.uuid4().hex
    src_name = f"fuse4dbricks_e2e_rename_src_{suffix}.bin"
    dst_name = f"fuse4dbricks_e2e_rename_dst_{suffix}.bin"
    src = os.path.join(mountpoint, base, src_name)
    dst = os.path.join(mountpoint, base, dst_name)
    payload = bytes(range(256)) * 4  # 1024 distinctive bytes
    try:
        with open(src, "wb") as fobj:
            fobj.write(payload)
        # Readable immediately after close (flush() uploaded synchronously).
        _read_back(src, payload)

        os.rename(src, dst)

        # New name carries the content; old name no longer exists.
        _read_back(dst, payload)
        with pytest.raises(FileNotFoundError):
            os.stat(src)
    finally:
        for p in (src, dst):
            try:
                os.remove(p)
            except FileNotFoundError:
                pass


@requires_live_mount
def test_ftruncate_shrinks_file(mountpoint):
    # Exercises setattr(update_size) on an open writable handle: write content,
    # truncate it shorter, close -> the upload must carry the truncated bytes.
    name = f"fuse4dbricks_e2e_trunc_{uuid.uuid4().hex}.bin"
    path = os.path.join(mountpoint, _volume_relpath(), name)
    original = bytes(range(256))  # 256 bytes
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o644)
        try:
            assert os.write(fd, original) == len(original)
            os.ftruncate(fd, 10)
        finally:
            os.close(fd)
        _read_back(path, original[:10])
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
