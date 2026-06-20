"""
Tests for fuse4dbricks.fs.write_buffer.WriteBuffer.

The critical invariant: after finalize(), the full content is readable from
self.path via a *separate* open() — this is what the upload path does. Writes
go through a buffered file object, so a write smaller than the buffer (~8 KB)
would be read back as an empty file and silently uploaded as zero bytes unless
the handle is flushed/closed first.
"""

import os

import pytest

from fuse4dbricks.fs.write_buffer import WriteBuffer


@pytest.fixture
def writes_dir(tmp_path):
    d = tmp_path / "writes"
    d.mkdir()
    return str(d)


@pytest.mark.parametrize(
    "data",
    [
        b"",                         # empty
        b"hello world",             # 11 bytes, well under the buffer
        bytes(range(256)) * 8,      # 2048 bytes, still under the default ~8 KB buffer
        b"x" * 100_000,             # larger than the buffer
    ],
)
def test_finalize_makes_full_content_readable_from_path(writes_dir, data):
    """After finalize(), a fresh open(path) (as the upload does) sees every byte.

    This is the regression guard for the zero-byte-upload bug: small writes
    must not be lost in the buffered write handle.
    """
    wb = WriteBuffer(writes_dir)
    wb.write(0, data)
    assert wb.size() == len(data)

    wb.finalize()

    with open(wb.path, "rb") as f:
        on_disk = f.read()
    assert on_disk == data

    wb.close()


@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"hello world",              # well under the buffer
        bytes(range(256)) * 8,       # 2048 bytes, under the default ~8 KB buffer
        b"x" * 100_000,              # larger than the buffer
    ],
)
def test_flush_to_disk_makes_full_content_readable_from_path(writes_dir, data):
    """After flush_to_disk(), a fresh open(path) (as the upload does) sees every
    byte -- same guarantee as finalize() but without closing the handle, which
    is what the flush()-time upload relies on."""
    wb = WriteBuffer(writes_dir)
    wb.write(0, data)

    wb.flush_to_disk()

    with open(wb.path, "rb") as f:
        assert f.read() == data
    wb.close()


def test_flush_to_disk_keeps_handle_writable(writes_dir):
    """Unlike finalize(), flush_to_disk() leaves the handle open so further
    writes succeed -- flush() may be delivered more than once for one open file
    (e.g. a duplicated fd) with writes in between."""
    wb = WriteBuffer(writes_dir)
    wb.write(0, b"first")
    wb.flush_to_disk()
    with open(wb.path, "rb") as f:
        assert f.read() == b"first"

    # Handle still usable: append more, flush again, full content visible.
    wb.write(5, b"-second")
    wb.flush_to_disk()
    with open(wb.path, "rb") as f:
        assert f.read() == b"first-second"
    wb.close()


def test_flush_to_disk_is_idempotent_and_safe_after_close(writes_dir):
    wb = WriteBuffer(writes_dir)
    wb.write(0, b"data")
    wb.flush_to_disk()
    wb.flush_to_disk()  # must not raise
    wb.finalize()
    wb.flush_to_disk()  # no-op once closed; must not raise
    wb.close()


def test_finalize_is_idempotent(writes_dir):
    wb = WriteBuffer(writes_dir)
    wb.write(0, b"data")
    wb.finalize()
    wb.finalize()  # must not raise
    with open(wb.path, "rb") as f:
        assert f.read() == b"data"
    wb.close()


def test_close_deletes_tempfile(writes_dir):
    wb = WriteBuffer(writes_dir)
    wb.write(0, b"data")
    path = wb.path
    assert os.path.exists(path)
    wb.close()
    assert not os.path.exists(path)


def test_close_after_finalize_still_deletes(writes_dir):
    wb = WriteBuffer(writes_dir)
    wb.write(0, b"data")
    path = wb.path
    wb.finalize()
    assert os.path.exists(path)
    wb.close()
    assert not os.path.exists(path)


def test_partial_overwrite_preserves_tail_on_disk(writes_dir):
    """Mirror of the FUSE partial-write case at the buffer level: overwriting
    the leading bytes must leave the rest intact in the uploaded file."""
    wb = WriteBuffer(writes_dir)
    wb.write(0, b"0123456789")
    wb.write(0, b"AAA")
    wb.finalize()
    with open(wb.path, "rb") as f:
        assert f.read() == b"AAA3456789"
    wb.close()


def test_truncate_shrinks(writes_dir):
    wb = WriteBuffer(writes_dir)
    wb.write(0, b"0123456789")
    wb.truncate(4)
    assert wb.size() == 4
    wb.finalize()
    with open(wb.path, "rb") as f:
        assert f.read() == b"0123"
    wb.close()


def test_truncate_grows_with_zeros(writes_dir):
    wb = WriteBuffer(writes_dir)
    wb.write(0, b"abc")
    wb.truncate(6)
    assert wb.size() == 6
    wb.finalize()
    with open(wb.path, "rb") as f:
        assert f.read() == b"abc\x00\x00\x00"
    wb.close()


def test_truncate_to_zero(writes_dir):
    wb = WriteBuffer(writes_dir)
    wb.write(0, b"data")
    wb.truncate(0)
    assert wb.size() == 0
    wb.finalize()
    with open(wb.path, "rb") as f:
        assert f.read() == b""
    wb.close()
