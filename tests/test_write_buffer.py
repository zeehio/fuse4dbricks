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
