"""
Core FUSE operations module.
"""

import errno
import logging
import stat
import time
from itertools import islice

import pyfuse3

from fuse4dbricks.api.errors import (
    UcBadRequest,
    UcConflict,
    UcError,
    UcNotFound,
    UcPermissionDenied,
    UcRateLimited,
    UcUnavailable,
)
from fuse4dbricks.api.uc_client import UnityCatalogClient
from fuse4dbricks.fs.auth_manager import AuthManager
from fuse4dbricks.fs.data_manager import DataManager
from fuse4dbricks.fs.inode_manager import InodeEntry, InodeEntryAttr, InodeManager
from fuse4dbricks.fs.metadata_manager import MetadataManager
from fuse4dbricks.fs.utils import fs_to_uc_path
from fuse4dbricks.fs.write_buffer import WriteBuffer

logger = logging.getLogger(__name__)

# Open-flag constants (POSIX values)
_O_WRONLY = 1
_O_RDWR = 2
_O_TRUNC = 0o1000  # 512

_R_OK = 4
_W_OK = 2


class UnityCatalogFS(pyfuse3.Operations):
    def __init__(
        self,
        inode_manager: InodeManager,
        metadata_manager: MetadataManager,
        data_manager: DataManager,
        auth_manager: AuthManager,
        uc_client: UnityCatalogClient,
        writes_dir: str,
        read_only: bool = False,
    ):
        super(UnityCatalogFS, self).__init__()
        self.inodes = inode_manager
        self.metadata_manager = metadata_manager
        self.data_manager = data_manager
        self.auth_manager = auth_manager
        self.uc_client = uc_client
        self._writes_dir = writes_dir
        self._read_only = read_only
        self._readdir_state: dict[int, dict] = {}
        self._readdir_fh_count = 0
        self._open_fh_count = 0
        self._open_state: dict[int, dict] = {}

    def _dispatch(self, fs_path: str) -> str:
        if fs_path.startswith("/.auth/") or fs_path in ("/.auth", "/README.txt"):
            return "auth"
        else:
            return "unity_catalog"

    async def _check_permissions(self, inode: int, mode: int, ctx: pyfuse3.RequestContext):
        entry = self.inodes.get_entry(inode)
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        # Check permissions
        try:
            if self._dispatch(entry.fs_path) == "auth":
                granted = await self.auth_manager.check_access(entry, mode, ctx)
            else:
                granted = await self.metadata_manager.check_access(entry, mode, ctx)
        except pyfuse3.FUSEError:
            # Already-mapped errno: a genuine denial (EACCES) or a transient
            # failure surfaced as EAGAIN by a coalescing follower. Do not
            # collapse a rate-limit/outage into a permanent "permission denied".
            raise
        except Exception as exc:
            # Map domain errors (transient -> EAGAIN, UcPermissionDenied ->
            # EACCES, ...); falls back to EIO rather than a false EACCES.
            self._raise_fuse_error(exc, fs_path=entry.fs_path, op="check_permissions")
            raise pyfuse3.FUSEError(errno.EIO)  # defensive; _raise_fuse_error always raises
        if not granted:
            raise pyfuse3.FUSEError(errno.EACCES)
        return True

    def _raise_fuse_error(self, exc: Exception, *, fs_path: str | None = None, op: str | None = None):
        # Domain -> errno mapping
        if isinstance(exc, UcPermissionDenied):
            raise pyfuse3.FUSEError(errno.EACCES) from exc
        if isinstance(exc, UcNotFound):
            raise pyfuse3.FUSEError(errno.ENOENT) from exc
        if isinstance(exc, UcBadRequest):
            raise pyfuse3.FUSEError(errno.EINVAL) from exc
        if isinstance(exc, UcConflict):
            raise pyfuse3.FUSEError(errno.EEXIST) from exc
        if isinstance(exc, (UcRateLimited, UcUnavailable)):
            # transient
            raise pyfuse3.FUSEError(errno.EAGAIN) from exc
        if isinstance(exc, UcError):
            raise pyfuse3.FUSEError(errno.EIO) from exc

        # fall back
        raise pyfuse3.FUSEError(errno.EIO) from exc

    async def access(self, inode: int, mode: int, ctx: pyfuse3.RequestContext) -> bool:
        return await self._check_permissions(inode, mode, ctx)

    async def getattr(
        self, inode: int, ctx: pyfuse3.RequestContext
    ) -> pyfuse3.EntryAttributes:
        entry = self.inodes.get_entry(inode)
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        try:
            if self._dispatch(entry.fs_path) == "auth":
                attr = await self.auth_manager.get_attributes(entry, ctx)
            else:
                attr = await self.metadata_manager.get_attributes(entry, ctx)
        except pyfuse3.FUSEError:
            # Already-mapped errno (e.g. EACCES from a permission check): propagate as-is.
            raise
        except Exception as exc:
            # Map domain errors (UcNotFound -> ENOENT, etc.); falls back to EIO.
            self._raise_fuse_error(exc, fs_path=entry.fs_path, op="getattr")
            raise pyfuse3.FUSEError(errno.EIO)

        if attr is None:
            # File was deleted or is now a folder or... inode not valid anyway
            raise pyfuse3.FUSEError(errno.ENOENT)
        # Apply the refreshed attributes to the inode in-place so the reply
        # reflects the current size/mtime instead of whatever was cached at
        # add_entry time.
        entry.attr.update(attr)
        return self._entry_to_fuse_attr(entry)

    async def lookup(
        self, parent_inode: int, name_b: bytes, ctx: pyfuse3.RequestContext
    ) -> pyfuse3.EntryAttributes:
        await self._check_permissions(parent_inode, mode=4, ctx=ctx)  # R_OK
        name = name_b.decode("utf-8")
        parent_entry = self.inodes.get_entry(parent_inode)
        if not parent_entry:
            raise pyfuse3.FUSEError(errno.ENOENT)

        # 1. Check Local Inodes
        if parent_entry.fs_path == "/":
            full_path = f"/{name}"
        else:
            full_path = f"{parent_entry.fs_path}/{name}"

        existing = self.inodes.get_inode_by_path(full_path)
        if existing:
            # Every lookup reply increments the kernel lookup count; mirror it
            # by pinning the inode BEFORE the getattr await. The get+increment
            # pair is synchronous (atomic under trio's cooperative scheduler),
            # so a forget() racing during the await decrements from 2->1 instead
            # of 1->0 and cannot free a still-referenced inode.
            self.inodes.increment_lookup_count(existing)
            try:
                return await self.getattr(existing, ctx=ctx)
            except BaseException:
                # The reply never reached the kernel, so no matching forget will
                # come; release the pin to avoid leaking the inode.
                self.inodes.forget(existing, 1)
                raise

        # 2. Ask Cache/API
        try:
            if self._dispatch(full_path) == "auth":
                attr = await self.auth_manager.lookup_child(parent_entry, name, ctx)
            else:
                attr = await self.metadata_manager.lookup_child(parent_entry, name, ctx)
        except pyfuse3.FUSEError:
            raise
        except Exception as exc:
            self._raise_fuse_error(exc, fs_path=full_path, op="lookup")

        if attr is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        # 3. Create the inode (or pick up one a concurrent lookup created during
        # the awaits above). Pin before the getattr await, same as the existing
        # path: add_entry + increment are synchronous and therefore atomic.
        entry = self.inodes.add_entry(parent_inode, name, attr)
        self.inodes.increment_lookup_count(entry.inode)
        try:
            return await self.getattr(entry.inode, ctx)
        except BaseException:
            self.inodes.forget(entry.inode, 1)
            raise

    async def opendir(self, inode: int, ctx: pyfuse3.RequestContext):
        logger.debug(f"opendir(inode={inode})")
        R_OK = 4
        await self._check_permissions(inode, mode=R_OK, ctx=ctx)
        entry = self.inodes.get_entry(inode)
        if not entry:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if not entry.is_dir:
            raise pyfuse3.FUSEError(errno.ENOTDIR)

        fh = self._readdir_fh_count
        self._readdir_fh_count += 1
        self._readdir_state[fh] = {
            "inode": inode,
            "ctx": ctx,
        }
        return pyfuse3.FileHandleT(fh)

    async def releasedir(self, fh: pyfuse3.FileHandleT) -> None:
        logger.debug(f"releasedir(fh={fh})")
        if fh in self._readdir_state:
            del self._readdir_state[fh]

    async def readdir(self, fh: pyfuse3.FileHandleT, start_id: int, token) -> None:
        logger.debug(f"readdir(fh={fh}, start_id={start_id})")
        if fh not in self._readdir_state:
            raise pyfuse3.FUSEError(errno.EIO)

        inode = self._readdir_state[fh]["inode"]
        ctx = self._readdir_state[fh]["ctx"]

        entry = self.inodes.get_entry(inode)
        if not entry:
            raise pyfuse3.FUSEError(errno.ENOENT)

        # Yield . and ..
        if start_id <= 0:
            logger.debug(" - .")
            attr = await self.getattr(inode, ctx)
            ret = pyfuse3.readdir_reply(token, b".", attr, 1)  # type:ignore[arg-type]
            if not ret:
                return

        if start_id <= 1:
            logger.debug(" - ..")
            p_attr = await self.getattr(entry.parent_inode, ctx)
            ret = pyfuse3.readdir_reply(token, b"..", p_attr, 2)  # type:ignore[arg-type]
            if not ret:
                return

        # Virtual entries overlaid on the root (the auth files). The auth
        # manager owns their names/attributes; readdir only handles the
        # incremental-offset bookkeeping. They follow . (next_id 1) and
        # .. (next_id 2), so overlay entry i sits at next_id = 3 + i and is
        # emitted only once the cursor (start_id) has reached it.
        if inode == pyfuse3.ROOT_INODE:
            overlay = self.auth_manager.root_overlay()
        else:
            overlay = {}

        for i, (overlay_name, overlay_attr) in enumerate(overlay.items()):
            threshold = 2 + i
            if start_id > threshold:
                continue
            logger.debug(f" - {overlay_name}")
            child_entry = self.inodes.add_entry(
                parent_inode=inode,
                name=overlay_name,
                attr=overlay_attr,
            )
            ret = pyfuse3.readdir_reply(
                token,
                name=child_entry.name.encode("utf-8"),
                attr=self._entry_to_fuse_attr(child_entry),
                next_id=threshold + 1,
            )
            if ret:
                self.inodes.increment_lookup_count(child_entry.inode)
            else:
                return

        meta_attr = 2 + len(overlay)

        # Get Children from Cache/API
        if start_id <= meta_attr:
            if self._dispatch(entry.fs_path) == "auth":
                items = await self.auth_manager.list_directory(entry, ctx)
            else:
                try:
                    items = await self.metadata_manager.list_directory(entry, ctx)
                except pyfuse3.FUSEError as exc:
                    if exc.errno == errno.EACCES and inode == pyfuse3.ROOT_INODE:
                        return
                    raise
                except Exception as exc:
                    self._raise_fuse_error(exc, fs_path=entry.fs_path, op="readdir")
                    raise
            if items is None:
                raise pyfuse3.FUSEError(errno.EIO)
            self._readdir_state[fh]["children"] = items
        else:
            items = self._readdir_state[fh]["children"]
        logger.debug(f"items: {list(items.keys())}")

        to_skip = max(0, start_id - meta_attr)
        for i, (name, attr) in enumerate(islice(items.items(), to_skip, None)):
            child_entry = self.inodes.add_entry(
                parent_inode=inode,
                name=name,
                attr=attr,
            )
            ret = pyfuse3.readdir_reply(
                token,
                name=child_entry.name.encode("utf-8"),
                attr=self._entry_to_fuse_attr(child_entry),
                next_id=meta_attr + 1 + to_skip + i,
            )
            if ret:
                self.inodes.increment_lookup_count(child_entry.inode)
            else:
                return

    async def open(self, inode, flags, ctx):
        if flags & _O_RDWR:
            required_mode = _R_OK | _W_OK
        elif flags & _O_WRONLY:
            required_mode = _W_OK
        else:
            required_mode = _R_OK
        if not await self._check_permissions(inode, required_mode, ctx):
            raise pyfuse3.FUSEError(errno.EACCES)
        entry = self.inodes.get_entry(inode)
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if entry.is_dir:
            raise pyfuse3.FUSEError(errno.EISDIR)
        if self._read_only and (flags & (_O_WRONLY | _O_RDWR)) and self._dispatch(entry.fs_path) == "unity_catalog":
            raise pyfuse3.FUSEError(errno.EROFS)

        writable = bool(flags & (_O_WRONLY | _O_RDWR))
        write_buffer: WriteBuffer | None = None

        if writable and self._dispatch(entry.fs_path) == "unity_catalog":
            if (flags & _O_RDWR) and not (flags & _O_TRUNC):
                # O_RDWR without O_TRUNC: stream existing content into the
                # write buffer one chunk at a time so memory usage stays at
                # O(chunk_size) regardless of file size.
                logger.info(
                    "O_RDWR open on %s (size=%d): streaming into write buffer",
                    entry.fs_path, entry.attr.st_size,
                )
                write_buffer = WriteBuffer(self._writes_dir)
                try:
                    file_size = entry.attr.st_size
                    pos = 0
                    chunk_size = self.data_manager.chunk_size
                    while pos < file_size:
                        length = min(chunk_size, file_size - pos)
                        chunk = await self.data_manager.read(
                            entry.fs_path, pos, length,
                            entry.attr.st_mtime, file_size, ctx=ctx,
                        )
                        if len(chunk) == 0:
                            break
                        write_buffer.write(pos, chunk)
                        pos += len(chunk)
                except Exception as e:
                    write_buffer.close()
                    self._raise_fuse_error(e, fs_path=entry.fs_path, op="open/rdwr")
            else:
                write_buffer = WriteBuffer(self._writes_dir)

        fh = self._open_fh_count
        self._open_fh_count += 1
        self._open_state[fh] = {
            "inode": inode,
            "ctx": ctx,
            "write_buffer": write_buffer,
            "writable": writable,
            "dirty": False,
        }
        return pyfuse3.FileInfo(fh=fh)

    async def create(self, parent_inode, name, mode, flags, ctx):
        if self._read_only:
            raise pyfuse3.FUSEError(errno.EROFS)
        await self._check_permissions(parent_inode, _W_OK, ctx)

        parent_entry = self.inodes.get_entry(parent_inode)
        if parent_entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if self._dispatch(parent_entry.fs_path) != "unity_catalog":
            raise pyfuse3.FUSEError(errno.EACCES)

        name_str = name.decode("utf-8")
        now = time.time()
        attr = InodeEntryAttr(
            st_mode=(stat.S_IFREG | 0o644),
            st_nlink=1,
            st_size=0,
            st_ctime=now,
            st_mtime=now,
            st_atime=now,
            st_uid=ctx.uid,
            st_gid=ctx.gid,
        )
        entry = self.inodes.add_entry(parent_inode, name_str, attr)

        fh = self._open_fh_count
        self._open_fh_count += 1
        self._open_state[fh] = {
            "inode": entry.inode,
            "ctx": ctx,
            "write_buffer": WriteBuffer(self._writes_dir),
            "writable": True,
            # Mark dirty immediately: create() semantics guarantee the file
            # exists after release(), even if nothing is written into it.
            "dirty": True,
        }
        return (self._entry_to_fuse_attr(entry), pyfuse3.FileInfo(fh=fh))

    async def release(self, fh: pyfuse3.FileHandleT) -> None:
        if fh not in self._open_state:
            # This should not happen, but we want to be resilient to it. Just ignore.
            return

        state = self._open_state[fh]
        inode = state["inode"]
        ctx = state["ctx"]
        write_buffer: WriteBuffer | None = state.get("write_buffer")
        dirty: bool = state.get("dirty", False)

        entry = self.inodes.get_entry(inode)
        if entry is None:
            if write_buffer is not None:
                write_buffer.close()
            del self._open_state[fh]
            raise pyfuse3.FUSEError(errno.ENOENT)

        try:
            if write_buffer is not None and dirty:
                uc_path = fs_to_uc_path(entry.fs_path)
                try:
                    await self.uc_client.upload_file(
                        uc_path, write_buffer.path, ctx=ctx
                    )
                except Exception as e:
                    logger.error("Upload failed for %s: %s", entry.fs_path, e)
                    # Remote file is unchanged: do NOT invalidate the cache.
                    raise pyfuse3.FUSEError(errno.EIO) from e
                else:
                    # Bust caches so subsequent reads see the new file.
                    self.metadata_manager.invalidate(entry.fs_path, is_dir=False)
            elif write_buffer is not None:
                # Opened writable but never written — discard buffer silently.
                pass
            elif self._dispatch(entry.fs_path) == "auth":
                try:
                    await self.auth_manager.release(entry.fs_path, ctx=ctx)
                except Exception as e:
                    logger.error(f"release/close error on {entry.fs_path}: {e}")
                    raise pyfuse3.FUSEError(errno.EIO)
        finally:
            if write_buffer is not None:
                write_buffer.close()
            if fh in self._open_state:
                del self._open_state[fh]

    async def read(self, fh, offset, length):
        if fh not in self._open_state:
            raise pyfuse3.FUSEError(errno.EIO)

        state = self._open_state[fh]
        inode = state["inode"]
        ctx = state["ctx"]

        entry = self.inodes.get_entry(inode)
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if entry.is_dir:
            raise pyfuse3.FUSEError(errno.EISDIR)

        # O_RDWR handles: serve reads from the local write buffer, which holds
        # the full file content downloaded at open() plus any in-progress writes.
        write_buffer: WriteBuffer | None = state.get("write_buffer")
        if state.get("writable") and write_buffer is not None:
            return write_buffer.read(offset, length)

        try:
            if self._dispatch(entry.fs_path) == "auth":
                return await self.auth_manager.read(
                    entry.fs_path,
                    offset,
                    length,
                    entry.attr.st_mtime,
                    entry.attr.st_size,
                    ctx=ctx,
                )
            else:
                return await self.data_manager.read(
                    entry.fs_path,
                    offset,
                    length,
                    entry.attr.st_mtime,
                    entry.attr.st_size,
                    ctx=ctx,
                )
        except pyfuse3.FUSEError:
            raise
        except Exception as e:
            logger.exception("read failed op=read path=%s uid=%s. %s", entry.fs_path, getattr(ctx, "uid", None), e)
            self._raise_fuse_error(e, fs_path=entry.fs_path, op="read")
            raise pyfuse3.FUSEError(errno.EIO)

    async def write(self, fh, offset, buffer) -> int:
        if fh not in self._open_state:
            raise pyfuse3.FUSEError(errno.EIO)

        state = self._open_state[fh]
        inode = state["inode"]
        ctx = state["ctx"]

        entry = self.inodes.get_entry(inode)
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if entry.is_dir:
            raise pyfuse3.FUSEError(errno.EISDIR)

        try:
            # Auth files are handled by auth_manager (no write_buffer involved).
            if self._dispatch(entry.fs_path) == "auth":
                return await self.auth_manager.write(
                    entry.fs_path,
                    offset,
                    buffer,
                    ctx=ctx,
                )

            write_buffer: WriteBuffer | None = state.get("write_buffer")
            if write_buffer is None:
                # File was opened read-only.
                raise pyfuse3.FUSEError(errno.EACCES)

            n = write_buffer.write(offset, buffer)
            state["dirty"] = True
            # Keep st_size current so getattr() reflects the in-progress size.
            entry.attr.st_size = write_buffer.size()
            return n
        except pyfuse3.FUSEError:
            raise
        except Exception as e:
            logger.error("write error on %s: %s", entry.fs_path, e)
            raise pyfuse3.FUSEError(errno.EIO)

    async def mkdir(self, parent_inode, name, mode, ctx):
        if self._read_only:
            raise pyfuse3.FUSEError(errno.EROFS)
        await self._check_permissions(parent_inode, _W_OK, ctx)

        parent_entry = self.inodes.get_entry(parent_inode)
        if parent_entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if self._dispatch(parent_entry.fs_path) != "unity_catalog":
            raise pyfuse3.FUSEError(errno.EACCES)

        name_str = name.decode("utf-8")
        child_fs_path = f"{parent_entry.fs_path}/{name_str}".replace("//", "/")
        uc_path = fs_to_uc_path(child_fs_path)

        try:
            await self.uc_client.create_directory(uc_path, ctx=ctx)
        except pyfuse3.FUSEError:
            raise
        except Exception as e:
            self._raise_fuse_error(e, fs_path=child_fs_path, op="mkdir")

        now = time.time()
        attr = InodeEntryAttr(
            st_mode=(stat.S_IFDIR | 0o755),
            st_nlink=2,
            st_size=4096,
            st_ctime=now,
            st_mtime=now,
            st_atime=now,
            st_uid=ctx.uid,
            st_gid=ctx.gid,
        )
        entry = self.inodes.add_entry(parent_inode, name_str, attr)
        # Invalidate child path: deletes child's stale attr entry and the parent's
        # dir listing cache so the new directory is visible on the next readdir.
        self.metadata_manager.invalidate(entry.fs_path, is_dir=True)
        return self._entry_to_fuse_attr(entry)

    async def unlink(self, parent_inode, name, ctx):
        if self._read_only:
            raise pyfuse3.FUSEError(errno.EROFS)
        await self._check_permissions(parent_inode, _W_OK, ctx)

        parent_entry = self.inodes.get_entry(parent_inode)
        if parent_entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        name_str = name.decode("utf-8")
        child_fs_path = f"{parent_entry.fs_path}/{name_str}".replace("//", "/")
        uc_path = fs_to_uc_path(child_fs_path)

        try:
            await self.uc_client.delete_file(uc_path, ctx=ctx)
        except pyfuse3.FUSEError:
            raise
        except Exception as e:
            self._raise_fuse_error(e, fs_path=child_fs_path, op="unlink")

        child_inode = self.inodes.get_inode_by_path(child_fs_path)
        if child_inode is not None:
            self.inodes._prune_subtree(child_inode)
        self.metadata_manager.invalidate(child_fs_path, is_dir=False)

    async def rmdir(self, parent_inode, name, ctx):
        if self._read_only:
            raise pyfuse3.FUSEError(errno.EROFS)
        await self._check_permissions(parent_inode, _W_OK, ctx)

        parent_entry = self.inodes.get_entry(parent_inode)
        if parent_entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        name_str = name.decode("utf-8")
        child_fs_path = f"{parent_entry.fs_path}/{name_str}".replace("//", "/")
        uc_path = fs_to_uc_path(child_fs_path)

        try:
            await self.uc_client.delete_directory(uc_path, ctx=ctx)
        except UcBadRequest as e:
            # The Files API returns 400 when the directory is non-empty.
            raise pyfuse3.FUSEError(errno.ENOTEMPTY) from e
        except pyfuse3.FUSEError:
            raise
        except Exception as e:
            self._raise_fuse_error(e, fs_path=child_fs_path, op="rmdir")

        child_inode = self.inodes.get_inode_by_path(child_fs_path)
        if child_inode is not None:
            self.inodes._prune_subtree(child_inode)
        self.metadata_manager.invalidate(child_fs_path, is_dir=True)

    async def forget(self, inode_list):
        for inode, nlookup in inode_list:
            self.inodes.forget(inode, nlookup)

    def _entry_to_fuse_attr(self, entry: InodeEntry) -> pyfuse3.EntryAttributes:
        attr = pyfuse3.EntryAttributes()
        attr.st_mode = entry.attr.st_mode
        attr.st_nlink = entry.attr.st_nlink
        attr.st_uid = entry.attr.st_uid
        attr.st_gid = entry.attr.st_gid
        attr.st_size = entry.attr.st_size
        attr.st_atime_ns = int(entry.attr.st_atime * 1e9)
        attr.st_ctime_ns = int(entry.attr.st_ctime * 1e9)
        attr.st_mtime_ns = int(entry.attr.st_mtime * 1e9)
        attr.st_ino = entry.inode
        attr.st_blksize = 4096
        attr.st_blocks = (attr.st_size + 511) // 512
        return attr
