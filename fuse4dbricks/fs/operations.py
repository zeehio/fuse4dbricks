"""
Core FUSE operations module.
"""

import errno
import logging
from itertools import islice

try:
    import pyfuse3
except ImportError:
    import fuse4dbricks.mock.pyfuse3 as pyfuse3  # type: ignore[no-redef]

from fuse4dbricks.fs.auth_manager import AuthManager
from fuse4dbricks.fs.data_manager import DataManager
from fuse4dbricks.fs.inode_manager import InodeEntry, InodeManager
from fuse4dbricks.fs.metadata_manager import MetadataManager
from fuse4dbricks.api.errors import (
    UcBadRequest,
    UcConflict,
    UcError,
    UcNotFound,
    UcPermissionDenied,
    UcRateLimited,
    UcUnavailable,
)

logger = logging.getLogger(__name__)


class UnityCatalogFS(pyfuse3.Operations):
    def __init__(
        self,
        inode_manager: InodeManager,
        metadata_manager: MetadataManager,
        data_manager: DataManager,
        auth_manager: AuthManager,
    ):
        super(UnityCatalogFS, self).__init__()
        self.inodes = inode_manager
        self.metadata_manager = metadata_manager
        self.data_manager = data_manager
        self.auth_manager = auth_manager
        self._readdir_state: dict[int, dict] = {}
        self._readdir_fh_count = 0
        self._open_fh_count = 0
        self._open_state: dict[int, dict] = {}

    def _dispatch(self, fs_path: str) -> str:
        if fs_path.startswith("/.auth/") or fs_path in ("/.auth", "/README.txt"):
            return "auth"
        else:
            return "unity_catalog"

    async def _check_permissions(self, inode: int, mode: int, ctx):
        entry = self.inodes.get_entry(inode)
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        # Check permissions
        try:
            if self._dispatch(entry.fs_path) == "auth":
                granted = await self.auth_manager.check_access(entry, mode, ctx)
            else:
                granted = await self.metadata_manager.check_access(entry, mode, ctx)
            if not granted:
                raise pyfuse3.FUSEError(errno.EACCES)
        except Exception as e:
            raise pyfuse3.FUSEError(errno.EACCES) from e
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
                # check ttl and update entry in-place
                attr = await self.metadata_manager.get_attributes(entry, ctx)
            if attr is None:
                # File was deleted or is now a folder or... inode not valid anyway
                raise pyfuse3.FUSEError(errno.ENOENT)
            return self._entry_to_fuse_attr(entry)
        except Exception:
            raise pyfuse3.FUSEError(errno.EIO)

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
            return await self.getattr(existing, ctx=ctx)

        # 2. Ask Cache/API
        try:
            if self._dispatch(full_path) == "auth":
                attr = await self.auth_manager.lookup_child(parent_entry, name, ctx)
            else:
                attr = await self.metadata_manager.lookup_child(parent_entry, name, ctx)
        except Exception as exc:
            self._raise_fuse_error(exc, fs_path=full_path, op="lookup")

        if attr is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        # 3. Create Inode
        entry = self.inodes.add_entry(parent_inode, name, attr)
        return await self.getattr(entry.inode, ctx)

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

        if inode == pyfuse3.ROOT_INODE and start_id <= 2:
            auth_attr = await self.auth_manager.lookup_child(entry, ".auth", ctx)
            if auth_attr is None:
                raise RuntimeError("Unexpected error: .auth not found. This should not happen")
            logger.debug(" - .auth")
            child_entry = self.inodes.add_entry(
                parent_inode=inode,
                name=".auth",
                attr=auth_attr,
            )
            ret = pyfuse3.readdir_reply(
                token,
                name=child_entry.name.encode("utf-8"),
                attr=self._entry_to_fuse_attr(child_entry),
                next_id=3,
            )
            if ret:
                self.inodes.increment_lookup_count(child_entry.inode)
            else:
                return

        if inode == pyfuse3.ROOT_INODE and start_id <= 3:
            readme_attr = await self.auth_manager.lookup_child(entry, "README.txt", ctx)
            if readme_attr is None:
                raise RuntimeError("Unexpected error: README.txt not found. This should not happen")
            logger.debug(" - .README.txt")
            child_entry = self.inodes.add_entry(
                parent_inode=inode,
                name="README.txt",
                attr=readme_attr,
            )
            ret = pyfuse3.readdir_reply(
                token,
                name=child_entry.name.encode("utf-8"),
                attr=self._entry_to_fuse_attr(child_entry),
                next_id=4,
            )
            if ret:
                self.inodes.increment_lookup_count(child_entry.inode)
            else:
                return

        if inode == pyfuse3.ROOT_INODE:
            meta_attr = 4
        else:
            meta_attr = 2

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
        R_OK = 4
        W_OK = 2
        O_RDWR = 2
        O_WRONLY = 1
        #O_RDONLY = 0
        if flags & O_RDWR:
            required_mode = R_OK | W_OK
        elif flags & O_WRONLY:
            required_mode = W_OK
        else:
            required_mode = R_OK
        if not await self._check_permissions(inode, required_mode, ctx):
            raise pyfuse3.FUSEError(errno.EACCES)
        entry = self.inodes.get_entry(inode)
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if entry.is_dir:
            raise pyfuse3.FUSEError(errno.EISDIR)
        fh = self._open_fh_count
        self._open_fh_count += 1
        self._open_state[fh] = {
            "inode": inode,
            "ctx": ctx,
        }
        return pyfuse3.FileInfo(fh=fh)

    async def release(self, fh: pyfuse3.FileHandleT) -> None:
        if fh in self._open_state:
            del self._open_state[fh]

    async def read(self, fh, offset, length):
        if fh not in self._open_state:
            raise pyfuse3.FUSEError(errno.EIO)

        inode = self._open_state[fh]["inode"]
        ctx = self._open_state[fh]["ctx"]

        entry = self.inodes.get_entry(inode)
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if entry.is_dir:
            raise pyfuse3.FUSEError(errno.EISDIR)
        try:
            if self._dispatch(entry.fs_path) == "auth":
                return await self.auth_manager.read(
                    entry.fs_path,
                    offset,
                    length,
                    entry.attr.st_mtime,
                    entry.attr.st_size,
                    ctx_uid=ctx.uid,
                )
            else:
                return await self.data_manager.read(
                    entry.fs_path,
                    offset,
                    length,
                    entry.attr.st_mtime,
                    entry.attr.st_size,
                    ctx_uid=ctx.uid,
                )
        except Exception as e:
            logger.exception("read failed op=read path=%s uid=%s. %s", entry.fs_path, getattr(ctx, "uid", None), e)
            self._raise_fuse_error(e, fs_path=entry.fs_path, op="read")
            raise pyfuse3.FUSEError(errno.EIO)

    async def write(self, fh, offset, buffer) -> int:
        if fh not in self._open_state:
            raise pyfuse3.FUSEError(errno.EIO)

        inode = self._open_state[fh]["inode"]
        ctx = self._open_state[fh]["ctx"]

        entry = self.inodes.get_entry(inode)
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if entry.is_dir:
            raise pyfuse3.FUSEError(errno.EISDIR)
        try:
            if self._dispatch(entry.fs_path) == "auth":
                return await self.auth_manager.write(
                    entry.fs_path,
                    offset,
                    buffer,
                    ctx_uid=ctx.uid,
                )
            else:
                raise pyfuse3.FUSEError(errno.EACCES)
        except Exception as e:
            logger.error(f"Read error reading {entry.fs_path}: {e}")
            raise pyfuse3.FUSEError(errno.EIO)


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
