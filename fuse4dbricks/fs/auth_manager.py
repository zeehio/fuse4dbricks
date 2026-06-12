from enum import Enum
import stat
import errno
import logging
import pyfuse3
from fuse4dbricks.fs.inode_manager import InodeEntryAttr, InodeEntry

logger = logging.getLogger(__name__)


class AuthInode(Enum):
    AUTH_DIR = "AUTH_DIR"
    ACCESS_TOKEN = "ACCESS_TOKEN"
    README = "README"

_FS_PATH_TO_AUTHINODE = {
    "/.auth": AuthInode.AUTH_DIR,
    "/.auth/personal_access_token": AuthInode.ACCESS_TOKEN,
    "/.auth/README.txt": AuthInode.README,
    "/README.txt": AuthInode.README,
}



class AuthManager:
    def __init__(self, uc_client, auth_provider, workspace):
        self._uc_client = uc_client
        self._auth_provider = auth_provider
        self._workspace = workspace
        self._write_buffers = {}

    def _gen_readme(self) -> bytes:
        readme = f"""README
=========

If you are not seeing your catalogs or you get permission errors, you may need to provide an access token.

* Visit {self._workspace}/settings/user/developer/access-tokens

* Provide the access token using a command like:

    echo "dapi0000000000000000000-2" > /Volumes/.auth/personal_access_token

""".encode("utf-8")
        return readme

    def _gen_attr(self, auth_inode):
        if auth_inode == AuthInode.AUTH_DIR:
            mode = (stat.S_IFDIR | 0o555)
            size = 4096
        elif auth_inode == AuthInode.ACCESS_TOKEN:
            mode = (stat.S_IFREG | 0o222)
            size = 0
        elif auth_inode == AuthInode.README:
            mode = (stat.S_IFREG | 0o444)
            size = len(self._gen_readme())
        return InodeEntryAttr(
            st_mode = mode,
            st_nlink = 2 if auth_inode == AuthInode.AUTH_DIR else 1,
            st_size = size,
            st_ctime = 0,
            st_mtime = 0,
            st_atime = 0,
            st_uid = 0,
            st_gid = 0,
        )

    def _file_exists(self, fs_path) -> bool:
        return fs_path in _FS_PATH_TO_AUTHINODE

    async def check_access(self, entry: InodeEntry, mode: int, ctx) -> bool:
        if not self._file_exists(entry.fs_path):
            return False
        existing_permissions = self._gen_attr(_FS_PATH_TO_AUTHINODE[entry.fs_path]).st_mode & 0o777
        return (mode & existing_permissions) == mode

    async def get_attributes(self, entry: InodeEntry, ctx) -> InodeEntryAttr | None:
        "Returns attributes for the given entry if it's one of our special auth files, otherwise None"
        # Check the given entry is one of our special auth files
        if not self._file_exists(entry.fs_path):
            return None
        # If it is, the attributes are static:
        return entry.attr

    async def lookup_child(
        self, parent_entry: InodeEntry, name: str, ctx
    ) -> InodeEntryAttr | None:
        """
        Returns attr or None if not found
        """
        # Construct path
        if parent_entry.fs_path == "/":
            child_fs_path = f"/{name}"
        else:
            child_fs_path = f"{parent_entry.fs_path}/{name}"
        
        # Return existing entry if available
        if not self._file_exists(child_fs_path):
            return None
        file_type = _FS_PATH_TO_AUTHINODE[child_fs_path]
        return self._gen_attr(file_type)

    def root_overlay(self) -> dict[str, InodeEntryAttr]:
        """Virtual entries overlaid on the filesystem root.

        operations.readdir merges these with the Unity Catalog listing at the
        root. Keeping the definition here (instead of hardcoding names in
        readdir) makes AuthManager the single source of truth for the auth
        namespace: adding or renaming an auth entry is confined to this class.

        Insertion order is significant — it determines the readdir offsets of
        these entries.
        """
        return {
            ".auth": self._gen_attr(AuthInode.AUTH_DIR),
            "README.txt": self._gen_attr(AuthInode.README),
        }

    async def list_directory(
        self, entry: InodeEntry, ctx: pyfuse3.RequestContext
    ) -> dict[str, InodeEntryAttr] | None:
        ftype = _FS_PATH_TO_AUTHINODE.get(entry.fs_path)
        # The filesystem root is handled by operations.readdir, which merges
        # root_overlay() with the Unity Catalog listing. This manager is only
        # ever asked to list the "/.auth" directory.
        if ftype == AuthInode.AUTH_DIR:
            return {
                "README.txt":  self._gen_attr(AuthInode.README),
                "personal_access_token": self._gen_attr(AuthInode.ACCESS_TOKEN),
            }
        return None


    def invalidate(self, fs_path: str, is_dir: bool):
        return

    async def read(
        self, fs_path: str, offset: int, length: int, mtime: float, file_size: int,
        ctx: pyfuse3.RequestContext
    ) -> bytes:
        if not self._file_exists(fs_path):
            raise pyfuse3.FUSEError(errno.EIO)
        fs_type = _FS_PATH_TO_AUTHINODE[fs_path]
        if fs_type == AuthInode.README:
            file_contents = self._gen_readme()
        else:
            raise pyfuse3.FUSEError(errno.EACCES)
        file_size = len(file_contents)
        if offset >= file_size:
            return bytes()
        end_range = offset + length
        if end_range >= file_size:
            end_range = file_size
        return file_contents[offset:end_range]

    async def release(self, fs_path: str, ctx: pyfuse3.RequestContext):
        if not self._file_exists(fs_path):
            raise pyfuse3.FUSEError(errno.EIO)
        fs_type = _FS_PATH_TO_AUTHINODE[fs_path]
        if fs_type == AuthInode.ACCESS_TOKEN:
            # Buffer the write per (fs_path, ctx.uid)
            key = (fs_path, ctx.uid)
            # On release, set the access token from the accumulated buffer
            buffer = self._write_buffers.pop(key, None)
            if buffer is not None:
                try:
                    buffer_txt = buffer.decode("utf-8").strip()
                except Exception:
                    buffer_txt = ""
                self._auth_provider.set_access_token(ctx_uid=ctx.uid, access_token=buffer_txt)
        return

    async def write(self, fs_path: str, offset: int, buffer: bytes, ctx: pyfuse3.RequestContext) -> int:
        if not self._file_exists(fs_path):
            raise pyfuse3.FUSEError(errno.EIO)
        fs_type = _FS_PATH_TO_AUTHINODE[fs_path]
        if fs_type == AuthInode.ACCESS_TOKEN:
            # Buffer the write per (fs_path, ctx.uid)
            key = (fs_path, ctx.uid)
            # Prepare overwrite:
            if offset == 0:
                self._write_buffers[key] = bytearray()
            # If offset is beyond current buffer, we reject:
            if offset > len(self._write_buffers.get(key, b"")):
                raise pyfuse3.FUSEError(errno.EIO)
            # Insert/overwrite at offset
            buf = self._write_buffers.get(key, bytearray())
            # Expand buffer if needed
            if len(buf) < offset:
                buf.extend(b"\x00" * (offset - len(buf)))
            # Overwrite or append
            if len(buf) < offset + len(buffer):
                buf[offset:] = buffer
            else:
                buf[offset:offset+len(buffer)] = buffer
            self._write_buffers[key] = buf
            return len(buffer)
        raise pyfuse3.FUSEError(errno.EACCES)
