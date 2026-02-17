from enum import Enum
import stat
import errno
import logging
import pyfuse3
from fuse4dbricks.fs.inode_manager import InodeEntryAttr, InodeEntry

logger = logging.getLogger(__name__)


LOGIN_SCRIPT_CONTENT = b"""#!/bin/bash
AUTH_DIR="$(dirname "$0")"
CTRL_FILE="$AUTH_DIR/.login_ctrl"
STATUS_FILE="$AUTH_DIR/.login_status"
GREEN='\\033[0;32m'
BLUE='\\033[0;34m'
RED='\\033[0;31m'
NC='\\033[0m'

echo -e "${BLUE}=== Databricks Fuse Driver Login ===${NC}"
echo "init" > "$CTRL_FILE" || { echo "Failed to init"; exit 1; }

# Read initial status
DATA=$(cat "$STATUS_FILE")
STATE=$(echo "$DATA" | cut -d'|' -f1)
URL=$(echo "$DATA" | cut -d'|' -f2)
CODE=$(echo "$DATA" | cut -d'|' -f3)

if [ "$STATE" != "PENDING" ]; then
    echo -e "${RED}Error: $STATE${NC}"
    exit 1
fi

echo ""
echo -e "Visit:  ${BLUE}$URL${NC}"
echo -e "Code:   ${GREEN}$CODE${NC}"
echo ""
echo "Waiting for browser login..."

# Simple polling loop
while true; do
    CURRENT_STATUS=$(cat "$STATUS_FILE")
    if [[ "$CURRENT_STATUS" == "SUCCESS"* ]]; then
        echo -e "${GREEN}Success! Logged in.${NC}"
        break
    fi
    sleep 2
done
"""

class AuthInode(Enum):
    AUTH_DIR = "AUTH_DIR"
    SCRIPT = "SCRIPT"
    CTRL = "CTRL"
    STATUS = "STATUS"
    ACCESS_TOKEN = "ACCESS_TOKEN"
    README = "README"

_FS_PATH_TO_AUTHINODE = {
    "/.auth": AuthInode.AUTH_DIR,
    "/.auth/login.sh": AuthInode.SCRIPT,
    "/.auth/.login_ctrl": AuthInode.CTRL,
    "/.auth/.login_status": AuthInode.STATUS,
    "/.auth/personal_access_token": AuthInode.ACCESS_TOKEN,
    "/.auth/README.txt": AuthInode.README,
    "/README.txt": AuthInode.README,
}



class AuthManager:
    def __init__(self, uc_client, auth_provider, workspace):
        self._uc_client = uc_client
        self._auth_provider = auth_provider
        self._workspace = workspace

    def _gen_readme(self) -> bytes:
        readme = f"""README
=========

If you are not seeing your catalogs or you get permission errors, you may need to provide an access token.

* Visit {self._workspace}/settings/user/developer/access-tokens

* Provide the access token using a command like:

    echo "dapi0000000000000000000-2" > /Volumes/.auth/personal_access_token

Alternatively, if the /Volumes/.auth/login.sh script exists, you can execute
it to obtain a token using a device authorization flow. If you don't see it,
the device authorization flow is not available.
""".encode("utf-8")
        return readme

    def _gen_attr(self, auth_inode):
        if auth_inode == AuthInode.AUTH_DIR:
            mode = (stat.S_IFDIR | 0o555)
            size = 4096
        elif auth_inode == AuthInode.SCRIPT:
            mode = (stat.S_IFREG | 0o555)
            size = len(LOGIN_SCRIPT_CONTENT)
        elif auth_inode == AuthInode.CTRL:
            mode = (stat.S_IFREG | 0o222)
            size = 0
        elif auth_inode == AuthInode.STATUS:
            mode = (stat.S_IFREG | 0o444)
            size = 1024  # Arbitrary size for status file
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
        if fs_path not in _FS_PATH_TO_AUTHINODE:
            return False
        auth_inode = _FS_PATH_TO_AUTHINODE[fs_path]
        if self._auth_provider.has_external_provider:
            return True
        # No external provider only few files available:
        available_files = [
            AuthInode.AUTH_DIR,
            AuthInode.README,
            AuthInode.ACCESS_TOKEN,
        ]
        return auth_inode in available_files
        

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

    async def list_directory(
        self, entry: InodeEntry, ctx: pyfuse3.RequestContext
    ) -> dict[str, InodeEntryAttr] | None:
        ftype = _FS_PATH_TO_AUTHINODE.get(entry.fs_path)
        if entry == pyfuse3.ROOT_INODE:
            return {
                "README.txt":  self._gen_attr(AuthInode.README),
            }
        if ftype == AuthInode.AUTH_DIR:
            if self._auth_provider.has_external_provider:
                return {
                    "README.txt":  self._gen_attr(AuthInode.README),
                    "personal_access_token": self._gen_attr(AuthInode.ACCESS_TOKEN),
                    "login.sh": self._gen_attr(AuthInode.SCRIPT),
                    ".login_ctrl": self._gen_attr(AuthInode.CTRL),
                    ".login_status": self._gen_attr(AuthInode.STATUS),
                }
            else:
                return {
                    "README.txt":  self._gen_attr(AuthInode.README),
                    "personal_access_token": self._gen_attr(AuthInode.ACCESS_TOKEN),
                }
        return None


    def invalidate(self, fs_path: str, is_dir: bool):
        return

    async def read(
        self, fs_path: str, offset: int, length: int, mtime: float, file_size: int,
        ctx_uid: int
    ) -> bytes:
        if not self._file_exists(fs_path):
            raise pyfuse3.FUSEError(errno.EIO)
        fs_type = _FS_PATH_TO_AUTHINODE[fs_path]
        if fs_type == AuthInode.README:
            file_contents = self._gen_readme()
        elif fs_type == AuthInode.SCRIPT:
            file_contents = LOGIN_SCRIPT_CONTENT
        elif fs_type == AuthInode.STATUS:
            # For this simplified example, we'll cheat and say "Lookup generic state"
            
            # SIMULATION logic:
            # We construct the status string dynamically
            file_contents = b"PENDING|https://databricks.com/login|ABCD-1234\n"
            
            # If the user script loops, eventually we want to return SUCCESS
            # This is where you would check self.user_states[uid].
        else:
            raise pyfuse3.FUSEError(errno.EACCES)
        file_size = len(file_contents)
        if offset >= file_size:
            return bytes()
        end_range = offset + length
        if end_range >= file_size:
            end_range = file_size
        return file_contents[offset:end_range]

    async def write(self, fs_path: str, offset: int, buffer: bytes, ctx_uid: int) -> int:
        if not self._file_exists(fs_path):
            raise pyfuse3.FUSEError(errno.EIO)
        if offset != 0:
            # For simplicity, we only allow full overwrites from the start of the file
            raise pyfuse3.FUSEError(errno.EIO)
        fs_type = _FS_PATH_TO_AUTHINODE[fs_path]
        # All writes are expected to be just text, so we can turn buffer to text
        # We can also strip whitespace since that's probably user error
        try:
            buffer_txt = buffer.decode("utf-8").strip()
        except Exception:
            raise pyfuse3.FUSEError(errno.EIO)
        if fs_type == AuthInode.CTRL:
            cmd = buffer_txt
            if cmd == "init":
                print("Received INIT command. Starting OAuth flow...")
                self._auth_provider._external_provider.initiate_device_flow()
                # 3. Spawn background task to poll for token
            return len(buffer)
        if fs_type == AuthInode.ACCESS_TOKEN:
            self._auth_provider.set_access_token(ctx_uid=ctx_uid, access_token=buffer_txt)
            return len(buffer)
        raise pyfuse3.FUSEError(errno.EACCES)
