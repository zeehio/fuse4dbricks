"""
Manages the mapping between Linux Inodes and Unity Catalog Paths.
"""

import stat
import time
import logging

try:
    import pyfuse3
except ImportError:
    import fuse4dbricks.mock.pyfuse3 as pyfuse3  # type: ignore[no-redef]
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class InodeEntryAttr:
    st_mode: int
    st_nlink: int
    st_size: int
    st_ctime: float
    st_mtime: float
    st_atime: float
    st_uid: int
    st_gid: int

    def update(self, other: "InodeEntryAttr"):
        self.st_mode = other.st_mode
        self.st_nlink = other.st_nlink
        self.st_size = other.st_size
        self.st_ctime = other.st_ctime
        self.st_mtime = other.st_mtime
        self.st_atime = other.st_atime
        self.st_uid = other.st_uid
        self.st_gid = other.st_gid


@dataclass
class InodeEntry:
    inode: int
    parent_inode: int
    name: str
    is_dir: bool
    fs_path: str
    "The filesystem path /catalog/schema/volume/folder/file..."
    attr: InodeEntryAttr
    ref_count: int = 0
    is_stale: bool = False


class InodeManager:
    def __init__(self):
        # Inode ID -> InodeEntry
        self._inode_map: dict[int, InodeEntry] = {}

        # fs path -> Inode ID
        self._path_map: dict[str, int] = {}

        # parent_inode -> set of children
        self._children_map: dict[int, set] = {}

        self._next_inode = pyfuse3.ROOT_INODE + 1
        self._create_root()

    def _create_root(self):
        now = time.time()
        root_attr = InodeEntryAttr(
            st_mode=(stat.S_IFDIR | 0o755),
            st_nlink=2,
            st_size=4096,
            st_ctime=now,
            st_mtime=now,
            st_atime=now,
            st_uid=0,
            st_gid=0,
        )
        entry = InodeEntry(
            inode=pyfuse3.ROOT_INODE,
            parent_inode=pyfuse3.ROOT_INODE,
            name="/",
            is_dir=True,
            fs_path="/",
            attr=root_attr,
            ref_count=1,
        )
        self._inode_map[pyfuse3.ROOT_INODE] = entry
        self._path_map["/"] = pyfuse3.ROOT_INODE
        self._children_map[pyfuse3.ROOT_INODE] = set()

    def get_entry(self, inode: int) -> InodeEntry | None:
        return self._inode_map.get(inode)

    def get_inode_by_path(self, path: str) -> int | None:
        return self._path_map.get(path)

    def _delete_inode_internal(self, inode: int):
        entry = self._inode_map.get(inode)
        if not entry:
            return

        del self._inode_map[inode]

        if entry.fs_path in self._path_map:
            if self._path_map[entry.fs_path] == inode:
                del self._path_map[entry.fs_path]

        if entry.parent_inode in self._children_map:
            self._children_map[entry.parent_inode].discard(inode)

        if inode in self._children_map:
            # the deletion of children inodes is handled elsewhere
            del self._children_map[inode]

    def _prune_subtree(self, inode: int):
        entry = self._inode_map.get(inode)
        if not entry:
            return

        children = list(self._children_map.get(inode, []))
        for child_inode in children:
            self._prune_subtree(child_inode)

        entry.is_stale = True

        # do this now to let the fs reuse the path for another inode
        if entry.fs_path in self._path_map:
            if self._path_map[entry.fs_path] == inode:
                del self._path_map[entry.fs_path]

        if entry.ref_count <= 0:
            self._delete_inode_internal(inode)

    def add_entry(
        self, parent_inode: int, name: str, is_dir: bool, attr: InodeEntryAttr
    ):
        parent = self.get_entry(parent_inode)
        if not parent:
            raise ValueError(f"Parent inode {parent_inode} not found.")

        if parent_inode == pyfuse3.ROOT_INODE:
            fs_path = f"/{name}"
        else:
            fs_path = f"{parent.fs_path}/{name}"

        if fs_path in self._path_map:
            existing_inode = self._path_map[fs_path]
            existing_entry = self._inode_map[existing_inode]

            if existing_entry.is_dir == is_dir:
                if attr is not None:
                    existing_entry.attr.update(attr)
                return existing_entry
            # path changed fron dir to file or viceversa.
            # we need a new inode.
            logger.debug(
                f"Type change detected for {fs_path} (Old Inode {existing_inode}). Pruning..."
            )

            self._prune_subtree(existing_inode)

        inode = self._next_inode
        self._next_inode += 1

        entry = InodeEntry(
            inode=inode,
            parent_inode=parent_inode,
            name=name,
            is_dir=is_dir,
            fs_path=fs_path,
            attr=attr,
        )
        self._inode_map[inode] = entry
        self._path_map[fs_path] = inode

        if parent_inode not in self._children_map:
            self._children_map[parent_inode] = set()
        self._children_map[parent_inode].add(inode)
        return entry

    def forget(self, inode: int, count):
        if inode not in self._inode_map:
            return
        entry = self._inode_map[inode]
        entry.ref_count -= count
        if entry.ref_count <= 0:
            self._delete_inode_internal(inode)

    def increment_lookup_count(self, inode: int, count=1):
        """increments the reference counter that prevents inode deletion when the kernel asks to forget it"""
        if inode in self._inode_map:
            self._inode_map[inode].ref_count += count
