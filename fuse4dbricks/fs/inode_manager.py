"""
Manages the mapping between Linux Inodes and Unity Catalog Paths.
"""

import stat
import time
import logging
import pyfuse3
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Constants for Node Types
TYPE_ROOT = 0
TYPE_CATALOG = 1
TYPE_SCHEMA = 2
TYPE_VOLUME = 3
TYPE_DIRECTORY = 4
TYPE_FILE = 5

@dataclass
class InodeEntry:
    inode: int
    parent_inode: int
    name: str
    entry_type: int
    full_path: str
    attr: dict
    ref_count: int = 0

class InodeManager:
    def __init__(self):
        self._inode_map = {}
        self._path_map = {}
        self._next_inode = pyfuse3.ROOT_INODE + 1
        self._create_root()

    def _create_root(self):
        root_attr = {
            "st_mode": (stat.S_IFDIR | 0o755),
            "st_nlink": 2,
            "st_size": 4096,
            "st_ctime": time.time(),
            "st_mtime": time.time(),
            "st_atime": time.time(),
            "st_uid": 0,
            "st_gid": 0,
        }
        entry = InodeEntry(
            inode=pyfuse3.ROOT_INODE,
            parent_inode=pyfuse3.ROOT_INODE,
            name="/",
            entry_type=TYPE_ROOT,
            full_path="",
            attr=root_attr,
            ref_count=1
        )
        self._inode_map[pyfuse3.ROOT_INODE] = entry
        self._path_map["/"] = pyfuse3.ROOT_INODE

    def get_entry(self, inode):
        return self._inode_map.get(inode)

    def get_inode_by_path(self, path):
        return self._path_map.get(path)

    def add_entry(self, parent_inode, name, entry_type, attr=None):
        parent = self.get_entry(parent_inode)
        if not parent:
            raise ValueError(f"Parent inode {parent_inode} not found.")

        # Path Construction
        if parent.entry_type == TYPE_ROOT:
            full_path = f"/{name}"
        elif parent.full_path == "/":
            full_path = f"/{name}"
        else:
            full_path = f"{parent.full_path}/{name}"

        # Existence Check
        if full_path in self._path_map:
            return self._inode_map[self._path_map[full_path]]

        # Create New
        inode = self._next_inode
        self._next_inode += 1

        if attr is None:
            now = time.time()
            is_dir = entry_type != TYPE_FILE
            mode = (stat.S_IFDIR | 0o755) if is_dir else (stat.S_IFREG | 0o644)
            attr = {
                "st_mode": mode,
                "st_nlink": 2 if is_dir else 1,
                "st_size": 4096 if is_dir else 0,
                "st_ctime": now,
                "st_mtime": now,
                "st_atime": now,
                "st_uid": 0,
                "st_gid": 0,
            }

        entry = InodeEntry(inode, parent_inode, name, entry_type, full_path, attr)
        self._inode_map[inode] = entry
        self._path_map[full_path] = inode
        
        return entry

    def forget(self, inode, count):
        if inode in self._inode_map:
            entry = self._inode_map[inode]
            entry.ref_count -= count
            if entry.ref_count <= 0 and inode != pyfuse3.ROOT_INODE:
                del self._inode_map[inode]
                if entry.full_path in self._path_map:
                    del self._path_map[entry.full_path]

    def infer_child_type(self, parent_inode):
        parent = self.get_entry(parent_inode)
        if not parent: return -1
        
        mapping = {
            TYPE_ROOT: TYPE_CATALOG,
            TYPE_CATALOG: TYPE_SCHEMA,
            TYPE_SCHEMA: TYPE_VOLUME,
            TYPE_VOLUME: TYPE_DIRECTORY,
            TYPE_DIRECTORY: TYPE_DIRECTORY
        }
        return mapping.get(parent.entry_type, -1)
