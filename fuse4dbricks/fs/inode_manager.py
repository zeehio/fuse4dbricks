"""
Manages the mapping between Linux Inodes and Unity Catalog Paths.
"""

import stat
import time
import logging
import pyfuse3
from dataclasses import dataclass, field

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
    is_stale: bool = False

class InodeManager:
    def __init__(self):
        # Inode ID -> InodeEntry
        self._inode_map = {}

        # full path -> Inode ID
        self._path_map = {}

        # parent_inode -> set of children
        self._children_map = {}

        self._next_inode = pyfuse3.ROOT_INODE + 1
        self._create_root()

    def _create_root(self):
        now = time.time()
        root_attr = {
            "st_mode": (stat.S_IFDIR | 0o755),
            "st_nlink": 2,
            "st_size": 4096,
            "st_ctime": now,
            "st_mtime": now,
            "st_atime": now,
            "st_uid": 0,
            "st_gid": 0,
        }
        entry = InodeEntry(
            inode=pyfuse3.ROOT_INODE,
            parent_inode=pyfuse3.ROOT_INODE,
            name="/",
            entry_type=TYPE_ROOT,
            full_path="/",
            attr=root_attr,
            ref_count=1
        )
        self._inode_map[pyfuse3.ROOT_INODE] = entry
        self._path_map["/"] = pyfuse3.ROOT_INODE
        self._children_map[pyfuse3.ROOT_INODE] = set()

    def get_entry(self, inode):
        return self._inode_map.get(inode)

    def get_inode_by_path(self, path):
        return self._path_map.get(path)

    def _delete_inode_internal(self, inode):
        entry = self._inode_map.get(inode)
        if not entry:
            return

        del self._inode_map[inode]

        if entry.full_path in self._path_map:
            if self._path_map[entry.full_path] == inode:
                del self._path_map[entry.full_path]

        if entry.parent_inode in self._children_map:
            self._children_map[entry.parent_inode].discard(inode)

        if inode in self._children_map:
            # the deletion of children inodes is handled elsewhere
            del self._children_map[inode]

    def _prune_subtree(self, inode):
        entry = self._inode_map.get(inode)
        if not entry:
            return

        children = list(self._children_map.get(inode, []))
        for child_inode in children:
            self._prune_subtree(child_inode)

        entry.is_stale = True

        # do this now to let the fs reuse the path for another inode 
        if entry.full_path in self._path_map:
            if self._path_map[entry.full_path] == inode:
                del self._path_map[entry.full_path]

        if entry.ref_count <= 0:
            self._delete_inode_internal(inode)

    def add_entry(self, parent_inode, name, entry_type, attr=None):
        parent = self.get_entry(parent_inode)
        if not parent:
            raise ValueError(f"Parent inode {parent_inode} not found.")

        if parent.entry_type == TYPE_ROOT:
            full_path = f"/{name}"
        else:
            full_path = f"{parent.full_path}/{name}"

        if full_path in self._path_map:
            existing_inode = self._path_map[full_path]
            existing_entry = self._inode_map[existing_inode]

            if existing_entry.entry_type == entry_type:
                if attr is not None:
                    existing_entry.attr.update(attr)
                return existing_entry
            
            # path changed fron dir to file or viceversa.
            # we need a new inode.
            logger.debug(f"Type change detected for {full_path} (Old Inode {existing_inode}). Pruning...")

            self._prune_subtree(existing_inode)

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

        if parent_inode not in self._children_map:
            self._children_map[parent_inode] = set()
        self._children_map[parent_inode].add(inode)

        return entry

    def forget(self, inode, count):
        if inode not in self._inode_map:
            return
        entry = self._inode_map[inode]
        entry.ref_count -= count
        if entry.ref_count <= 0:
            self._delete_inode_internal(inode)

    def increment_lookup_count(self, inode, count=1):
        """Incrementa el contador de referencias (llamado en lookup)."""
        if inode in self._inode_map:
            self._inode_map[inode].ref_count += count

    def infer_child_type(self, parent_inode):
        """For storage objects we just care is a storage object and we return TYPE_DIRECTORY for simplicity.
        """
        parent = self.get_entry(parent_inode)
        if not parent:
            return -1
        
        mapping = {
            TYPE_ROOT: TYPE_CATALOG,
            TYPE_CATALOG: TYPE_SCHEMA,
            TYPE_SCHEMA: TYPE_VOLUME,
            TYPE_VOLUME: TYPE_DIRECTORY,
            TYPE_DIRECTORY: TYPE_DIRECTORY
        }
        return mapping.get(parent.entry_type, -1)