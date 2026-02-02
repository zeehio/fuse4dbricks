"""
Core FUSE operations module.
"""
import os
import stat
import errno
import logging
import pyfuse3

from fs.inode_manager import (
    TYPE_ROOT, TYPE_CATALOG, TYPE_SCHEMA, TYPE_VOLUME, TYPE_DIRECTORY, TYPE_FILE
)

logger = logging.getLogger(__name__)

class UnityCatalogFS(pyfuse3.Operations):
    def __init__(self, uc_client, inode_manager, cache_manager):
        super(UnityCatalogFS, self).__init__()
        self.uc_client = uc_client # Kept for ref, but Cache handles calls
        self.inodes = inode_manager
        self.cache = cache_manager
        self.supports_writeback_cache = True

    async def getattr(self, inode, ctx=None):
        entry = self.inodes.get_entry(inode)
        if not entry:
            raise pyfuse3.FUSEError(errno.ENOENT)

        try:
            # Delegate TTL check to Cache
            await self.cache.get_attr(entry)
            return self._entry_to_fuse_attr(entry)
        except Exception:
            raise pyfuse3.FUSEError(errno.EIO)

    async def lookup(self, parent_inode, name_b, ctx=None):
        name = name_b.decode('utf-8')
        parent_entry = self.inodes.get_entry(parent_inode)
        if not parent_entry: raise pyfuse3.FUSEError(errno.ENOENT)
            
        # 1. Check Local Inodes
        full_path = f"{parent_entry.full_path}/{name}"
        if parent_entry.full_path == "/": full_path = f"/{name}"
            
        existing = self.inodes.get_inode_by_path(full_path)
        if existing: return await self.getattr(existing)

        # 2. Ask Cache/API
        found = await self.cache.lookup_name(parent_entry, name)
        if not found:
            raise pyfuse3.FUSEError(errno.ENOENT)

        # 3. Create Inode
        attr = None
        if 'size' in found:
            # Physical file Attr Construction
            is_dir = found['type'] == TYPE_DIRECTORY
            mode = (stat.S_IFDIR | 0o755) if is_dir else (stat.S_IFREG | 0o644)
            attr = {
                "st_mode": mode,
                "st_nlink": 2 if is_dir else 1,
                "st_size": found['size'],
                "st_mtime": found['mtime'],
                "st_ctime": found['mtime'],
                "st_atime": found['mtime'],
                "st_uid": os.getuid(),
                "st_gid": os.getgid(),
            }
            
        entry = self.inodes.add_entry(parent_inode, name, found['type'], attr)
        return await self.getattr(entry.inode)

    async def readdir(self, inode, start_id, token):
        entry = self.inodes.get_entry(inode)
        if not entry: raise pyfuse3.FUSEError(errno.ENOENT)

        # Yield . and ..
        if start_id == 0:
            attr = await self.getattr(inode)
            pyfuse3.readdir_reply(token, b".", attr, inode)
            
            p_attr = await self.getattr(entry.parent_inode)
            pyfuse3.readdir_reply(token, b"..", p_attr, entry.parent_inode)
        
        # Get Children from Cache/API
        items = await self.cache.list_children(entry)
        
        for item in items:
            # Construct Attr for Inode creation
            attr = None
            if item['type'] in [TYPE_FILE, TYPE_DIRECTORY]:
                 is_dir = item['type'] == TYPE_DIRECTORY
                 attr = {
                    "st_mode": (stat.S_IFDIR | 0o755) if is_dir else (stat.S_IFREG | 0o644),
                    "st_size": item.get('size', 0),
                    "st_mtime": item.get('mtime', 0),
                    "st_ctime": item.get('mtime', 0),
                    "st_atime": item.get('mtime', 0),
                    "st_nlink": 2 if is_dir else 1,
                    "st_uid": os.getuid(), "st_gid": os.getgid()
                }

            child_entry = self.inodes.add_entry(inode, item['name'], item['type'], attr)
            
            # Simple check to skip already-sent entries if pagination was implemented in kernel
            # Since we don't track offset perfectly here, we rely on pyfuse3 buffering
            child_attr = self._entry_to_fuse_attr(child_entry)
            pyfuse3.readdir_reply(token, item['name'].encode(), child_attr, child_entry.inode)

    async def open(self, inode, flags, ctx):
        entry = self.inodes.get_entry(inode)
        if entry.entry_type != TYPE_FILE: raise pyfuse3.FUSEError(errno.EISDIR)
        return pyfuse3.FileInfo(fh=inode)

    async def read(self, fh, offset, length):
        entry = self.inodes.get_entry(fh)
        try:
            return await self.cache.read_file(
                entry.full_path, offset, length, entry.attr['st_mtime']
            )
        except Exception as e:
            logger.error(f"Read error: {e}")
            raise pyfuse3.FUSEError(errno.EIO)

    async def forget(self, inode_list):
        for (inode, nlookup) in inode_list:
            self.inodes.forget(inode, nlookup)

    def _entry_to_fuse_attr(self, entry):
        attr = pyfuse3.EntryAttributes()
        attr.st_mode = entry.attr['st_mode']
        attr.st_nlink = entry.attr['st_nlink']
        attr.st_uid = entry.attr['st_uid']
        attr.st_gid = entry.attr['st_gid']
        attr.st_size = entry.attr['st_size']
        # Convert Seconds (Float) to Nanoseconds (Int)
        attr.st_atime_ns = int(entry.attr['st_atime'] * 1e9)
        attr.st_ctime_ns = int(entry.attr['st_ctime'] * 1e9)
        attr.st_mtime_ns = int(entry.attr['st_mtime'] * 1e9)
        attr.st_ino = entry.inode
        attr.st_blksize = 4096 
        attr.st_blocks = (attr.st_size + 511) // 512
        return attr

