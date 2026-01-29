import pytest
import time
import stat
import pyfuse3
from fuse4dbricks.fs.inode_manager import (
    InodeManager, 
    TYPE_ROOT, TYPE_CATALOG, TYPE_SCHEMA, TYPE_VOLUME, TYPE_DIRECTORY, TYPE_FILE
)

@pytest.fixture
def manager():
    """Fixture to provide a fresh InodeManager for each test."""
    return InodeManager()

def test_root_initialization(manager):
    """Verify the root inode is created correctly on init."""
    root = manager.get_entry(pyfuse3.ROOT_INODE)
    assert root is not None
    assert root.inode == pyfuse3.ROOT_INODE
    assert root.name == "/"
    assert root.entry_type == TYPE_ROOT
    assert root.ref_count == 1  # Root should have 1 ref by default

def test_add_catalog_entry(manager):
    """Test adding a first-level child (Catalog)."""
    # Action
    entry = manager.add_entry(
        parent_inode=pyfuse3.ROOT_INODE, 
        name="my_catalog", 
        entry_type=TYPE_CATALOG
    )

    # Assertions
    assert entry.inode > pyfuse3.ROOT_INODE
    assert entry.name == "my_catalog"
    assert entry.parent_inode == pyfuse3.ROOT_INODE
    assert entry.full_path == "/my_catalog"
    
    # Check lookup by Inode
    fetched = manager.get_entry(entry.inode)
    assert fetched == entry

def test_deep_path_construction(manager):
    """Verify full_path is constructed correctly for nested items."""
    # 1. Create Catalog
    cat = manager.add_entry(pyfuse3.ROOT_INODE, "cat1", TYPE_CATALOG)
    
    # 2. Create Schema
    sch = manager.add_entry(cat.inode, "sch1", TYPE_SCHEMA)
    assert sch.full_path == "/cat1/sch1"
    
    # 3. Create Volume
    vol = manager.add_entry(sch.inode, "vol1", TYPE_VOLUME)
    assert vol.full_path == "/cat1/sch1/vol1"

def test_lookup_by_path(manager):
    """Test retrieving an inode using its string path."""
    # Setup
    cat = manager.add_entry(pyfuse3.ROOT_INODE, "finance", TYPE_CATALOG)
    sch = manager.add_entry(cat.inode, "reports", TYPE_SCHEMA)

    # Action
    found_inode = manager.get_inode_by_path("/finance/reports")

    # Assert
    assert found_inode == sch.inode
    assert manager.get_inode_by_path("/non/existent") is None

def test_infer_child_type(manager):
    """Test the logic that tells readdir what to look for."""
    root = manager.get_entry(pyfuse3.ROOT_INODE)
    
    # Root children are Catalogs
    assert manager.infer_child_type(root.inode) == TYPE_CATALOG
    
    # Catalog children are Schemas
    cat = manager.add_entry(root.inode, "c", TYPE_CATALOG)
    assert manager.infer_child_type(cat.inode) == TYPE_SCHEMA
    
    # Volume children are Directories (Physical)
    vol = manager.add_entry(cat.inode, "v", TYPE_VOLUME)
    assert manager.infer_child_type(vol.inode) == TYPE_DIRECTORY # or FILE logic

def test_forget_logic(manager):
    """Test reference counting and eviction."""
    entry = manager.add_entry(pyfuse3.ROOT_INODE, "temp", TYPE_CATALOG)
    inode = entry.inode
    
    # Simulate FUSE increasing refcount (lookup)
    manager.increment_lookup_count(inode, 1) # Now refs = 1
    
    # 1. Forget partial count
    manager.forget(inode, 1) # Refs = 0
    
    # Since we didn't implement strict eviction on 0 for add_entry (it defaults to 0 refs in constructor logic I gave you?)
    # Wait, looking at source: add_entry creates with ref_count=0 usually, unless increment_lookup_count is called.
    # Let's check source logic: 
    # "if entry.ref_count <= 0 ... del self._inode_map[inode]"
    
    # If I just added it, ref_count is 0. If I increment to 1, then forget 1, it becomes 0.
    # It should be deleted.
    
    assert manager.get_entry(inode) is None
    assert manager.get_inode_by_path("/temp") is None

def test_duplicate_add_returns_existing(manager):
    """Adding the same entry twice should return the same inode."""
    entry1 = manager.add_entry(pyfuse3.ROOT_INODE, "shared", TYPE_CATALOG)
    entry2 = manager.add_entry(pyfuse3.ROOT_INODE, "shared", TYPE_CATALOG)
    
    assert entry1.inode == entry2.inode
    assert entry1 is entry2

def test_directory_vs_file_attributes(manager):
    """Verify default attributes differ for directories and files."""
    # Directory (Catalog)
    d_entry = manager.add_entry(pyfuse3.ROOT_INODE, "d", TYPE_CATALOG)
    assert d_entry.attr['st_mode'] & stat.S_IFDIR
    assert d_entry.attr['st_nlink'] == 2

    # File
    # Need a volume parent first
    vol = manager.add_entry(pyfuse3.ROOT_INODE, "v", TYPE_VOLUME) 
    f_entry = manager.add_entry(vol.inode, "file.txt", TYPE_FILE)
    
    assert f_entry.attr['st_mode'] & stat.S_IFREG
    assert f_entry.attr['st_nlink'] == 1
