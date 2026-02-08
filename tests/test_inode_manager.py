import pytest
import stat

try:
    import pyfuse3
except ImportError:
    import fuse4dbricks.mock.pyfuse3 as pyfuse3  # type: ignore[no-redef]

from fuse4dbricks.fs.inode_manager import InodeManager, InodeEntry, InodeEntryAttr

@pytest.fixture
def manager():
    """Fixture to provide a fresh InodeManager for each test."""
    return InodeManager()

@pytest.fixture
def dir_entry_attr():
    """Fixture to provide a dummy directory InodeEntryAttr instance for each test."""
    return InodeEntryAttr(
        st_mode = (stat.S_IFDIR | 0o755),
        st_nlink = 2,
        st_size = 0,
        st_ctime = 0,
        st_mtime = 0,
        st_atime = 0,
        st_uid = 0,
        st_gid = 0,
    )

@pytest.fixture
def file_entry_attr():
    """Fixture to provide a dummy file InodeEntryAttr instance for each test."""
    return InodeEntryAttr(
        st_mode = (stat.S_IFREG | 0o644),
        st_nlink = 1,
        st_size = 0,
        st_ctime = 0,
        st_mtime = 0,
        st_atime = 0,
        st_uid = 0,
        st_gid = 0,
    )

def test_root_initialization(manager):
    """Verify the root inode is created correctly on init."""
    root = manager.get_entry(pyfuse3.ROOT_INODE)
    assert root is not None
    assert root.inode == pyfuse3.ROOT_INODE
    assert root.name == "/"
    assert root.fs_path == "/"
    assert root.is_dir == True
    assert root.ref_count == 1  # Root should have 1 ref by default

def test_add_catalog_entry(manager, dir_entry_attr):
    """Test adding a first-level child (Catalog)."""
    # Action
    entry = manager.add_entry(
        parent_inode=pyfuse3.ROOT_INODE, 
        name="my_catalog",
        is_dir = True,
        attr = dir_entry_attr,
    )

    # Assertions
    assert entry.inode > pyfuse3.ROOT_INODE
    assert entry.name == "my_catalog"
    assert entry.parent_inode == pyfuse3.ROOT_INODE
    assert entry.fs_path == "/my_catalog"
    
    # Check lookup by Inode
    fetched = manager.get_entry(entry.inode)
    assert fetched == entry

def test_deep_path_construction(manager, dir_entry_attr):
    """Verify full_path is constructed correctly for nested items."""
    # 1. Create Catalog
    cat = manager.add_entry(pyfuse3.ROOT_INODE, "cat1", is_dir=True, attr=dir_entry_attr)
    
    # 2. Create Schema
    sch = manager.add_entry(cat.inode, "sch1", is_dir=True, attr=dir_entry_attr)
    assert sch.fs_path == "/cat1/sch1"
    
    # 3. Create Volume
    vol = manager.add_entry(sch.inode, "vol1", is_dir=True, attr=dir_entry_attr)
    assert vol.fs_path == "/cat1/sch1/vol1"

def test_lookup_by_path(manager, dir_entry_attr):
    """Test retrieving an inode using its string path."""
    # Setup
    cat = manager.add_entry(pyfuse3.ROOT_INODE, "finance", is_dir=True, attr=dir_entry_attr)
    sch = manager.add_entry(cat.inode, "reports", is_dir=True, attr=dir_entry_attr)

    # Action
    found_inode = manager.get_inode_by_path("/finance/reports")

    # Assert
    assert found_inode == sch.inode
    assert manager.get_inode_by_path("/non/existent") is None

def test_forget_logic(manager, dir_entry_attr):
    """Test reference counting and eviction."""
    entry = manager.add_entry(pyfuse3.ROOT_INODE, "temp", is_dir=True, attr=dir_entry_attr)
    inode = entry.inode
    
    # Simulate FUSE increasing refcount (lookup)
    manager.increment_lookup_count(inode, 1) # Now refs = 1
    
    # 1. Forget partial count
    manager.forget(inode, 1) # Refs = 0
    assert manager.get_entry(inode) is None
    assert manager.get_inode_by_path("/temp") is None

def test_duplicate_add_returns_existing(manager, dir_entry_attr):
    """Adding the same entry twice should return the same inode."""
    entry1 = manager.add_entry(pyfuse3.ROOT_INODE, "shared", is_dir=True, attr=dir_entry_attr)
    entry2 = manager.add_entry(pyfuse3.ROOT_INODE, "shared", is_dir=True, attr=dir_entry_attr)
    
    assert entry1.inode == entry2.inode
    assert entry1 is entry2

def test_directory_vs_file_attributes(manager, dir_entry_attr, file_entry_attr):
    """Verify default attributes differ for directories and files."""
    # Directory (Catalog)
    d_entry = manager.add_entry(pyfuse3.ROOT_INODE, "d", is_dir=True, attr=dir_entry_attr)
    assert d_entry.attr.st_mode & stat.S_IFDIR
    assert d_entry.attr.st_nlink == 2

    # Realistic file path:
    cat_entry = manager.add_entry(pyfuse3.ROOT_INODE, "c", is_dir=True, attr=dir_entry_attr)
    sch_entry = manager.add_entry(cat_entry.inode, "s", is_dir=True, attr=dir_entry_attr)
    vol_entry = manager.add_entry(sch_entry.inode, "v", is_dir=True, attr=dir_entry_attr)
    f_entry = manager.add_entry(vol_entry.inode, "file.txt", is_dir=False, attr=file_entry_attr)
    
    assert f_entry.attr.st_mode & stat.S_IFREG
    assert f_entry.attr.st_nlink == 1

def test_idempotency_with_attribute_merge(manager, dir_entry_attr):
    """
    If add_entry is called for an existing inode, it should update 
    the attributes (merge) instead of ignoring them.
    """
    # Realistic file path:
    cat_entry = manager.add_entry(pyfuse3.ROOT_INODE, "c", is_dir=True, attr=dir_entry_attr)
    sch_entry = manager.add_entry(cat_entry.inode, "s", is_dir=True, attr=dir_entry_attr)
    vol_entry = manager.add_entry(sch_entry.inode, "v", is_dir=True, attr=dir_entry_attr)
    # 1. Create a file with size 100
    f_attr = InodeEntryAttr(st_mode=0, st_nlink=2, st_size=100,st_ctime=0, st_mtime=1000, st_atime=0, st_uid=0, st_gid=0)
    entry_v1 = manager.add_entry(vol_entry.inode, "data.csv", is_dir=False, attr=f_attr)

    
    assert entry_v1.attr.st_size == 100
    inode_id = entry_v1.inode

    # 2. Simulate readdir finding a newer version (size 200)
    f_attr_v2 = InodeEntryAttr(st_mode=0, st_nlink=2, st_size=200,st_ctime=0, st_mtime=2000, st_atime=0, st_uid=0, st_gid=0)
    entry_v2 = manager.add_entry(vol_entry.inode, "data.csv", is_dir=False, attr=f_attr_v2)

    # Assertions
    assert entry_v2.inode == inode_id, "Inode ID must remain constant"
    assert entry_v2.attr.st_size == 200, "Attributes should be updated in memory"
    assert entry_v2.attr.st_mtime == 2000

def test_type_collision_creates_zombie(manager, dir_entry_attr):
    """
    If a directory is replaced by a file, the old directory inode 
    should become a 'Zombie' (stale) but remain in memory if ref_count > 0.
    """
    # 1. Create a directory and simulate Kernel holding it
    dir_entry = manager.add_entry(pyfuse3.ROOT_INODE, "workspace", is_dir=True, attr=dir_entry_attr)
    manager.increment_lookup_count(dir_entry.inode)  # ref_count = 1
    
    old_inode = dir_entry.inode

    # 2. Replace it with a file (Type Mismatch)
    file_entry = manager.add_entry(pyfuse3.ROOT_INODE, "workspace", is_dir=False, attr=dir_entry_attr)
    
    # Assertions
    assert file_entry.inode != old_inode, "New inode must be created for type change"
    
    # Check Path Map: Should point to the NEW file
    assert manager.get_inode_by_path("/workspace") == file_entry.inode
    
    # Check Old Inode (Zombie Status)
    old_entry_fetched = manager.get_entry(old_inode)
    assert old_entry_fetched is not None, "Old inode must persist because ref_count > 0"
    assert old_entry_fetched.is_stale is True, "Old inode must be marked as stale"
    assert old_entry_fetched.ref_count == 1

def test_zombie_reaping_on_forget(manager, dir_entry_attr, file_entry_attr):
    """
    Verifies that a Zombie inode is physically deleted once 
    the Kernel releases the last reference (forget).
    """
    # Setup: Create zombie
    dir_entry = manager.add_entry(pyfuse3.ROOT_INODE, "temp", is_dir=True, attr=dir_entry_attr)
    manager.increment_lookup_count(dir_entry.inode) # ref=1
    
    # Overwrite to make it stale
    manager.add_entry(pyfuse3.ROOT_INODE, "temp", is_dir=False, attr=file_entry_attr)
    
    # Confirm it still exists
    assert manager.get_entry(dir_entry.inode) is not None
    
    # Action: Kernel forgets it
    manager.forget(dir_entry.inode, 1) # ref=0
    
    # Assert: Should be gone from memory
    assert manager.get_entry(dir_entry.inode) is None

def test_recursive_pruning(manager, dir_entry_attr, file_entry_attr):
    """
    Deleting/Replacing a parent folder should recursively mark 
    children as stale and free up the path map.
    """
    # 1. Build hierarchy: /catalog/schema/volume
    cat = manager.add_entry(pyfuse3.ROOT_INODE, "cat", is_dir=True, attr=dir_entry_attr)
    sch = manager.add_entry(cat.inode, "sch", is_dir=True, attr=dir_entry_attr)
    vol = manager.add_entry(sch.inode, "vol", is_dir=True, attr=dir_entry_attr)
    
    # 2. Hold reference to the deepest child (Volume)
    manager.increment_lookup_count(vol.inode) # vol ref=1
    
    # 3. Replace the TOP level catalog with a file (drastic change)
    # This triggers _prune_subtree starting at 'cat'
    new_file = manager.add_entry(pyfuse3.ROOT_INODE, "cat", is_dir=False, attr=file_entry_attr)
    
    # Assertions
    
    # The path "/cat" should now point to the new file
    assert manager.get_inode_by_path("/cat") == new_file.inode
    
    # The old catalog should be gone (ref=0)
    assert manager.get_entry(cat.inode) is None
    
    # The old schema should be gone (ref=0)
    assert manager.get_entry(sch.inode) is None
    
    # The volume MUST survive because we held it (ref=1), but marked stale
    saved_vol = manager.get_entry(vol.inode)
    assert saved_vol is not None
    assert saved_vol.is_stale is True
    
    # The volume's path should be removed from map (orphan)
    # Note: Depending on implementation, checking path_map for the old path usually returns None
    # because the parent was removed or the key deleted.
    assert manager.get_inode_by_path("/cat/sch/vol") is None

def test_path_construction_edge_cases(manager, dir_entry_attr):
    """Ensure no double slashes in paths."""
    root = manager.get_entry(pyfuse3.ROOT_INODE)
    assert root.fs_path == "/" # Or "" depending on implementation, let's check behavior
    
    # If root.full_path is "/", constructing child shouldn't be "//child"
    child = manager.add_entry(pyfuse3.ROOT_INODE, "child", is_dir=True, attr=dir_entry_attr)
    assert child.fs_path == "/child"
    
    grandchild = manager.add_entry(child.inode, "grand", is_dir=True, attr=dir_entry_attr)
    assert grandchild.fs_path == "/child/grand"

def test_strict_garbage_collection(manager, file_entry_attr):
    """
    Verify that entries are deleted immediately when ref_count hits 0,
    even if they are not stale (Standard eviction).
    """
    entry = manager.add_entry(pyfuse3.ROOT_INODE, "temp_file", is_dir=False, attr=file_entry_attr)
    manager.increment_lookup_count(entry.inode) # ref=1
    
    # Kernel forgets it
    manager.forget(entry.inode, 1)
    
    # Should be deleted to save RAM
    assert manager.get_entry(entry.inode) is None
    assert manager.get_inode_by_path("/temp_file") is None
