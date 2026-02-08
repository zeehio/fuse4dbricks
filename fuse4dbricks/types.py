from enum import Enum

class UcNodeType(Enum):
    """Unity Catalog node types, with the root node"""
    ROOT = 0
    CATALOG = 1
    SCHEMA = 2
    VOLUME = 3
    DIRECTORY = 4
    FILE = 5



UC_NODE_IS_DIR = {
    UcNodeType.ROOT: True,
    UcNodeType.CATALOG: True,
    UcNodeType.SCHEMA: True,
    UcNodeType.VOLUME: True,
    UcNodeType.DIRECTORY: True,
    UcNodeType.FILE: False,
}