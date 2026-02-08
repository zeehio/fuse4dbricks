#!/usr/bin/env python3
import os
import sys
import logging
import argparse
import trio
try:
    import pyfuse3
except ImportError:
    import fuse4dbricks.mock.pyfuse3 as pyfuse3  # type: ignore[no-redef]

from fuse4dbricks.identity.keyring_store import LinuxKeyringManager
from fuse4dbricks.identity.provider import EntraIDAuthProvider
from fuse4dbricks.api.uc_client import UnityCatalogClient
from fuse4dbricks.storage.persistence import DiskPersistence, clear_cache
from fuse4dbricks.fs.inode_manager import InodeManager
from fuse4dbricks.fs.metadata_manager import MetadataManager
from fuse4dbricks.fs.data_manager import DataManager
from fuse4dbricks.fs.operations import UnityCatalogFS

# Default Azure Databricks App ID (Standard Public Client)
DEFAULT_CLIENT_ID = "96df0c21-d705-4e78-2936-2475e72d2459" 

def parse_args():
    parser = argparse.ArgumentParser(description="Mount Databricks Unity Catalog volumes.")
    parser.add_argument("--workspace", required=True, help="https://adb-xxxx.azuredatabricks.net")
    parser.add_argument("--tenant-id", required=True, help="Azure Tenant ID")
    parser.add_argument("--client-id", default=DEFAULT_CLIENT_ID, help="Azure App Client ID")
    parser.add_argument("mountpoint", help="Local directory to mount")
    parser.add_argument("--cache-dir", default="/tmp/db_fuse_cache", help="Local disk cache location")
    parser.add_argument("--clear-cache", action="store_true", help="Clear disk cache on startup")
    parser.add_argument("--debug", action="store_true", help="Enable verbose logging")
    return parser.parse_args()

def setup_logging(debug_mode):
    level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(format='%(asctime)s [%(name)s] %(levelname)s: %(message)s', level=level)
    if not debug_mode:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("pyfuse3").setLevel(logging.INFO)

async def start_fuse(ops, mountpoint, debug_mode):
    mount_options = ["ro", "fsname=fuse4dbricks", "noatime", "default_permissions"]
    if debug_mode:
        mount_options.append("debug")

    try:
        pyfuse3.init(ops, mountpoint, mount_options)
    except Exception as e:
        logging.error(f"Failed to initialize FUSE: {e}")
        sys.exit(1)

    logging.info(f"Mounted at {mountpoint}")
    try:
        await pyfuse3.main()
    except KeyboardInterrupt:
        pass
    finally:
        pyfuse3.close()

async def async_main():
    args = parse_args()
    setup_logging(args.debug)

    mountpoint = os.path.abspath(args.mountpoint)
    if not os.path.isdir(mountpoint):
        logging.error(f"Mountpoint does not exist: {mountpoint}")
        sys.exit(1)

    cache_dir = os.path.abspath(args.cache_dir)
    if args.clear_cache:
        clear_cache(cache_dir)
    os.makedirs(cache_dir, exist_ok=True, mode=0o700)

    # Auth
    logging.info("Initializing Authentication...")
    keyring = LinuxKeyringManager(service_prefix="fuse4dbricks")
    auth_provider = EntraIDAuthProvider(args.tenant_id, args.client_id, keyring)

    try:
        auth_provider.get_access_token()
        logging.info("Authentication successful.")
    except Exception as e:
        logging.critical(f"Authentication failed: {e}")
        sys.exit(1)

    # Init Components
    uc_client = UnityCatalogClient(args.workspace, auth_provider)
    persistence = DiskPersistence(cache_dir, max_size_gb=10,max_age_days=30)


    inode_manager = InodeManager()
    data_manager = DataManager(uc_client, persistence)
    metadata_manager = MetadataManager(uc_client, ttl=30)
    operations = UnityCatalogFS(inode_manager, metadata_manager, data_manager)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(start_fuse, operations, mountpoint, args.debug)


def cli_entry_point():
    try:
        trio.run(async_main)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.critical(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    cli_entry_point()
