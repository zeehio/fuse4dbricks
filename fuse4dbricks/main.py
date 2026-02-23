#!/usr/bin/env python3
import argparse
import logging
import os
import traceback
import sys

import trio

try:
    import pyfuse3
except ImportError:
    import fuse4dbricks.mock.pyfuse3 as pyfuse3  # type: ignore[no-redef]

from fuse4dbricks.api.uc_client import UnityCatalogClient
from fuse4dbricks.fs.data_manager import DataManager
from fuse4dbricks.fs.inode_manager import InodeManager
from fuse4dbricks.fs.metadata_manager import MetadataManager
from fuse4dbricks.fs.auth_manager import AuthManager
from fuse4dbricks.fs.operations import UnityCatalogFS
from fuse4dbricks.auth.provider import AuthProvider
from fuse4dbricks.auth.entra_id import EntraIDPublicAuthProvider
from fuse4dbricks.storage.persistence import DiskPersistence, clear_cache

logger = logging.getLogger(__name__)

def _is_system_account() -> bool:
    try:
        uid = os.geteuid()
        return uid < 1000
    except Exception as exc:
        logger.error("_is_system_account failed to determine if uid is a system account, assuming it is a user.\nException: %s\n%s", exc, traceback.format_exc())
        return False

def _get_default_cache_dir():
    # Assume POSIX system. if uid is root return /var/cache/fuse4dbricks else return something xdg compliant else $HOME/.cache/fuse4dbricks
    if _is_system_account():
        return "/var/cache/fuse4dbricks"
    xdg_cache_home = os.getenv("XDG_CACHE_HOME")
    if xdg_cache_home:
        return os.path.join(xdg_cache_home, "fuse4dbricks")
    return os.path.expanduser("~/.cache/fuse4dbricks")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Mount Databricks Unity Catalog volumes."
    )
    parser.add_argument(
        "--workspace", default="", help="https://adb-xxxx.azuredatabricks.net"
    )
    parser.add_argument("--tenant-id", help="Azure Tenant ID (required for device auth)", required=False, default="")
    parser.add_argument(
        "--client-id", default="", required=False, help="Azure App Client ID (required for device auth)"
    )
    parser.add_argument("mountpoint", help="Local directory to mount")
    parser.add_argument(
        "--disk-cache-dir", default=_get_default_cache_dir(), help="Local disk cache location"
    )
    
    parser.add_argument("--allow-other", action="store_true",
        help="Use -o allow_other only if /etc/fuse.conf has user_allow_other"
    )

    parser.add_argument(
        "--clear-cache", action="store_true", help="Clear disk cache on startup"
    )
    parser.add_argument(
        "--disk-cache-gb",
        type=float,
        default=10,
        help="Maximum disk cache size in GB",
    )
    parser.add_argument(
        "--disk-cache-max-days",
        type=int,
        default=30,
        help="Maximum age of disk cache entries in days",
    )
    parser.add_argument(
        "--ram-cache-mb",
        type=int,
        default=512,
        help="In-memory data cache size in MB",
    )
    parser.add_argument(
        "--metadata-cache-ttl-sec",
        type=int,
        default=30,
        help="Default TTL for metadata cache entries in seconds",
    )
    parser.add_argument(
        "--metadata-cache-ttl-catalog-sec",
        type=int,
        default=600,
        help="TTL for catalog metadata cache entries in seconds",
    )
    parser.add_argument(
        "--metadata-cache-max-entries",
        type=int,
        default=20000,
        help="Maximum number of metadata cache entries",
    )
    parser.add_argument("--debug", action="store_true", help="Enable verbose logging")
    return parser.parse_args()


def setup_logging(debug_mode):
    level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", level=level
    )
    if not debug_mode:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("pyfuse3").setLevel(logging.INFO)


async def start_fuse(ops, mountpoint, allow_other, debug_mode, stopped_evt: trio.Event | None = None):
    mount_options = ["fsname=fuse4dbricks", "noatime"]
    if allow_other:
        mount_options.append("allow_other")
    if debug_mode:
        mount_options.append("debug")

    try:
        pyfuse3.init(ops, mountpoint, mount_options)
    except Exception as e:
        logging.error(f"Failed to initialize FUSE: {e}")
        raise  # allow nursery to handle cancellation/propagation

    logging.info(f"Mounted at {mountpoint}")
    try:
        await pyfuse3.main()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            pyfuse3.close()
        finally:
            if stopped_evt is not None:
                stopped_evt.set()


async def async_main():
    args = parse_args()
    setup_logging(args.debug)

    mountpoint = os.path.abspath(args.mountpoint)
    if not os.path.isdir(mountpoint):
        logging.error(f"Mountpoint does not exist: {mountpoint}")
        sys.exit(1)

    root_cache_dir = os.path.abspath(args.disk_cache_dir)
    os.makedirs(root_cache_dir, exist_ok=True, mode=0o700)
    os.chmod(root_cache_dir, mode=0o700)
    # auth cache and data cache
    auth_cache_dir = os.path.join(root_cache_dir, "auth")
    data_cache_dir = os.path.join(root_cache_dir, "data")
    if args.clear_cache:
        clear_cache(data_cache_dir)
    os.makedirs(auth_cache_dir, exist_ok=True, mode=0o700)
    os.chmod(auth_cache_dir, mode=0o700)
    os.makedirs(data_cache_dir, exist_ok=True, mode=0o700)
    os.chmod(data_cache_dir, mode=0o700)

    if args.workspace != "":
        workspace = args.workspace
    else:
        workspace = os.getenv("DATABRICKS_HOST")
        if workspace is None:
            logging.critical("Mising databricks workspace. Either pass --workspace or set DATABRICKS_HOST to 'https://adb-xxxx.azuredatabricks.net'")
            sys.exit(1)

    # Auth
    logging.info("Initializing Authentication...")
    external_provider = None
    if args.tenant_id and args.client_id:
        external_provider = EntraIDPublicAuthProvider(tenant_id=args.tenant_id, client_id = args.client_id, cache_dir=auth_cache_dir)

    auth_provider: AuthProvider = AuthProvider(provider=external_provider)
    # Init Components
    uc_client = UnityCatalogClient(workspace, auth_provider)
    persistence = DiskPersistence(data_cache_dir, max_size_gb=args.disk_cache_gb, max_age_days=args.disk_cache_max_days)

    inode_manager = InodeManager()
    data_manager = DataManager(uc_client, persistence, ram_cache_mb=args.ram_cache_mb)
    metadata_manager = MetadataManager(uc_client, ttl=args.metadata_cache_ttl_sec, max_entries=args.metadata_cache_max_entries, ttl_catalog=args.metadata_cache_ttl_catalog_sec)
    auth_manager = AuthManager(uc_client, auth_provider, workspace=workspace)
    operations = UnityCatalogFS(inode_manager, metadata_manager, data_manager, auth_manager)
    stopped_evt = trio.Event()
    try:
        async with trio.open_nursery() as nursery:
            persistence.run_services(nursery)
            data_manager.run_services(nursery)
            nursery.start_soon(start_fuse, operations, mountpoint, args.allow_other, args.debug, stopped_evt)

            await stopped_evt.wait()
            nursery.cancel_scope.cancel()

    except ExceptionGroup as eg:
        # eg is the top-level ExceptionGroup
        logger.error("Top-level group message: %s", eg)
        for i, exc in enumerate(eg.exceptions, 1):
            logger.error("[%d] type=%s -> %s\n%s", i, type(exc).__name__, exc, traceback.format_exc())
    finally:
        data_manager.close()
        await uc_client.close()


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
