# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`fuse4dbricks` is a Python FUSE driver (async, built on `trio` + `pyfuse3`) that mounts a Databricks
Unity Catalog's Volumes as a local POSIX filesystem, talking to the public Databricks REST API (Unity
Catalog API + Files API v2). Not an official Databricks package.

## Commands

Dependencies are managed with `uv` (see `uv.lock`); the dev dependency group is defined in
`pyproject.toml` under `[dependency-groups] dev`.

```bash
# Install dev dependencies (builds the pyfuse3 C extension — see platform note below)
uv sync --group dev

# Run the full test suite (also generates htmlcov/ coverage report, see pytest.ini_options in pyproject.toml)
uv run pytest .

# Run a single test file / test
uv run pytest tests/test_metadata_manager.py
uv run pytest tests/test_metadata_manager.py::test_some_specific_case -v

# Lint (as run in CI — ruff is a dev dependency but is NOT what CI enforces; flake8 is authoritative)
uv run flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
uv run flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

# Type check
uv run mypy fuse4dbricks

# Run the mounted filesystem locally
uv run fuse4dbricks --workspace https://adb-xxxx.azuredatabricks.net /path/to/mountpoint
```

**Platform: Linux only.** `pyfuse3` is a C extension linked against `libfuse3`; there is no Windows build.
On Windows, do all of the above inside WSL (`wsl -d <distro>`), operating on the repo from there. Building
it requires the system packages `libfuse3-dev` + `pkg-config` (Debian/Ubuntu — `fuse3-devel` on
RedHat/Fedora/SUSE); without them `uv sync`/`pip install` fails at the `pyfuse3` build step with
`Package fuse3 was not found in the pkg-config search path`. Running the actual mounted filesystem (not
just the mocked test suite) additionally needs `/dev/fuse` and `fusermount3`/`fusermount` available.

### Test tiers

Most tests are unit tests that mock `UnityCatalogClient`/`AuthProvider`/`DiskPersistence` and use
`pytest-trio` (`@pytest.mark.trio`) for async coverage — these run in plain CI with no external
dependencies.

Two files require a live Databricks workspace and are skipped automatically unless configured (see
`.env.example`):
- `tests/test_uc_client_live.py` — needs `DATABRICKS_HOST`/`DATABRICKS_TOKEN` (+ a writable volume for
  the write-path tests).
- `tests/test_e2e_mount.py` — additionally needs `FUSE4DBRICKS_TEST_VOLUME` and real FUSE support
  (`/dev/fuse` + `fusermount3`/`fusermount`); it mounts the real filesystem and exercises it through the
  kernel (the only tier covering real `readdir`, `getattr`/ENOENT, chunk streaming, read-only
  enforcement). Run with:

  ```bash
  set -a; source .env; set +a
  uv run pytest tests/test_e2e_mount.py -v
  ```

### Skill tooling

Installed via `npx skills` (see README "Development"). `skills-lock.json` is the tracked source of
truth; installed skill content under `.agents/skills/` / `.claude/skills/` is gitignored. After cloning
or pulling a change to the lock file, restore with `npx skills experimental_install`.

## Architecture

### Request flow

`fuse4dbricks/main.py` parses CLI args, sets up cache directories, and starts one `trio` nursery running
concurrently: the FUSE event loop, `DataManager`'s download workers, `DiskPersistence`'s background
maintenance, and a signal handler that turns SIGTERM/SIGINT/SIGHUP into a clean cancel-and-unmount (so
`systemctl stop` doesn't leave a stale mount).

`fs/operations.py` (`UnityCatalogFS`, a `pyfuse3.Operations` subclass) is the single entry point the
kernel calls into. It **dispatches every path** between two namespaces:
- `unity_catalog` — real catalog/schema/volume/file/directory paths, backed by the managers below.
- `auth` — a virtual overlay at `/.auth/` and `/README.txt` (see "Auth overlay" below), owned by
  `AuthManager` and merged into root's `readdir` alongside the real listing.

A single UC path flows through a fixed manager pipeline:

```
operations.py → InodeManager (inode↔path, ref-counting)
             → MetadataManager (attr/dir-listing/permission caches, RAM, TTL-based)
             → UnityCatalogClient (api/uc_client.py — HTTP calls to Databricks)
             → DataManager (file content: RAM LRU → DiskPersistence on-disk chunks → network)
```

- **InodeManager** (`fs/inode_manager.py`): maps kernel inode numbers to `fs_path`s and tracks
  `ref_count` per the kernel's lookup/forget protocol. `add_entry`/`move_inode` handle type changes
  (file↔dir) and rename by pruning/rebuilding subtrees.
- **MetadataManager** (`fs/metadata_manager.py`): caches attributes, directory listings and permission
  checks in RAM with per-node-type TTLs (`--metadata-cache-ttl-sec` for files/dirs,
  `--metadata-cache-ttl-catalog-sec` for catalog/schema, `--metadata-cache-ttl-negative-sec` for
  not-found results). Uses `InflightCoalescer` (`fs/utils.py`) everywhere to collapse concurrent
  identical requests into one API call ("thundering herd" protection). The **positive** attribute/dir
  cache is global (shared across all users of the mount — see README "Known limitations"), but the
  **negative** (not-found) cache and the **permission** cache are keyed by resolved *principal*, not
  uid, so one user's 404/denial is never leaked to or reused by another. `invalidate()` also proactively
  tells the *kernel* to drop its own attr/dentry cache (`pyfuse3.invalidate_entry_async` /
  `invalidate_inode`), which is necessary — the kernel caches independently of this process.
- **UnityCatalogClient** (`api/uc_client.py`): async httpx client. Centralizes retry-with-backoff+jitter
  on 429/5xx and connection errors, automatic token refresh on 401 (via `AuthProvider`), and maps HTTP
  status codes to typed exceptions (`api/errors.py`: `UcNotFound`, `UcPermissionDenied`, `UcConflict`,
  `UcRateLimited`, `UcPreconditionFailed`, ...), which `operations.py` maps back to `errno` values.
  Catalog/schema/volume listing uses the Unity Catalog API; file/directory content uses the Files API
  (`/api/2.0/fs/files`, `/api/2.0/fs/directories`); uploads go through `databricks-sdk`'s
  `WorkspaceClient` so multipart upload kicks in transparently for files >5GB.
- **DataManager** (`fs/data_manager.py`): reads file content in fixed 8MB chunks, keyed by
  `(fs_path, chunk_id, mtime, gen)`. Order of lookup: RAM `RamCache` (LRU) → `DiskPersistence` on-disk
  chunk → network via a pool of background `DownloadScheduler` workers. `mtime` guards against the
  remote file changing (sent as `If-Unmodified-Since`); `gen` is a local per-path epoch bumped by
  `invalidate_path()` on every local write, needed because server `mtime` only has 1-second resolution
  and can't tell apart two same-second writes. Reads trigger read-ahead prefetch of the next ~10 chunks.
- **DiskPersistence** (`storage/persistence.py`): on-disk chunk cache, sharded by
  `sha256(fs_path)[:2]/chunk_index//1000`, with lazy LRU eviction (`--disk-cache-gb`) and an hourly
  age-based sweep (`--disk-cache-max-days`).

### Writes

Unity Catalog's Files API has no partial-write or server-side-move primitive, which shapes the write
path:
- `open()`/`write()` buffer everything into a local tempfile (`fs/write_buffer.py`, in
  `{cache-dir}/writes/`), so memory use is O(1) regardless of file size. A writable open **without**
  `O_TRUNC` on a non-empty file first downloads the full existing content into the buffer (chunked), so
  partial writes don't truncate the rest of the file.
- The whole buffer is uploaded on `flush()` (i.e. on `close()`, so a failed upload surfaces as a `close`
  error) and again defensively on `release()` if still dirty.
- `rename()` has no server primitive: it downloads the source, uploads it under the new name, then
  deletes the original. Directory rename is unsupported and returns `EXDEV` (so `mv` falls back to
  recursive copy).
- After any mutation, both `MetadataManager.invalidate()` (attr/dir caches, incl. kernel-side) and
  `DataManager.invalidate_path()` (chunk cache generation bump) must be called so subsequent reads see
  fresh data — grep existing call sites in `operations.py` before adding a new mutating op.

### Auth

Two layers, both in `auth_manager`/`auth/provider.py`:
- **Token resolution** (`AuthProvider`/`DatabricksUnifiedAuthProvider`): per-uid token cache. Default
  ("unified auth") resolves a token from the *requesting process's* `DATABRICKS_TOKEN` env var or its
  `.databrickscfg` profile by reading `/proc/{pid}/environ`, which needs `CAP_SYS_PTRACE`/root when
  `--allow-other` serves other users. `--single-principal` instead resolves one token from
  fuse4dbricks's own process identity for every uid — the mode to use on single-user/WSL setups where
  request uids are unreliable (e.g. Windows Explorer via WSL).
- **Auth overlay** (`fs/auth_manager.py`): a virtual `/.auth/personal_access_token` write-only file and
  `/README.txt` / `/.auth/README.txt` read-only files, overlaid onto the mount root regardless of
  Unity Catalog state. Writing a PAT there calls `AuthProvider.set_access_token` +
  `MetadataManager.reauthorize()`. This is the fallback path when unified auth can't resolve a token.

Permission checks (`MetadataManager.check_access`) call Unity Catalog's effective-permissions API per
securable (catalog: `USE_CATALOG`, schema: `USE_SCHEMA`, volume: `READ_VOLUME`/`WRITE_VOLUME`) — POSIX
`st_uid`/`st_gid`/`st_mode` on inodes are cosmetic only and never gate access (see README "Known
limitations").

### Securable filtering

`fs/securable_filter.py` (`SecurableFilter`) implements `--securable-allowlist`/`--securable-denylist`
as a pure path-prefix predicate consulted in `operations.py`'s `lookup`/`readdir`/`create`/`rename` —
independent of, and layered on top of, the real Unity Catalog permission checks above. Deny always wins;
an allowlisted securable's ancestors stay navigable so it's reachable. The auth overlay is exempt from
filtering.

### Concurrency conventions

The whole codebase is single-process `trio`-async; there is no threading except
`trio.to_thread.run_sync` for blocking file I/O. Recurring patterns to follow when touching these
managers:
- **Request coalescing**: any cache-miss path that hits the network wraps the fetch in
  `InflightCoalescer.join_or_lead()`/`notify_done()` (see `fs/utils.py`) so concurrent identical requests
  share one in-flight call instead of stampeding the API.
- **Cache invalidation must reach the kernel**, not just this process's dicts — see
  `MetadataManager._invalidate_kernel_cache`.
- **Lookup-count discipline**: every FUSE reply that hands the kernel an inode (`lookup`, `create`,
  `readdir` entries) must pair with an `increment_lookup_count`, mirrored by a `forget()` on failure
  paths before the kernel would send its own `forget`.

## Agent skills

### Issue tracker

GitHub Issues (`gh` CLI), repo `zeehio/fuse4dbricks`. See `docs/agents/issue-tracker.md`.

### Domain docs

Single-context: `CONTEXT.md` + `docs/adr/` at repo root, created lazily. See `docs/agents/domain.md`.
