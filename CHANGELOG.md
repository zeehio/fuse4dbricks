# 0.7.1 (2026-06-17)

- Unmount cleanly on `SIGTERM`/`SIGHUP` (e.g. `systemctl stop`). trio only turns
  `SIGINT` (Ctrl-C) into a clean shutdown; other termination signals killed the
  process before `pyfuse3.close()` ran, leaving the kernel with a stale mount
  that showed up as `d?????????` in `ls`/`df`. All termination signals now run
  the same orderly unwind that unmounts the filesystem.

# 0.7.0 (2026-06-17)

- Fix files written from a Windows application through WSL being uploaded at
  the correct size but filled with zeros. A path-based `setattr(size)` (which
  WSL's 9P layer emits instead of an `ftruncate` carrying the handle) now
  resizes the open writer's buffer instead of reading the not-yet-uploaded
  remote object. A short read while preserving bytes in a true path-based
  truncate now fails with `EIO` instead of silently zero-filling.
- Add `--single-principal`: use one Databricks identity for every request
  regardless of the requesting uid, resolving the token from the fuse4dbricks
  process's own credentials (its environment and the launching user's
  `~/.databrickscfg`) or from a token written to `.auth`. Intended for
  single-user machines; combine with `--allow-other` so a request carrying an
  undocumented uid (e.g. the Windows file explorer over WSL) is served by the
  one token without fuse4dbricks needing root to read the requesting process.
- Add `--securable-allowlist` and `--securable-denylist` to restrict which
  Unity Catalog securables are listable and accessible. Each takes a
  comma-separated list of dotted securables (`catalog`, `catalog.schema` or
  `catalog.schema.volume`). A denylist hides the listed securables and
  everything beneath them; an allowlist exposes only the listed securables
  (their parent catalog/schema stay navigable). Deny wins when both are given.
  This is a mount-wide visibility filter, not a per-user boundary, and does not
  replace Unity Catalog's own permission checks.
- Implement `rename` and `setattr`. Files can be moved/renamed (via
  copy-then-delete, since the Files API has no server-side move) and truncated;
  directory rename returns `EXDEV` so `mv` falls back to a recursive copy.
  `chmod`/`chown`/`utimes` are accepted as no-ops (Unity Catalog has no POSIX
  metadata) so tools such as `tar` and `cp -p` don't fail.
- Implement `statfs` so `df` works and tools that pre-check free space before
  writing succeed. A large synthetic capacity is reported (Unity Catalog
  exposes no quota), with zero availability on a `--read-only` mount.
- Fix silent data loss on writes: opening a file `O_WRONLY` without `O_TRUNC`
  now pre-loads the existing content so untouched bytes are preserved, and
  small buffered writes are flushed before upload (they could previously be
  uploaded as zero bytes).
- Security: validate that the default `~/.databrickscfg` is a regular file
  owned by the requesting uid, the same way the env-supplied config path is
  already checked. Prevents a symlink planted by one user (under `--allow-other`
  + root) from making root read and cache another user's token.
- Fix `create` not incrementing the kernel lookup count: it returns an entry
  the kernel will later `forget`, so the new inode could be freed while still
  referenced.
- Map a 412 (the file changed under an `If-Unmodified-Since` read) to `ESTALE`
  instead of `EIO`, so a mid-read modification is reported accurately.
- Honor the HTTP-date form of the `Retry-After` header (previously only the
  integer-seconds form was used).
- Refresh the access token on a 401 during an SDK file upload; the upload path
  bypassed the request-layer 401 retry and so failed permanently on an expired
  token.
- Don't let an inode-side `st_size` mutation (from a write or truncate) corrupt
  the shared metadata cache: `add_entry` now stores a copy of the attributes.
- A coalescing `lookup_child` follower that wakes to a cache miss now surfaces a
  retryable `EAGAIN` instead of running its own lookup and notifying a coalescer
  key it does not own.
- Performance: stop running a blocking `os.makedirs` on every chunk read; cache
  shard directories are created only on the write path, off the event loop.
- Security: A user could use DATABRICKS_CONFIG_PROFILE to make fuse4dbricks read any user profile if it ran as root.
- Retry transient API failures (429 rate limits, 5xx server errors and
  connection errors) with exponential backoff and jitter, honoring the
  `Retry-After` header on 429 responses.
- Robustness: a failed chunk download no longer escapes the download worker.
  Previously the exception propagated to the trio nursery and unmounted the
  whole filesystem for every user; now the worker logs and survives, and the
  affected read fails in isolation with `EIO`.
- Fix `getattr` swallowing `ENOENT` (deleted files) and other domain errors
  into `EIO`; already-mapped errnos now propagate and `getattr` updates the
  inode attributes in place so replies reflect the current size/mtime.
- Fix writes to read-only Unity Catalog paths returning `EIO` instead of
  `EACCES` (the catch-all `except` was masking the intended errno). `read`
  likewise preserves already-mapped errnos.
- Fix `lookup` not incrementing the kernel lookup count, which could let
  `forget` free a still-referenced inode.
- Refactor: the auth virtual entries overlaid at the root (`.auth`,
  `README.txt`) are now defined by `AuthManager.root_overlay()` instead of
  being hardcoded in `readdir`, removing the names/offset coupling. Dead,
  type-confused root branch in `AuthManager.list_directory` removed.
- Security: don't turn transient authorization failures into a permanent
  `EACCES`. A rate limit or server outage during a permission check (and a
  coalescing follower that woke to no cached decision) previously looked like
  "permission denied"; transient failures now surface a retryable `EAGAIN`,
  reserving `EACCES` for genuine denials.
- Re-evaluate cached identity when a user's token changes: writing a new token
  refreshes the cached principal and drops that user's cached permission grants
  only if the principal actually changed.
- Cache catalog/schema permission grants with the longer catalog TTL (volume
  grants keep the shorter TTL for faster revocation).
- Key the permission cache by principal (not uid) and drop the cached principal
  whenever a token is invalidated (auth-file write or 401), so a uid switching
  tokens to a different principal can never read the previous principal's
  cached grants.
- Add a short-lived, per-principal negative (not-found) cache so repeated
  lookups of missing paths are answered without re-hitting Unity Catalog, and a
  coalescing follower can tell "genuinely absent" (ENOENT) from "the lookup
  failed" (retryable). TTL via `--metadata-cache-ttl-negative-sec` (5s default).
- Surface a retryable `EAGAIN` (instead of a permanent `EACCES`) when identity
  resolution fails transiently and a coalescing follower wakes to no cached
  principal.
- Clear the listing principal's stale negative entries when a directory is
  listed, so a freshly-created file shown by `ls` is no longer reported as
  missing by a subsequent `stat` until the negative TTL expires.
- Fix a `lookup` race where a `forget` arriving during the `getattr` await
  could free a still-referenced inode (spurious `ENOENT`); the inode is now
  pinned before the await.
- Robustness: read `/proc/<pid>/environ` as bytes so a single non-UTF-8
  variable no longer discards the requesting process's whole environment.
- CLI: replace the inert `--unified-auth` flag (it defaulted to on and could
  never be turned off) with `--no-unified-auth`.
- Raise a clear error for an unknown auth inode instead of an
  `UnboundLocalError`; drop a stray `print(traceback)` in the directory-listing
  error path in favor of `logger.exception`.
- Document known limitations: the attribute cache is global (names, sizes and
  modification times may leak between users, but never file contents), users
  map to tokens 1-to-1, and reported file owner/group (`st_uid`/`st_gid`) are
  meaningless.
- Improve test coverage

# 0.6.1 (2026-05-07)

- Fix type issue
- Depend on pyfuse3.4.2 to make sure 3.4.1 is not used (it's yanked)

# 0.6.0 (2026-05-07)

- Allow setting long access tokens (when token splits in more than one
  write syscall).
- Add Databricks Unified Auth support (Needs root permissions if running multiuser).


# 0.5.3 (2026-02-24)

- Improved documentation
- Fixed tests

# 0.5.2 (2026-02-24)

- Improved error handling.

# 0.5.1 (2026-02-23)

- Extended README.
- Better handling closing of running services.

# 0.5.0 (2026-02-20)

- Initial release
