# Unreleased

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
