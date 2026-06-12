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
