# 0.6.2 (2026-06-12)

- Security: A user could use DATABRICKS_CONFIG_PROFILE to make fuse4dbricks read any user profile if it ran as root.

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
