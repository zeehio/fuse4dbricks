"""
Secure storage for Refresh Tokens using the Linux Kernel Keyring.
Includes concurrency protection and explicit C-type definitions.
"""

import ctypes
import ctypes.util
import logging

logger = logging.getLogger(__name__)

# Load the keyutils library
libkeyutils = ctypes.util.find_library("keyutils")
if not libkeyutils:
    raise ImportError(
        "libkeyutils.so not found. Please install keyutils-libs (RPM) or libkeyutils-dev (Debian)."
    )

keyutils = ctypes.CDLL(libkeyutils)

# --- CTYPES DEFINITIONS (Crucial for 64-bit stability) ---

# key_serial_t add_key(const char *type, const char *description, const void *payload, size_t plen, key_serial_t ringid);
keyutils.add_key.argtypes = [
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_void_p,
    ctypes.c_size_t,
    ctypes.c_int32,
]
keyutils.add_key.restype = ctypes.c_int32

# key_serial_t request_key(const char *type, const char *description, const char *callout_info, key_serial_t destringid);
keyutils.request_key.argtypes = [
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_int32,
]
keyutils.request_key.restype = ctypes.c_int32

# long keyctl_read(key_serial_t key, char *buffer, size_t buflen);
keyutils.keyctl_read.argtypes = [ctypes.c_int32, ctypes.c_char_p, ctypes.c_size_t]
keyutils.keyctl_read.restype = ctypes.c_long

# long keyctl_unlink(key_serial_t key, key_serial_t ring);
keyutils.keyctl_unlink.argtypes = [ctypes.c_int32, ctypes.c_int32]
keyutils.keyctl_unlink.restype = ctypes.c_long

# Linux Keyring Constants
KEY_SPEC_USER_KEYRING = -4  # Current user's keyring


class LinuxKeyringManager:
    """
    Interfaces with the Linux Kernel Keyring to store secrets in protected memory.
    """

    def __init__(self, service_prefix="dbfuse"):
        """
        :param service_prefix: Prefix to identify keys belonging to this app.
        """
        self.prefix = service_prefix

    def _get_key_desc(self, name):
        return f"{self.prefix}:{name}"

    def save_refresh_token(self, token_value):
        """
        Adds or updates a 'user' type key in the user keyring.
        """
        desc = self._get_key_desc("refresh_token")
        val_bytes = token_value.encode("utf-8")

        # add_key handles atomic updates/replacements internally in the kernel
        res = keyutils.add_key(
            b"user",
            desc.encode("utf-8"),
            val_bytes,
            len(val_bytes),
            KEY_SPEC_USER_KEYRING,
        )
        if res == -1:
            raise OSError("Failed to add key to Linux Keyring.")
        return res

    def get_refresh_token(self):
        """
        Requests and reads the key payload from the keyring.
        Handles Time-Of-Check to Time-Of-Use (TOCTOU) race conditions via retry loop.
        """
        desc = self._get_key_desc("refresh_token")

        # Request the key ID (thread-safe kernel look-up)
        key_id = keyutils.request_key(
            b"user", desc.encode("utf-8"), None, KEY_SPEC_USER_KEYRING
        )
        if key_id == -1:
            return None

        # Retry loop to handle the rare case where the key is updated
        # (changing its size) between the size check and the read.
        for attempt in range(3):
            # 1. Determine payload size
            # Passing NULL (0) returns the size needed
            required_len = keyutils.keyctl_read(key_id, None, 0)
            if required_len < 0:
                return None

            # 2. Create buffer of correct size
            buffer = ctypes.create_string_buffer(required_len)

            # 3. Read the actual payload
            bytes_read = keyutils.keyctl_read(key_id, buffer, required_len)

            # 4. Consistency Check
            if bytes_read < 0:
                return None  # Read error

            if bytes_read > required_len:
                # The key grew in size between step 1 and step 3.
                # The buffer was too small, so the kernel likely truncated it
                # or we just want to be safe and re-read properly.
                logger.debug(
                    f"Key size changed during read (Attempt {attempt+1}/3). Retrying..."
                )
                continue

            # Success
            return buffer.raw[:bytes_read].decode("utf-8")

        logger.warning("Failed to read consistent key payload after 3 attempts.")
        return None

    def clear_all(self):
        """
        Unlinks the key from the user keyring.
        """
        desc = self._get_key_desc("refresh_token")
        key_id = keyutils.request_key(
            b"user", desc.encode("utf-8"), None, KEY_SPEC_USER_KEYRING
        )
        if key_id != -1:
            keyutils.keyctl_unlink(key_id, KEY_SPEC_USER_KEYRING)
