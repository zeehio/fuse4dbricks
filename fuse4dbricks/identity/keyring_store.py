"""
Secure storage for Refresh Tokens using the Linux Kernel Keyring.
"""
import ctypes
import ctypes.util

# Load the keyutils library
libkeyutils = ctypes.util.find_library('keyutils')
if not libkeyutils:
    raise ImportError("libkeyutils.so not found. Please install keyutils-libs (RPM) or libkeyutils-dev (Debian).")

keyutils = ctypes.CDLL(libkeyutils)

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
        val_bytes = token_value.encode('utf-8')
        
        # add_key(type, description, payload, plen, ring_id)
        # Returns key_serial_t (int) on success, -1 on failure
        res = keyutils.add_key(b"user", desc.encode('utf-8'), 
                               val_bytes, len(val_bytes), 
                               KEY_SPEC_USER_KEYRING)
        if res == -1:
            raise OSError("Failed to add key to Linux Keyring.")
        return res

    def get_refresh_token(self):
        """
        Requests and reads the key payload from the keyring.
        """
        desc = self._get_key_desc("refresh_token")
        
        # Request the key ID
        key_id = keyutils.request_key(b"user", desc.encode('utf-8'), None, KEY_SPEC_USER_KEYRING)
        if key_id == -1:
            return None

        # Determine payload size
        # passing NULL buffer returns the size needed
        buffer_len = keyutils.keyctl_read(key_id, None, 0)
        if buffer_len < 0:
            return None

        # Read the actual payload
        buffer = ctypes.create_string_buffer(buffer_len)
        read_len = keyutils.keyctl_read(key_id, buffer, buffer_len)
        
        return buffer.raw[:read_len].decode('utf-8') if read_len > 0 else None

    def clear_all(self):
        """
        Unlinks the key from the user keyring.
        """
        desc = self._get_key_desc("refresh_token")
        key_id = keyutils.request_key(b"user", desc.encode('utf-8'), None, KEY_SPEC_USER_KEYRING)
        if key_id != -1:
            keyutils.keyctl_unlink(key_id, KEY_SPEC_USER_KEYRING)

