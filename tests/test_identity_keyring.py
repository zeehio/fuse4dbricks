import ctypes
from unittest.mock import patch

import pytest

# We need to patch ctypes.util.find_library and ctypes.CDLL BEFORE importing the module
# to prevent it from crashing on non-Linux systems or containers missing libkeyutils.
with (
    patch("ctypes.util.find_library", return_value="mock_libkeyutils.so"),
    patch("ctypes.CDLL") as MockCDLL,
):

    # Import the module under test
    from fuse4dbricks.identity.keyring_store import LinuxKeyringManager

    # We get a reference to the mocked library object that the module uses
    mock_lib = MockCDLL.return_value


@pytest.fixture
def keyring():
    # Reset mocks before each test
    mock_lib.reset_mock()
    return LinuxKeyringManager(service_prefix="test_fuse")


# --- BASIC TESTS ---


def test_save_refresh_token(keyring):
    """
    Verifies that save calls add_key with correct arguments.
    """
    mock_lib.add_key.return_value = 123  # Success ID

    keyring.save_refresh_token("my_secret_token")

    # add_key(type, desc, payload, plen, ring)
    # verify it was called with "user" type and our description
    args, _ = mock_lib.add_key.call_args
    assert args[0] == b"user"
    assert args[1] == b"test_fuse:refresh_token"
    assert args[2] == b"my_secret_token"
    assert args[3] == len(b"my_secret_token")
    assert args[4] == -4  # KEY_SPEC_USER_KEYRING


def test_get_refresh_token_found(keyring):
    """
    Happy path: Key exists and is read successfully.
    """
    # 1. request_key returns a valid ID
    mock_lib.request_key.return_value = 999

    # 2. First keyctl_read returns size (5 bytes)
    # 3. Second keyctl_read writes data and returns size
    def read_side_effect(key_id, buffer, buflen):
        if buffer is None:
            return 5  # "hello" length
        else:
            # Simulate kernel writing to the C buffer
            ctypes.memmove(buffer, b"hello", 5)
            return 5

    mock_lib.keyctl_read.side_effect = read_side_effect

    token = keyring.get_refresh_token()

    assert token == "hello"
    assert mock_lib.keyctl_read.call_count == 2


def test_get_refresh_token_not_found(keyring):
    """
    Verifies None is returned if key doesn't exist.
    """
    mock_lib.request_key.return_value = -1
    assert keyring.get_refresh_token() is None


# --- CRITICAL CONCURRENCY TESTS ---


def test_get_refresh_token_concurrency_retry(keyring):
    """
    CRITICAL: Simulates the TOCTOU Race Condition.
    The key grows in size between the size-check and the read.
    Verifies the loop retries and succeeds.
    """
    mock_lib.request_key.return_value = 999

    # We simulate a sequence of calls to keyctl_read:
    # Call 1 (Size Check): Returns 5 bytes.
    # Call 2 (Read Attempt): Returns 10 bytes (Simulating key changed in background).
    # -- Code triggers retry --
    # Call 3 (Size Check Retry): Returns 10 bytes.
    # Call 4 (Read Attempt Retry): Returns 10 bytes (Success).

    def side_effect(key_id, buffer, buflen):
        # Call 1: check size (buffer is None)
        if buffer is None and mock_lib.keyctl_read.call_count == 1:
            return 5

        # Call 2: read (buffer provided) -> Race Condition!
        if buffer is not None and mock_lib.keyctl_read.call_count == 2:
            # We return a value larger than buflen (5) to signal growth
            return 10

        # Call 3: Retry check size
        if buffer is None and mock_lib.keyctl_read.call_count == 3:
            return 10

        # Call 4: Retry read
        if buffer is not None and mock_lib.keyctl_read.call_count == 4:
            ctypes.memmove(buffer, b"helloworld", 10)
            return 10

        return -1  # Should not reach here

    mock_lib.keyctl_read.side_effect = side_effect

    token = keyring.get_refresh_token()

    assert token == "helloworld"
    assert mock_lib.keyctl_read.call_count == 4  # Proves retry happened


def test_get_refresh_token_max_retries_exceeded(keyring):
    """
    Verifies that if the key keeps changing forever, we eventually give up (return None)
    instead of looping infinitely.
    """
    mock_lib.request_key.return_value = 999

    # Always return a size mismatch
    def fail_forever(key_id, buffer, buflen):
        if buffer is None:
            return 5
        else:
            return 10  # Always larger than buffer

    mock_lib.keyctl_read.side_effect = fail_forever

    token = keyring.get_refresh_token()

    assert token is None
    # 3 attempts * 2 calls each = 6 calls total
    assert mock_lib.keyctl_read.call_count == 6


def test_clear_all(keyring):
    """
    Verifies unlinking/deleting the key.
    """
    mock_lib.request_key.return_value = 555
    mock_lib.keyctl_unlink.return_value = 0

    keyring.clear_all()

    mock_lib.keyctl_unlink.assert_called_with(555, -4)
