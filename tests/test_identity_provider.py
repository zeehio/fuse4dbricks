import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import pytest

from fuse4dbricks.identity.provider import EntraIDAuthProvider

# --- FIXTURES ---


@pytest.fixture
def mock_keyring():
    return MagicMock()


@pytest.fixture
def mock_msal_app():
    with patch("msal.PublicClientApplication") as mock_cls:
        instance = mock_cls.return_value
        # Default behavior: No accounts, no token to start
        instance.get_accounts.return_value = []
        instance.acquire_token_silent.return_value = None
        instance.acquire_token_by_refresh_token.return_value = None
        yield instance


@pytest.fixture
def provider(mock_keyring, mock_msal_app):
    return EntraIDAuthProvider("tenant_id", "client_id", mock_keyring)


# --- TESTS ---


def test_get_token_cached_valid(provider):
    """
    Scenario: Token is in memory and valid.
    Expectation: Returns token immediately without calling MSAL.
    """
    # Setup a valid token in memory
    provider._access_token = "valid_token"
    provider._expires_at = time.time() + 3600  # Expires in 1 hour

    token = provider.get_access_token()

    assert token == "valid_token"
    # Ensure no calls were made to MSAL
    provider.app.acquire_token_silent.assert_not_called()


def test_get_token_force_refresh(provider):
    """
    Scenario: Token is valid, but force_refresh=True.
    Expectation: Ignores cache and calls MSAL.
    """
    provider._access_token = "old_token"
    provider._expires_at = time.time() + 3600

    # Mock successful refresh
    provider.app.get_accounts.return_value = [{"username": "user"}]
    provider.app.acquire_token_silent.return_value = {
        "access_token": "new_token",
        "expires_in": 3600,
    }

    token = provider.get_access_token(force_refresh=True)

    assert token == "new_token"
    provider.app.acquire_token_silent.assert_called_once()


def test_refresh_silently_via_keyring(provider, mock_keyring):
    """
    Scenario: Memory cache empty, but Keyring has a refresh token.
    Expectation: Calls acquire_token_by_refresh_token using the keyring value.
    """
    mock_keyring.get_refresh_token.return_value = "rt_from_keyring"

    provider.app.acquire_token_by_refresh_token.return_value = {
        "access_token": "token_from_rt",
        "refresh_token": "new_rt",
        "expires_in": 3600,
    }

    token = provider.get_access_token()

    assert token == "token_from_rt"
    provider.app.acquire_token_by_refresh_token.assert_called_with(
        "rt_from_keyring", scopes=provider.scopes
    )
    # Verify we updated the keyring with the NEW refresh token
    mock_keyring.save_refresh_token.assert_called_with("new_rt")


def test_fallback_to_device_code(provider):
    """
    Scenario: All silent methods fail.
    Expectation: Initiates Device Code Flow.
    """
    # Mock flow object
    flow_mock = {"message": "Please sign in", "user_code": "123"}
    provider.app.initiate_device_flow.return_value = flow_mock

    # Mock successful login after flow
    provider.app.acquire_token_by_device_flow.return_value = {
        "access_token": "interactive_token",
        "expires_in": 3600,
    }

    # We patch 'print' to keep test output clean
    with patch("builtins.print"):
        token = provider.get_access_token()

    assert token == "interactive_token"
    provider.app.initiate_device_flow.assert_called_once()
    provider.app.acquire_token_by_device_flow.assert_called_with(flow_mock)


# --- CRITICAL CONCURRENCY TEST ---


def test_concurrency_thundering_herd(provider):
    """
    The 'Thundering Herd' Test.
    Simulates 50 threads requesting a token simultaneously when expired.
    Verifies that ONLY ONE request is sent to the backend (MSAL).
    """
    # 1. Setup: Provider has no token (expired state)

    # 2. Mock MSAL to be slow (simulate network latency)
    # This ensures multiple threads hit the lock while the first one is working.
    real_acquire = MagicMock(
        return_value={"access_token": "concurrent_token", "expires_in": 3600}
    )

    def slow_acquire(*args, **kwargs):
        time.sleep(0.1)  # 100ms latency
        return real_acquire()

    # We set up get_accounts so it attempts a silent refresh
    provider.app.get_accounts.return_value = [{"username": "user"}]
    provider.app.acquire_token_silent.side_effect = slow_acquire

    # 3. Execution: Launch 50 threads
    results = []

    def worker():
        return provider.get_access_token()

    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(worker) for _ in range(50)]
        for f in futures:
            results.append(f.result())

    # 4. Assertions

    # All threads must get the valid token
    assert all(t == "concurrent_token" for t in results)

    # CRITICAL: MSAL should be called EXACTLY ONCE.
    # If the lock wasn't working, this would be > 1 (likely 50).
    assert provider.app.acquire_token_silent.call_count == 1
