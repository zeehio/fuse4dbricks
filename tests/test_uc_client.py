import pytest
import httpx
from unittest.mock import MagicMock, AsyncMock, patch, ANY
from email.utils import formatdate
from datetime import datetime, timezone

from fuse4dbricks.api.uc_client import UnityCatalogClient

# --- FIXTURES ---

@pytest.fixture
def mock_auth_provider():
    """Mocks the EntraIDAuthProvider."""
    provider = MagicMock()
    provider.get_access_token.return_value = "fake_token_123"
    return provider

@pytest.fixture
def client(mock_auth_provider):
    """
    Creates the UnityCatalogClient but replaces the internal httpx client 
    with an AsyncMock to prevent real network calls.
    """
    uc_client = UnityCatalogClient("https://test-workspace.net", mock_auth_provider)
    
    # Replace the real httpx client with a Mock
    uc_client.client = AsyncMock(spec=httpx.AsyncClient)
    
    # Configure build_request to return a dummy object (needed for client.send)
    uc_client.client.build_request.return_value = MagicMock()
    
    return uc_client

# --- TESTS: AUTHENTICATION & RETRIES ---

@pytest.mark.trio
async def test_request_success_200(client, mock_auth_provider):
    """Verify standard request flow: Auth header injection + Success."""
    # Setup Mock Response
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"key": "value"}
    client.client.send.return_value = mock_response

    # Action
    result = await client._request("GET", "/test")

    # Assertions
    assert result == {"key": "value"}
    
    # Verify Auth Token was fetched
    mock_auth_provider.get_access_token.assert_called_with()
    
    # Verify Header Injection
    call_kwargs = client.client.build_request.call_args[1]
    assert call_kwargs["headers"]["Authorization"] == "Bearer fake_token_123"

@pytest.mark.trio
async def test_request_401_retry_success(client, mock_auth_provider):
    """
    Critical: Verify that a 401 triggers a token refresh and a retry.
    """
    # Setup Mock: First call -> 401, Second call -> 200
    response_401 = MagicMock(spec=httpx.Response)
    response_401.status_code = 401
    
    response_200 = MagicMock(spec=httpx.Response)
    response_200.status_code = 200
    response_200.json.return_value = {"success": True}

    # side_effect allows returning different values on consecutive calls
    client.client.send.side_effect = [response_401, response_200]

    # Action
    result = await client._request("GET", "/test")

    # Assertions
    assert result == {"success": True}
    
    # Verify we called send twice
    assert client.client.send.call_count == 2

    # Verify we forced a refresh on the provider
    mock_auth_provider.get_access_token.assert_any_call(force_refresh=True)


@pytest.mark.trio
async def test_request_401_retry_failure(client):
    """
    Verify that if the retry also fails with 401 (or other error), we raise the error.
    """
    response_401 = MagicMock(spec=httpx.Response)
    response_401.status_code = 401
    response_401.raise_for_status.side_effect = httpx.HTTPStatusError("401", request=None, response=response_401)

    # Both calls fail
    client.client.send.side_effect = [response_401, response_401]

    # Action & Assert
    with pytest.raises(httpx.HTTPStatusError):
        await client._request("GET", "/test")

    assert client.client.send.call_count == 2

# --- TESTS: PATH & METADATA ---

def test_quote_path_logic(client):
    """Ensure paths are URL-encoded and normalized."""
    assert client._quote_path("folder/file.txt") == "/folder/file.txt"
    assert client._quote_path("/already/absolute") == "/already/absolute"
    assert client._quote_path("space name") == "/space%20name"

@pytest.mark.trio
async def test_get_file_metadata_parsing(client):
    """
    Verify HEAD request and RFC 7231 Date Parsing.
    """
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.headers = {
        "Content-Length": "1024",
        # HTTP Standard Date Format
        "Last-Modified": "Sat, 01 Feb 2026 12:00:00 GMT" 
    }
    client.client.send.return_value = mock_response

    # Action
    meta = await client.get_file_metadata("/data.csv")

    # Assertions
    assert meta["size"] == 1024
    assert meta["is_dir"] is False
    
    # Verify timestamp conversion (12:00 GMT -> timestamp)
    expected_dt = datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert meta["mtime"] == expected_dt.timestamp()

@pytest.mark.trio
async def test_get_file_metadata_bad_date(client):
    """Robustness check: Malformed date should not crash the FUSE driver."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.headers = {
        "Content-Length": "500",
        "Last-Modified": "Not A Date"
    }
    client.client.send.return_value = mock_response

    meta = await client.get_file_metadata("/bad_date.txt")
    
    assert meta["size"] == 500
    assert meta["mtime"] == 0.0 # Should fallback to 0.0

@pytest.mark.trio
async def test_get_file_metadata_404(client):
    """Verify 404 returns None (used for lookup)."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 404
    client.client.send.return_value = mock_response

    result = await client.get_file_metadata("/missing")
    assert result is None

# --- TESTS: DIRECTORY LISTING ---

@pytest.mark.trio
async def test_list_directory_404_empty(client):
    """
    Verify 404 on directory listing returns empty list (crucial for ls).
    """
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 404
    client.client.send.return_value = mock_response

    result = await client.list_directory_contents("/empty_folder")
    assert result == []

# --- TESTS: DOWNLOAD & CONSISTENCY ---

@pytest.mark.trio
async def test_download_chunk_headers(client):
    """Verify Range header injection."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 206
    mock_response.aread = AsyncMock(return_value=b"data")
    client.client.send.return_value = mock_response

    # Read 100 bytes starting at 0
    await client.download_chunk("/file.bin", offset=0, length=100)

    # Check Build Request call
    call_kwargs = client.client.build_request.call_args[1]
    headers = call_kwargs["headers"]
    
    assert headers["Range"] == "bytes=0-99"

@pytest.mark.trio
async def test_download_chunk_consistency_header(client):
    """
    Verify 'If-Unmodified-Since' is sent correctly when requested.
    """
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 206
    mock_response.aread = AsyncMock(return_value=b"data")
    client.client.send.return_value = mock_response

    # Timestamp for "Sat, 01 Feb 2026 12:00:00 GMT"
    ts = datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp()

    await client.download_chunk("/file.bin", 0, 100, if_unmodified_since=ts)

    # Assert Header presence
    call_kwargs = client.client.build_request.call_args[1]
    headers = call_kwargs["headers"]
    
    assert "If-Unmodified-Since" in headers
    assert headers["If-Unmodified-Since"] == "Sun, 01 Feb 2026 12:00:00 GMT"

@pytest.mark.trio
async def test_download_chunk_412_precondition_failed(client):
    """
    Verify that if the file changed (412), the error is raised properly.
    """
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 412
    # Configure raise_for_status to actually raise
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError("412 Precondition Failed", request=None, response=mock_response)
    
    client.client.send.return_value = mock_response

    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        await client.download_chunk("/file.bin", 0, 100)
    
    assert excinfo.value.response.status_code == 412
