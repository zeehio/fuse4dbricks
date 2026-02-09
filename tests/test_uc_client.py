from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

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
    Creates the UnityCatalogClient with a mocked httpx.AsyncClient.
    """
    uc_client = UnityCatalogClient("https://test-workspace.net", mock_auth_provider)

    uc_client.client = AsyncMock(spec=httpx.AsyncClient)

    mock_request = MagicMock(spec=httpx.Request)
    uc_client.client.build_request.return_value = mock_request

    return uc_client


# --- TESTS: AUTHENTICATION & RETRIES ---


@pytest.mark.trio
async def test_request_success_200(client, mock_auth_provider):
    """Verify standard request flow: Auth header injection + Success."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"key": "value"}
    client.client.send.return_value = mock_response

    result = await client._request("GET", "/test")

    assert result == {"key": "value"}
    mock_auth_provider.get_access_token.assert_called_with()

    # Verify headers
    call_kwargs = client.client.build_request.call_args[1]
    assert call_kwargs["headers"]["Authorization"] == "Bearer fake_token_123"


@pytest.mark.trio
async def test_request_401_retry_success(client, mock_auth_provider):
    """Verify that a 401 triggers a token refresh and a retry."""
    response_401 = MagicMock(spec=httpx.Response)
    response_401.status_code = 401

    response_200 = MagicMock(spec=httpx.Response)
    response_200.status_code = 200
    response_200.json.return_value = {"success": True}

    client.client.send.side_effect = [response_401, response_200]

    result = await client._request("GET", "/test")

    assert result == {"success": True}
    assert client.client.send.call_count == 2
    mock_auth_provider.get_access_token.assert_any_call(force_refresh=True)


# --- TESTS: PATH & METADATA ---


def test_quote_path_logic(client):
    """Ensure paths are URL-encoded and normalized."""
    assert client._quote_path("folder/file.txt") == "/folder/file.txt"
    assert client._quote_path("space name") == "/space%20name"


@pytest.mark.trio
async def test_get_file_metadata_parsing(client):
    """Verify HEAD request and RFC 7231 Date Parsing."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.headers = {
        "Content-Length": "1024",
        "Last-Modified": "Sun, 01 Feb 2026 12:00:00 GMT",
    }
    client.client.send.return_value = mock_response

    meta = await client._get_file_metadata("/data.csv")

    assert meta.size == 1024
    expected_dt = datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert meta.mtime == expected_dt.timestamp()


@pytest.mark.trio
async def test_get_file_metadata_404(client):
    """Verify 404 returns None."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 404
    client.client.send.return_value = mock_response

    result = await client._get_file_metadata("/missing")
    assert result is None


# --- TESTS: DOWNLOAD STREAM & CONSISTENCY ---


@pytest.mark.trio
async def test_download_chunk_stream_headers(client):
    """Verify Range header injection in streaming mode."""
    mock_response = AsyncMock(spec=httpx.Response)
    mock_response.status_code = 206

    async def mock_iter_bytes(chunk_size=None):
        yield b"chunk1"
        yield b"chunk2"

    mock_response.aiter_bytes = mock_iter_bytes

    client.client.send.return_value = mock_response

    chunks = []
    async for chunk in client.download_chunk_stream("/file.bin", offset=0, length=100):
        chunks.append(chunk)

    assert b"".join(chunks) == b"chunk1chunk2"

    last_build_call = client.client.build_request.call_args_list[-1]
    headers = last_build_call[1]["headers"]
    assert headers["Range"] == "bytes=0-99"


@pytest.mark.trio
async def test_download_chunk_stream_412_precondition_failed(client):
    """Verify that if the file changed (412), it raises error."""
    mock_response = AsyncMock(spec=httpx.Response)
    mock_response.status_code = 412
    # Simulamos el comportamiento de raise_for_status
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "412 Precondition Failed", request=MagicMock(), response=mock_response
    )

    client.client.send.return_value = mock_response

    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        async for _ in client.download_chunk_stream("/file.bin", 0, 100):
            pass

    assert excinfo.value.response.status_code == 412
