import pytest
import respx
import httpx
from datetime import datetime, timezone
from email.utils import formatdate
from unittest.mock import MagicMock

from fuse4dbricks.api.uc_client import UnityCatalogClient

# --- FIXTURES ---


@pytest.fixture
def auth_mock():
    """Simple mock for the Auth Provider."""
    provider = MagicMock()
    provider.get_access_token.return_value = "fake_token"
    return provider


# --- HELPERS ---


async def consume_stream(stream):
    """Helper to drain the async generator into a single byte string."""
    chunks = []
    async for chunk in stream:
        chunks.append(chunk)
    return b"".join(chunks)


# --- TESTS ---


@pytest.mark.trio
async def test_consistency_precondition_failed_with_respx(auth_mock):
    """
    Verifies that the client raises 412 Precondition Failed if the file
    changes on the server.
    """
    base_url = "https://adb-mock.net"
    file_path = "/Volumes/main/default/vol/data.bin"

    t1_old = datetime(2026, 2, 1, 10, 0, 0, tzinfo=timezone.utc)

    async with respx.mock(base_url=base_url, assert_all_called=False) as respx_mock:
        client = UnityCatalogClient(base_url, auth_mock)

        # Mock route for 412 error
        respx_mock.get(path=f"/api/2.0/fs/files{file_path}").mock(
            return_value=httpx.Response(412, text="Precondition Failed")
        )

        # Execution: Intentar descargar. El error debe saltar al iniciar el stream.
        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            stream = client.download_chunk_stream(
                file_path, offset=0, length=10, if_unmodified_since=t1_old.timestamp()
            )
            await consume_stream(stream)

        assert excinfo.value.response.status_code == 412
        await client.close()


@pytest.mark.trio
async def test_consistency_success_with_respx(auth_mock):
    """
    Verifies successful download with conditional header using streaming.
    """
    base_url = "https://adb-mock.net"
    file_path = "/data.bin"

    t_now = datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
    t_str = formatdate(t_now.timestamp(), usegmt=True)

    async with respx.mock(base_url=base_url) as respx_mock:
        client = UnityCatalogClient(base_url, auth_provider=auth_mock)

        respx_mock.get(path="/api/2.0/fs/files/data.bin").mock(
            return_value=httpx.Response(
                206, content=b"consistent data", headers={"Last-Modified": t_str}
            )
        )

        # Action: Consumir el stream
        stream = client.download_chunk_stream(
            file_path, offset=0, length=15, if_unmodified_since=t_now.timestamp()
        )
        data = await consume_stream(stream)

        assert data == b"consistent data"

        # Verify header was actually sent
        sent_request = respx_mock.calls.last.request
        assert sent_request.headers["If-Unmodified-Since"] == t_str

        await client.close()


@pytest.mark.trio
async def test_download_without_conditional_header(auth_mock):
    """
    Ensures the client works normally without 412 checks if timestamp is missing.
    """
    base_url = "https://adb-mock.net"
    file_path = "/simple.txt"

    async with respx.mock(base_url=base_url) as respx_mock:
        client = UnityCatalogClient(base_url, auth_mock)

        respx_mock.get(path="/api/2.0/fs/files/simple.txt").mock(
            return_value=httpx.Response(200, content=b"just data")
        )

        stream = client.download_chunk_stream(file_path, 0, 9)
        data = await consume_stream(stream)

        sent_request = respx_mock.calls.last.request
        assert "If-Unmodified-Since" not in sent_request.headers
        assert data == b"just data"

        await client.close()
