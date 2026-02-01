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

# --- TESTS ---

@pytest.mark.trio
async def test_consistency_precondition_failed_with_respx(auth_mock):
    """
    The 'Frankenstein' Test:
    Verifies that the client raises 412 Precondition Failed if the file 
    changes on the server between metadata fetch and data download.
    """
    base_url = "https://adb-mock.net"
    file_path = "/Volumes/main/default/vol/data.bin"
    
    # Setup Timestamps
    # T1: What we think the file is (stale)
    # T2: What the server actually has (new)
    t1_old = datetime(2026, 2, 1, 10, 0, 0, tzinfo=timezone.utc)
    t2_new = datetime(2026, 2, 1, 11, 0, 0, tzinfo=timezone.utc)
    
    t1_str = formatdate(t1_old.timestamp(), usegmt=True)
    t2_str = formatdate(t2_new.timestamp(), usegmt=True)

    async with respx.mock(base_url=base_url, assert_all_called=False) as respx_mock:
        client = UnityCatalogClient(base_url, auth_mock)

        def side_effect(request):
            # Check the conditional header sent by our client
            if_unmodified = request.headers.get("If-Unmodified-Since")
            
            # Logic: If client sends T1, we reject with 412 because 
            # the server "current" state is T2.
            if if_unmodified == t1_str:
                return httpx.Response(412, text="Precondition Failed")
            
            return httpx.Response(200, content=b"new data", headers={"Last-Modified": t2_str})

        # Register the route with the corrected 'path' lookup
        respx_mock.get(path=f"/api/2.0/fs/files{file_path}").mock(side_effect=side_effect)

        # Execution: Attempt to download using the stale T1 timestamp
        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            await client.download_chunk(
                file_path, 
                offset=0, 
                length=10, 
                if_unmodified_since=t1_old.timestamp()
            )
        
        # Verify the exception is indeed the 412 error from our logic
        await excinfo.value.response.aread()
        assert excinfo.value.response.status_code == 412
        assert "Precondition Failed" in excinfo.value.response.text
        
        await client.close()

@pytest.mark.trio
async def test_consistency_success_with_respx(auth_mock):
    """
    Verifies that if the file has NOT changed on the server, 
    the download completes successfully with the conditional header.
    """
    base_url = "https://adb-mock.net"
    file_path = "/data.bin"
    
    # Current timestamp
    t_now = datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
    t_str = formatdate(t_now.timestamp(), usegmt=True)

    async with respx.mock(base_url=base_url) as respx_mock:
        client = UnityCatalogClient(base_url, auth_mock)

        # Mock server that accepts the timestamp as current
        respx_mock.get(path="/api/2.0/fs/files/data.bin").mock(
            return_value=httpx.Response(
                206, 
                content=b"consistent data", 
                headers={"Last-Modified": t_str}
            )
        )

        # Action: Request with current timestamp
        data = await client.download_chunk(
            file_path, 
            offset=0, 
            length=15, 
            if_unmodified_since=t_now.timestamp()
        )
        
        assert data == b"consistent data"
        await client.close()

@pytest.mark.trio
async def test_download_without_conditional_header(auth_mock):
    """
    Ensures the client works normally (without 412 checks) 
    if if_unmodified_since is not provided.
    """
    base_url = "https://adb-mock.net"
    file_path = "/simple.txt"

    async with respx.mock(base_url=base_url) as respx_mock:
        client = UnityCatalogClient(base_url, auth_mock)

        route = respx_mock.get(path="/api/2.0/fs/files/simple.txt").mock(
            return_value=httpx.Response(200, content=b"just data")
        )

        data = await client.download_chunk(file_path, 0, 9)
        
        # Verify no conditional header was sent
        sent_request = route.calls.last.request
        assert "If-Unmodified-Since" not in sent_request.headers
        assert data == b"just data"
        
        await client.close()
