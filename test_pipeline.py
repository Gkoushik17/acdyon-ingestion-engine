import pytest
import httpx
import asyncio
from main import app
from database import get_stats, get_all_jobs

@pytest.mark.asyncio
async def test_health_endpoint():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.get("/api/health")
        assert res.status_code == 200
        data = res.json()
        assert data["status"] == "healthy"
        assert "SandboxSource" in data["registered_sources"]

@pytest.mark.asyncio
async def test_ingest_sse_stream():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream("GET", "/api/ingest?primary=SandboxSource&fallback=WeWorkRemotely&limit=3") as stream:
            assert stream.status_code == 200
            events = []
            async for line in stream.aiter_lines():
                if line.startswith("data:"):
                    events.append(line)
            assert len(events) >= 5

@pytest.mark.asyncio
async def test_chaos_and_circuit_breaker_failover():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # Trigger rate limit
        await client.post("/api/chaos", json={"rate_limit": True, "bot_block": False, "schema_drift": False})
        
        # Test stream failover
        async with client.stream("GET", "/api/ingest?primary=SandboxSource&fallback=WeWorkRemotely&limit=2") as stream:
            assert stream.status_code == 200
            events = [line async for line in stream.aiter_lines() if line.startswith("data:")]
            assert any("FAILOVER" in e or "FETCH_FAILED" in e for e in events)

        # Reset chaos
        await client.post("/api/chaos", json={"rate_limit": False, "bot_block": False, "schema_drift": False})
