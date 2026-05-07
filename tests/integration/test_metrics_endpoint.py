from __future__ import annotations

import httpx
import pytest

from tests.integration.client import sw_client
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_metrics_endpoint_exposes_relay_counters() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as client:
            await client.call_tool("session_create", {})
        async with httpx.AsyncClient() as http:
            resp = await http.get(f"{base}/metrics")
        assert resp.status_code == 200
        body = resp.text
        assert "sessionweave_sessions_active" in body
        assert "sessionweave_commands_total" in body
        assert resp.headers["content-type"].startswith("text/plain")
