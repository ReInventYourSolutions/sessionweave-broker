from __future__ import annotations

import httpx
import pytest

from tests.integration.harness import relay_subprocess

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_INIT = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "t", "version": "0"},
    },
}
_HEADERS = {
    "content-type": "application/json",
    "accept": "application/json, text/event-stream",
}


async def test_initialize_is_limited_per_ip_but_session_calls_are_not() -> None:
    env = {"SW_MCP_NEW_SESSIONS_PER_MINUTE": "2"}
    async with relay_subprocess(env_overrides=env) as base, httpx.AsyncClient(
        base_url=base
    ) as c:
        first = await c.post("/mcp", json=_INIT, headers=_HEADERS)
        assert first.status_code == 200, first.text
        sid = first.headers["mcp-session-id"]
        assert (await c.post("/mcp", json=_INIT, headers=_HEADERS)).status_code == 200
        r = await c.post("/mcp", json=_INIT, headers=_HEADERS)
        assert r.status_code == 429
        assert r.json()["error"]["code"] == -32000
        assert int(r.headers["retry-after"]) >= 1
        for _ in range(5):
            r = await c.post(
                "/mcp",
                json={"jsonrpc": "2.0", "method": "notifications/initialized"},
                headers={**_HEADERS, "mcp-session-id": sid},
            )
            assert r.status_code != 429
