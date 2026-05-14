"""Integration test: CORS preflight + response headers."""
from __future__ import annotations

import httpx
import pytest

from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_preflight_allows_arbitrary_origin():
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            resp = await c.options(
                f"{base}/v1/info",
                headers={
                    "Origin": "https://random-origin.example",
                    "Access-Control-Request-Method": "GET",
                },
            )
    assert resp.status_code in (200, 204)
    assert resp.headers["access-control-allow-origin"] in ("*", "https://random-origin.example")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_response_carries_cors_header():
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            resp = await c.get(
                f"{base}/v1/info",
                headers={"Origin": "https://random-origin.example"},
            )
    assert resp.status_code == 200
    assert resp.headers.get("access-control-allow-origin") in ("*", "https://random-origin.example")
