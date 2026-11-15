from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest

from tests.integration.harness import relay_subprocess

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_oversized_bodies_are_413_on_api_and_mcp() -> None:
    async with relay_subprocess(env_overrides={"SW_MAX_REQUEST_BYTES": "1000"}) as base:
        async with httpx.AsyncClient(base_url=base) as c:
            big = b"{" + b" " * 2000 + b"}"
            r = await c.post("/v1/sessions", content=big,
                             headers={"content-type": "application/json"})
            assert r.status_code == 413
            assert r.json()["error"]["code"] == "payload_too_large"

            r = await c.post("/mcp", content=big, headers={
                "content-type": "application/json",
                "accept": "application/json, text/event-stream",
            })
            assert r.status_code == 413

            async def chunks() -> AsyncIterator[bytes]:
                for _ in range(10):
                    yield b" " * 300

            r = await c.post("/v1/sessions", content=chunks(),
                             headers={"content-type": "application/json"})
            assert r.status_code == 413

            r = await c.post("/v1/sessions", json={})
            assert r.status_code == 201
