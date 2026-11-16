from __future__ import annotations

import asyncio
import contextlib
from urllib.parse import urlparse

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

            r = await c.post("/v1/sessions", json={})
            assert r.status_code == 201


async def test_chunked_body_over_cap_is_413_raw_socket() -> None:
    async with relay_subprocess(env_overrides={"SW_MAX_REQUEST_BYTES": "1000"}) as base:
        u = urlparse(base)
        reader, writer = await asyncio.open_connection(u.hostname, u.port)
        try:
            writer.write(
                b"POST /v1/sessions HTTP/1.1\r\nHost: x\r\n"
                b"Content-Type: application/json\r\n"
                b"Transfer-Encoding: chunked\r\n\r\n"
                + format(2000, "x").encode() + b"\r\n" + b" " * 2000 + b"\r\n"
            )  # one oversized chunk; the terminating 0-chunk is never sent
            with contextlib.suppress(ConnectionError):
                await writer.drain()
            status = await asyncio.wait_for(reader.readline(), timeout=10)
            assert b" 413 " in status, status
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()


async def test_413_carries_cors_headers() -> None:
    async with relay_subprocess(env_overrides={"SW_MAX_REQUEST_BYTES": "1000"}) as base:
        async with httpx.AsyncClient(base_url=base) as c:
            r = await c.post(
                "/v1/sessions",
                content=b"{" + b" " * 2000 + b"}",
                headers={"content-type": "application/json", "origin": "https://app.example"},
            )
            assert r.status_code == 413
            assert r.headers["access-control-allow-origin"] == "*"
