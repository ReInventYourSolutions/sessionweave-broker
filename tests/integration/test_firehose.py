"""WS /v1/sessions/{sid}/firehose delivers all events for the session."""
from __future__ import annotations

import asyncio
import json

import httpx
import pytest
import websockets

from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_firehose_receives_participant_join():
    async with relay_subprocess() as base:
        base_ws = base.replace("http://", "ws://")
        async with httpx.AsyncClient() as c:
            sid = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()["session_id"]
        ws_url = f"{base_ws}/v1/sessions/{sid}/firehose?token={sid}"
        async with websockets.connect(ws_url) as ws:
            async with httpx.AsyncClient() as c:
                r = await c.post(
                    f"{base}/v1/sessions/{sid}/participants",
                    json={"role": "worker"},
                )
            assert r.status_code in (200, 201)
            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            frame = json.loads(raw)
            assert frame["name"] == "participant.joined"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_firehose_rejects_wrong_token():
    async with relay_subprocess() as base:
        base_ws = base.replace("http://", "ws://")
        async with httpx.AsyncClient() as c:
            sid = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()["session_id"]
        ws_url = f"{base_ws}/v1/sessions/{sid}/firehose?token=wrong"
        # Wrong token → server closes during handshake. Both modern
        # (InvalidStatus) and older (InvalidStatusCode) websockets builds
        # are accepted; ConnectionClosed covers post-handshake close.
        from websockets.exceptions import (
            ConnectionClosed,
            InvalidStatus,
        )
        with pytest.raises((InvalidStatus, ConnectionClosed, ConnectionError)):
            async with websockets.connect(ws_url):
                pass
