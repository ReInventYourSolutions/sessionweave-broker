"""WS /v1/sessions/{sid}/firehose delivers all events for the session.

Allowed: supervisor token, creator token, relay admin token (``?token=``).
"""
from __future__ import annotations

import asyncio
import json

import httpx
import pytest
import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus

from tests.integration.harness import relay_subprocess

ADMIN = "test-admin-secret-d3b07384"


async def _create(base: str) -> dict:
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
    assert r.status_code == 201, r.text
    return r.json()


async def _join(base: str, made: dict) -> None:
    async with httpx.AsyncClient() as c:
        r = await c.post(
            f"{base}/v1/sessions/{made['session_id']}/participants",
            headers={"authorization": f"Bearer {made['join_token']}"},
            json={"role": "worker"},
        )
    assert r.status_code in (200, 201), r.text


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("who", ["supervisor_token", "creator_token", "admin"])
async def test_firehose_receives_participant_join(who: str):
    async with relay_subprocess(env_overrides={"SW_ADMIN_TOKEN": ADMIN}) as base:
        base_ws = base.replace("http://", "ws://")
        made = await _create(base)
        token = ADMIN if who == "admin" else made[who]
        url = f"{base_ws}/v1/sessions/{made['session_id']}/firehose?token={token}"
        async with websockets.connect(url) as ws:
            await _join(base, made)
            frame = json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))
            assert frame["name"] == "participant.joined"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_firehose_refuses_everything_else():
    async with relay_subprocess(env_overrides={"SW_ADMIN_TOKEN": ADMIN}) as base:
        base_ws = base.replace("http://", "ws://")
        made = await _create(base)
        sid = made["session_id"]
        joined = None
        async with httpx.AsyncClient() as c:
            joined = (
                await c.post(
                    f"{base}/v1/sessions/{sid}/participants",
                    headers={"authorization": f"Bearer {made['join_token']}"},
                    json={"role": "worker"},
                )
            ).json()
        for bad in ("wrong", sid, made["join_token"], joined["participant_token"],
                    joined["participant_id"]):
            url = f"{base_ws}/v1/sessions/{sid}/firehose?token={bad}"
            with pytest.raises((InvalidStatus, ConnectionClosed, ConnectionError)):
                async with websockets.connect(url) as ws:
                    await ws.recv()
