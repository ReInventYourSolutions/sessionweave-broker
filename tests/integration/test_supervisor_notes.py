"""POST /v1/sessions/{sid}/supervisor/notes broadcasts supervisor events."""
from __future__ import annotations

import asyncio
import json

import httpx
import pytest
import websockets

from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_post_note_broadcasts_supervisor_note():
    async with relay_subprocess() as base:
        base_ws = base.replace("http://", "ws://")
        async with httpx.AsyncClient() as c:
            sid = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()["session_id"]
        async with websockets.connect(
            f"{base_ws}/v1/sessions/{sid}/firehose?token={sid}"
        ) as ws:
            async with httpx.AsyncClient() as c:
                r = await c.post(
                    f"{base}/v1/sessions/{sid}/supervisor/notes",
                    headers={"authorization": f"Bearer {sid}"},
                    json={"kind": "note", "target": "all", "message": "remember constraint X"},
                )
                assert r.status_code == 200
                body = r.json()
                assert body["kind"] == "note"
                assert body["target"] == "all"
                assert "note_id" in body
            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            frame = json.loads(raw)
            assert frame["name"] == "supervisor.note"
            assert frame["payload"]["message"] == "remember constraint X"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_post_note_rejects_missing_auth():
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            sid = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()["session_id"]
            r = await c.post(
                f"{base}/v1/sessions/{sid}/supervisor/notes",
                json={"kind": "note", "target": "all", "message": "hi"},
            )
        assert r.status_code == 401


@pytest.mark.integration
@pytest.mark.asyncio
async def test_post_note_rate_limited():
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            sid = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()["session_id"]
            statuses = []
            for _ in range(35):  # default per-minute cap is 30
                r = await c.post(
                    f"{base}/v1/sessions/{sid}/supervisor/notes",
                    headers={"authorization": f"Bearer {sid}"},
                    json={"kind": "note", "target": "all", "message": "x"},
                )
                statuses.append(r.status_code)
        assert 429 in statuses
