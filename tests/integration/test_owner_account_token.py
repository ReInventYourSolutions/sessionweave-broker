"""The owning account's token acts as the session creator."""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import websockets

from tests.integration.accounts_fixtures import ADMIN, Seeded, seeded_relay
from tests.integration.client import bearer, sw_client

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def relay(tmp_path: Path) -> AsyncIterator[Seeded]:
    async with seeded_relay(tmp_path, mode="account") as s:
        yield s


async def _create(s: Seeded, token: str | None = None, **body: Any) -> dict[str, Any]:
    async with httpx.AsyncClient(base_url=s.base) as c:
        r = await c.post("/v1/sessions", json=body, headers=bearer(token or s.token))
    assert r.status_code == 201, r.text
    out: dict[str, Any] = r.json()
    return out


async def test_owner_token_manages_session(relay: Seeded) -> None:
    s = relay
    made = await _create(s)
    sid = made["session_id"]
    h = bearer(s.token)
    async with httpx.AsyncClient(base_url=s.base) as c:
        r = await c.post(f"/v1/sessions/{sid}/join-tokens", headers=h)
        assert r.status_code == 201, r.text
        jt = r.json()["join_token"]
        r = await c.post(f"/v1/sessions/{sid}/supervisor-tokens", headers=h)
        assert r.status_code == 201, r.text
        tid = r.json()["token_id"]
        r = await c.get(f"/v1/sessions/{sid}/supervisor-tokens", headers=h)
        assert r.status_code == 200, r.text
        r = await c.delete(f"/v1/sessions/{sid}/supervisor-tokens/{tid}", headers=h)
        assert r.status_code in (200, 204), r.text
        r = await c.patch(f"/v1/sessions/{sid}/purpose", headers=h, json={"purpose": {"kind": "x"}})
        assert r.status_code == 200, r.text
        r = await c.post(
            f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "worker"}
        )
        assert r.status_code in (200, 201), r.text
        pid = r.json()["participant_id"]
        r = await c.post(
            f"/v1/sessions/{sid}/supervisor/notes", headers=h,
            json={"kind": "note", "target": "all", "message": "hi"},
        )
        assert r.status_code in (200, 202), r.text
        r = await c.delete(f"/v1/sessions/{sid}/participants/{pid}", headers=h)
        assert r.status_code == 200, r.text
        ws_url = f"{s.base.replace('http://', 'ws://')}/v1/sessions/{sid}/firehose?token={s.token}"
        async with websockets.connect(ws_url) as ws:
            r = await c.post(f"/v1/sessions/{sid}/join-tokens", headers=h)
            jt2 = r.json()["join_token"]
            r = await c.post(
                f"/v1/sessions/{sid}/participants", headers=bearer(jt2), json={"role": "w2"}
            )
            assert r.status_code in (200, 201), r.text
            frame = json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))
            assert frame["name"] == "participant.joined"
        r = await c.delete(f"/v1/sessions/{sid}", headers=h)
        assert r.status_code == 200, r.text


async def test_other_account_token_refused(relay: Seeded) -> None:
    s = relay
    sid = (await _create(s))["session_id"]
    async with httpx.AsyncClient(base_url=s.base) as c:
        for _ in range(5):
            r = await c.delete(f"/v1/sessions/{sid}", headers=bearer(s.other_token))
            assert r.status_code == 403
            assert r.json()["error"]["code"] == "permission_denied"
        r = await c.delete(f"/v1/sessions/{sid}", headers=bearer(s.other_token))
        assert r.status_code == 429
        r = await c.delete(f"/v1/sessions/{sid}", headers=bearer(s.token))
        assert r.status_code == 200


async def test_owner_token_refused_when_revoked_before_start(relay: Seeded) -> None:
    s = relay
    sid = (await _create(s, ADMIN, owner_account_id=s.acc.account_id))["session_id"]
    async with httpx.AsyncClient(base_url=s.base) as c:
        r = await c.delete(f"/v1/sessions/{sid}", headers=bearer(s.revoked_token))
        assert r.status_code == 403
        assert r.json()["error"]["code"] == "permission_denied"


async def test_mcp_management_tool_with_header(relay: Seeded) -> None:
    s = relay
    sid = (await _create(s))["session_id"]
    async with sw_client(s.base, headers=bearer(s.token)) as c:
        out = await c.call_tool("session_mint_join_token", {"session_id": sid})
        assert out["join_token"]
        out = await c.call_tool(
            "session_post_order",
            {"session_id": sid, "kind": "note", "target": "all", "message": "hi"},
        )
        assert out["kind"] == "note"
        await c.call_tool("session_delete", {"session_id": sid})


async def test_creator_token_still_works(relay: Seeded) -> None:
    s = relay
    made = await _create(s)
    async with sw_client(s.base) as c:
        out = await c.call_tool(
            "session_mint_join_token",
            {"session_id": made["session_id"], "creator_token": made["creator_token"]},
        )
        assert out["join_token"]
