"""POST /v1/sessions/{sid}/supervisor/notes posts a supervisor order.

The route delegates to ``Session.post_order`` (state changes, delivery,
persistence and firehose are covered in ``test_supervision_orders.py``); these
tests pin the HTTP surface: auth, rate limit, validation and response shape.

Allowed callers: supervisor token, creator token, relay admin token. Participants,
join tokens and the session id are refused.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
import websockets

from tests.integration.harness import relay_subprocess

ADMIN = "notes-admin-secret-cafebabe"


async def _create(c: httpx.AsyncClient, base: str, **body: Any) -> dict[str, Any]:
    r = await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}, **body})
    assert r.status_code == 201, r.text
    out: dict[str, Any] = r.json()
    return out


def _h(token: str) -> dict[str, str]:
    return {"authorization": f"Bearer {token}"}


def _note(**over: Any) -> dict[str, Any]:
    return {"kind": "note", "target": "all", "message": "m", **over}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_post_note_broadcasts_supervisor_note():
    async with relay_subprocess() as base:
        base_ws = base.replace("http://", "ws://")
        async with httpx.AsyncClient() as c:
            made = await _create(c, base)
        sid, sup = made["session_id"], made["supervisor_token"]
        async with websockets.connect(
            f"{base_ws}/v1/sessions/{sid}/firehose?token={sup}"
        ) as ws:
            note = _note(message="remember constraint X")
            # The firehose subscribes only AFTER the WS handshake and is
            # live-only, so re-post until the note is observed.
            frame = None
            first = True
            deadline = asyncio.get_event_loop().time() + 10.0
            while frame is None and asyncio.get_event_loop().time() < deadline:
                async with httpx.AsyncClient() as c:
                    r = await c.post(
                        f"{base}/v1/sessions/{sid}/supervisor/notes",
                        headers=_h(sup), json=note,
                    )
                    assert r.status_code == 200
                    if first:
                        assert r.json()["kind"] == "note"
                        assert "note_id" in r.json()
                        first = False
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                except TimeoutError:
                    continue
                candidate = json.loads(raw)
                if candidate.get("name") == "supervisor.note":
                    frame = candidate
            assert frame is not None, "supervisor.note not observed on firehose"
            assert frame["payload"]["message"] == "remember constraint X"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_post_note_rejects_missing_auth():
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            sid = (await _create(c, base))["session_id"]
            r = await c.post(f"{base}/v1/sessions/{sid}/supervisor/notes", json=_note())
        assert r.status_code == 401


@pytest.mark.integration
@pytest.mark.asyncio
async def test_post_note_unknown_participant_returns_404():
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            made = await _create(c, base)
            r = await c.post(
                f"{base}/v1/sessions/{made['session_id']}/supervisor/notes",
                headers=_h(made["supervisor_token"]),
                json={"kind": "halt", "target": "p-does-not-exist"},
            )
        assert r.status_code == 404
        assert r.json()["error"] == "participant_not_found"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_post_note_rate_limited():
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            made = await _create(c, base)
            statuses = []
            for _ in range(35):  # default per-minute cap is 30
                r = await c.post(
                    f"{base}/v1/sessions/{made['session_id']}/supervisor/notes",
                    headers=_h(made["supervisor_token"]), json=_note(message="x"),
                )
                statuses.append(r.status_code)
        assert 429 in statuses


@pytest.mark.integration
@pytest.mark.asyncio
async def test_post_note_unknown_kind_rejected() -> None:
    """The kind allowlist (note/halt/continue/redirect) rejects unknown kinds with 400."""
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            made = await _create(c, base)
            r = await c.post(
                f"{base}/v1/sessions/{made['session_id']}/supervisor/notes",
                headers=_h(made["supervisor_token"]),
                json=_note(kind="poison-pill", message="x"),
            )
            assert r.status_code == 400
            assert r.json()["error"] == "invalid_request"
            assert "kind" in r.json()["detail"].lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_post_note_accepted_for_supervisor_creator_admin() -> None:
    async with relay_subprocess(env_overrides={"SW_ADMIN_TOKEN": ADMIN}) as base:
        async with httpx.AsyncClient() as c:
            made = await _create(c, base)
            sid = made["session_id"]
            for who in (made["supervisor_token"], made["creator_token"], ADMIN):
                r = await c.post(
                    f"{base}/v1/sessions/{sid}/supervisor/notes",
                    headers=_h(who), json=_note(message="ok"),
                )
                assert r.status_code == 200, (who, r.text)
                assert r.json()["message"] == "ok"
                # A note changes no participant's state.
                assert r.json()["affected"] == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_post_note_refused_for_participant_joiner_sid_and_garbage() -> None:
    async with relay_subprocess(env_overrides={"SW_ADMIN_TOKEN": ADMIN}) as base:
        async with httpx.AsyncClient() as c:
            made = await _create(c, base)
            sid = made["session_id"]
            joined = (
                await c.post(
                    f"{base}/v1/sessions/{sid}/participants",
                    headers=_h(made["join_token"]), json={"role": "orchestrator"},
                )
            ).json()
            refused = [
                joined["participant_token"],
                joined["participant_id"],
                made["join_token"],
                sid,
                made["creator_token"][:-4] + "xxxx",
            ]
            for who in refused:
                r = await c.post(
                    f"{base}/v1/sessions/{sid}/supervisor/notes",
                    headers=_h(who), json=_note(kind="halt", message="denied"),
                )
                assert r.status_code == 403, (who, r.status_code)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_revoked_supervisor_token_refused() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            made = await _create(c, base)
            sid = made["session_id"]
            rv = await c.delete(
                f"{base}/v1/sessions/{sid}/supervisor-tokens/{made['supervisor_token_id']}",
                headers=_h(made["creator_token"]),
            )
            assert rv.status_code == 204, rv.text
            r = await c.post(
                f"{base}/v1/sessions/{sid}/supervisor/notes",
                headers=_h(made["supervisor_token"]), json=_note(),
            )
            assert r.status_code == 403


@pytest.mark.integration
@pytest.mark.asyncio
async def test_post_undo_is_not_available_yet() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            made = await _create(c, base)
            r = await c.post(
                f"{base}/v1/sessions/{made['session_id']}/supervisor/notes",
                headers=_h(made["supervisor_token"]),
                json={"kind": "undo", "target": "all"},
            )
        assert r.status_code == 400
        assert r.json()["error"] == "invalid_request"
