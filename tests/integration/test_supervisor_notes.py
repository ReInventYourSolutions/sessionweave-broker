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
async def test_post_note_unknown_participant_returns_404():
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            sid = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()["session_id"]
            r = await c.post(
                f"{base}/v1/sessions/{sid}/supervisor/notes",
                headers={"authorization": f"Bearer {sid}"},
                json={"kind": "halt", "target": "p-does-not-exist"},
            )
        assert r.status_code == 404
        body = r.json()
        assert body["error"] == "participant_not_found"


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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_post_note_admin_token_mode_rejects_session_id() -> None:
    """When ``SW_SUPERVISOR_ADMIN_TOKEN`` is set, a bearer equal to
    the session_id must be rejected — only the admin token is
    accepted. Closes the authz gap from #55: a session participant
    who knows its own ``session_id`` cannot post supervisor notes
    (including ``kind=halt``) without the admin secret.
    """
    admin = "test-supervisor-secret-cafebabe"
    async with relay_subprocess(
        env_overrides={"SW_SUPERVISOR_ADMIN_TOKEN": admin}
    ) as base:
        async with httpx.AsyncClient() as c:
            sid = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()["session_id"]

            # Bearer == session_id must FAIL in admin-token mode.
            r = await c.post(
                f"{base}/v1/sessions/{sid}/supervisor/notes",
                headers={"authorization": f"Bearer {sid}"},
                json={"kind": "halt", "target": "all", "message": "denied"},
            )
            assert r.status_code == 403

            # Bearer == admin_token must succeed.
            r = await c.post(
                f"{base}/v1/sessions/{sid}/supervisor/notes",
                headers={"authorization": f"Bearer {admin}"},
                json={"kind": "note", "target": "all", "message": "ok"},
            )
            assert r.status_code == 200
            body = r.json()
            assert body["kind"] == "note"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_post_note_unknown_kind_rejected() -> None:
    """The server-side kind allowlist (``note`` / ``halt`` /
    ``continue`` / ``redirect``) rejects unknown kinds with 400.
    Stops a compromised cockpit from injecting custom kinds the
    skill doesn't document, which client-side custom handlers
    might act on.
    """
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            sid = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()["session_id"]
            r = await c.post(
                f"{base}/v1/sessions/{sid}/supervisor/notes",
                headers={"authorization": f"Bearer {sid}"},
                json={"kind": "poison-pill", "target": "all", "message": "x"},
            )
            assert r.status_code == 400
            body = r.json()
            assert body["error"] == "invalid_request"
            assert "kind" in body["detail"].lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_in_session_participant_can_post_supervisor_note_with_admin_token_set() -> None:
    """An in-session participant's participant_token is accepted by
    /supervisor/notes even when SW_SUPERVISOR_ADMIN_TOKEN is set.
    This enables broadcast-as-self patterns (e.g. a director posting a
    session_summary kind=note target=all) without exposing the relay-wide
    admin token to in-session callers."""
    admin = "deadbeefcafebabe-admin-secret"
    async with relay_subprocess(
        env_overrides={"SW_SUPERVISOR_ADMIN_TOKEN": admin}
    ) as base:
        async with httpx.AsyncClient() as c:
            sid = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"x": 1}})
            ).json()["session_id"]
            joined = (
                await c.post(
                    f"{base}/v1/sessions/{sid}/participants",
                    json={"role": "orchestrator", "capabilities": ["coord"]},
                )
            ).json()
            p_token = joined["participant_token"]
            # Bearer == participant_token must succeed even with admin set.
            r = await c.post(
                f"{base}/v1/sessions/{sid}/supervisor/notes",
                headers={"authorization": f"Bearer {p_token}"},
                json={"kind": "note", "target": "all",
                      "message": "session-summary from orchestrator"},
            )
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["kind"] == "note"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_non_participant_token_rejected_when_admin_set() -> None:
    """A token that is neither admin_token, nor sid, nor an in-session
    participant_token must be rejected with 403 (regression guard)."""
    admin = "deadbeefcafebabe-admin-secret-2"
    async with relay_subprocess(
        env_overrides={"SW_SUPERVISOR_ADMIN_TOKEN": admin}
    ) as base:
        async with httpx.AsyncClient() as c:
            sid = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"x": 1}})
            ).json()["session_id"]
            r = await c.post(
                f"{base}/v1/sessions/{sid}/supervisor/notes",
                headers={"authorization": "Bearer some-random-string"},
                json={"kind": "note", "target": "all", "message": "denied"},
            )
            assert r.status_code == 403
