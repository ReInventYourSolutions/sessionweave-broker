"""End-to-end tests for the HTTP REST surface (`/v1/...`).

Spins the relay subprocess via the shared fixture and drives it with
``httpx`` — no MCP client, just plain HTTP, to catch regressions on
that path independently from the MCP transport.
"""
from __future__ import annotations

from typing import Any

import httpx
import pytest

from tests.integration.client import bearer, create_and_invite_http
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_info_endpoint_reports_version() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{base}/v1/info")
        assert r.status_code == 200
        body = r.json()
        assert body["name"] == "sessionweave"
        assert "version" in body
        assert "http" in body["transports"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_join_send_respond_round_trip() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        # 1. Create
        r = await c.post(
            "/v1/sessions",
            json={"purpose": {"kind": "rest-smoke", "instructions": "hi"}},
        )
        assert r.status_code == 201
        sid, jt = r.json()["session_id"], r.json()["join_token"]
        assert r.json()["purpose"] == {"kind": "rest-smoke", "instructions": "hi"}

        # 2. Two participants join
        sender = await c.post(
            f"/v1/sessions/{sid}/participants",
            headers=bearer(jt),
            json={"role": "sender", "capabilities": ["s"]},
        )
        assert sender.status_code == 201
        sender_token = sender.json()["participant_token"]

        receiver = await c.post(
            f"/v1/sessions/{sid}/participants",
            headers=bearer(jt),
            json={"role": "receiver", "capabilities": ["r"]},
        )
        receiver_token = receiver.json()["participant_token"]
        receiver_pid = receiver.json()["participant_id"]

        sender_h = {"Authorization": f"Bearer {sender_token}"}
        receiver_h = {"Authorization": f"Bearer {receiver_token}"}

        # 3. Sender dispatches a command
        send = await c.post(
            f"/v1/sessions/{sid}/commands",
            headers=sender_h,
            json={
                "dispatch": "specific",
                "target": {"participant_ids": [receiver_pid]},
                "payload": {"q": "ping"},
                "timeout_seconds": 30,
            },
        )
        assert send.status_code == 201, send.text
        cid = send.json()["command_id"]

        # 4. Receiver drains inbox
        drain = await c.post(
            f"/v1/sessions/{sid}/participants/me/inbox/drain",
            headers=receiver_h,
        )
        assert drain.status_code == 200
        msgs: list[dict[str, Any]] = drain.json()["messages"]
        assert any(
            m["name"] == "command.received"
            and m["payload"]["command_id"] == cid
            for m in msgs
        )

        # 5. Receiver responds
        resp = await c.post(
            f"/v1/sessions/{sid}/commands/{cid}/responses",
            headers=receiver_h,
            json={"status": "ok", "payload": {"a": "pong"}},
        )
        assert resp.status_code == 201

        # 6. Sender drains and observes result.received + command.completed
        drain_s = await c.post(
            f"/v1/sessions/{sid}/participants/me/inbox/drain",
            headers=sender_h,
        )
        assert drain_s.status_code == 200
        names = [m["name"] for m in drain_s.json()["messages"]]
        assert "result.received" in names
        assert "command.completed" in names


@pytest.mark.integration
@pytest.mark.asyncio
async def test_missing_bearer_returns_401() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        sid, _creator, _jt = await create_and_invite_http(c)
        r = await c.get(f"/v1/sessions/{sid}/participants")
        assert r.status_code == 401
        assert r.json()["error"]["code"] == "not_in_session"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bad_bearer_returns_401() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        sid, _creator, _jt = await create_and_invite_http(c)
        r = await c.get(
            f"/v1/sessions/{sid}/participants",
            headers={"Authorization": "Bearer 00000000-0000-0000-0000-000000000000"},
        )
        # Token unknown to this session -> permission_denied.
        assert r.status_code == 403
        assert r.json()["error"]["code"] == "permission_denied"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_session_id_and_participant_id_are_not_bearers() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        sid, _creator, jt = await create_and_invite_http(c)
        joined = (
            await c.post(
                f"/v1/sessions/{sid}/participants",
                headers=bearer(jt),
                json={"role": "p"},
            )
        ).json()
        assert joined["participant_token"] != joined["participant_id"]
        for bad in (sid, joined["participant_id"]):
            r = await c.get(
                f"/v1/sessions/{sid}/participants", headers=bearer(bad)
            )
            assert r.status_code == 403
            assert r.json()["error"]["code"] == "permission_denied"
        ok = await c.get(
            f"/v1/sessions/{sid}/participants",
            headers=bearer(joined["participant_token"]),
        )
        assert ok.status_code == 200


@pytest.mark.integration
@pytest.mark.asyncio
async def test_describe_unknown_session_returns_404() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        r = await c.get(
            "/v1/sessions/00000000-0000-0000-0000-000000000000",
            headers=bearer("whatever"),
        )
        assert r.status_code == 404
        assert r.json()["error"]["code"] == "session_not_found"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_leave_returns_204() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        sid, _creator, jt = await create_and_invite_http(c)
        join = await c.post(
            f"/v1/sessions/{sid}/participants",
            headers=bearer(jt),
            json={"role": "p"},
        )
        token = join.json()["participant_token"]
        r = await c.delete(
            f"/v1/sessions/{sid}/participants/me",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 204
        # After leave, the same token can no longer act.
        r2 = await c.get(
            f"/v1/sessions/{sid}/participants",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r2.status_code in (401, 403)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_leave_with_feedback_propagates_on_participant_left() -> None:
    """Skill v0.11.0 makes ``feedback`` part of the standard close-out
    flow. The relay must accept a JSON body with ``feedback`` on
    DELETE /participants/me and carry it on the broadcast
    ``participant.left`` event so other participants can collect it.
    """
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        sid, _creator, jt = await create_and_invite_http(c)
        observer = await c.post(
            f"/v1/sessions/{sid}/participants",
            headers=bearer(jt),
            json={"role": "observer"},
        )
        ot = observer.json()["participant_token"]
        leaver = await c.post(
            f"/v1/sessions/{sid}/participants",
            headers=bearer(jt),
            json={"role": "worker"},
        )
        lt = leaver.json()["participant_token"]
        leaver_pid = leaver.json()["participant_id"]
        # Drain the observer's pending participant.joined first so the
        # later assertion sees only the leave event we care about.
        await c.post(
            f"/v1/sessions/{sid}/participants/me/inbox/drain",
            headers={"Authorization": f"Bearer {ot}"},
        )
        # Leaver leaves with structured feedback.
        feedback: dict[str, Any] = {
            "summary": "all good",
            "rating": 5,
            "issues": [],
            "metrics": {"commands_received": 2, "commands_responded": 2},
        }
        r = await c.request(
            "DELETE",
            f"/v1/sessions/{sid}/participants/me",
            headers={"Authorization": f"Bearer {lt}"},
            json={"feedback": feedback},
        )
        assert r.status_code == 204
        drain = await c.post(
            f"/v1/sessions/{sid}/participants/me/inbox/drain",
            headers={"Authorization": f"Bearer {ot}"},
        )
        msgs = drain.json()["messages"]
        left = next(m for m in msgs if m["name"] == "participant.left")
        assert left["payload"]["participant_id"] == leaver_pid
        assert left["payload"]["reason"] == "left"
        assert left["payload"]["feedback"] == feedback


@pytest.mark.integration
@pytest.mark.asyncio
async def test_leave_feedback_too_large_returns_413() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        sid, _creator, jt = await create_and_invite_http(c)
        join = await c.post(
            f"/v1/sessions/{sid}/participants",
            headers=bearer(jt),
            json={"role": "p"},
        )
        token = join.json()["participant_token"]
        oversize = "x" * 5000  # > 4 KiB cap
        r = await c.request(
            "DELETE",
            f"/v1/sessions/{sid}/participants/me",
            headers={"Authorization": f"Bearer {token}"},
            json={"feedback": {"summary": oversize}},
        )
        assert r.status_code == 413
        assert r.json()["error"]["code"] == "payload_too_large"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_purpose_round_trip() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        sid, creator, jt = await create_and_invite_http(
            c, purpose={"kind": "v1", "step": 1}
        )
        join = await c.post(
            f"/v1/sessions/{sid}/participants",
            headers=bearer(jt),
            json={"role": "host"},
        )
        ptoken = join.json()["participant_token"]
        # A participant may not change the purpose; the creator may.
        denied = await c.patch(
            f"/v1/sessions/{sid}/purpose",
            headers=bearer(ptoken),
            json={"purpose": {"kind": "v1", "step": 9}},
        )
        assert denied.status_code == 403
        r = await c.patch(
            f"/v1/sessions/{sid}/purpose",
            headers=bearer(creator),
            json={"purpose": {"kind": "v1", "step": 2}},
        )
        assert r.status_code == 200
        assert r.json()["purpose"] == {"kind": "v1", "step": 2}
        # describe reflects it
        d = await c.get(f"/v1/sessions/{sid}", headers=bearer(ptoken))
        assert d.json()["purpose"] == {"kind": "v1", "step": 2}
