"""Integration tests for POST /v1/sessions/{sid}/participants/me/inbox/wait.

Spins the relay as a subprocess (via ``relay_subprocess``) and drives it with
``httpx``.  All participants are joined via the HTTP endpoint, which forces
``delivery_mode=pull``, so push-mode rejection is covered in the MCP test file.
"""
from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest

from tests.integration.client import bearer
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_wait_for_event_http_wakes_on_send() -> None:
    """wait_for_event returns messages that were buffered before the call.

    Buffers a ``command.received`` event in participant A's inbox by
    sending a command from B first, then calls inbox/wait which drains
    the backlog and returns immediately — confirming the endpoint
    surfaces buffered events and reports ``timed_out: false``.
    """
    async with relay_subprocess() as base, httpx.AsyncClient(
        base_url=base, timeout=15.0
    ) as c:
        # 1. Create session.
        r = await c.post("/v1/sessions", json={})
        assert r.status_code == 201, r.text
        sid = r.json()["session_id"]
        jt = r.json()["join_token"]
        # 2. Join two participants (HTTP join always forces pull mode).
        r = await c.post(
            f"/v1/sessions/{sid}/participants",
            headers=bearer(jt),
            json={"role": "waiter", "capabilities": ["listen"]},
        )
        assert r.status_code == 201, r.text
        pid_a = r.json()["participant_id"]
        token_a = r.json()["participant_token"]

        r = await c.post(
            f"/v1/sessions/{sid}/participants",
            headers=bearer(jt),
            json={"role": "sender"},
        )
        assert r.status_code == 201, r.text
        token_b = r.json()["participant_token"]

        # 3. Sender dispatches a command targeting participant A.  This
        # buffers a ``command.received`` event in A's inbox before the wait.
        r = await c.post(
            f"/v1/sessions/{sid}/commands",
            headers={"Authorization": f"Bearer {token_b}"},
            json={
                "dispatch": "specific",
                "target": {"participant_ids": [pid_a]},
                "payload": {"hello": "world"},
                "timeout_seconds": 10,
            },
        )
        assert r.status_code == 201, r.text

        # 4. inbox/wait should return immediately with the buffered event.
        r = await c.post(
            f"/v1/sessions/{sid}/participants/me/inbox/wait",
            headers={"Authorization": f"Bearer {token_a}"},
            json={"timeout_seconds": 5},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["timed_out"] is False
        assert any(
            m["name"] == "command.received" for m in body["messages"]
        ), f"expected command.received in messages, got: {body['messages']}"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_wait_for_event_http_concurrent_waiters_do_not_collide() -> None:
    """Regression: two participants must be able to long-poll concurrently.

    Before per-request HTTPParticipantHandle instances, the module-level
    singleton serialised on ConnectionManager.bind: the second concurrent
    inbox/wait would raise ValueError("handle already bound") and the
    relay returned 500. Now each HTTP request creates its own handle, so
    parallel waiters succeed independently — and a command broadcast
    while both are waiting wakes both.
    """
    async with relay_subprocess() as base, httpx.AsyncClient(
        base_url=base, timeout=15.0
    ) as c:
        r = await c.post("/v1/sessions", json={})
        assert r.status_code == 201, r.text
        sid = r.json()["session_id"]
        jt = r.json()["join_token"]
        # Two listeners + one sender, all pull-mode (HTTP forces it).
        async def join(role: str, capabilities: list[str]) -> tuple[str, str]:
            r = await c.post(
                f"/v1/sessions/{sid}/participants",
                headers=bearer(jt),
                json={"role": role, "capabilities": capabilities},
            )
            assert r.status_code == 201, r.text
            data = r.json()
            return data["participant_id"], data["participant_token"]

        _, tok_a = await join("waiter-a", ["echo"])
        _, tok_b = await join("waiter-b", ["echo"])
        _, tok_sender = await join("sender", [])

        # Drain the participant.joined notices first so the two waiters
        # below actually block instead of returning the buffered noise.
        for tok in (tok_a, tok_b):
            await c.post(
                f"/v1/sessions/{sid}/participants/me/inbox/drain",
                headers={"Authorization": f"Bearer {tok}"},
            )

        async def wait(token: str) -> dict[str, Any]:
            r = await c.post(
                f"/v1/sessions/{sid}/participants/me/inbox/wait",
                headers={"Authorization": f"Bearer {token}"},
                json={"names": ["command.received"], "timeout_seconds": 5},
            )
            assert r.status_code == 200, f"{r.status_code} {r.text}"
            data: dict[str, Any] = r.json()
            return data

        async def send_after_delay() -> None:
            await asyncio.sleep(0.2)
            r = await c.post(
                f"/v1/sessions/{sid}/commands",
                headers={"Authorization": f"Bearer {tok_sender}"},
                json={
                    "dispatch": "all",
                    "target": {"capability": "echo"},
                    "payload": {"q": "wake up"},
                    "timeout_seconds": 10,
                },
            )
            assert r.status_code == 201, r.text

        # Two concurrent long-polls + one parallel sender — would have
        # tripped the singleton-bind collision pre-fix.
        results = await asyncio.gather(wait(tok_a), wait(tok_b), send_after_delay())
        body_a, body_b, _ = results

        for who, body in (("A", body_a), ("B", body_b)):
            assert body["timed_out"] is False, f"{who} timed out: {body}"
            names = [m["name"] for m in body["messages"]]
            assert "command.received" in names, f"{who} got {names}"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_wait_for_event_http_returns_timed_out() -> None:
    """Returns ``timed_out: true`` with an empty message list when no event arrives."""
    async with relay_subprocess() as base, httpx.AsyncClient(
        base_url=base, timeout=10.0
    ) as c:
        r = await c.post("/v1/sessions", json={})
        assert r.status_code == 201, r.text
        sid = r.json()["session_id"]
        jt = r.json()["join_token"]
        r = await c.post(
            f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "p"}
        )
        assert r.status_code == 201, r.text
        token = r.json()["participant_token"]

        r2 = await c.post(
            f"/v1/sessions/{sid}/participants/me/inbox/wait",
            headers={"Authorization": f"Bearer {token}"},
            json={"timeout_seconds": 0.3},
        )
        assert r2.status_code == 200, r2.text
        body = r2.json()
        assert body["timed_out"] is True
        assert body["messages"] == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_wait_releases_in_flight_when_client_times_out() -> None:
    """Regression: a client whose HTTP timeout is shorter than the
    server-side wait must NOT lock itself out with 409.

    The cockpit-side fan-out repro (2026-05-23) hit this: the SDK's
    httpx default timeout (30 s) is shorter than the cockpit's
    requested ``timeout_seconds=90``. When httpx gave up, the relay
    kept the wait alive for the full 90 s and every subsequent
    wait_for_event from the same participant returned HTTP 409
    ``wait_already_in_flight`` until the original wait expired on its
    own.

    The fix: the HTTP handler watches the ASGI receive channel for an
    ``http.disconnect`` event and cancels the wait_for_event task,
    which then runs its outer finally to release ``_wait_in_flight``.
    This test forces the timeout mismatch and asserts that a follow-up
    wait proceeds normally.
    """
    async with relay_subprocess() as base:
        # First, an httpx client with a SHORT timeout that we will use
        # to abandon a long wait. Then a fresh client with a normal
        # timeout to verify the participant is no longer stuck.
        async with httpx.AsyncClient(base_url=base, timeout=15.0) as setup:
            r = await setup.post("/v1/sessions", json={})
            assert r.status_code == 201, r.text
            sid = r.json()["session_id"]
            jt = r.json()["join_token"]
            r = await setup.post(
                f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "p"}
            )
            assert r.status_code == 201, r.text
            token = r.json()["participant_token"]

        # 1. Client with a 1 s read timeout calls wait_for_event with a
        # server-side budget of 30 s. httpx will give up at 1 s and
        # close the TCP connection while the relay's wait is still
        # running.
        async with httpx.AsyncClient(base_url=base, timeout=1.0) as fast:
            with pytest.raises(httpx.ReadTimeout):
                await fast.post(
                    f"/v1/sessions/{sid}/participants/me/inbox/wait",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"timeout_seconds": 30},
                )

        # 2. Give the relay a brief window to notice the disconnect
        # and cancel the wait. The watcher polls at 1 Hz, so 2.5 s is
        # enough for at least one tick + cleanup.
        await asyncio.sleep(2.5)

        # 3. A fresh client retries the wait. With the disconnect-
        # cancellation in place, _wait_in_flight has been cleared and
        # the call must NOT return 409.
        async with httpx.AsyncClient(base_url=base, timeout=10.0) as fresh:
            r = await fresh.post(
                f"/v1/sessions/{sid}/participants/me/inbox/wait",
                headers={"Authorization": f"Bearer {token}"},
                json={"timeout_seconds": 0.3},
            )
            assert r.status_code == 200, (
                f"expected 200 timed_out=true; got {r.status_code} {r.text}"
            )
            body = r.json()
            assert body["timed_out"] is True
            assert body["messages"] == []


async def _pull_participant(c: httpx.AsyncClient) -> tuple[dict[str, Any], dict[str, Any]]:
    made = (await c.post("/v1/sessions", json={})).json()
    r = await c.post(
        f"/v1/sessions/{made['session_id']}/participants",
        headers=bearer(made["join_token"]), json={"role": "worker"},
    )
    assert r.status_code == 201, r.text
    return made, r.json()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_waiter_learns_the_session_ended_when_it_is_deleted() -> None:
    """A REST pull participant blocked in inbox/wait while the creator
    deletes the session receives ``session.ended`` as the wait's result."""
    async with relay_subprocess() as base, httpx.AsyncClient(
        base_url=base, timeout=15.0
    ) as c:
        made, me = await _pull_participant(c)
        sid = made["session_id"]
        wait = asyncio.create_task(c.post(
            f"/v1/sessions/{sid}/participants/me/inbox/wait",
            headers=bearer(me["participant_token"]),
            json={"names": ["command.received"], "timeout_seconds": 10},
        ))
        await asyncio.sleep(0.3)
        d = await c.delete(f"/v1/sessions/{sid}", headers=bearer(made["creator_token"]))
        assert d.status_code in (200, 204), d.text

        r = await asyncio.wait_for(wait, 5)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["timed_out"] is False
        assert [m["name"] for m in body["messages"]] == ["session.ended"]
        assert body["messages"][0]["payload"]["reason"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_removal_during_a_wait_returns_the_halt_then_removed() -> None:
    """The halt queued by a removal is the wait's result; the next call
    answers ``participant_removed`` (never ``participant_evicted``)."""
    async with relay_subprocess() as base, httpx.AsyncClient(
        base_url=base, timeout=15.0
    ) as c:
        made, me = await _pull_participant(c)
        sid, h = made["session_id"], bearer(me["participant_token"])
        wait = asyncio.create_task(c.post(
            f"/v1/sessions/{sid}/participants/me/inbox/wait", headers=h,
            json={"timeout_seconds": 10},
        ))
        await asyncio.sleep(0.3)
        rm = await c.delete(
            f"/v1/sessions/{sid}/participants/{me['participant_id']}",
            headers=bearer(made["creator_token"]),
        )
        assert rm.status_code == 200, rm.text

        r = await asyncio.wait_for(wait, 5)
        assert r.status_code == 200, r.text
        msgs = r.json()["messages"]
        assert [(m["name"], m["payload"]["kind"]) for m in msgs] == [
            ("supervisor.note", "halt")
        ]
        again = await c.post(
            f"/v1/sessions/{sid}/participants/me/inbox/wait", headers=h,
            json={"timeout_seconds": 1},
        )
        assert again.status_code == 403, again.text
        assert again.json()["error"]["code"] == "participant_removed"
