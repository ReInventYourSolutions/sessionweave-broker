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

        # 2. Join two participants (HTTP join always forces pull mode).
        r = await c.post(
            f"/v1/sessions/{sid}/participants",
            json={"role": "waiter", "capabilities": ["listen"]},
        )
        assert r.status_code == 201, r.text
        pid_a = r.json()["participant_id"]
        token_a = r.json()["participant_token"]

        r = await c.post(
            f"/v1/sessions/{sid}/participants",
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

        # Two listeners + one sender, all pull-mode (HTTP forces it).
        async def join(role: str, capabilities: list[str]) -> tuple[str, str]:
            r = await c.post(
                f"/v1/sessions/{sid}/participants",
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

        r = await c.post(
            f"/v1/sessions/{sid}/participants", json={"role": "p"}
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
