"""End-to-end tests for the ``inbox/wait`` (wait_for_event) feature.

These tests spin up a real relay subprocess and exercise the full
request→relay→response path via both the HTTP REST API and the MCP
Streamable-HTTP transport.

HTTP concurrency note
---------------------
The relay's HTTP transport uses a module-level ``_HTTP_HANDLE`` singleton
that can only bind one participant at a time within the same server process.
Two *concurrent* HTTP requests against the same relay would collide on that
singleton.  We therefore use the **buffered-drain pattern** for the HTTP
tests: the command is sent first (buffering a ``command.received`` event in
participant A's inbox), then ``inbox/wait`` is called and drains immediately
without any concurrent binding conflict.

The MCP tests do not have this limitation — each MCP session opens its own
independent connection, so a true long-poll-concurrent-with-send scenario is
possible and is exercised in ``test_wait_for_event_mcp_wakes_on_send``.
"""
from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest

from tests.integration.client import bearer, create_and_invite, sw_client
from tests.integration.harness import relay_subprocess

# ---------------------------------------------------------------------------
# HTTP-transport E2E tests (buffered-drain pattern)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_wait_for_event_http_wakes_on_buffered_event() -> None:
    """inbox/wait returns immediately when a buffered event is already waiting.

    Scenario:
    1. Relay subprocess starts.
    2. Participant A (waiter) and B (sender) join via HTTP (forces pull mode).
    3. B sends a targeted command to A — buffered in A's inbox.
    4. A calls inbox/wait, drains the buffer, returns timed_out=false.
    """
    async with relay_subprocess() as base, httpx.AsyncClient(
        base_url=base, timeout=15.0
    ) as c:
        # Create session.
        r = await c.post("/v1/sessions", json={})
        assert r.status_code == 201, r.text
        sid = r.json()["session_id"]
        jt = r.json()["join_token"]
        # Join two participants (HTTP join always forces pull mode).
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

        # Sender dispatches a command targeting participant A; this buffers
        # a command.received event in A's inbox *before* the wait call.
        r = await c.post(
            f"/v1/sessions/{sid}/commands",
            headers={"Authorization": f"Bearer {token_b}"},
            json={
                "dispatch": "specific",
                "target": {"participant_ids": [pid_a]},
                "payload": {"greeting": "hello"},
                "timeout_seconds": 10,
            },
        )
        assert r.status_code == 201, r.text

        # inbox/wait should drain the backlog and return immediately.
        r = await c.post(
            f"/v1/sessions/{sid}/participants/me/inbox/wait",
            headers={"Authorization": f"Bearer {token_a}"},
            json={"timeout_seconds": 5},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["timed_out"] is False, f"expected timed_out=false, got: {body}"
        assert any(
            m["name"] == "command.received" for m in body["messages"]
        ), f"expected command.received in messages, got: {body['messages']}"


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_wait_for_event_http_timeout() -> None:
    """inbox/wait returns timed_out=true with an empty message list when no event arrives."""
    async with relay_subprocess() as base, httpx.AsyncClient(
        base_url=base, timeout=10.0
    ) as c:
        r = await c.post("/v1/sessions", json={})
        assert r.status_code == 201, r.text
        sid = r.json()["session_id"]
        jt = r.json()["join_token"]
        r = await c.post(
            f"/v1/sessions/{sid}/participants",
            headers=bearer(jt),
            json={"role": "lone-participant"},
        )
        assert r.status_code == 201, r.text
        token = r.json()["participant_token"]

        r = await c.post(
            f"/v1/sessions/{sid}/participants/me/inbox/wait",
            headers={"Authorization": f"Bearer {token}"},
            json={"timeout_seconds": 0.4},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["timed_out"] is True, f"expected timed_out=true, got: {body}"
        assert body["messages"] == [], f"expected empty messages, got: {body['messages']}"


# ---------------------------------------------------------------------------
# MCP-transport E2E tests (true concurrent long-poll + send)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_wait_for_event_mcp_wakes_on_send() -> None:
    """MCP client A long-polls; MCP client B sends; A wakes before timeout.

    Unlike the HTTP tests, the MCP transport is not limited to a single
    concurrent handle, so this test exercises the genuine long-poll wake-up
    path end-to-end.
    """
    async with relay_subprocess() as base:
        async with sw_client(base) as client_a, sw_client(base) as client_b:
            sid, _creator, jt = await create_and_invite(client_a, {})

            # Both participants join in pull mode.
            join_a = await client_a.call_tool(
                "session_join",
                {"session_id": sid, "join_token": jt, "role": "waiter", "delivery_mode": "pull"},
            )
            pid_a = join_a["participant_id"]

            join_b = await client_b.call_tool(
                "session_join",
                {"session_id": sid, "join_token": jt, "role": "sender", "delivery_mode": "pull"},
            )
            pid_b = join_b["participant_id"]
            assert pid_a != pid_b

            # Drain client_a's inbox so the participant.joined notification
            # (emitted when client_b joined) does not satisfy the wait early.
            await client_a.call_tool("session_check_inbox", {"session_id": sid})

            waiter_result: dict[str, Any] = {}

            async def waiter() -> None:
                out = await client_a.call_tool(
                    "session_wait_for_event",
                    {"session_id": sid, "timeout_seconds": 5},
                )
                waiter_result.update(out)

            async def sender() -> None:
                # Give the waiter coroutine time to start blocking.
                await asyncio.sleep(0.3)
                await client_b.call_tool(
                    "session_send",
                    {
                        "dispatch": "specific",
                        "target": {"participant_ids": [pid_a]},
                        "payload": {"wake": True},
                        "timeout_seconds": 10,
                    },
                )

            await asyncio.gather(waiter(), sender())

            assert waiter_result.get("timed_out") is False, (
                f"expected timed_out=false, got: {waiter_result}"
            )
            assert any(
                m["name"] == "command.received"
                for m in waiter_result.get("messages", [])
            ), f"expected command.received, got: {waiter_result.get('messages')}"


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_wait_for_event_mcp_timeout() -> None:
    """MCP session_wait_for_event returns timed_out=true when no event arrives."""
    async with relay_subprocess() as base:
        async with sw_client(base) as client:
            sid, _creator, jt = await create_and_invite(client, {})
            await client.call_tool(
                "session_join",
                {"session_id": sid, "join_token": jt, "role": "lone", "delivery_mode": "pull"},
            )

            out = await client.call_tool(
                "session_wait_for_event",
                {"session_id": sid, "timeout_seconds": 0.4},
            )

            assert out["timed_out"] is True, f"expected timed_out=true, got: {out}"
            assert out["messages"] == [], f"expected empty messages, got: {out['messages']}"
