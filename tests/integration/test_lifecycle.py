from __future__ import annotations

import asyncio
from typing import Any

import pytest

from tests.integration.client import create_and_invite, sw_client
from tests.integration.harness import relay_subprocess


async def _wait_for(
    predicate: Any, *, timeout: float = 4.0, message: str = "predicate not met"
) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(message)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sender_leave_cancels_inflight_command() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as a, sw_client(base) as b:
            sid, _creator, jt = await create_and_invite(a, {})
            await a.call_tool("session_join", {"session_id": sid, "join_token": jt, "role": "s"})
            pb = await b.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "r"}
            )

            received_b: list[tuple[str, dict[str, Any]]] = []
            b.on_notification(lambda n, p: received_b.append((n, p)))

            send = await a.call_tool(
                "session_send",
                {
                    "dispatch": "specific",
                    "target": {"participant_ids": [pb["participant_id"]]},
                    "payload": {},
                    "timeout_seconds": 60,
                },
            )
            cmd_id = send["command_id"]

            await a.call_tool("session_leave", {})

            await _wait_for(
                lambda: any(
                    n == "command.cancelled"
                    and p["command_id"] == cmd_id
                    and p["reason"] == "sender_gone"
                    for n, p in received_b
                ),
                message=f"command.cancelled not received: {received_b}",
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_double_respond_returns_already_responded() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as a, sw_client(base) as b:
            sid, _creator, jt = await create_and_invite(a, {})
            await a.call_tool("session_join", {"session_id": sid, "join_token": jt, "role": "s"})
            pb = await b.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "r"}
            )
            send = await a.call_tool(
                "session_send",
                {
                    "dispatch": "specific",
                    "target": {"participant_ids": [pb["participant_id"]]},
                    "payload": {},
                    "timeout_seconds": 60,
                },
            )
            await b.call_tool(
                "session_respond",
                {
                    "command_id": send["command_id"],
                    "status": "ok",
                    "payload": {},
                },
            )
            with pytest.raises(Exception) as exc:
                await b.call_tool(
                    "session_respond",
                    {
                        "command_id": send["command_id"],
                        "status": "ok",
                        "payload": {},
                    },
                )
            assert "already_responded" in str(exc.value)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_respond_after_deadline_command_not_found() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as a, sw_client(base) as b:
            sid, _creator, jt = await create_and_invite(a, {})
            await a.call_tool("session_join", {"session_id": sid, "join_token": jt, "role": "s"})
            pb = await b.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "r"}
            )
            send = await a.call_tool(
                "session_send",
                {
                    "dispatch": "specific",
                    "target": {"participant_ids": [pb["participant_id"]]},
                    "payload": {},
                    "timeout_seconds": 1,
                },
            )
            await asyncio.sleep(2.5)
            with pytest.raises(Exception) as exc:
                await b.call_tool(
                    "session_respond",
                    {
                        "command_id": send["command_id"],
                        "status": "ok",
                        "payload": {},
                    },
                )
            assert "command_not_found" in str(exc.value)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_self_propagates_participant_updated() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as a, sw_client(base) as b:
            sid, _creator, jt = await create_and_invite(a, {})
            await a.call_tool("session_join", {"session_id": sid, "join_token": jt, "role": "s"})
            await b.call_tool("session_join", {"session_id": sid, "join_token": jt, "role": "r"})

            received_a: list[tuple[str, dict[str, Any]]] = []
            a.on_notification(lambda n, p: received_a.append((n, p)))

            await b.call_tool(
                "session_update_self",
                {"role": "reviewer", "capabilities": ["sec", "perf"]},
            )

            await _wait_for(
                lambda: any(
                    n == "participant.updated" for n, _ in received_a
                ),
                message=f"participant.updated not received: {received_a}",
            )
            updated = next(
                p for n, p in received_a if n == "participant.updated"
            )
            assert updated["participant"]["role"] == "reviewer"
            assert "sec" in updated["participant"]["capabilities"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_participants_returns_full_roster() -> None:
    async with relay_subprocess() as base:
        async with (
            sw_client(base) as a,
            sw_client(base) as b,
            sw_client(base) as c,
        ):
            sid, _creator, jt = await create_and_invite(a, {})
            await a.call_tool("session_join", {"session_id": sid, "join_token": jt, "role": "s"})
            await b.call_tool("session_join", {"session_id": sid, "join_token": jt, "role": "r"})
            await c.call_tool("session_join", {"session_id": sid, "join_token": jt, "role": "r"})

            roster = await a.call_tool("session_list_participants", {})
            roles = [p["role"] for p in roster["roster"]]
            assert sorted(roles) == ["r", "r", "s"]
            assert all(p["online"] for p in roster["roster"])
