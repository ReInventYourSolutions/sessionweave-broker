from __future__ import annotations

import asyncio
from typing import Any

import pytest

from tests.integration.client import sw_client
from tests.integration.harness import relay_subprocess


async def _wait_for(
    predicate: Any, *, timeout: float = 2.0, message: str = "predicate not met"
) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(message)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_specific_happy_path_round_trip() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as a, sw_client(base) as b:
            sid = (await a.call_tool("session_create", {}))["session_id"]
            await a.call_tool(
                "session_join", {"session_id": sid, "role": "sender"}
            )
            pb = await b.call_tool(
                "session_join", {"session_id": sid, "role": "reviewer"}
            )

            received_a: list[tuple[str, dict[str, Any]]] = []
            received_b: list[tuple[str, dict[str, Any]]] = []
            a.on_notification(lambda n, p: received_a.append((n, p)))
            b.on_notification(lambda n, p: received_b.append((n, p)))

            send = await a.call_tool(
                "session_send",
                {
                    "dispatch": "specific",
                    "target": {"participant_ids": [pb["participant_id"]]},
                    "payload": {"q": 1},
                },
            )
            cmd_id = send["command_id"]

            await _wait_for(
                lambda: any(
                    n == "command.received" and p["command_id"] == cmd_id
                    for n, p in received_b
                )
            )

            await b.call_tool(
                "session_respond",
                {"command_id": cmd_id, "status": "ok", "payload": {"r": 2}},
            )

            await _wait_for(
                lambda: any(
                    n == "command.completed" and p["reason"] == "all_responded"
                    for n, p in received_a
                )
            )
            assert any(n == "result.received" for n, _ in received_a)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_all_to_three_reviewers() -> None:
    async with (
        relay_subprocess() as base,
    ):
        async with (
            sw_client(base) as a,
            sw_client(base) as b,
            sw_client(base) as c,
            sw_client(base) as d,
        ):
            sid = (await a.call_tool("session_create", {}))["session_id"]
            await a.call_tool(
                "session_join", {"session_id": sid, "role": "sender"}
            )
            await b.call_tool(
                "session_join", {"session_id": sid, "role": "reviewer"}
            )
            await c.call_tool(
                "session_join", {"session_id": sid, "role": "reviewer"}
            )
            await d.call_tool(
                "session_join", {"session_id": sid, "role": "reviewer"}
            )

            received_a: list[tuple[str, dict[str, Any]]] = []
            received: dict[str, list[tuple[str, dict[str, Any]]]] = {
                "b": [],
                "c": [],
                "d": [],
            }
            a.on_notification(lambda n, p: received_a.append((n, p)))
            b.on_notification(lambda n, p: received["b"].append((n, p)))
            c.on_notification(lambda n, p: received["c"].append((n, p)))
            d.on_notification(lambda n, p: received["d"].append((n, p)))

            send = await a.call_tool(
                "session_send",
                {
                    "dispatch": "all",
                    "target": {"role": "reviewer"},
                    "payload": {"q": 1},
                },
            )
            cmd_id = send["command_id"]
            assert len(send["delivered_to"]) == 3

            for letter in ("b", "c", "d"):
                await _wait_for(
                    lambda li=letter: any(
                        n == "command.received" for n, _ in received[li]
                    )
                )

            for letter, client in (("b", b), ("c", c), ("d", d)):
                await client.call_tool(
                    "session_respond",
                    {
                        "command_id": cmd_id,
                        "status": "ok",
                        "payload": {letter: 1},
                    },
                )

            await _wait_for(
                lambda: any(
                    n == "command.completed" and p["reason"] == "all_responded"
                    for n, p in received_a
                )
            )
            results = [n for n, _ in received_a if n == "result.received"]
            assert len(results) == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_any_picks_one_recipient() -> None:
    async with relay_subprocess() as base:
        async with (
            sw_client(base) as a,
            sw_client(base) as b,
            sw_client(base) as c,
        ):
            sid = (await a.call_tool("session_create", {}))["session_id"]
            await a.call_tool(
                "session_join", {"session_id": sid, "role": "sender"}
            )
            await b.call_tool(
                "session_join", {"session_id": sid, "role": "reviewer"}
            )
            await c.call_tool(
                "session_join", {"session_id": sid, "role": "reviewer"}
            )

            received_b: list[tuple[str, dict[str, Any]]] = []
            received_c: list[tuple[str, dict[str, Any]]] = []
            b.on_notification(lambda n, p: received_b.append((n, p)))
            c.on_notification(lambda n, p: received_c.append((n, p)))

            send = await a.call_tool(
                "session_send",
                {
                    "dispatch": "any",
                    "target": {"role": "reviewer"},
                    "payload": {},
                },
            )
            assert len(send["delivered_to"]) == 1

            await _wait_for(
                lambda: any(n == "command.received" for n, _ in received_b)
                or any(n == "command.received" for n, _ in received_c)
            )

            recv_b = sum(1 for n, _ in received_b if n == "command.received")
            recv_c = sum(1 for n, _ in received_c if n == "command.received")
            assert recv_b + recv_c == 1
