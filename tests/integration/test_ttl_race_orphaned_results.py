"""Coverage for issue #44 — TTL expiry race at session_resume + orphaned
result.received events when the sender of a multi-recipient command leaves
before all responders have answered.

Two behaviours:

1. ``command.received`` for an already-expired command must NOT be handed
   out at buffer drain time (``check_inbox`` / WS attach / resume push).
   The buffer still carries the entry, but the relay filters it because
   the underlying tracker is gone from ``_inflight`` (or expired in
   place). The participant should only see the corresponding
   ``command.cancelled reason=ttl_expired`` from the same buffer.

2. When a sender leaves a session with pre-existing ``result.received``
   for one of its inflight commands, the relay must surface a
   ``command.completed reason=sender_gone`` on the firehose carrying
   ``orphaned_results: [responder_id, ...]`` so observers can detect the
   data-loss scenario instead of seeing a silent eviction of the
   sender's buffer.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
import websockets

from tests.integration.client import sw_client
from tests.integration.harness import relay_subprocess


async def _wait_for(
    predicate: Any, *, timeout: float = 5.0, message: str = "predicate not met"
) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(message)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_check_inbox_filters_expired_command_received() -> None:
    """Pull-mode B is offline-buffered; the TTL fires before B drains.
    `session_check_inbox` must NOT return the now-stale `command.received`
    — only the `command.cancelled reason=ttl_expired` that the relay
    enqueued when the TTL deadline ticked.
    """
    async with relay_subprocess() as base:
        async with sw_client(base) as a, sw_client(base) as b:
            sid = (await a.call_tool("session_create", {}))["session_id"]
            await a.call_tool(
                "session_join", {"session_id": sid, "role": "sender"}
            )
            pb = await b.call_tool(
                "session_join",
                {
                    "session_id": sid,
                    "role": "worker",
                    "capabilities": ["respond"],
                    "delivery_mode": "pull",
                },
            )

            await a.call_tool(
                "session_send",
                {
                    "dispatch": "specific",
                    "target": {"participant_ids": [pb["participant_id"]]},
                    "payload": {"q": "ping"},
                    "timeout_seconds": 1,
                },
            )

            # Wait past the TTL plus one registry-tick margin so the relay
            # has fired command.cancelled into B's buffer.
            await asyncio.sleep(2.0)

            drained = await b.call_tool("session_check_inbox", {})
            messages = drained["messages"]
            kinds = [m["name"] for m in messages]

            assert "command.received" not in kinds, (
                "expired command.received leaked through buffer drain; "
                f"got events: {kinds}"
            )
            assert "command.cancelled" in kinds, (
                f"expected command.cancelled in drain, got: {kinds}"
            )
            cancellations = [
                m for m in messages if m["name"] == "command.cancelled"
            ]
            assert any(
                m["payload"].get("reason") == "ttl_expired" for m in cancellations
            ), f"missing ttl_expired cancellation: {cancellations}"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sender_gone_emits_orphaned_results_on_firehose() -> None:
    """When the sender of a `dispatch="all"` command leaves with at
    least one responder having already answered, the relay must emit
    `command.completed reason=sender_gone` on the firehose with the
    list of orphaned responder ids — so a buffer eviction never
    silently swallows a `result.received`.
    """
    async with relay_subprocess() as base:
        base_ws = base.replace("http://", "ws://")
        async with httpx.AsyncClient() as h:
            sid = (
                await h.post(
                    f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}}
                )
            ).json()["session_id"]

        firehose_url = f"{base_ws}/v1/sessions/{sid}/firehose?token={sid}"
        async with websockets.connect(firehose_url) as ws:
            received: list[dict[str, Any]] = []

            async def _reader() -> None:
                try:
                    while True:
                        raw = await ws.recv()
                        received.append(json.loads(raw))
                except websockets.exceptions.ConnectionClosed:
                    return

            reader_task = asyncio.create_task(_reader())

            async with (
                sw_client(base) as a,
                sw_client(base) as b,
                sw_client(base) as c,
            ):
                pa = await a.call_tool(
                    "session_join", {"session_id": sid, "role": "sender"}
                )
                pb = await b.call_tool(
                    "session_join",
                    {
                        "session_id": sid,
                        "role": "worker",
                        "capabilities": ["respond"],
                    },
                )
                await c.call_tool(
                    "session_join",
                    {
                        "session_id": sid,
                        "role": "worker",
                        "capabilities": ["respond"],
                    },
                )

                received_b: list[tuple[str, dict[str, Any]]] = []
                b.on_notification(lambda n, p: received_b.append((n, p)))

                send = await a.call_tool(
                    "session_send",
                    {
                        "dispatch": "all",
                        "target": {"role": "worker"},
                        "payload": {"q": "ping"},
                        "timeout_seconds": 30,
                    },
                )
                cmd_id = send["command_id"]

                # B responds promptly. C is left intentionally silent so
                # the command is still inflight when A leaves.
                await _wait_for(
                    lambda: any(
                        n == "command.received" and p["command_id"] == cmd_id
                        for n, p in received_b
                    )
                )
                await b.call_tool(
                    "session_respond",
                    {
                        "command_id": cmd_id,
                        "status": "ok",
                        "payload": {"ack": True},
                    },
                )

                # Wait for the result.received to land on the firehose so
                # we know the relay's tracker recorded B as a responder
                # BEFORE A leaves.
                await _wait_for(
                    lambda: any(
                        f["name"] == "result.received"
                        and f["payload"]["command_id"] == cmd_id
                        for f in received
                    )
                )

                await a.call_tool("session_leave", {})

            # A is gone; the relay's _cancel_inflight_for_sender should
            # have fired command.cancelled (to C) and the new
            # command.completed reason=sender_gone with orphaned_results.
            await _wait_for(
                lambda: any(
                    f["name"] == "command.completed"
                    and f["payload"].get("reason") == "sender_gone"
                    and f["payload"]["command_id"] == cmd_id
                    for f in received
                ),
                message="no command.completed reason=sender_gone on firehose",
            )

            completed = next(
                f
                for f in received
                if f["name"] == "command.completed"
                and f["payload"].get("reason") == "sender_gone"
                and f["payload"]["command_id"] == cmd_id
            )
            orphaned = completed["payload"].get("orphaned_results")
            assert orphaned == [pb["participant_id"]], (
                f"orphaned_results mismatch: expected [{pb['participant_id']}], "
                f"got {orphaned}"
            )
            assert completed["payload"]["received_count"] == 1
            assert completed["payload"]["expected_count"] == 2

            # And the cancellation to the still-pending recipient is
            # still emitted as before.
            assert any(
                f["name"] == "command.cancelled"
                and f["payload"].get("reason") == "sender_gone"
                and f["payload"]["command_id"] == cmd_id
                for f in received
            ), "missing command.cancelled reason=sender_gone for pending recipient"

            reader_task.cancel()
            try:
                await reader_task
            except asyncio.CancelledError:
                pass

            # Silence the lint about pa being unused in narrative tests.
            assert pa["participant_id"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sender_gone_delivers_completed_to_responders() -> None:
    """Follow-up from issue #49 / #48 item 4: when a sender leaves
    with at least one ``result.received`` already recorded, the
    relay must deliver ``command.completed reason=sender_gone`` to
    each responder's own inbox — not only to the firehose.

    The pending recipients already get ``command.cancelled``; this
    test asserts the *responders* get the new completed event so
    they can correlate their submitted result with the eventual
    orphaning, without having to subscribe to the firehose.
    """
    async with relay_subprocess() as base:
        async with sw_client(base) as a, sw_client(base) as b, sw_client(base) as c:
            sid = (await a.call_tool("session_create", {}))["session_id"]
            await a.call_tool(
                "session_join", {"session_id": sid, "role": "sender"}
            )
            pb = await b.call_tool(
                "session_join",
                {
                    "session_id": sid,
                    "role": "worker",
                    "capabilities": ["respond"],
                },
            )
            await c.call_tool(
                "session_join",
                {
                    "session_id": sid,
                    "role": "worker",
                    "capabilities": ["respond"],
                },
            )
            received_b: list[tuple[str, dict[str, Any]]] = []
            b.on_notification(lambda n, p: received_b.append((n, p)))

            send = await a.call_tool(
                "session_send",
                {
                    "dispatch": "all",
                    "target": {"role": "worker"},
                    "payload": {"q": "ping"},
                    "timeout_seconds": 30,
                },
            )
            cmd_id = send["command_id"]

            # B responds, C remains pending.
            await _wait_for(
                lambda: any(
                    n == "command.received" and p["command_id"] == cmd_id
                    for n, p in received_b
                )
            )
            await b.call_tool(
                "session_respond",
                {
                    "command_id": cmd_id,
                    "status": "ok",
                    "payload": {"ack": True},
                },
            )

            # A leaves. The relay fires _cancel_inflight_for_sender,
            # which must deliver command.completed reason=sender_gone
            # to B (the responder), in addition to the firehose
            # mirror and the command.cancelled to C.
            await a.call_tool("session_leave", {})

            await _wait_for(
                lambda: any(
                    n == "command.completed"
                    and p.get("reason") == "sender_gone"
                    and p["command_id"] == cmd_id
                    for n, p in received_b
                ),
                message=(
                    "B (responder) did not receive command.completed "
                    "reason=sender_gone in its inbox"
                ),
            )
            completed = next(
                p
                for n, p in received_b
                if n == "command.completed"
                and p.get("reason") == "sender_gone"
                and p["command_id"] == cmd_id
            )
            assert completed["orphaned_results"] == [pb["participant_id"]]
            assert completed["received_count"] == 1
            assert completed["expected_count"] == 2
