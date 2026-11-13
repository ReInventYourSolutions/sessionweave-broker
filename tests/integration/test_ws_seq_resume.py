"""Coverage for issue #43 — WS reconnect must not deliver duplicates.

The relay assigns a per-participant monotonic ``seq`` to every event it
fans out and surfaces it on the WS frame envelope. A reconnecting WS
client passes ``?last_acked_seq=N`` on the upgrade URL; the relay
filters the buffer replay to skip events with ``seq <= N``.

Three behaviours exercised:

1. Every event delivered over WS carries a strictly-monotonic ``seq``
   starting at 1 for the first event for that participant.
2. A reconnect with ``?last_acked_seq=N`` does NOT replay any event
   with ``seq <= N``. All seq>N events are still delivered exactly
   once.
3. The REST inbox-drain endpoint accepts the same ``?last_acked_seq``
   filter and returns ``seq`` on every message.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
import websockets

from tests.integration.client import bearer, sw_client
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


async def _join_and_get_token(
    base: str, sid: str, jt: str, role: str = "worker"
) -> tuple[str, str]:
    async with httpx.AsyncClient() as c:
        r = await c.post(
            f"{base}/v1/sessions/{sid}/participants",
            headers=bearer(jt),
            json={"role": role, "capabilities": ["respond"]},
        )
        r.raise_for_status()
        body = r.json()
    return body["participant_id"], body["participant_token"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ws_frames_carry_monotonic_seq() -> None:
    """Every event delivered over the per-participant WS endpoint
    must carry a ``seq`` field; the values must be strictly
    increasing and start at 1.
    """
    async with relay_subprocess() as base:
        base_ws = base.replace("http://", "ws://")
        async with httpx.AsyncClient() as c:
            _created = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()
            sid, jt = _created["session_id"], _created["join_token"]

        pid_b, tok_b = await _join_and_get_token(base, sid, jt, role="worker")

        ws_url = (
            f"{base_ws}/v1/sessions/{sid}/participants/me/events?token={tok_b}"
        )
        async with websockets.connect(ws_url) as ws:
            # A joins via MCP and dispatches three commands at B so the
            # relay enqueues three events on B's WS.
            async with sw_client(base) as a:
                await a.call_tool(
                    "session_join", {"session_id": sid, "join_token": jt, "role": "sender"}
                )
                for i in range(3):
                    await a.call_tool(
                        "session_send",
                        {
                            "dispatch": "specific",
                            "target": {"participant_ids": [pid_b]},
                            "payload": {"i": i},
                            "timeout_seconds": 30,
                        },
                    )

            seqs: list[int] = []
            received_payloads: list[dict[str, Any]] = []
            for _ in range(3):
                raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
                frame = json.loads(raw)
                # New envelope shape: name + payload + seq.
                assert "seq" in frame, f"frame missing seq: {frame}"
                seqs.append(frame["seq"])
                received_payloads.append(frame["payload"])
            # The participant.joined event from A would have landed at
            # seq 1 since the WS is attached after A joins; depending on
            # timing it may or may not be in the WS stream. We only
            # assert: the seqs we observe are STRICTLY MONOTONIC.
            assert seqs == sorted(seqs)
            assert len(set(seqs)) == 3, f"duplicate seq observed: {seqs}"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ws_reconnect_with_last_acked_seq_skips_acked_events() -> None:
    """The canonical scenario from the issue: client consumes events
    1..N over WS, the socket dies mid-stream, the client reconnects
    with ``?last_acked_seq=N``. The relay must NOT re-deliver events
    1..N; only seq>N events surface on the new connection.
    """
    async with relay_subprocess() as base:
        base_ws = base.replace("http://", "ws://")
        async with httpx.AsyncClient() as c:
            _created = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()
            sid, jt = _created["session_id"], _created["join_token"]

        pid_b, tok_b = await _join_and_get_token(base, sid, jt)

        # Keep A alive across both WS phases. If A left mid-test, its
        # inflight trackers would be cancelled (sender_gone), at which
        # point ``_filter_stale_buffer_items`` (issue #44) drops the
        # buffered command.received events as stale and the reconnect
        # scenario can't be observed end-to-end. Real clients that
        # reconnect normally don't lose their sender peer, so the test
        # holds A open throughout.
        async with sw_client(base) as a:
            await a.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "sender"}
            )

            # Push 6 events. We'll consume 3 over the first socket,
            # close it, then reconnect and assert the remaining 3 land
            # exactly once.
            for i in range(6):
                await a.call_tool(
                    "session_send",
                    {
                        "dispatch": "specific",
                        "target": {"participant_ids": [pid_b]},
                        "payload": {"i": i},
                        "timeout_seconds": 60,
                    },
                )

            ws_url = (
                f"{base_ws}/v1/sessions/{sid}/participants/me/events?token={tok_b}"
            )
            first_phase_seqs: list[int] = []
            last_acked: int = 0
            async with websockets.connect(ws_url) as ws:
                # Consume the first 3 events from the application's point
                # of view and explicitly ACK them so the relay's unacked
                # tracking can trim them. The 4th-Nth frames the server
                # send_json'd may sit in the kernel TCP buffer unread; on
                # disconnect the relay must push the un-ACKed ones back.
                for _ in range(3):
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    frame = json.loads(raw)
                    first_phase_seqs.append(frame["seq"])
                    last_acked = max(last_acked, frame["seq"])
                await ws.send(json.dumps({"ack": last_acked}))
                # Brief settle so the relay processes the ACK before the
                # close-frame races it.
                await asyncio.sleep(0.1)

            # First-phase events must be from the start of the per-
            # participant stream and monotonic.
            assert first_phase_seqs == sorted(first_phase_seqs)
            assert min(first_phase_seqs) >= 1
            assert last_acked == max(first_phase_seqs)

            # Reconnect with last_acked_seq=last_acked. The relay must
            # filter the buffer replay so we see only seq>last_acked.
            reconnect_url = f"{ws_url}&last_acked_seq={last_acked}"
            second_phase_seqs: list[int] = []
            # Wait for the CONDITION (all six events recovered), not an
            # arbitrary idle timeout. A reconnect can race the previous
            # connection's server-side teardown: events_ws re-buffers the
            # un-ACKed events (seq>last_acked) in its `finally` only AFTER it
            # observes the disconnect, so the first attach may see an empty
            # backlog. Re-attach until the buffer has settled and every
            # seq>last_acked event has been recovered (deduped across attaches),
            # bounded by a generous overall deadline.
            expected_total = 6
            deadline = asyncio.get_event_loop().time() + 15.0
            while (
                len(set(first_phase_seqs) | set(second_phase_seqs)) < expected_total
                and asyncio.get_event_loop().time() < deadline
            ):
                async with websockets.connect(reconnect_url) as ws2:
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws2.recv(), timeout=1.0)
                        except TimeoutError:
                            break
                        seq = json.loads(raw)["seq"]
                        if seq not in second_phase_seqs:
                            second_phase_seqs.append(seq)
                if len(set(first_phase_seqs) | set(second_phase_seqs)) < expected_total:
                    await asyncio.sleep(0.1)

            # No overlap between the two phases.
            assert set(first_phase_seqs).isdisjoint(set(second_phase_seqs)), (
                f"duplicates! first={first_phase_seqs} second={second_phase_seqs}"
            )
            # Every second-phase seq must be strictly greater than the ACK.
            assert all(s > last_acked for s in second_phase_seqs), (
                f"got seq <= last_acked={last_acked}: {second_phase_seqs}"
            )
            # Together, the two phases cover at least all 6
            # command.received events without dupes. The first phase
            # may also include the participant.joined frame for A
            # (seq=1) and similar noise that we don't have visibility
            # into without inspecting payloads, so we only assert
            # no-duplicates + completeness w.r.t. the union.
            all_seqs = sorted(first_phase_seqs + second_phase_seqs)
            assert len(all_seqs) == len(set(all_seqs)), (
                f"duplicates across phases: first={first_phase_seqs} "
                f"second={second_phase_seqs}"
            )
            # We dispatched 6 commands and B was offline-buffered for
            # all of them, so the relay must surface at least 6 events
            # across the two phases.
            assert len(all_seqs) >= 6, (
                f"only {len(all_seqs)} events observed; first={first_phase_seqs} "
                f"second={second_phase_seqs}"
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rest_inbox_drain_includes_seq_and_respects_last_acked_seq() -> None:
    """REST clients (which never get a live WS) get the same seq
    annotation on every drained message, and can pass
    ``?last_acked_seq=N`` to skip events they've already consumed
    via some other channel.
    """
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            _created = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()
            sid, jt = _created["session_id"], _created["join_token"]

        _, tok_b = await _join_and_get_token(base, sid, jt)

        async with sw_client(base) as a:
            await a.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "sender"}
            )
            for i in range(4):
                await a.call_tool(
                    "session_send",
                    {
                        "dispatch": "all",
                        "target": {"capability": "respond"},
                        "payload": {"i": i},
                        "timeout_seconds": 30,
                    },
                )

            # Full drain — assert every entry has an int seq, and the
            # seqs are strictly monotonic.
            async with httpx.AsyncClient() as c:
                pid_b = (await c.get(
                    f"{base}/v1/sessions/{sid}", headers=bearer(tok_b)
                )).json()["roster"]
                # find B's participant_id
                target_pid = next(
                    p["participant_id"] for p in pid_b if p["role"] != "sender"
                )
                assert target_pid

                r = await c.post(
                    f"{base}/v1/sessions/{sid}/participants/me/inbox/drain",
                    headers={"authorization": f"Bearer {tok_b}"},
                )
                r.raise_for_status()
                full = r.json()
            assert len(full["messages"]) >= 4
            cmd_msgs = [
                m for m in full["messages"] if m["name"] == "command.received"
            ]
            assert len(cmd_msgs) == 4
            seqs = [m["seq"] for m in cmd_msgs]
            assert all(isinstance(s, int) for s in seqs), (
                f"non-int seq in drained payload: {seqs}"
            )
            assert seqs == sorted(seqs)
            assert len(set(seqs)) == 4

            # Now push 2 more, then drain with last_acked_seq=highest
            # seen so far. Only the 2 new ones must come back.
            ack = max(seqs)
            for i in range(2):
                await a.call_tool(
                    "session_send",
                    {
                        "dispatch": "all",
                        "target": {"capability": "respond"},
                        "payload": {"i": 100 + i},
                        "timeout_seconds": 30,
                    },
                )

            async with httpx.AsyncClient() as c:
                r = await c.post(
                    f"{base}/v1/sessions/{sid}/participants/me/inbox/drain"
                    f"?last_acked_seq={ack}",
                    headers={"authorization": f"Bearer {tok_b}"},
                )
                r.raise_for_status()
                filtered = r.json()
            cmd_msgs2 = [
                m for m in filtered["messages"] if m["name"] == "command.received"
            ]
            assert len(cmd_msgs2) == 2, (
                f"expected 2 new command.received after ack={ack}, "
                f"got: {filtered['messages']}"
            )
            for m in filtered["messages"]:
                assert m["seq"] > ack, (
                    f"event with seq<=ack leaked through: {m}"
                )
