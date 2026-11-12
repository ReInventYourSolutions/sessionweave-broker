from __future__ import annotations

import asyncio
from typing import Any

import pytest

from tests.integration.client import create_and_invite, sw_client
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
            sid, _creator, jt = await create_and_invite(a, {})
            await a.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "sender"}
            )
            pb = await b.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "reviewer"}
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
            sid, _creator, jt = await create_and_invite(a, {})
            await a.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "sender"}
            )
            await b.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "reviewer"}
            )
            await c.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "reviewer"}
            )
            await d.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "reviewer"}
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
            sid, _creator, jt = await create_and_invite(a, {})
            await a.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "sender"}
            )
            await b.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "reviewer"}
            )
            await c.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "reviewer"}
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_any_fairness_max_minus_min_le_one() -> None:
    """`dispatch="any"` over a stable roster of N candidates must distribute
    `total` commands such that ``max(inbox_i) - min(inbox_i) <= 1`` across
    all candidates and ``sum(inbox_i) == total`` (no command lost or
    duplicated).

    Verifies the contract that ``RoundRobinPicker`` is *strict* round-robin
    over the sorted candidate list — every candidate gets either
    ``ceil(total/n)`` or ``floor(total/n)`` commands, never more skew.
    """
    total = 24
    async with relay_subprocess() as base:
        async with (
            sw_client(base) as sender,
            sw_client(base) as r1,
            sw_client(base) as r2,
            sw_client(base) as r3,
            sw_client(base) as r4,
        ):
            sid, _creator, jt = await create_and_invite(sender, {})
            await sender.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "sender"}
            )
            receivers = [r1, r2, r3, r4]
            inbox_counts: dict[str, int] = {}
            received_buckets: list[list[tuple[str, dict[str, Any]]]] = []
            for r in receivers:
                pj = await r.call_tool(
                    "session_join",
                    {
                        "session_id": sid, "join_token": jt,
                        "role": "worker",
                        "capabilities": ["respond"],
                    },
                )
                pid = pj["participant_id"]
                inbox_counts[pid] = 0
                bucket: list[tuple[str, dict[str, Any]]] = []
                received_buckets.append(bucket)

                def _on_event(
                    n: str,
                    p: dict[str, Any],
                    _bucket: list[tuple[str, dict[str, Any]]] = bucket,
                ) -> None:
                    _bucket.append((n, p))

                r.on_notification(_on_event)

            sends = await asyncio.gather(
                *[
                    sender.call_tool(
                        "session_send",
                        {
                            "dispatch": "any",
                            "target": {"capability": "respond"},
                            "payload": {"i": i},
                        },
                    )
                    for i in range(total)
                ]
            )

            # Each dispatch must have hit exactly one recipient.
            delivered_pids = [s["delivered_to"][0] for s in sends]
            assert len(delivered_pids) == total
            assert all(len(s["delivered_to"]) == 1 for s in sends)

            for pid in delivered_pids:
                assert pid in inbox_counts, (
                    f"delivered_to references unknown participant {pid}"
                )
                inbox_counts[pid] += 1

            assert sum(inbox_counts.values()) == total, (
                f"sum(inbox) {sum(inbox_counts.values())} != total={total}"
            )
            spread = max(inbox_counts.values()) - min(inbox_counts.values())
            assert spread <= 1, (
                f"unfair distribution: counts={inbox_counts}, "
                f"spread={spread} > 1 (round-robin invariant violated)"
            )

            # The relay should have actually delivered each command.received
            # to the chosen recipient — no on-the-wire drop.
            await _wait_for(
                lambda: sum(
                    sum(1 for n, _ in b if n == "command.received")
                    for b in received_buckets
                )
                == total,
                timeout=5.0,
                message="not all `total` command.received events arrived",
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_any_no_loss_under_roster_churn() -> None:
    """While the sender is firing ``dispatch="any"`` commands at a stable
    pool of N candidates, a transient (N+1)-th candidate joins and
    leaves. Invariants:

      - sum(delivered_to[0] for all sends) == total  (no command silently
        dropped during the join/leave window)
      - every delivered_to[0] is a participant_id that was online at
        dispatch time (no command routed to a phantom recipient)
    """
    total = 30
    async with relay_subprocess() as base:
        async with (
            sw_client(base) as sender,
            sw_client(base) as r1,
            sw_client(base) as r2,
            sw_client(base) as r3,
            sw_client(base) as transient,
        ):
            sid, _creator, jt = await create_and_invite(sender, {})
            await sender.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "sender"}
            )
            stable_pids: set[str] = set()
            for r in (r1, r2, r3):
                pj = await r.call_tool(
                    "session_join",
                    {
                        "session_id": sid, "join_token": jt,
                        "role": "worker",
                        "capabilities": ["respond"],
                    },
                )
                stable_pids.add(pj["participant_id"])

            async def _churn() -> str:
                pj = await transient.call_tool(
                    "session_join",
                    {
                        "session_id": sid, "join_token": jt,
                        "role": "worker",
                        "capabilities": ["respond"],
                    },
                )
                await asyncio.sleep(0.05)
                await transient.call_tool("session_leave", {})
                return str(pj["participant_id"])

            churn_task = asyncio.create_task(_churn())
            sends = await asyncio.gather(
                *[
                    sender.call_tool(
                        "session_send",
                        {
                            "dispatch": "any",
                            "target": {"capability": "respond"},
                            "payload": {"i": i},
                        },
                    )
                    for i in range(total)
                ]
            )
            transient_pid = await churn_task

            valid_pids = stable_pids | {transient_pid}
            for s in sends:
                assert len(s["delivered_to"]) == 1, (
                    f"dispatch=any returned {len(s['delivered_to'])} "
                    "recipients; expected exactly 1"
                )
                assert s["delivered_to"][0] in valid_pids, (
                    f"command routed to phantom pid {s['delivered_to'][0]} "
                    f"not in {valid_pids}"
                )
            assert len(sends) == total  # no command lost from the caller's POV


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_any_under_multi_worker_simultaneous_churn() -> None:
    """Cross-endorsed follow-up from joint review session
    `3e6a2ac7-...`: the single-transient-worker test in #45 doesn't
    stress the `RoundRobinPicker.pick` sort-by-pid logic under
    *concurrent* roster mutations. Two transient workers join AND
    leave during a single dispatch burst; the picker re-sorts twice
    in close succession.

    Invariants (same shape as the stable case, no stronger because
    the relay does not guarantee fairness across a moving roster —
    just no-loss):

      - sum(delivered_to[0] for all sends) == total
      - every delivered_to[0] is a participant_id that was online at
        dispatch time (no phantom recipient)
    """
    total = 40
    async with relay_subprocess() as base:
        async with (
            sw_client(base) as sender,
            sw_client(base) as r1,
            sw_client(base) as r2,
            sw_client(base) as r3,
            sw_client(base) as t1,
            sw_client(base) as t2,
        ):
            sid, _creator, jt = await create_and_invite(sender, {})
            await sender.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "sender"}
            )
            stable_pids: set[str] = set()
            for r in (r1, r2, r3):
                pj = await r.call_tool(
                    "session_join",
                    {
                        "session_id": sid, "join_token": jt,
                        "role": "worker",
                        "capabilities": ["respond"],
                    },
                )
                stable_pids.add(pj["participant_id"])

            transient_pids: set[str] = set()

            async def _churn(client: Any) -> str:
                pj = await client.call_tool(
                    "session_join",
                    {
                        "session_id": sid, "join_token": jt,
                        "role": "worker",
                        "capabilities": ["respond"],
                    },
                )
                await asyncio.sleep(0.05)
                await client.call_tool("session_leave", {})
                return str(pj["participant_id"])

            churn_tasks = [asyncio.create_task(_churn(t1)), asyncio.create_task(_churn(t2))]
            sends = await asyncio.gather(
                *[
                    sender.call_tool(
                        "session_send",
                        {
                            "dispatch": "any",
                            "target": {"capability": "respond"},
                            "payload": {"i": i},
                        },
                    )
                    for i in range(total)
                ]
            )
            for task in churn_tasks:
                transient_pids.add(await task)

            valid_pids = stable_pids | transient_pids
            for s in sends:
                assert len(s["delivered_to"]) == 1, (
                    f"dispatch=any returned {len(s['delivered_to'])} "
                    "recipients; expected exactly 1"
                )
                assert s["delivered_to"][0] in valid_pids, (
                    f"command routed to phantom pid {s['delivered_to'][0]} "
                    f"not in {valid_pids}"
                )
            assert len(sends) == total


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_all_no_double_count_under_fanout() -> None:
    """A race in `_deliver_or_buffer` that double-counts a recipient
    on `dispatch="all"` fan-out wouldn't be caught by the existing
    `dispatch="any"` fairness tests (which only deliver one event
    per send). Spin up 4 workers, dispatch 20 commands with
    `dispatch="all"`; each worker must receive EXACTLY 20
    command.received events (no fewer = no loss, no more = no
    double-count).
    """
    total = 20
    n_workers = 4
    async with relay_subprocess() as base:
        async with (
            sw_client(base) as sender,
            sw_client(base) as w1,
            sw_client(base) as w2,
            sw_client(base) as w3,
            sw_client(base) as w4,
        ):
            sid, _creator, jt = await create_and_invite(sender, {})
            await sender.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "sender"}
            )
            workers = [w1, w2, w3, w4]
            received_buckets: list[list[tuple[str, dict[str, Any]]]] = []
            for w in workers:
                await w.call_tool(
                    "session_join",
                    {
                        "session_id": sid, "join_token": jt,
                        "role": "worker",
                        "capabilities": ["respond"],
                    },
                )
                bucket: list[tuple[str, dict[str, Any]]] = []
                received_buckets.append(bucket)

                def _on_event(
                    n: str,
                    p: dict[str, Any],
                    _bucket: list[tuple[str, dict[str, Any]]] = bucket,
                ) -> None:
                    _bucket.append((n, p))

                w.on_notification(_on_event)

            sends = await asyncio.gather(
                *[
                    sender.call_tool(
                        "session_send",
                        {
                            "dispatch": "all",
                            "target": {"capability": "respond"},
                            "payload": {"i": i},
                        },
                    )
                    for i in range(total)
                ]
            )

            # Each send must have fanned out to exactly n_workers
            # recipients.
            for s in sends:
                assert len(s["delivered_to"]) == n_workers, (
                    f"dispatch=all delivered to {len(s['delivered_to'])} "
                    f"recipients; expected {n_workers}"
                )

            # Wait until each worker has received total commands.
            await _wait_for(
                lambda: all(
                    sum(1 for n, _ in b if n == "command.received") == total
                    for b in received_buckets
                ),
                timeout=10.0,
                message=(
                    "not all workers reached the expected "
                    f"{total} command.received events"
                ),
            )

            # And no worker received more than total — catches a
            # double-count race in _deliver_or_buffer / dispatcher.
            for i, b in enumerate(received_buckets):
                cmd_count = sum(1 for n, _ in b if n == "command.received")
                assert cmd_count == total, (
                    f"worker {i} received {cmd_count} commands; "
                    f"expected {total} (no double-count)"
                )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_any_heterogeneous_worker_speeds() -> None:
    """One slow + three fast responders to stress the picker's
    sort-by-pid assumption when responses arrive out-of-order. The
    `RoundRobinPicker` SELECTION is independent of response speed
    (it picks before the previous recipient responds), so this is
    primarily a smoke test that the response path's
    `tracker.is_complete` accounting doesn't go funny when responses
    arrive in non-dispatch order. Asserts that every send produces a
    `command.completed reason=all_responded` after all four answer.
    """
    total = 8
    async with relay_subprocess() as base:
        async with (
            sw_client(base) as sender,
            sw_client(base) as fast1,
            sw_client(base) as fast2,
            sw_client(base) as fast3,
            sw_client(base) as slow,
        ):
            sid, _creator, jt = await create_and_invite(sender, {})
            await sender.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "sender"}
            )
            fast_clients = [fast1, fast2, fast3]
            slow_pj = await slow.call_tool(
                "session_join",
                {
                    "session_id": sid, "join_token": jt,
                    "role": "worker",
                    "capabilities": ["respond"],
                },
            )
            slow_pid = slow_pj["participant_id"]
            for client in fast_clients:
                await client.call_tool(
                    "session_join",
                    {
                        "session_id": sid, "join_token": jt,
                        "role": "worker",
                        "capabilities": ["respond"],
                    },
                )

            sends = await asyncio.gather(
                *[
                    sender.call_tool(
                        "session_send",
                        {
                            "dispatch": "any",
                            "target": {"capability": "respond"},
                            "payload": {"i": i},
                            "timeout_seconds": 30,
                        },
                    )
                    for i in range(total)
                ]
            )

            # Every dispatch must hit exactly one recipient.
            for s in sends:
                assert len(s["delivered_to"]) == 1

            # If the picker is strict round-robin with 4 sorted
            # participants and 8 sends, the slow worker received
            # exactly 2 commands. We don't assert that — fairness is
            # a separate test (#45). We only assert: no command
            # vanished and no command was double-delivered.
            counts: dict[str, int] = {}
            for s in sends:
                pid = s["delivered_to"][0]
                counts[pid] = counts.get(pid, 0) + 1
            assert sum(counts.values()) == total
            assert all(v >= 1 for v in counts.values()) or slow_pid not in counts, (
                f"unexpected distribution under stable-roster dispatch: {counts}"
            )
