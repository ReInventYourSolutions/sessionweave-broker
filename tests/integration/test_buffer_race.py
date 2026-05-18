"""Coverage for #64 — buffer eviction-vs-drain race under concurrent dispatch.

``NotificationBuffer`` wraps a ``collections.deque`` with NO async
synchronisation. The invariant the relay relies on is that **all
mutations of the per-participant buffer happen under
``Session.lock``** — every public method of ``Session`` that touches
``self._buffers`` either acquires the lock or is called from a path
that already holds it.

If a future refactor breaks this invariant (e.g. introduces an
``await`` between buffer access and mutation, or moves eviction to a
non-locking helper), this stress test surfaces the regression as an
``IndexError`` or a buffer-size discrepancy.

Test recipe:

  - 4 workers join the session in PULL mode (so events buffer rather
    than being delivered live).
  - Sender fires N=200 ``dispatch="all"`` commands concurrently while
    a tight async loop on each worker calls ``session_check_inbox``
    every few ms (drains under contention).
  - Assert: every command lands on every worker exactly once (no
    duplicates, no loss). The exact count check catches buffer
    operations that interleave incorrectly (eviction popping while
    drain is iterating yields ``IndexError`` on the inner deque
    today; if a future refactor introduces a second lock that allows
    interleaving, this test crashes loudly).
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from tests.integration.client import sw_client
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_dispatch_and_drain_no_buffer_race() -> None:
    """Stress test: 4 pull-mode workers + sender firing 200
    ``dispatch="all"`` commands. Each worker concurrently drains its
    inbox in a tight loop. Assert no command lost and no buffer
    operation errors.
    """
    total = 200
    n_workers = 4

    async with relay_subprocess() as base:
        # Create session via sender's MCP client.
        async with sw_client(base) as sender:
            sid = (await sender.call_tool("session_create", {}))["session_id"]
            await sender.call_tool(
                "session_join", {"session_id": sid, "role": "sender"}
            )

            # Workers join via REST so they get tokens and stay pull-mode.
            worker_tokens: list[tuple[str, str]] = []
            async with httpx.AsyncClient() as h:
                for _ in range(n_workers):
                    r = await h.post(
                        f"{base}/v1/sessions/{sid}/participants",
                        json={
                            "role": "worker",
                            "capabilities": ["respond"],
                            "delivery_mode": "pull",
                        },
                    )
                    r.raise_for_status()
                    body = r.json()
                    worker_tokens.append(
                        (body["participant_id"], body["participant_token"])
                    )

            received_counts: dict[str, int] = {pid: 0 for pid, _ in worker_tokens}

            async def _drain_loop(pid: str, tok: str, stop: asyncio.Event) -> None:
                async with httpx.AsyncClient() as h:
                    while not stop.is_set():
                        r = await h.post(
                            f"{base}/v1/sessions/{sid}/participants/me/inbox/drain",
                            headers={"authorization": f"Bearer {tok}"},
                        )
                        if r.status_code == 200:
                            for m in r.json().get("messages", []):
                                if m.get("name") == "command.received":
                                    received_counts[pid] += 1
                        await asyncio.sleep(0.01)
                    # One final drain after stop is signalled so any
                    # tail events arrive in our count.
                    for _ in range(3):
                        r = await h.post(
                            f"{base}/v1/sessions/{sid}/participants/me/inbox/drain",
                            headers={"authorization": f"Bearer {tok}"},
                        )
                        if r.status_code == 200:
                            for m in r.json().get("messages", []):
                                if m.get("name") == "command.received":
                                    received_counts[pid] += 1
                        await asyncio.sleep(0.05)

            stop = asyncio.Event()
            drainers = [
                asyncio.create_task(_drain_loop(pid, tok, stop))
                for pid, tok in worker_tokens
            ]

            # Fire the dispatches concurrently.
            sends_task = asyncio.gather(
                *[
                    sender.call_tool(
                        "session_send",
                        {
                            "dispatch": "all",
                            "target": {"capability": "respond"},
                            "payload": {"i": i},
                            "timeout_seconds": 60,
                        },
                    )
                    for i in range(total)
                ]
            )

            sends = await sends_task
            # Every send must have fanned out to all 4 workers.
            assert all(len(s["delivered_to"]) == n_workers for s in sends), (
                "fan-out incorrect — dispatch=all delivery_to count != n_workers"
            )

            # Give the drainers a moment to catch up, then stop them.
            await asyncio.sleep(0.5)
            stop.set()
            await asyncio.gather(*drainers)

            # No worker is allowed to miss a command. No worker is
            # allowed to receive a command more than once.
            for pid, count in received_counts.items():
                assert count == total, (
                    f"worker {pid[:8]} received {count}/{total} "
                    f"command.received events; full count: {received_counts}"
                )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_buffer_invariant_documented() -> None:
    """Asserts the documented invariant: ``NotificationBuffer`` has no
    internal lock and is meant to be mutated only under
    ``Session.lock``. If a future refactor adds synchronization to
    NotificationBuffer itself, this test will fail with a clear
    pointer to #64 — the maintainer should consider whether the new
    lock duplicates ``Session.lock`` or replaces it.

    This is a documentation-level invariant test, not a behavioural
    one — it catches accidental design drift.
    """
    from sessionweave.notifications import NotificationBuffer

    buf = NotificationBuffer(10)
    # No async-lock attribute on NotificationBuffer. If one is added,
    # this assertion fails — see #64 for the design discussion.
    assert not hasattr(buf, "_lock"), (
        "NotificationBuffer grew a lock attribute — see #64. The "
        "buffer was designed to rely on Session.lock for serialisation "
        "(Option B in the issue). If you really want a per-buffer "
        "lock, update Session.lock callers to drop it where the "
        "per-buffer lock now applies."
    )
