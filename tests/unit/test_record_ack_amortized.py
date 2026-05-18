"""Coverage for #66 — ``Session.record_ack`` is amortized O(1) per ACK.

The previous implementation called ``NotificationBuffer.drop_where``
which iterates the entire deque each call. A burst of N ACK frames
during reconnect was therefore O(N^2) total.

Since ``_seq`` is monotonically increasing per participant AND buffer
items are in arrival order, ``popleft_while`` can stop at the first
non-matching head: amortized O(1) per ACK.

This test compares the wall-clock cost of N=2000 individual ACKs
against a baseline; the linear-implementation cost would be roughly
N^2 = 4_000_000 list comparisons, which is observably slow.
"""

from __future__ import annotations

import time

import pytest

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.session import Session
from tests.unit.fakes import FakeHandle


@pytest.mark.asyncio
async def test_record_ack_amortized_o1_under_burst() -> None:
    """Push 2000 events into the buffer; ACK them one-by-one and
    assert the total wall-clock is comfortably linear, not quadratic.

    On the previous ``drop_where`` implementation this loop took
    ~5+ seconds locally (2000 * 2000 = 4M comparisons). With
    ``popleft_while`` it runs in well under 1 second on the same
    hardware. The assertion picks a conservative 5s ceiling so the
    test is reliable on slow CI but still catches a regression to
    quadratic complexity.
    """
    # Need a larger per-participant cap than the 1000 default so we
    # can stress 2000 ACKs in a row without triggering count-based
    # eviction (which would empty the buffer and short-circuit the
    # perf test).
    s = Session(
        "s",
        SessionConfig(max_pending_notifications_per_participant=5000),
        ConnectionManager(),
    )
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")
    pid = p.participant_id

    # Push 2000 events via _deliver_or_buffer so they land in the
    # buffer with monotonic _seq.
    n = 2000
    for i in range(n):
        s._deliver_or_buffer(pid, "command.received", {"i": i})

    # Confirm they're all buffered. ``_buffer_for`` lazily creates the
    # buffer on first enqueue; access via the helper so the test is
    # robust to that.
    buf = s._buffer_for(pid)
    assert buf.size() == n, f"expected {n} buffered, got {buf.size()}"

    # ACK them one-by-one. Each call should pop exactly one item
    # off the head (popleft_while stops at first non-match).
    start = time.monotonic()
    for i in range(1, n + 1):
        await s.record_ack(pid, i)
    elapsed = time.monotonic() - start

    # Buffer must be empty afterwards.
    assert buf.size() == 0

    # 5s ceiling — generous for CI. The popleft_while implementation
    # comfortably runs under 0.5s; the drop_where implementation
    # would take 5-30s for n=2000. If this fires the perf has
    # regressed back to quadratic and we need to find out why.
    assert elapsed < 5.0, (
        f"record_ack took {elapsed:.2f}s for {n} ACKs — that's "
        "quadratic territory. Did popleft_while get replaced "
        "with a full-scan? See #66."
    )


@pytest.mark.asyncio
async def test_record_ack_partial_ack_keeps_unacked_items() -> None:
    """Behavioural equivalence: ACKing seq=5 with 10 events buffered
    keeps items 6..10. Same contract ``drop_where`` had.
    """
    s = Session("s", SessionConfig(), ConnectionManager())
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")
    pid = p.participant_id

    for i in range(10):
        s._deliver_or_buffer(pid, "command.received", {"i": i})

    await s.record_ack(pid, 5)
    remaining = await s.check_inbox(pid)
    seqs = [m["seq"] for m in remaining]
    assert seqs == [6, 7, 8, 9, 10], f"got {seqs!r}"


@pytest.mark.asyncio
async def test_record_ack_idempotent_below_watermark() -> None:
    """A second ACK with a lower seq is a no-op (the participant's
    ``last_acked_seq`` watermark never regresses). Same contract.
    """
    s = Session("s", SessionConfig(), ConnectionManager())
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")
    pid = p.participant_id

    for i in range(5):
        s._deliver_or_buffer(pid, "command.received", {"i": i})

    await s.record_ack(pid, 3)
    await s.record_ack(pid, 1)  # below watermark — no-op
    remaining = await s.check_inbox(pid)
    seqs = [m["seq"] for m in remaining]
    assert seqs == [4, 5]
