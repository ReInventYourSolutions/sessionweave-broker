from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterator
from typing import Any


class BufferOverflow(Exception):  # noqa: N818
    """Raised when enqueueing would exceed the buffer cap."""


class NotificationBuffer:
    """Per-participant FIFO queue with a hard cap.

    Enqueue raises BufferOverflow when full; the caller decides what to do
    (e.g., evict the participant). The buffer never silently drops on cap.

    `drop_where` removes items matching a predicate (used to garbage-collect
    stale notifications such as command.received for an expired command).

    **Concurrency invariant (issue #64):** this class has NO internal
    synchronisation. All mutations (``enqueue`` / ``drain`` /
    ``drop_where`` / ``pop_where`` / ``popleft``) are expected to be
    serialised by the **caller** via ``Session.lock``. Every code path
    in ``session.py`` that touches a ``NotificationBuffer`` either
    acquires ``Session.lock`` directly or runs from a path that does
    (eviction, ACK trim, byte-budget enforcement, drain at
    ``check_inbox`` / ``attach_subscriber`` / ``resume``). The
    integration-level stress test in
    ``tests/integration/test_buffer_race.py`` exercises the invariant
    under concurrent dispatch + drain.

    A future maintainer who introduces a buffer-local lock should
    re-evaluate every ``Session.lock`` site to avoid double-locking
    or invariant duplication. See #64 for the design rationale.
    """

    __slots__ = ("_cap", "_q")

    def __init__(self, cap: int) -> None:
        if cap < 0:
            raise ValueError("cap must be >= 0")
        self._cap = cap
        self._q: deque[Any] = deque()

    def size(self) -> int:
        return len(self._q)

    def enqueue(self, notification: Any) -> None:
        if len(self._q) >= self._cap:
            raise BufferOverflow(f"buffer cap {self._cap} reached")
        self._q.append(notification)

    def drain(self) -> Iterator[Any]:
        while self._q:
            yield self._q.popleft()

    def head(self) -> Any | None:
        """Peek the oldest item without popping. Returns None if empty.

        Used by ``Session._enforce_session_byte_budget`` to find the
        next item to evict when the session-wide byte cap is exceeded.
        """
        return self._q[0] if self._q else None

    def popleft(self) -> Any:
        """Pop the oldest item. Raises IndexError if empty (mirrors
        ``deque.popleft`` semantics). Caller is responsible for
        decrementing any external size accounting.
        """
        return self._q.popleft()

    def drop_where(self, predicate: Callable[[Any], bool]) -> int:
        kept: deque[Any] = deque()
        dropped = 0
        for item in self._q:
            if predicate(item):
                dropped += 1
            else:
                kept.append(item)
        self._q = kept
        return dropped

    def pop_where(self, predicate: Callable[[Any], bool]) -> list[Any]:
        """Like ``drop_where`` but returns the removed items instead of
        a count. Use when the caller needs to perform per-item
        bookkeeping (e.g. ``Session._buffered_bytes`` accounting) on
        the eviction set.
        """
        kept: deque[Any] = deque()
        removed: list[Any] = []
        for item in self._q:
            if predicate(item):
                removed.append(item)
            else:
                kept.append(item)
        self._q = kept
        return removed
