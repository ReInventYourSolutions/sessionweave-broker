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
