"""Domain layer for supervisor notes: parsing, validation, rate limiting."""
from __future__ import annotations

import time
import uuid
from collections import deque
from dataclasses import dataclass

_VALID_KINDS: set[str] = {"note", "halt", "continue", "redirect"}
_MAX_MESSAGE_BYTES = 2048


class SupervisorRequestError(ValueError):
    """Raised when an incoming supervisor request is malformed."""


@dataclass(frozen=True)
class SupervisorNote:
    note_id: str
    kind: str
    target: str
    message: str | None
    ts_ms: int


def parse_supervisor_request(body: object) -> SupervisorNote:
    if not isinstance(body, dict):
        raise SupervisorRequestError("body must be an object")

    kind = body.get("kind")
    if kind not in _VALID_KINDS:
        raise SupervisorRequestError(f"kind must be one of {sorted(_VALID_KINDS)}, got {kind!r}")

    target = body.get("target")
    if not isinstance(target, str) or not target:
        raise SupervisorRequestError("target must be 'all' or a participant_id string")

    message = body.get("message")
    if message is not None:
        if not isinstance(message, str):
            raise SupervisorRequestError("message must be a string when present")
        if len(message.encode("utf-8")) > _MAX_MESSAGE_BYTES:
            raise SupervisorRequestError(
                f"message exceeds {_MAX_MESSAGE_BYTES} bytes"
            )

    return SupervisorNote(
        note_id=f"sn-{uuid.uuid4().hex[:12]}",
        kind=kind,
        target=target,
        message=message,
        ts_ms=int(time.time() * 1000),
    )


class NoteRateLimiter:
    """Sliding-window rate limiter keyed by session_id.

    Tracks a per-minute and a per-hour cap. ``try_acquire`` is O(k) where
    k is the count of stamps older than the per-hour cutoff (those get
    dropped). Memory is bounded by per_hour.
    """

    def __init__(self, per_minute: int, per_hour: int) -> None:
        self._per_minute = per_minute
        self._per_hour = per_hour
        self._stamps: dict[str, deque[float]] = {}

    def try_acquire(self, sid: str) -> bool:
        now = time.monotonic()
        bucket = self._stamps.setdefault(sid, deque())
        cutoff_hour = now - 3600
        cutoff_minute = now - 60
        while bucket and bucket[0] < cutoff_hour:
            bucket.popleft()
        in_hour = len(bucket)
        in_minute = sum(1 for s in bucket if s >= cutoff_minute)
        if in_hour >= self._per_hour or in_minute >= self._per_minute:
            return False
        bucket.append(now)
        return True
