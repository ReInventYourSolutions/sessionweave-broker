"""Creator-token utilities and per-sid rate limiter for session_delete.

A creator_token is a 256-bit URL-safe random string returned exactly once
by session_create. Only the SHA-256 digest is stored on the server side.
Verification is constant-time. A simple rolling-window counter limits
brute-force attempts on session_delete.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from collections import deque


def generate_creator_token() -> str:
    """Return a fresh 256-bit URL-safe token (~43 chars)."""
    return secrets.token_urlsafe(32)


def hash_creator_token(token: str) -> bytes:
    """Return the sha256 raw digest (32 bytes) of the token."""
    return hashlib.sha256(token.encode("utf-8")).digest()


def verify_creator_token(token: str, stored_hash: bytes) -> bool:
    """Constant-time compare of sha256(token) against stored_hash."""
    return hmac.compare_digest(hash_creator_token(token), stored_hash)


class DeleteRateLimiter:
    """Per-sid sliding-window failure counter for session_delete attempts.

    A session is considered "banned" while at least ``max_failures``
    failures have been recorded within the last ``window_seconds``.
    The ban naturally expires as failures age out. ``reset(sid)`` clears
    the counter (used e.g. on a successful delete or operator action).

    Not durable: counters live in RAM and reset on relay restart. That
    is fine: a brute-force attempt is already cost-prohibitive against
    a 256-bit token; this limiter just adds a small cost for misconfigured
    callers and stops obvious abuse.
    """

    __slots__ = ("_buckets", "_max", "_window")

    def __init__(self, max_failures: int = 5, window_seconds: float = 60.0) -> None:
        self._max = max_failures
        self._window = window_seconds
        self._buckets: dict[str, deque[float]] = {}

    def record_failure(self, sid: str) -> None:
        bucket = self._buckets.setdefault(sid, deque())
        bucket.append(time.monotonic())
        self._gc(bucket)

    def is_banned(self, sid: str) -> bool:
        bucket = self._buckets.get(sid)
        if bucket is None:
            return False
        self._gc(bucket)
        return len(bucket) >= self._max

    def reset(self, sid: str) -> None:
        self._buckets.pop(sid, None)

    def _gc(self, bucket: deque[float]) -> None:
        cutoff = time.monotonic() - self._window
        while bucket and bucket[0] < cutoff:
            bucket.popleft()
