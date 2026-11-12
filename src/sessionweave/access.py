"""Session credentials: join/supervisor/participant tokens and failure throttling.

All tokens are 256-bit URL-safe random strings. Only SHA-256 digests are
kept; plaintext is returned once, at mint time. Lookups go through the
digest of the presented token, so no plaintext comparison happens.
Times are integer milliseconds supplied by the caller.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Literal

JOIN_TOKEN_TTL_SECONDS: int = 900

Role = Literal["admin", "creator", "supervisor", "participant", "joiner"]


@dataclass(frozen=True)
class Principal:
    role: Role
    participant_id: str | None = None


def generate_token() -> str:
    """Return a fresh 256-bit URL-safe token."""
    return secrets.token_urlsafe(32)


def hash_token(token: str) -> bytes:
    """Return the SHA-256 raw digest of the token."""
    return hashlib.sha256(token.encode("utf-8")).digest()


@dataclass
class IssuedToken:
    token_id: str
    kind: Literal["join", "supervisor"]
    token_hash: bytes
    created_at_ms: int
    expires_at_ms: int | None
    revoked_at_ms: int | None = None


class SessionAuth:
    """Per-session credential registry (hashes only)."""

    def __init__(self, creator_token_hash: bytes | None) -> None:
        self._creator_hash = creator_token_hash
        self._issued: dict[bytes, IssuedToken] = {}
        self._participants: dict[bytes, str] = {}

    def _mint(
        self, kind: Literal["join", "supervisor"], now_ms: int, ttl_ms: int | None
    ) -> tuple[str, IssuedToken]:
        token = generate_token()
        issued = IssuedToken(
            token_id=secrets.token_hex(8),
            kind=kind,
            token_hash=hash_token(token),
            created_at_ms=now_ms,
            expires_at_ms=None if ttl_ms is None else now_ms + ttl_ms,
        )
        self._issued[issued.token_hash] = issued
        return token, issued

    def mint_join(self, now_ms: int) -> tuple[str, IssuedToken]:
        return self._mint("join", now_ms, JOIN_TOKEN_TTL_SECONDS * 1000)

    def mint_supervisor(self, now_ms: int) -> tuple[str, IssuedToken]:
        return self._mint("supervisor", now_ms, None)

    def revoke_supervisor(self, token_id: str, now_ms: int) -> IssuedToken:
        for issued in self._issued.values():
            if issued.kind == "supervisor" and issued.token_id == token_id:
                if issued.revoked_at_ms is None:
                    issued.revoked_at_ms = now_ms
                return issued
        raise KeyError(token_id)

    def supervisor_tokens(self) -> list[IssuedToken]:
        return [t for t in self._issued.values() if t.kind == "supervisor"]

    def register_participant(self, participant_id: str, token_hash: bytes) -> None:
        self._participants[token_hash] = participant_id

    def forget_participant(self, participant_id: str) -> None:
        for h in [h for h, pid in self._participants.items() if pid == participant_id]:
            del self._participants[h]

    def restore(self, tokens: Iterable[IssuedToken]) -> None:
        for t in tokens:
            self._issued[t.token_hash] = t

    def match(self, token: str, now_ms: int) -> Principal | None:
        digest = hash_token(token)
        if self._creator_hash is not None and hmac.compare_digest(digest, self._creator_hash):
            return Principal("creator")
        issued = self._issued.get(digest)
        if issued is not None and issued.kind == "supervisor" and issued.revoked_at_ms is None:
            return Principal("supervisor")
        pid = self._participants.get(digest)
        if pid is not None:
            return Principal("participant", pid)
        if (
            issued is not None
            and issued.kind == "join"
            and issued.expires_at_ms is not None
            and now_ms < issued.expires_at_ms
        ):
            return Principal("joiner")
        return None

    def join_token_expired(self, token: str, now_ms: int) -> bool:
        issued = self._issued.get(hash_token(token))
        return (
            issued is not None
            and issued.kind == "join"
            and issued.expires_at_ms is not None
            and now_ms >= issued.expires_at_ms
        )


class AuthFailureLimiter:
    """Sliding-window counter of invalid-token attempts per (sid, source IP)."""

    __slots__ = ("_buckets", "_clock", "_max", "_window")

    def __init__(
        self,
        max_failures: int = 5,
        window_seconds: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._max = max_failures
        self._window = window_seconds
        self._clock = clock
        self._buckets: dict[tuple[str, str], deque[float]] = {}

    def record_failure(self, sid: str, ip: str) -> None:
        bucket = self._buckets.setdefault((sid, ip), deque())
        bucket.append(self._clock())
        self._gc(bucket)

    def is_throttled(self, sid: str, ip: str) -> bool:
        bucket = self._buckets.get((sid, ip))
        if bucket is None:
            return False
        self._gc(bucket)
        if not bucket:
            del self._buckets[(sid, ip)]
            return False
        return len(bucket) >= self._max

    def _gc(self, bucket: deque[float]) -> None:
        cutoff = self._clock() - self._window
        while bucket and bucket[0] < cutoff:
            bucket.popleft()


def admin_matches(token: str, admin_token: str | None) -> bool:
    """Constant-time compare against the relay admin token; False when unset."""
    if not admin_token:
        return False
    return hmac.compare_digest(token.encode("utf-8"), admin_token.encode("utf-8"))
