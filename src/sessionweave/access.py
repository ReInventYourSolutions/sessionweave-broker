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
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from sessionweave.errors import ErrorCode, ToolError

if TYPE_CHECKING:
    from sessionweave.session import Session
    from sessionweave.tools import ToolDeps

JOIN_TOKEN_TTL_SECONDS: int = 900

# An expired join token is kept in memory this long after expiry, so a stale
# invite link still gets the "join link expired" message; after that it is
# pruned (lazily, on the next mint) and reads as an unknown token.
JOIN_TOKEN_EXPIRED_GRACE_MS: int = 3_600_000

_SWEEP_EVERY = 256

Role = Literal["admin", "creator", "supervisor", "participant", "joiner"]


@dataclass(frozen=True)
class Principal:
    role: Role
    participant_id: str | None = None
    # Which issued supervisor token authenticated this principal (so a live
    # stream opened with it can be cut off on revocation). Not part of
    # equality: the role and participant are the identity.
    token_id: str | None = field(default=None, compare=False)


def _now_ms() -> int:
    return int(time.time() * 1000)


def generate_token() -> str:
    """Return a fresh 256-bit URL-safe token."""
    return secrets.token_urlsafe(32)


def hash_token(token: str) -> bytes:
    """Return the SHA-256 raw digest of the token."""
    return hashlib.sha256(token.encode("utf-8", "surrogatepass")).digest()


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

    @property
    def creator_token_hash(self) -> bytes | None:
        return self._creator_hash

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

    def prune_expired_joins(self, now_ms: int) -> None:
        cutoff = now_ms - JOIN_TOKEN_EXPIRED_GRACE_MS
        for digest in [
            d for d, t in self._issued.items()
            if t.kind == "join" and t.expires_at_ms is not None and t.expires_at_ms < cutoff
        ]:
            del self._issued[digest]

    def mint_join(self, now_ms: int) -> tuple[str, IssuedToken]:
        self.prune_expired_joins(now_ms)
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

    def supervisor_revoked(self, token_id: str) -> bool:
        return any(
            t.kind == "supervisor" and t.token_id == token_id and t.revoked_at_ms is not None
            for t in self._issued.values()
        )

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
            return Principal("supervisor", token_id=issued.token_id)
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

    __slots__ = ("_buckets", "_clock", "_max", "_since_sweep", "_window")

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
        self._since_sweep = 0

    def record_failure(self, sid: str, ip: str) -> None:
        bucket = self._buckets.get((sid, ip))
        if bucket is None:
            bucket = self._buckets[(sid, ip)] = deque(maxlen=self._max)
        bucket.append(self._clock())
        self._gc(bucket)
        self._since_sweep += 1
        if self._since_sweep >= _SWEEP_EVERY:
            self._sweep()

    def _sweep(self) -> None:
        """Drop buckets whose failures have all aged out."""
        self._since_sweep = 0
        for key in list(self._buckets):
            bucket = self._buckets[key]
            self._gc(bucket)
            if not bucket:
                del self._buckets[key]

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
    return hmac.compare_digest(
        token.encode("utf-8", "surrogatepass"), admin_token.encode("utf-8", "surrogatepass")
    )


async def resolve_principal(
    deps: ToolDeps,
    sid: str,
    token: str | None,
    *,
    source_ip: str = "",
    hydrate: bool = True,
) -> Principal:
    """Resolve a presented token to a Principal for session ``sid``.

    Admin is checked first, then the session's own credentials. With
    ``hydrate=False`` a cold or ended persisted session is authenticated from
    its stored hashes without being loaded into RAM. Invalid and expired
    tokens are counted per (sid, source_ip); once throttled, further invalid
    tokens get RATE_LIMITED, while a valid token always passes.
    """
    registry = deps.registry
    # Session existence is checked before the admin token: an unknown sid is
    # SESSION_NOT_FOUND for every caller, admin included.
    auth: SessionAuth | None
    if hydrate:
        auth = (await registry.get_or_raise(sid)).auth
    else:
        hot = registry.get(sid)
        if hot is not None:
            auth = hot.auth
        else:
            store = registry._store
            auth = await store.load_auth(sid) if store is not None else None
    if auth is None:
        raise ToolError(ErrorCode.SESSION_NOT_FOUND, "session not found")

    if not token:
        raise ToolError(ErrorCode.PERMISSION_DENIED, "missing or invalid token")
    if admin_matches(token, deps.admin_token):
        return Principal("admin")
    now_ms = _now_ms()
    principal = auth.match(token, now_ms)
    if principal is not None:
        return principal

    limiter = deps.auth_limiter
    if limiter.is_throttled(sid, source_ip):
        raise ToolError(ErrorCode.RATE_LIMITED, "too many invalid tokens; retry later")
    limiter.record_failure(sid, source_ip)
    if auth.join_token_expired(token, now_ms):
        raise ToolError(
            ErrorCode.PERMISSION_DENIED,
            "join link expired \u2014 ask the session creator for a new invite link",
        )
    raise ToolError(ErrorCode.PERMISSION_DENIED, "missing or invalid token")


def require(principal: Principal, *allowed: Role) -> None:
    """Raise PERMISSION_DENIED unless the principal's role is one of ``allowed``."""
    if principal.role not in allowed:
        raise ToolError(
            ErrorCode.PERMISSION_DENIED, f"requires one of: {', '.join(allowed)}"
        )


def participant_id_for_token(
    sess: Session,
    token: str | None,
    *,
    limiter: AuthFailureLimiter | None = None,
    source_ip: str = "",
) -> str:
    """The participant a secret participant token belongs to, in ``sess``.

    Only a participant token qualifies: a participant_id, session id, join,
    supervisor or creator token is NOT_IN_SESSION, as is a token whose
    participant has since been removed. With a ``limiter``, each failure is
    counted per (sid, source_ip) like any other invalid token; once
    throttled, invalid tokens get RATE_LIMITED, while a valid participant
    token is never refused.
    """
    principal = sess.auth.match(token, _now_ms()) if token else None
    pid = principal.participant_id if principal is not None else None
    if principal is None or principal.role != "participant" or pid not in sess._participants:
        if limiter is not None:
            sid = sess.session_id
            if limiter.is_throttled(sid, source_ip):
                raise ToolError(
                    ErrorCode.RATE_LIMITED, "too many invalid tokens; retry later"
                )
            limiter.record_failure(sid, source_ip)
        raise ToolError(ErrorCode.NOT_IN_SESSION, "unknown or stale participant token")
    return pid
