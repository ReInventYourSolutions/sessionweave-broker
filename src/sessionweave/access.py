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

from sessionweave.account_events import Origin, request_client
from sessionweave.errors import NETWORK_NOT_APPROVED_MESSAGE, ErrorCode, ToolError

if TYPE_CHECKING:
    from sessionweave.session import Session
    from sessionweave.tools import ToolDeps

JOIN_TOKEN_TTL_SECONDS: int = 900

# An expired join token is kept in memory this long after expiry, so a stale
# invite link still gets the "join link expired" message; after that it is
# pruned (lazily, on the next mint) and reads as an unknown token.
JOIN_TOKEN_EXPIRED_GRACE_MS: int = 3_600_000

_SWEEP_EVERY = 256

# A removed participant's token is remembered this long (RAM only), so its
# next call is told it was removed instead of getting a generic
# not_in_session — the only way a pull-mode or offline agent learns of it.
REMOVED_TOMBSTONE_MS: int = 3_600_000

PARTICIPANT_REMOVED_MESSAGE = (
    "you were removed from this session by its creator \u2014 "
    "stop your work and your sub-agents"
)

Role = Literal["admin", "creator", "supervisor", "participant", "joiner"]


@dataclass(frozen=True)
class Principal:
    role: Role
    participant_id: str | None = None
    # Which issued supervisor token authenticated this principal (so a live
    # stream opened with it can be cut off on revocation). Not part of
    # equality: the role and participant are the identity.
    token_id: str | None = field(default=None, compare=False)
    # Set when the principal is the session's owner account acting through one of
    # its account tokens, so live streams can be cut when that token or account dies.
    account_id: str | None = field(default=None, compare=False)
    account_token_id: str | None = field(default=None, compare=False)
    # Set when a "joiner" presented a single-use launch credential (the issued
    # token's id): its join is bound to that launch's role and capabilities.
    launch_token_id: str | None = field(default=None, compare=False)


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
    kind: Literal["join", "supervisor", "launch"]
    token_hash: bytes
    created_at_ms: int
    expires_at_ms: int | None
    revoked_at_ms: int | None = None
    # Launch credentials only (kind "launch"; RAM only, never persisted): the
    # launch they belong to and the role/capabilities the join is bound to.
    launch_id: str | None = None
    role: str | None = None
    capabilities: tuple[str, ...] = ()


class SessionAuth:
    """Per-session credential registry (hashes only)."""

    def __init__(
        self, creator_token_hash: bytes | None, owner_account_id: str | None = None
    ) -> None:
        self._creator_hash = creator_token_hash
        self._owner_account_id = owner_account_id
        self._issued: dict[bytes, IssuedToken] = {}
        self._participants: dict[bytes, str] = {}
        # Token digest of a removed participant -> tombstone expiry (ms).
        self._removed: dict[bytes, int] = {}

    @property
    def creator_token_hash(self) -> bytes | None:
        return self._creator_hash

    @property
    def owner_account_id(self) -> str | None:
        return self._owner_account_id

    def _mint(
        self, kind: Literal["join", "supervisor", "launch"], now_ms: int, ttl_ms: int | None
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
            if (t.kind == "join" and t.expires_at_ms is not None and t.expires_at_ms < cutoff)
            or (t.kind == "launch" and t.expires_at_ms is not None and t.expires_at_ms < now_ms)
        ]:
            del self._issued[digest]

    def mint_join(self, now_ms: int) -> tuple[str, IssuedToken]:
        self.prune_expired_joins(now_ms)
        return self._mint("join", now_ms, JOIN_TOKEN_TTL_SECONDS * 1000)

    def mint_launch(
        self, now_ms: int, ttl_ms: int, *, launch_id: str, role: str,
        capabilities: Iterable[str],
    ) -> tuple[str, IssuedToken]:
        """A single-use credential that joins this session as one launched
        agent, bound to ``role`` and ``capabilities`` (kept in RAM only)."""
        self.prune_expired_joins(now_ms)
        token, issued = self._mint("launch", now_ms, ttl_ms)
        issued.launch_id = launch_id
        issued.role = role
        issued.capabilities = tuple(capabilities)
        return token, issued

    def consume_launch(self, token_id: str, now_ms: int) -> IssuedToken | None:
        """Mark a launch credential used; None when unknown, used or expired.

        Synchronous, so two concurrent joins with the same credential cannot
        both consume it.
        """
        for issued in self._issued.values():
            if issued.kind == "launch" and issued.token_id == token_id:
                if issued.revoked_at_ms is not None or (
                    issued.expires_at_ms is not None and now_ms >= issued.expires_at_ms
                ):
                    return None
                issued.revoked_at_ms = now_ms
                return issued
        return None

    def unconsume_launch(self, token_id: str) -> None:
        """Make a consumed launch credential usable again (its join failed)."""
        for issued in self._issued.values():
            if issued.kind == "launch" and issued.token_id == token_id:
                issued.revoked_at_ms = None

    def invalidate_launch(self, token_id: str) -> None:
        """Forget a launch credential: it reads as an unknown token from now on."""
        for digest in [
            d for d, t in self._issued.items() if t.kind == "launch" and t.token_id == token_id
        ]:
            del self._issued[digest]

    def mint_supervisor(self, now_ms: int) -> tuple[str, IssuedToken]:
        return self._mint("supervisor", now_ms, None)

    def revoke_supervisor(self, token_id: str, now_ms: int) -> IssuedToken:
        for issued in self._issued.values():
            if issued.kind == "supervisor" and issued.token_id == token_id:
                if issued.revoked_at_ms is None:
                    issued.revoked_at_ms = now_ms
                return issued
        raise KeyError(token_id)

    def valid_join_tokens(self, now_ms: int) -> list[IssuedToken]:
        """The join tokens that still admit a joiner at ``now_ms``."""
        return [
            t for t in self._issued.values()
            if t.kind == "join" and t.expires_at_ms is not None and now_ms < t.expires_at_ms
        ]

    def invalidate_join_tokens(self, now_ms: int) -> list[IssuedToken]:
        """Expire every still-valid join token now; return the ones changed.

        An invalidated link then gets the same "join link expired" answer
        as one that ran out on its own.
        """
        changed = self.valid_join_tokens(now_ms)
        for t in changed:
            t.expires_at_ms = now_ms
        return changed

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

    def tombstone_participant(self, token_hash: bytes, now_ms: int) -> None:
        """Remember a removed participant's token for ``REMOVED_TOMBSTONE_MS``."""
        self._prune_removed(now_ms)
        self._removed[token_hash] = now_ms + REMOVED_TOMBSTONE_MS

    def was_removed(self, token: str, now_ms: int) -> bool:
        """Whether ``token`` belonged to a participant removed within the hour."""
        self._prune_removed(now_ms)
        return hash_token(token) in self._removed

    def _prune_removed(self, now_ms: int) -> None:
        for digest in [d for d, exp in self._removed.items() if exp <= now_ms]:
            del self._removed[digest]

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
        if (
            issued is not None
            and issued.kind == "launch"
            and issued.revoked_at_ms is None
            and issued.expires_at_ms is not None
            and now_ms < issued.expires_at_ms
        ):
            return Principal("joiner", launch_token_id=issued.token_id)
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
    if auth.owner_account_id and deps.accounts is not None:
        # Ownership first, without an origin: a foreign account's token must not
        # create a pending approval or an event on that other account.
        owner_check = await deps.accounts.authenticate(
            token, now_ms, None, reject_origin=Origin(source_ip, request_client.get())
        )
        if (
            owner_check.account is not None
            and owner_check.account.account_id == auth.owner_account_id
        ):
            res = await deps.accounts.authenticate(
                token, now_ms, Origin(source_ip, request_client.get())
            )
            if res.account is not None and res.account.account_id == auth.owner_account_id:
                assert res.token_id is not None
                return Principal(
                    "creator",
                    account_id=res.account.account_id,
                    account_token_id=res.token_id,
                )
            if res.refusal == "network" and res.account_id == auth.owner_account_id:
                # The owner's own token, valid but not approved for this network.
                raise ToolError(ErrorCode.NETWORK_NOT_APPROVED, NETWORK_NOT_APPROVED_MESSAGE)

    if auth.was_removed(token, now_ms):
        raise ToolError(ErrorCode.PARTICIPANT_REMOVED, PARTICIPANT_REMOVED_MESSAGE)
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
    participant has since left. The token of a participant the creator
    removed within the last hour is PARTICIPANT_REMOVED instead (not counted
    as a failure). With a ``limiter``, each failure is
    counted per (sid, source_ip) like any other invalid token; once
    throttled, invalid tokens get RATE_LIMITED, while a valid participant
    token is never refused.
    """
    now_ms = _now_ms()
    principal = sess.auth.match(token, now_ms) if token else None
    pid = principal.participant_id if principal is not None else None
    if principal is None or principal.role != "participant" or pid not in sess._participants:
        if token and sess.auth.was_removed(token, now_ms):
            raise ToolError(ErrorCode.PARTICIPANT_REMOVED, PARTICIPANT_REMOVED_MESSAGE)
        if limiter is not None:
            sid = sess.session_id
            if limiter.is_throttled(sid, source_ip):
                raise ToolError(
                    ErrorCode.RATE_LIMITED, "too many invalid tokens; retry later"
                )
            limiter.record_failure(sid, source_ip)
        raise ToolError(ErrorCode.NOT_IN_SESSION, "unknown or stale participant token")
    return pid


def is_coordinator(sess: Session, principal: Principal) -> bool:
    """True when ``principal`` is a participant holding the session's coordinator role."""
    return (
        principal.role == "participant"
        and principal.participant_id is not None
        and principal.participant_id == sess.coordinator_id
    )
