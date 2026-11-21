from __future__ import annotations

import asyncio
import logging
import time
from contextlib import AsyncExitStack
from dataclasses import asdict
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, NamedTuple

from pydantic import ValidationError

from sessionweave.access import (
    AuthFailureLimiter,
    Principal,
    Role,
    admin_matches,
    generate_token,
    hash_token,
    participant_id_for_token,
    require,
    resolve_principal,
)
from sessionweave.account_events import EventLog, Origin, request_client
from sessionweave.accounts import Account, AccountStore, AuthResult
from sessionweave.connections import ConnectionHandle
from sessionweave.errors import NETWORK_NOT_APPROVED_MESSAGE, ErrorCode, ToolError
from sessionweave.invite import create_link, http_invite_url
from sessionweave.registry import SessionRegistry
from sessionweave.schemas import (
    AccessOutput,
    InboxMessage,
    JoinTokenOutput,
    ReplyOutcome,
    SessionCheckInboxOutput,
    SessionCreateInput,
    SessionCreateOutput,
    SessionDeleteInput,
    SessionDeleteOutput,
    SessionDescribeInput,
    SessionDescribeOutput,
    SessionGetEventsInput,
    SessionGetEventsOutput,
    SessionJoinInput,
    SessionJoinOutput,
    SessionListParticipantsOutput,
    SessionRespondInput,
    SessionResumeInput,
    SessionResumeOutput,
    SessionSendInput,
    SessionUpdatePurposeInput,
    SessionUpdatePurposeOutput,
    SessionUpdateSelfInput,
    SessionUpdateSelfOutput,
    SessionWaitForEventInput,
    SessionWaitForEventOutput,
    SupervisorTokenInfo,
    SupervisorTokenOutput,
)
from sessionweave.session import Session
from sessionweave.supervisor import (
    NoteRateLimiter,
    SupervisorRequestError,
    parse_supervisor_request,
)

if TYPE_CHECKING:
    from sessionweave.config import ConfigBounds
    from sessionweave.metrics import Metrics
    from sessionweave.persistence import PersistenceStore
    from sessionweave.schemas import PersistenceStatePublic


logger = logging.getLogger(__name__)


class ToolDeps:
    """Bag of dependencies passed to each tool handler."""

    __slots__ = (
        "account_create_lock",
        "account_max_sessions",
        "account_max_tokens",
        "accounts",
        "admin_token",
        "auth_limiter",
        "bounds",
        "events",
        "metrics",
        "registry",
        "relay_epoch",
        "session_creation",
    )

    def __init__(
        self,
        registry: SessionRegistry,
        relay_epoch: int = 0,
        bounds: ConfigBounds | None = None,
        metrics: Metrics | None = None,
        auth_limiter: AuthFailureLimiter | None = None,
        admin_token: str | None = None,
    ) -> None:
        self.registry = registry
        # Per-process startup epoch (UNIX seconds at server build_app).
        # Surfaced on session_join / session_resume outputs and on
        # /v1/info so clients can detect relay restart. See #65.
        self.relay_epoch = relay_epoch
        # Operator-configured bounds for TTL and config fields.
        # Falls back to ConfigBounds defaults when not provided.
        if bounds is None:
            from sessionweave.config import ConfigBounds
            self.bounds: ConfigBounds = ConfigBounds()
        else:
            self.bounds = bounds
        self.metrics = metrics
        self.auth_limiter = auth_limiter if auth_limiter is not None else AuthFailureLimiter()
        # Relay-wide admin token (SW_ADMIN_TOKEN); None means no admin access.
        self.admin_token = admin_token or None
        # Set in the server lifespan once the persistence store exists.
        self.accounts: AccountStore | None = None
        self.events: EventLog | None = None
        self.session_creation: Literal["open", "account"] = "open"
        # Per-account caps (SW_ACCOUNT_MAX_SESSIONS / SW_ACCOUNT_MAX_TOKENS).
        self.account_max_sessions: int = 100
        self.account_max_tokens: int = 20
        # Serialises count-then-create so concurrent creations cannot overshoot.
        self.account_create_lock = asyncio.Lock()


def _int_config(cfg_dict: dict[str, Any]) -> dict[str, int]:
    """Filter a SessionConfig asdict() to only int-valued fields.

    SessionConfig gained string fields (e.g. persistence_db_path) in the
    persistence sub-project, but the public schema exposes config as
    dict[str, int] (operator-visible knobs only). Non-int fields are
    internal deployment settings and are silently dropped here.
    """
    return {k: v for k, v in cfg_dict.items() if isinstance(v, int)}


def _persistence_public_for(sess: Session) -> PersistenceStatePublic | None:
    """Derive a public persistence state snapshot from a Session.

    Returns None for ephemeral sessions. For persisted sessions returns
    the mode; expires_at is left as None (only session_create returns
    the precise deadline; describe/join can refine this in a later task).
    """
    from sessionweave.schemas import PersistenceStatePublic
    if sess._persistence_mode is None:
        return None
    if sess._persistence_mode == "ttl":
        return PersistenceStatePublic(mode="ttl", expires_at=None)
    return PersistenceStatePublic(mode="until_deleted", expires_at=None)


async def _session_for_handle(
    deps: ToolDeps, handle: ConnectionHandle
) -> tuple[Session, str]:
    binding = deps.registry.connections.session_for(handle)
    if binding is None:
        raise ToolError(ErrorCode.NOT_IN_SESSION, "call session_join first")
    sess = await deps.registry.get_or_raise(binding[0])
    return sess, binding[1]


def _iso_from_ms(ms: int) -> str:
    return (
        datetime.fromtimestamp(ms / 1000, UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _invite_fields(
    public_base_url: str | None, sid: str, join_token: str
) -> tuple[str, str]:
    """(invite_url, invite_link) for a join token; empty when no base URL is known."""
    if not public_base_url:
        return "", ""
    return (
        http_invite_url(public_base_url, sid, join_token),
        create_link(public_base_url, sid, join_token),
    )


def current_origin(source_ip: str) -> Origin:
    """Origin of the request being served: its address and client label."""
    return Origin(source_ip, request_client.get())


async def emit_account_event(
    deps: ToolDeps, account_id: str, kind: str, data: dict[str, Any]
) -> None:
    """Record an account event; the action it describes has already happened,
    so a failure to record it is logged, never raised."""
    if deps.events is None:
        return
    try:
        await deps.events.emit(account_id, kind, data)
    except Exception:
        logger.exception("could not record account event %s", kind)


async def authenticate_account(deps: ToolDeps, token: str, origin: Origin) -> AuthResult:
    """Authenticate an account token for ``origin``; a successful result has
    ``account`` set, anything else raises.

    A valid token always passes, even from a throttled IP; failures are
    counted per source IP and throttled like other credential failures.
    A token refused for its network is not a credential failure (it is valid).
    """
    if deps.accounts is None:
        raise ToolError(ErrorCode.ACCOUNTS_UNAVAILABLE, "accounts are not enabled on this relay")
    res = await deps.accounts.authenticate(token, int(time.time() * 1000), origin)
    if res.account is not None:
        return res
    if res.refusal == "network":
        raise ToolError(ErrorCode.NETWORK_NOT_APPROVED, NETWORK_NOT_APPROVED_MESSAGE)
    if deps.auth_limiter.is_throttled("", origin.ip):
        raise ToolError(ErrorCode.RATE_LIMITED, "too many failed account-token attempts")
    deps.auth_limiter.record_failure("", origin.ip)
    raise ToolError(ErrorCode.INVALID_ACCOUNT_TOKEN, "invalid account token")


async def resolve_account(deps: ToolDeps, token: str, *, origin: Origin) -> Account:
    """The active account a bearer token belongs to, or a ToolError."""
    res = await authenticate_account(deps, token, origin)
    assert res.account is not None
    return res.account


class _Owner(NamedTuple):
    account_id: str | None
    # How the creation was authorised, as recorded in session.created.
    token: dict[str, str] | Literal["admin"] | None


async def _creation_owner(
    deps: ToolDeps, requested_owner: str | None, bearer: str | None, origin: Origin
) -> _Owner:
    """Owner account id for a new session, enforcing the deployment mode."""
    if not bearer:
        if deps.session_creation == "account":
            raise ToolError(
                ErrorCode.ACCOUNT_TOKEN_REQUIRED,
                "this broker requires an account token to create sessions: send "
                "Authorization: Bearer <account token> "
                "(ask the broker administrator for an account)",
            )
        if requested_owner is not None:
            raise ToolError(
                ErrorCode.PERMISSION_DENIED, "owner_account_id requires the admin token"
            )
        return _Owner(None, None)
    if admin_matches(bearer, deps.admin_token):
        if requested_owner is None:
            return _Owner(None, "admin")
        if deps.accounts is None:
            raise ToolError(
                ErrorCode.ACCOUNTS_UNAVAILABLE, "accounts are not enabled on this relay"
            )
        return _Owner((await deps.accounts.get(requested_owner)).account_id, "admin")
    if deps.accounts is None:
        # No accounts exist on this relay, so no non-admin token can be valid.
        raise ToolError(ErrorCode.INVALID_ACCOUNT_TOKEN, "invalid account token")
    res = await authenticate_account(deps, bearer, origin)
    assert res.account is not None and res.token_id is not None
    if requested_owner is not None:
        raise ToolError(ErrorCode.PERMISSION_DENIED, "owner_account_id requires the admin token")
    return _Owner(
        res.account.account_id, {"id": res.token_id, "label": res.token_label or ""}
    )


async def check_creation_credential(
    deps: ToolDeps, bearer: str | None, source_ip: str = "", *, origin: Origin | None = None
) -> None:
    """Reject a missing or invalid creation credential without needing the body.

    The same checks ``session_create`` makes, minus the ones that depend on the
    request body (``owner_account_id``), so HTTP callers can fail early.
    """
    origin = origin or current_origin(source_ip)
    if not bearer:
        if deps.session_creation == "account":
            await _creation_owner(deps, None, None, origin)  # raises
        return
    if admin_matches(bearer, deps.admin_token):
        return
    if deps.accounts is None:
        raise ToolError(ErrorCode.INVALID_ACCOUNT_TOKEN, "invalid account token")
    await resolve_account(deps, bearer, origin=origin)


def _purpose_title(purpose: dict[str, Any] | None) -> str | None:
    title = purpose.get("title") if isinstance(purpose, dict) else None
    return title[:200] if isinstance(title, str) and title else None


async def session_create(
    deps: ToolDeps,
    payload: dict[str, Any],
    *,
    public_base_url: str | None = None,
    bearer: str | None = None,
    source_ip: str = "",
    origin: Origin | None = None,
) -> dict[str, Any]:
    inp = SessionCreateInput.model_validate(payload)
    origin = origin or current_origin(source_ip)
    owner = await _creation_owner(deps, inp.owner_account_id, bearer, origin)
    owner_account_id = owner.account_id

    # Validate persistence ttl up-front against operator bounds
    if inp.persistence is not None and inp.persistence.mode == "ttl":
        b = deps.bounds
        assert inp.persistence.ttl_seconds is not None  # enforced by PersistenceConfig validator
        if not (
            b.persistence_ttl_seconds_min
            <= inp.persistence.ttl_seconds
            <= b.persistence_ttl_seconds_max
        ):
            raise ToolError(
                ErrorCode.INVALID_CONFIG,
                f"persistence.ttl_seconds out of range "
                f"[{b.persistence_ttl_seconds_min},{b.persistence_ttl_seconds_max}]",
            )

    # If persistence requested but no store on the relay, refuse explicitly
    if inp.persistence is not None and deps.registry._store is None:
        raise ToolError(
            ErrorCode.PERSISTENCE_FAILED,
            "persistence is not enabled on this relay",
        )

    token = generate_token()
    token_hash = hash_token(token)
    async with AsyncExitStack() as slot:
        if owner_account_id is not None:
            # Count and create under one lock so concurrent creations cannot overshoot.
            await slot.enter_async_context(deps.account_create_lock)
            live = len(await deps.registry.owned_session_ids(owner_account_id))
            if live >= deps.account_max_sessions:
                raise ToolError(
                    ErrorCode.ACCOUNT_SESSION_LIMIT,
                    f"this account already has {live} live sessions (limit "
                    f"{deps.account_max_sessions}); end one before creating another",
                )
        sid, expires_at, cfg, purpose, persistence_state = await deps.registry.create(
            inp.config or {},
            purpose=inp.purpose,
            persistence=inp.persistence,
            creator_token_hash=token_hash,
            owner_account_id=owner_account_id,
        )
    # Echo back a creator_id so the caller has a stable handle for display.
    # If the client passed one (e.g. a cockpit user_id), we use it verbatim;
    # otherwise we mint a uuid4. Not persisted on the relay yet — that
    # requires relay-side auth (sub-project #3), so right now this is a
    # forward-compat shim the cockpit (and future identities) can populate.
    from uuid import uuid4
    creator_id = inp.creator_id if inp.creator_id else str(uuid4())
    sess = await deps.registry.get_or_raise(sid)
    supervisor_token, supervisor_issued = await sess.mint_supervisor_token()
    join_token, join_issued = await sess.mint_join_token()
    assert join_issued.expires_at_ms is not None
    # Done after minting: a suspended session refuses new tokens.
    if owner_account_id is not None and deps.accounts is not None:
        # An admin may create a session for an account that is not active: it
        # starts suspended, like the account's other sessions. (No participants
        # yet, so there is nothing to halt.)
        if (await deps.accounts.get(owner_account_id)).status != "active":
            await sess.suspend()
    invite_url, invite_link = _invite_fields(public_base_url, sid, join_token)
    if owner_account_id is not None:
        await emit_account_event(
            deps,
            owner_account_id,
            "session.created",
            {
                "session_id": sid,
                "title": _purpose_title(purpose),
                "persistence": persistence_state.mode if persistence_state else "none",
                "token": owner.token,
                "ip": origin.ip,
                "client": origin.client,
            },
        )
    return SessionCreateOutput(
        session_id=sid,
        expires_at=expires_at,
        config=_int_config(cfg),
        purpose=purpose,
        persistence=persistence_state,
        creator_token=token,
        creator_id=creator_id,
        supervisor_token=supervisor_token,
        supervisor_token_id=supervisor_issued.token_id,
        join_token=join_token,
        join_token_expires_at=_iso_from_ms(join_issued.expires_at_ms),
        invite_url=invite_url,
        invite_link=invite_link,
        owner_account_id=owner_account_id,
    ).model_dump()


async def session_update_purpose(
    deps: ToolDeps, payload: dict[str, Any], *, source_ip: str = ""
) -> dict[str, Any]:
    """Replace or clear the purpose. Creator or admin only."""
    inp = SessionUpdatePurposeInput.model_validate(payload)
    sess = await _resolve_manager(deps, inp.session_id, inp.creator_token, source_ip)
    new_purpose = await sess.update_purpose(None, inp.purpose)
    return SessionUpdatePurposeOutput(purpose=new_purpose).model_dump()


_READ_ROLES: tuple[Role, ...] = ("participant", "joiner", "supervisor", "creator", "admin")


async def _resolve_reader(
    deps: ToolDeps,
    sid: str,
    token: str | None,
    handle: ConnectionHandle | None,
    *,
    source_ip: str,
    hydrate: bool = True,
) -> Principal:
    """Principal for a read of session ``sid``.

    A connection already bound as a participant of ``sid`` needs no token;
    anyone else presents one.
    """
    if not token and handle is not None:
        binding = deps.registry.connections.session_for(handle)
        if binding is not None and binding[0] == sid:
            return Principal("participant", binding[1])
    principal = await resolve_principal(
        deps, sid, token, source_ip=source_ip, hydrate=hydrate
    )
    require(principal, *_READ_ROLES)
    return principal


async def session_describe(
    deps: ToolDeps,
    payload: dict[str, Any],
    *,
    handle: ConnectionHandle | None = None,
    source_ip: str = "",
) -> dict[str, Any]:
    """Read-only snapshot of a session: purpose + config + roster.

    Needs a session credential (``access_token``) unless the connection is
    already a participant of the session. Does NOT bind the calling MCP
    connection to a participant.
    """
    inp = SessionDescribeInput.model_validate(payload)
    await _resolve_reader(
        deps, inp.session_id, inp.access_token, handle, source_ip=source_ip
    )
    sess = await deps.registry.get_or_raise(inp.session_id)
    return SessionDescribeOutput(
        session_id=sess.session_id,
        expires_at=sess.expires_at_iso(),
        config=_int_config(asdict(sess.config)),
        purpose=sess.purpose,
        roster=sess.roster_snapshot(),
        persistence=_persistence_public_for(sess),
        suspended=sess.suspended,
    ).model_dump()


async def session_join(
    deps: ToolDeps,
    handle: ConnectionHandle,
    payload: dict[str, Any],
    *,
    source_ip: str = "",
) -> dict[str, Any]:
    inp = SessionJoinInput.model_validate(payload)
    if deps.registry.connections.session_for(handle) is not None:
        raise ToolError(ErrorCode.ALREADY_IN_SESSION, "connection already in a session")
    sess = await deps.registry.get_or_raise(inp.session_id)
    principal = await resolve_principal(
        deps, inp.session_id, inp.join_token, source_ip=source_ip
    )
    require(principal, "joiner", "creator", "admin")
    p = await sess.join(
        handle,
        role=inp.role,
        objective=inp.objective,
        capabilities=inp.capabilities,
        metadata=inp.metadata,
        delivery_mode=inp.delivery_mode,
    )
    # The plaintext token leaves RAM here: only its hash stays on the Participant.
    token, p.issued_token = p.issued_token, None
    if token is None:
        raise RuntimeError("Session.join returned no participant token")
    return SessionJoinOutput(
        participant_id=p.participant_id,
        participant_token=token,
        expires_at=sess.expires_at_iso(),
        roster=sess.roster_snapshot(),
        config=_int_config(asdict(sess.config)),
        purpose=sess.purpose,
        relay_epoch=deps.relay_epoch,
        persistence=_persistence_public_for(sess),
    ).model_dump()


async def session_resume(
    deps: ToolDeps,
    handle: ConnectionHandle,
    payload: dict[str, Any],
    *,
    source_ip: str = "",
) -> dict[str, Any]:
    inp = SessionResumeInput.model_validate(payload)
    if deps.registry.connections.session_for(handle) is not None:
        raise ToolError(ErrorCode.ALREADY_IN_SESSION, "connection already in a session")
    sess = await deps.registry.get_or_raise(inp.session_id)
    pid = participant_id_for_token(
        sess, inp.participant_token, limiter=deps.auth_limiter, source_ip=source_ip
    )
    await sess.resume(
        handle, pid, last_acked_seq=inp.last_acked_seq
    )
    return SessionResumeOutput(
        expires_at=sess.expires_at_iso(),
        roster=sess.roster_snapshot(),
        config=_int_config(asdict(sess.config)),
        purpose=sess.purpose,
        relay_epoch=deps.relay_epoch,
    ).model_dump()


async def session_leave(
    deps: ToolDeps,
    handle: ConnectionHandle,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Leave the bound session, optionally attaching exit ``feedback``.

    The ``feedback`` field is propagated on the ``participant.left``
    event broadcast to remaining participants. Skill v0.11.0 makes
    sending it part of the standard close-out flow.
    """
    sess, pid = await _session_for_handle(deps, handle)
    feedback: dict[str, Any] | None = None
    if payload:
        fb = payload.get("feedback")
        if fb is not None and not isinstance(fb, dict):
            raise ToolError(
                ErrorCode.INVALID_CONFIG,
                f"feedback must be a JSON object, got {type(fb).__name__}",
            )
        feedback = fb
    await sess.leave(pid, feedback=feedback)
    return {}


async def session_update_self(
    deps: ToolDeps,
    handle: ConnectionHandle,
    payload: dict[str, Any],
) -> dict[str, Any]:
    sess, pid = await _session_for_handle(deps, handle)
    inp = SessionUpdateSelfInput.model_validate(payload)
    kwargs: dict[str, Any] = {}
    fields = inp.model_dump(exclude_unset=True)
    if "role" in fields:
        kwargs["role"] = fields["role"]
    if "objective" in fields:
        kwargs["objective"] = fields["objective"]
    if "capabilities" in fields:
        kwargs["capabilities"] = fields["capabilities"]
    if "metadata" in fields:
        kwargs["metadata"] = fields["metadata"]
    pub = await sess.update_self(pid, **kwargs)
    return SessionUpdateSelfOutput(participant=pub).model_dump()


async def session_list_participants(
    deps: ToolDeps, handle: ConnectionHandle
) -> dict[str, Any]:
    sess, _ = await _session_for_handle(deps, handle)
    return SessionListParticipantsOutput(roster=sess.roster_snapshot()).model_dump()


async def session_roster(
    deps: ToolDeps, sid: str, token: str | None, *, source_ip: str = ""
) -> dict[str, Any]:
    """The roster for any credential that may read the session (REST)."""
    await _resolve_reader(deps, sid, token, None, source_ip=source_ip)
    sess = await deps.registry.get_or_raise(sid)
    return SessionListParticipantsOutput(roster=sess.roster_snapshot()).model_dump()


async def session_check_inbox(
    deps: ToolDeps,
    handle: ConnectionHandle,
    *,
    last_acked_seq: int | None = None,
) -> dict[str, Any]:
    sess, pid = await _session_for_handle(deps, handle)
    items = await sess.check_inbox(pid, last_acked_seq=last_acked_seq)
    return SessionCheckInboxOutput(
        messages=[
            InboxMessage(name=i["name"], payload=i["payload"], seq=i.get("seq"))
            for i in items
        ]
    ).model_dump()


async def session_wait_for_event(
    deps: ToolDeps, handle: ConnectionHandle, payload: dict[str, Any]
) -> dict[str, Any]:
    # Inline the MCPConnectionHandle import to avoid a potential
    # top-level import cycle with sessionweave.mcp_adapter.
    from sessionweave.mcp_adapter import MCPConnectionHandle

    sess, pid = await _session_for_handle(deps, handle)
    inp = SessionWaitForEventInput.model_validate(payload)

    if isinstance(handle, MCPConnectionHandle):
        cap = sess.config.max_wait_seconds_mcp
        transport_label = "mcp"
    else:
        cap = sess.config.max_wait_seconds
        transport_label = "http"

    requested = float(inp.timeout_seconds)
    effective = min(requested, float(cap))

    if effective < requested and sess._metrics is not None:
        sess._metrics.wait_for_event_clamped_total.labels(
            transport=transport_label
        ).inc()

    result = await sess.wait_for_event(
        pid, names=inp.names, timeout_seconds=effective
    )
    # session.wait_for_event applies a third clamp term (remaining session
    # TTL), so the authoritative effective_timeout_seconds comes from the
    # session result, not the locally-computed transport cap.
    return SessionWaitForEventOutput(
        messages=[
            InboxMessage(name=m["name"], payload=m["payload"])
            for m in result.messages
        ],
        timed_out=result.timed_out,
        effective_timeout_seconds=result.effective_timeout_seconds,
    ).model_dump()


async def session_send(
    deps: ToolDeps,
    handle: ConnectionHandle,
    payload: dict[str, Any],
) -> dict[str, Any]:
    sess, pid = await _session_for_handle(deps, handle)
    inp = SessionSendInput.model_validate(payload)
    out = await sess.send(
        sender_id=pid,
        dispatch=inp.dispatch,
        target=inp.target,
        payload=inp.payload,
        timeout_seconds=inp.timeout_seconds,
        command_id=inp.command_id,
    )
    return out.model_dump()


async def session_respond(
    deps: ToolDeps,
    handle: ConnectionHandle,
    payload: dict[str, Any],
) -> dict[str, Any]:
    sess, pid = await _session_for_handle(deps, handle)
    inp = SessionRespondInput.model_validate(payload)
    await sess.respond(
        responder_id=pid,
        command_id=inp.command_id,
        status=inp.status,
        payload=inp.payload,
    )
    return {}


async def session_reply_to_note(
    deps: ToolDeps,
    handle: ConnectionHandle,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Reply in a supervisor note's thread (participant-bound)."""
    sess, pid = await _session_for_handle(deps, handle)
    note_id, message = payload.get("note_id"), payload.get("message")
    if not isinstance(note_id, str) or not note_id:
        raise ToolError(ErrorCode.INVALID_REQUEST, "note_id must be a non-empty string")
    if not isinstance(message, str):
        raise ToolError(ErrorCode.INVALID_REQUEST, "message must be a string")
    outcome: dict[str, Any] | None = None
    raw_outcome = payload.get("outcome")
    if raw_outcome is not None:
        try:
            outcome = ReplyOutcome.model_validate(raw_outcome).model_dump()
        except ValidationError as exc:
            raise ToolError(
                ErrorCode.INVALID_REQUEST, f"invalid outcome: {exc.errors()[0]['msg']}"
            ) from exc
    return await sess.reply_to_note(pid, note_id, message, outcome)


async def session_delete(
    deps: ToolDeps, payload: dict[str, Any], *, source_ip: str = ""
) -> dict[str, Any]:
    inp = SessionDeleteInput.model_validate(payload)
    try:
        principal = await resolve_principal(
            deps, inp.session_id, inp.creator_token, source_ip=source_ip
        )
        require(principal, "creator", "admin")
    except ToolError as exc:
        if exc.code in (ErrorCode.PERMISSION_DENIED, ErrorCode.RATE_LIMITED):
            _count_delete_failure(
                deps, "rate_limited" if exc.code is ErrorCode.RATE_LIMITED else "invalid_token"
            )
        raise
    reason = "admin_deleted" if principal.role == "admin" else "host_deleted"
    deleted_at = await deps.registry.delete(inp.session_id, reason=reason)
    return SessionDeleteOutput(deleted_at=deleted_at).model_dump()


async def _resolve_manager(
    deps: ToolDeps, session_id: str, token: str, source_ip: str
) -> Session:
    """Authenticate a creator or admin token and return the live session."""
    principal = await resolve_principal(deps, session_id, token, source_ip=source_ip)
    require(principal, "creator", "admin")
    return await deps.registry.get_or_raise(session_id)


async def session_mint_join_token(
    deps: ToolDeps,
    session_id: str,
    token: str,
    *,
    public_base_url: str | None = None,
    source_ip: str = "",
) -> dict[str, Any]:
    sess = await _resolve_manager(deps, session_id, token, source_ip)
    join_token, issued = await sess.mint_join_token()
    assert issued.expires_at_ms is not None
    invite_url, invite_link = _invite_fields(public_base_url, session_id, join_token)
    return JoinTokenOutput(
        join_token=join_token,
        expires_at=_iso_from_ms(issued.expires_at_ms),
        invite_url=invite_url,
        invite_link=invite_link,
    ).model_dump()


async def session_mint_supervisor_token(
    deps: ToolDeps, session_id: str, token: str, *, source_ip: str = ""
) -> dict[str, Any]:
    sess = await _resolve_manager(deps, session_id, token, source_ip)
    supervisor_token, issued = await sess.mint_supervisor_token()
    return SupervisorTokenOutput(
        token_id=issued.token_id, supervisor_token=supervisor_token
    ).model_dump()


async def session_list_supervisor_tokens(
    deps: ToolDeps, session_id: str, token: str, *, source_ip: str = ""
) -> list[dict[str, Any]]:
    sess = await _resolve_manager(deps, session_id, token, source_ip)
    return [
        SupervisorTokenInfo(
            token_id=t.token_id,
            created_at=_iso_from_ms(t.created_at_ms),
            revoked_at=None if t.revoked_at_ms is None else _iso_from_ms(t.revoked_at_ms),
        ).model_dump()
        for t in sess.auth.supervisor_tokens()
    ]


async def session_revoke_supervisor_token(
    deps: ToolDeps,
    session_id: str,
    token: str,
    token_id: str,
    *,
    source_ip: str = "",
) -> None:
    sess = await _resolve_manager(deps, session_id, token, source_ip)
    await sess.revoke_supervisor_token(token_id)


async def session_remove_participant(
    deps: ToolDeps,
    session_id: str,
    token: str,
    participant_id: str,
    *,
    source_ip: str = "",
) -> dict[str, Any]:
    """Remove one participant: creator or admin token only."""
    sess = await _resolve_manager(deps, session_id, token, source_ip)
    return await sess.remove_participant(participant_id)


async def session_post_order(
    deps: ToolDeps,
    session_id: str,
    token: str,
    body: dict[str, Any],
    *,
    limiter: NoteRateLimiter,
    source_ip: str = "",
) -> dict[str, Any]:
    """Post a supervisor order: the MCP twin of ``POST .../supervisor/notes``.

    Same gate (supervisor, creator or admin token) and the same per-(session,
    source ip) rate limit as the HTTP route.
    """
    principal = await resolve_principal(deps, session_id, token, source_ip=source_ip)
    require(principal, "supervisor", "creator", "admin")
    if not limiter.try_acquire(session_id, source_ip):
        raise ToolError(ErrorCode.RATE_LIMITED, "too many supervisor orders; retry later")
    try:
        note = parse_supervisor_request(body)
    except SupervisorRequestError as exc:
        raise ToolError(ErrorCode.INVALID_REQUEST, str(exc)) from None
    sess = await deps.registry.get_or_raise(session_id)
    return await sess.post_order(note)


async def session_access(
    deps: ToolDeps, session_id: str, token: str | None, *, source_ip: str = ""
) -> dict[str, Any]:
    principal = await resolve_principal(deps, session_id, token, source_ip=source_ip)
    return AccessOutput(
        role=principal.role, participant_id=principal.participant_id
    ).model_dump()


def _count_delete_failure(deps: ToolDeps, reason: str) -> None:
    if deps.metrics is not None:
        try:
            deps.metrics.session_delete_failed_total.labels(reason=reason).inc()
        except Exception:
            pass


_THREAD_KINDS = frozenset({"supervisor.note", "supervisor.note_reply"})


def _visible_to(event: dict[str, Any], participant_id: str | None) -> bool:
    """Whether a participant (or joiner, ``None``) may read ``event`` in history.

    A private note and its replies belong to supervision holders and the
    addressed participant; everyone else sees only threads sent to ``all``.
    ``next_cursor`` is left as computed on the unfiltered page, so paging
    never stalls on a run of hidden events.
    """
    if event.get("kind") not in _THREAD_KINDS:
        return True
    target = (event.get("payload") or {}).get("target", "all")
    return target == "all" or (participant_id is not None and target == participant_id)


async def session_get_events(
    deps: ToolDeps,
    payload: dict[str, Any],
    *,
    handle: ConnectionHandle | None = None,
    source_ip: str = "",
) -> dict[str, Any]:
    inp = SessionGetEventsInput.model_validate(payload)
    sid = inp.session_id
    # Cold and ended sessions are authenticated from their stored hashes
    # without being loaded into RAM.
    principal = await _resolve_reader(
        deps, sid, inp.access_token, handle, source_ip=source_ip, hydrate=False
    )
    state: Literal["hot", "cold", "ended"]
    event_store: PersistenceStore
    # First check the in-RAM map without triggering hydrate
    if sid in deps.registry._sessions:
        sess = deps.registry._sessions[sid]
        if sess._persistence_mode is None:
            raise ToolError(
                ErrorCode.NO_HISTORY,
                "session is not persisted; no history available",
            )
        if deps.registry._store is None:
            raise ToolError(ErrorCode.SESSION_NOT_FOUND, "session not found")
        event_store = deps.registry._store
        state = "hot"
    else:
        store = deps.registry._store
        if store is None:
            raise ToolError(ErrorCode.SESSION_NOT_FOUND, "session not found")
        if not await store.exists(sid):
            raise ToolError(ErrorCode.SESSION_NOT_FOUND, "session not found")
        # Peek at ended_at without full load_session
        async with store._read_pool[0].execute(
            "SELECT ended_at FROM sessions WHERE session_id = ?", (sid,),
        ) as cur:
            row = await cur.fetchone()
        state = "ended" if (row is not None and row[0] is not None) else "cold"
        event_store = store

    page = await event_store.fetch_events(
        sid=sid,
        after_seq=inp.after_seq,
        before_seq=inp.before_seq,
        kinds=tuple(inp.kind) if inp.kind else None,
        participant_ids=tuple(inp.participant_id) if inp.participant_id else None,
        limit=inp.limit,
        order=inp.order,
    )
    events = page.events
    if principal.role in ("participant", "joiner"):
        events = [
            e for e in events if _visible_to(e, principal.participant_id)
        ]
    if deps.metrics is not None:
        try:
            deps.metrics.events_browse_total.labels(session_state=state).inc()
            deps.metrics.events_browse_returned_total.inc(len(events))
        except Exception:
            pass
    return SessionGetEventsOutput(
        session_id=sid,
        events=events,
        next_cursor=page.next_cursor,
        has_more=page.has_more,
        session_state=state,
    ).model_dump()
