from __future__ import annotations

import asyncio
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Literal

from sessionweave.connections import ConnectionHandle
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.registry import SessionRegistry
from sessionweave.schemas import (
    InboxMessage,
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
)
from sessionweave.session import Session

if TYPE_CHECKING:
    from sessionweave.config import ConfigBounds
    from sessionweave.metrics import Metrics
    from sessionweave.persistence import PersistenceStore
    from sessionweave.persistence_token import DeleteRateLimiter
    from sessionweave.schemas import PersistenceStatePublic


class ToolDeps:
    """Bag of dependencies passed to each tool handler."""

    __slots__ = ("bounds", "delete_rate_limiter", "metrics", "registry", "relay_epoch")

    def __init__(
        self,
        registry: SessionRegistry,
        relay_epoch: int = 0,
        bounds: ConfigBounds | None = None,
        metrics: Metrics | None = None,
        delete_rate_limiter: DeleteRateLimiter | None = None,
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
        self.delete_rate_limiter = delete_rate_limiter


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


async def session_create(deps: ToolDeps, payload: dict[str, Any]) -> dict[str, Any]:
    from sessionweave.persistence_token import generate_creator_token, hash_creator_token

    inp = SessionCreateInput.model_validate(payload)

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

    token = generate_creator_token()
    token_hash = hash_creator_token(token)
    sid, expires_at, cfg, purpose, persistence_state = await deps.registry.create(
        inp.config or {},
        purpose=inp.purpose,
        persistence=inp.persistence,
        creator_token_hash=token_hash,
    )
    # Echo back a creator_id so the caller has a stable handle for display.
    # If the client passed one (e.g. a cockpit user_id), we use it verbatim;
    # otherwise we mint a uuid4. Not persisted on the relay yet — that
    # requires relay-side auth (sub-project #3), so right now this is a
    # forward-compat shim the cockpit (and future identities) can populate.
    from uuid import uuid4
    creator_id = inp.creator_id if inp.creator_id else str(uuid4())
    return SessionCreateOutput(
        session_id=sid,
        expires_at=expires_at,
        config=_int_config(cfg),
        purpose=purpose,
        persistence=persistence_state,
        creator_token=token,
        creator_id=creator_id,
    ).model_dump()


async def session_update_purpose(
    deps: ToolDeps,
    handle: ConnectionHandle,
    payload: dict[str, Any],
) -> dict[str, Any]:
    sess, pid = await _session_for_handle(deps, handle)
    inp = SessionUpdatePurposeInput.model_validate(payload)
    new_purpose = await sess.update_purpose(pid, inp.purpose)
    return SessionUpdatePurposeOutput(purpose=new_purpose).model_dump()


async def session_describe(deps: ToolDeps, payload: dict[str, Any]) -> dict[str, Any]:
    """Read-only snapshot of a session: purpose + config + roster.

    Does NOT bind the calling MCP connection to a participant. Use it as a
    pre-flight before deciding whether to ``session_join`` and which role
    to take.
    """
    inp = SessionDescribeInput.model_validate(payload)
    sess = await deps.registry.get_or_raise(inp.session_id)
    return SessionDescribeOutput(
        session_id=sess.session_id,
        expires_at=sess.expires_at_iso(),
        config=_int_config(asdict(sess.config)),
        purpose=sess.purpose,
        roster=sess.roster_snapshot(),
        persistence=_persistence_public_for(sess),
    ).model_dump()


async def session_join(
    deps: ToolDeps,
    handle: ConnectionHandle,
    payload: dict[str, Any],
) -> dict[str, Any]:
    inp = SessionJoinInput.model_validate(payload)
    if deps.registry.connections.session_for(handle) is not None:
        raise ToolError(ErrorCode.ALREADY_IN_SESSION, "connection already in a session")
    sess = await deps.registry.get_or_raise(inp.session_id)
    p = await sess.join(
        handle,
        role=inp.role,
        objective=inp.objective,
        capabilities=inp.capabilities,
        metadata=inp.metadata,
        delivery_mode=inp.delivery_mode,
    )
    return SessionJoinOutput(
        participant_id=p.participant_id,
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
) -> dict[str, Any]:
    inp = SessionResumeInput.model_validate(payload)
    if deps.registry.connections.session_for(handle) is not None:
        raise ToolError(ErrorCode.ALREADY_IN_SESSION, "connection already in a session")
    sess = await deps.registry.get_or_raise(inp.session_id)
    await sess.resume(
        handle, inp.participant_id, last_acked_seq=inp.last_acked_seq
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


async def session_delete(deps: ToolDeps, payload: dict[str, Any]) -> dict[str, Any]:
    from sessionweave.persistence_token import (
        verify_creator_token,
    )
    inp = SessionDeleteInput.model_validate(payload)
    rl = deps.delete_rate_limiter
    # Uniform back-pressure sleep on EVERY failure path so an attacker
    # can't distinguish "rate-limited" from "invalid token" via response
    # latency. This costs callers a 0.5 s tax on their first wrong guess,
    # which is negligible against the 256-bit creator_token entropy.
    if rl is not None and rl.is_banned(inp.session_id):
        if deps.metrics is not None:
            try:
                deps.metrics.session_delete_failed_total.labels(
                    reason="rate_limited"
                ).inc()
            except Exception:
                pass
        await asyncio.sleep(0.5)
        raise ToolError(ErrorCode.PERMISSION_DENIED, "rate-limited")
    sess = await deps.registry.get_or_raise(inp.session_id)
    if sess.creator_token_hash is None or not verify_creator_token(
        inp.creator_token, sess.creator_token_hash
    ):
        if rl is not None:
            rl.record_failure(inp.session_id)
        if deps.metrics is not None:
            try:
                deps.metrics.session_delete_failed_total.labels(
                    reason="invalid_token"
                ).inc()
            except Exception:
                pass
        # Same 0.5 s tax as the rate-limited path — see comment above.
        await asyncio.sleep(0.5)
        raise ToolError(ErrorCode.PERMISSION_DENIED, "invalid creator_token")
    deleted_at = await deps.registry.delete(inp.session_id, reason="host_deleted")
    if rl is not None:
        rl.reset(inp.session_id)
    return SessionDeleteOutput(deleted_at=deleted_at).model_dump()


async def session_get_events(deps: ToolDeps, payload: dict[str, Any]) -> dict[str, Any]:
    inp = SessionGetEventsInput.model_validate(payload)
    sid = inp.session_id
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
    if deps.metrics is not None:
        try:
            deps.metrics.events_browse_total.labels(session_state=state).inc()
            deps.metrics.events_browse_returned_total.inc(len(page.events))
        except Exception:
            pass
    return SessionGetEventsOutput(
        session_id=sid,
        events=page.events,
        next_cursor=page.next_cursor,
        has_more=page.has_more,
        session_state=state,
    ).model_dump()
