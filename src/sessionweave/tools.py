from __future__ import annotations

from dataclasses import asdict
from typing import Any

from sessionweave.connections import ConnectionHandle
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.registry import SessionRegistry
from sessionweave.schemas import (
    InboxMessage,
    SessionCheckInboxOutput,
    SessionCreateInput,
    SessionCreateOutput,
    SessionDescribeInput,
    SessionDescribeOutput,
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


class ToolDeps:
    """Bag of dependencies passed to each tool handler."""

    __slots__ = ("registry",)

    def __init__(self, registry: SessionRegistry) -> None:
        self.registry = registry


def _session_for_handle(
    deps: ToolDeps, handle: ConnectionHandle
) -> tuple[Session, str]:
    binding = deps.registry.connections.session_for(handle)
    if binding is None:
        raise ToolError(ErrorCode.NOT_IN_SESSION, "call session_join first")
    sess = deps.registry.get_or_raise(binding[0])
    return sess, binding[1]


async def session_create(deps: ToolDeps, payload: dict[str, Any]) -> dict[str, Any]:
    inp = SessionCreateInput.model_validate(payload)
    sid, expires_at, cfg, purpose = await deps.registry.create(
        inp.config or {}, purpose=inp.purpose
    )
    return SessionCreateOutput(
        session_id=sid, expires_at=expires_at, config=cfg, purpose=purpose
    ).model_dump()


async def session_update_purpose(
    deps: ToolDeps,
    handle: ConnectionHandle,
    payload: dict[str, Any],
) -> dict[str, Any]:
    sess, pid = _session_for_handle(deps, handle)
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
    sess = deps.registry.get_or_raise(inp.session_id)
    return SessionDescribeOutput(
        session_id=sess.session_id,
        expires_at=sess.expires_at_iso(),
        config=asdict(sess.config),
        purpose=sess.purpose,
        roster=sess.roster_snapshot(),
    ).model_dump()


async def session_join(
    deps: ToolDeps,
    handle: ConnectionHandle,
    payload: dict[str, Any],
) -> dict[str, Any]:
    inp = SessionJoinInput.model_validate(payload)
    if deps.registry.connections.session_for(handle) is not None:
        raise ToolError(ErrorCode.ALREADY_IN_SESSION, "connection already in a session")
    sess = deps.registry.get_or_raise(inp.session_id)
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
        config=asdict(sess.config),
        purpose=sess.purpose,
    ).model_dump()


async def session_resume(
    deps: ToolDeps,
    handle: ConnectionHandle,
    payload: dict[str, Any],
) -> dict[str, Any]:
    inp = SessionResumeInput.model_validate(payload)
    if deps.registry.connections.session_for(handle) is not None:
        raise ToolError(ErrorCode.ALREADY_IN_SESSION, "connection already in a session")
    sess = deps.registry.get_or_raise(inp.session_id)
    await sess.resume(handle, inp.participant_id)
    return SessionResumeOutput(
        expires_at=sess.expires_at_iso(),
        roster=sess.roster_snapshot(),
        config=asdict(sess.config),
        purpose=sess.purpose,
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
    sess, pid = _session_for_handle(deps, handle)
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
    sess, pid = _session_for_handle(deps, handle)
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
    sess, _ = _session_for_handle(deps, handle)
    return SessionListParticipantsOutput(roster=sess.roster_snapshot()).model_dump()


async def session_check_inbox(
    deps: ToolDeps, handle: ConnectionHandle
) -> dict[str, Any]:
    sess, pid = _session_for_handle(deps, handle)
    items = await sess.check_inbox(pid)
    return SessionCheckInboxOutput(
        messages=[InboxMessage(name=i["name"], payload=i["payload"]) for i in items]
    ).model_dump()


async def session_wait_for_event(
    deps: ToolDeps, handle: ConnectionHandle, payload: dict[str, Any]
) -> dict[str, Any]:
    # Inline the MCPConnectionHandle import to avoid a potential
    # top-level import cycle with sessionweave.mcp_adapter.
    from sessionweave.mcp_adapter import MCPConnectionHandle

    sess, pid = _session_for_handle(deps, handle)
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
    return SessionWaitForEventOutput(
        messages=[
            InboxMessage(name=m["name"], payload=m["payload"])
            for m in result.messages
        ],
        timed_out=result.timed_out,
        effective_timeout_seconds=effective,
    ).model_dump()


async def session_send(
    deps: ToolDeps,
    handle: ConnectionHandle,
    payload: dict[str, Any],
) -> dict[str, Any]:
    sess, pid = _session_for_handle(deps, handle)
    inp = SessionSendInput.model_validate(payload)
    out = await sess.send(
        sender_id=pid,
        dispatch=inp.dispatch,
        target=inp.target,
        payload=inp.payload,
        timeout_seconds=inp.timeout_seconds,
    )
    return out.model_dump()


async def session_respond(
    deps: ToolDeps,
    handle: ConnectionHandle,
    payload: dict[str, Any],
) -> dict[str, Any]:
    sess, pid = _session_for_handle(deps, handle)
    inp = SessionRespondInput.model_validate(payload)
    await sess.respond(
        responder_id=pid,
        command_id=inp.command_id,
        status=inp.status,
        payload=inp.payload,
    )
    return {}
