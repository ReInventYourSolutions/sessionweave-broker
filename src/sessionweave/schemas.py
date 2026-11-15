from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from sessionweave.access import Role


class DispatchMode(StrEnum):
    ALL = "all"
    ANY = "any"
    SPECIFIC = "specific"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ReplyOutcome(_Strict):
    """Structured result of an order, carried by a supervisor note reply."""

    status: Literal["done", "partial", "impossible"]
    undone: list[Annotated[str, StringConstraints(max_length=256)]] = Field(
        default_factory=list, max_length=64
    )
    not_reversible: list[Annotated[str, StringConstraints(max_length=256)]] = Field(
        default_factory=list, max_length=64
    )


class ParticipantPublic(_Strict):
    participant_id: str
    role: str | None
    objective: str | None
    capabilities: list[str]
    metadata: dict[str, Any]
    joined_at: str
    online: bool
    supervision_state: Literal["active", "paused"]


class Target(_Strict):
    role: str | None = None
    capability: str | None = None
    participant_ids: list[str] | None = None


class PersistenceConfig(_Strict):
    """How long a session's contents persist on disk."""
    mode: Literal["ttl", "until_deleted"]
    ttl_seconds: int | None = None

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.mode == "ttl" and self.ttl_seconds is None:
            raise ValueError("ttl_seconds is required when mode='ttl'")
        if self.mode == "until_deleted" and self.ttl_seconds is not None:
            raise ValueError("ttl_seconds must be omitted when mode='until_deleted'")
        return self


class PersistenceStatePublic(_Strict):
    """Server-side view of a session's persistence state, exposed to clients."""
    mode: Literal["ttl", "until_deleted"]
    expires_at: str | None = None


class SessionCreateInput(_Strict):
    config: dict[str, Any] | None = None
    # Free-form description of WHY this session exists. Persisted on the
    # Session and echoed back to every joiner so a recipient of an invite
    # link can act without an out-of-band briefing. Capped at 16 KiB JSON.
    purpose: dict[str, Any] | None = None
    persistence: PersistenceConfig | None = None
    # Optional opaque handle identifying the creator (the cockpit user_id,
    # or any future authenticated user identity on the relay side). If the
    # client doesn't pass one, the relay mints a uuid4 so the output field
    # is always populated. Not persisted in v0.16 — the cockpit echoes it
    # locally for display; relay-side enforcement (creator-only ops bound
    # to a user identity) is a future change once relay auth lands.
    creator_id: str | None = None
    # Admin-only: create the session on behalf of this account.
    owner_account_id: str | None = None


class SessionCreateOutput(_Strict):
    session_id: str
    expires_at: str
    config: dict[str, int]
    purpose: dict[str, Any] | None = None
    persistence: PersistenceStatePublic | None = None
    creator_token: str = ""
    creator_id: str = ""
    supervisor_token: str = ""
    supervisor_token_id: str = ""
    join_token: str = ""
    join_token_expires_at: str = ""
    invite_url: str = ""
    invite_link: str = ""
    owner_account_id: str | None = None


class JoinTokenOutput(_Strict):
    join_token: str
    expires_at: str
    invite_url: str
    invite_link: str


class SupervisorTokenOutput(_Strict):
    token_id: str
    supervisor_token: str


class SupervisorTokenInfo(_Strict):
    token_id: str
    created_at: str
    revoked_at: str | None = None


class AccessOutput(_Strict):
    role: Role
    participant_id: str | None = None


class SessionJoinInput(_Strict):
    session_id: str
    # Join token from the invite link (the creator or admin token also
    # works). Optional in the schema so a missing one is a permission
    # error rather than a validation error.
    join_token: str | None = None
    role: str | None = None
    objective: str | None = None
    capabilities: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    delivery_mode: Literal["push", "pull"] = "push"


class SessionJoinOutput(_Strict):
    participant_id: str
    # Secret credential for this participant, returned exactly once.
    participant_token: str
    expires_at: str
    roster: list[ParticipantPublic]
    config: dict[str, int]
    purpose: dict[str, Any] | None = None
    # Per-relay-process startup epoch — clients store this at join
    # time and compare on resume to detect relay restart. See #65.
    relay_epoch: int = 0
    persistence: PersistenceStatePublic | None = None


class SessionResumeInput(_Strict):
    session_id: str
    participant_token: str
    # Highest seq the caller has already consumed in memory over some
    # prior channel (typically a WS that died mid-stream). When set,
    # the push-mode buffer replay on resume filters out events with
    # ``seq <= last_acked_seq``. Equivalent to the WS attach's
    # ``?last_acked_seq=N`` query param — closes the parity gap between
    # the two reconnect paths.
    last_acked_seq: int | None = None


class SessionResumeOutput(_Strict):
    expires_at: str
    roster: list[ParticipantPublic]
    config: dict[str, int]
    purpose: dict[str, Any] | None = None
    # Per-relay-process monotonic startup epoch (UNIX seconds at boot).
    # Clients should remember this at join time and compare on resume;
    # if it changes the relay has restarted and the client's local seq
    # cursor refers to a now-empty buffer in a fresh process. Issue #65.
    relay_epoch: int = 0


class SessionUpdateSelfInput(_Strict):
    role: str | None = None
    objective: str | None = None
    capabilities: list[str] | None = None
    metadata: dict[str, Any] | None = None


class SessionUpdateSelfOutput(_Strict):
    participant: ParticipantPublic


class SessionListParticipantsOutput(_Strict):
    roster: list[ParticipantPublic]


class SessionDescribeInput(_Strict):
    session_id: str
    # Any session credential (participant, join, supervisor, creator, admin).
    # Optional only for a connection already bound as a participant.
    access_token: str | None = None


class SessionUpdatePurposeInput(_Strict):
    session_id: str
    creator_token: str
    # Pass `None` (or omit) to clear the purpose entirely.
    purpose: dict[str, Any] | None = None


class SessionUpdatePurposeOutput(_Strict):
    purpose: dict[str, Any] | None = None


class SessionDescribeOutput(_Strict):
    """Read-only snapshot of a session — no participant allocated."""

    session_id: str
    expires_at: str
    config: dict[str, int]
    purpose: dict[str, Any] | None = None
    roster: list[ParticipantPublic]
    persistence: PersistenceStatePublic | None = None
    suspended: bool = False


class InboxMessage(_Strict):
    name: str
    payload: dict[str, Any]
    # Per-participant monotonic sequence number assigned by the relay.
    # Optional for backwards-compat: pre-seq events buffered by older
    # builds (or items handed in via legacy paths) may not carry one.
    seq: int | None = None


class SessionCheckInboxOutput(_Strict):
    messages: list[InboxMessage]


class SessionWaitForEventInput(_Strict):
    session_id: str
    names: list[str] | None = None
    timeout_seconds: float = Field(default=60.0, ge=0.0)


class SessionWaitForEventOutput(_Strict):
    messages: list[InboxMessage]
    timed_out: bool
    effective_timeout_seconds: float


class SessionSendInput(_Strict):
    dispatch: DispatchMode
    target: Target
    payload: dict[str, Any]
    timeout_seconds: int | None = None
    # Optional caller-supplied command_id. When present, a retry of
    # session_send with the same command_id returns the ORIGINAL
    # dispatch's response (no duplicate event, no new tracker). Required
    # for retry-safe semantics on persisted sessions; otherwise the relay
    # generates a fresh uuid4 server-side.
    command_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_-]{1,64}$")

    @model_validator(mode="after")
    def _validate_target(self) -> SessionSendInput:
        if self.dispatch is DispatchMode.SPECIFIC:
            if not self.target.participant_ids:
                raise ValueError("dispatch=specific requires non-empty target.participant_ids")
        else:
            if not (self.target.role or self.target.capability):
                raise ValueError(
                    "dispatch=all|any requires at least one of target.role / target.capability"
                )
        return self


class SessionSendOutput(_Strict):
    command_id: str
    delivered_to: list[str]


class SessionRespondInput(_Strict):
    command_id: str
    payload: dict[str, Any]
    status: Literal["ok", "error"] = "ok"


# ---- Notification payload models ----

class ParticipantJoinedNotification(_Strict):
    participant: ParticipantPublic


class ParticipantUpdatedNotification(_Strict):
    participant: ParticipantPublic


class ParticipantLeftNotification(_Strict):
    participant_id: str
    reason: Literal["left", "offline_timeout"]


class CommandReceivedNotification(_Strict):
    command_id: str
    sender_id: str
    dispatch_mode: DispatchMode
    dispatched_to: list[str]
    payload: dict[str, Any]
    expires_at: str


class ResultReceivedNotification(_Strict):
    command_id: str
    responder_id: str
    status: Literal["ok", "error"]
    payload: dict[str, Any]


class CommandCompletedNotification(_Strict):
    command_id: str
    received_count: int
    expected_count: int
    reason: Literal["all_responded", "ttl_expired", "sender_gone", "no_recipients"]


class CommandCancelledNotification(_Strict):
    command_id: str
    reason: Literal["ttl_expired", "sender_gone"]


class SessionEndedNotification(_Strict):
    # NOTE: this class is documentation-only — the relay broadcasts the
    # `session.ended` event payload as a plain dict (see
    # `Session.broadcast_session_ended`) and never instantiates this
    # model at runtime. Kept here so SDK consumers and the OpenAPI export
    # see the full set of reasons.
    reason: Literal[
        "idle_ttl",
        "server_shutdown",
        "host_deleted",          # session_delete via creator_token
        "admin_deleted",         # session_delete via admin_token (HTTP)
        "persistence_expired",   # ttl / until_deleted safety sweep
    ]


# ---- Persistence tool I/O ----

class SessionDeleteInput(_Strict):
    session_id: str
    creator_token: str


class SessionDeleteOutput(_Strict):
    deleted_at: str


class SessionGetEventsInput(_Strict):
    session_id: str
    access_token: str | None = None
    after_seq: int | None = None
    before_seq: int | None = None
    kind: list[str] | None = None
    participant_id: list[str] | None = None
    limit: int = Field(default=200, ge=1, le=1000)
    order: Literal["asc", "desc"] = "asc"

    @field_validator("kind", "participant_id", mode="before")
    @classmethod
    def _csv_split(cls, v: Any) -> Any:
        if v is None or isinstance(v, list):
            return v
        if isinstance(v, str):
            return [s for s in (part.strip() for part in v.split(",")) if s]
        return v


class SessionGetEventsOutput(_Strict):
    session_id: str
    events: list[dict[str, Any]]
    next_cursor: int | None
    has_more: bool
    session_state: Literal["hot", "cold", "ended"]
