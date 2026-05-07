from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class DispatchMode(StrEnum):
    ALL = "all"
    ANY = "any"
    SPECIFIC = "specific"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ParticipantPublic(_Strict):
    participant_id: str
    role: str | None
    objective: str | None
    capabilities: list[str]
    metadata: dict[str, Any]
    joined_at: str
    online: bool


class Target(_Strict):
    role: str | None = None
    capability: str | None = None
    participant_ids: list[str] | None = None


class SessionCreateInput(_Strict):
    config: dict[str, Any] | None = None


class SessionCreateOutput(_Strict):
    session_id: str
    expires_at: str
    config: dict[str, int]


class SessionJoinInput(_Strict):
    session_id: str
    role: str | None = None
    objective: str | None = None
    capabilities: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    delivery_mode: Literal["push", "pull"] = "push"


class SessionJoinOutput(_Strict):
    participant_id: str
    expires_at: str
    roster: list[ParticipantPublic]
    config: dict[str, int]


class SessionResumeInput(_Strict):
    session_id: str
    participant_id: str


class SessionResumeOutput(_Strict):
    expires_at: str
    roster: list[ParticipantPublic]
    config: dict[str, int]


class SessionUpdateSelfInput(_Strict):
    role: str | None = None
    objective: str | None = None
    capabilities: list[str] | None = None
    metadata: dict[str, Any] | None = None


class SessionUpdateSelfOutput(_Strict):
    participant: ParticipantPublic


class SessionListParticipantsOutput(_Strict):
    roster: list[ParticipantPublic]


class InboxMessage(_Strict):
    name: str
    payload: dict[str, Any]


class SessionCheckInboxOutput(_Strict):
    messages: list[InboxMessage]


class SessionSendInput(_Strict):
    dispatch: DispatchMode
    target: Target
    payload: dict[str, Any]
    timeout_seconds: int | None = None

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
    reason: Literal["all_responded", "ttl_expired"]


class CommandCancelledNotification(_Strict):
    command_id: str
    reason: Literal["ttl_expired", "sender_gone"]


class SessionEndedNotification(_Strict):
    reason: Literal["idle_ttl", "server_shutdown"]
