from __future__ import annotations

import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sessionweave.schemas import DispatchMode, ParticipantPublic


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _isoformat(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(slots=True)
class Participant:
    participant_id: str
    role: str | None
    objective: str | None
    capabilities: list[str]
    metadata: dict[str, Any]
    joined_at: datetime
    online: bool = False
    last_online_at: datetime = field(default_factory=_utcnow)
    pending_notifications: deque[Any] = field(default_factory=deque)
    connection_ref: object | None = None  # opaque handle, set by ConnectionManager

    @staticmethod
    def create(
        role: str | None = None,
        objective: str | None = None,
        capabilities: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Participant:
        return Participant(
            participant_id=str(uuid.uuid4()),
            role=role,
            objective=objective,
            capabilities=capabilities or [],
            metadata=metadata or {},
            joined_at=_utcnow(),
        )

    def to_public(self) -> ParticipantPublic:
        return ParticipantPublic(
            participant_id=self.participant_id,
            role=self.role,
            objective=self.objective,
            capabilities=list(self.capabilities),
            metadata=dict(self.metadata),
            joined_at=_isoformat(self.joined_at),
            online=self.online,
        )


@dataclass(slots=True)
class CommandTracker:
    command_id: str
    sender_id: str
    dispatch_mode: DispatchMode
    dispatched_to: list[str]
    created_at: datetime
    deadline: datetime
    responses_received: set[str] = field(default_factory=set)

    @property
    def received_count(self) -> int:
        return len(self.responses_received)

    @property
    def expected_count(self) -> int:
        return len(self.dispatched_to)

    def record_response(self, responder_id: str) -> bool:
        if responder_id in self.responses_received:
            return False
        if responder_id not in self.dispatched_to:
            return False
        self.responses_received.add(responder_id)
        return True

    def pending_recipients(self) -> list[str]:
        return [pid for pid in self.dispatched_to if pid not in self.responses_received]

    def is_complete(self) -> bool:
        return self.received_count >= self.expected_count

    def is_expired(self, now: datetime | None = None) -> bool:
        return (now or _utcnow()) >= self.deadline
