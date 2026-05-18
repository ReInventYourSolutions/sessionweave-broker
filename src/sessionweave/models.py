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
    # ``push`` (default) ⇒ relay sends notifications via MCP transport.
    # ``pull``           ⇒ relay routes ALL notifications into the participant's
    # buffer, drained via session_check_inbox. Use for clients that don't
    # surface server notifications to their model.
    delivery_mode: str = "push"
    # Monotonically increasing per-participant sequence number. Incremented
    # by ``Session._deliver_or_buffer`` before every event is delivered (live
    # OR buffered) so each event carries a unique ``seq`` the client can
    # acknowledge. A reconnecting WS / drain client can pass
    # ``last_acked_seq=N`` to skip events it has already consumed in memory
    # before its socket died. Starts at 0; the first event carries ``seq=1``.
    next_seq: int = 0
    # Authoritative server-side high-water mark of acknowledged seq.
    # Advanced when the relay receives an upstream ``{"ack": N}`` frame
    # on the participant's WS, or when a client passes a stronger
    # ``last_acked_seq`` on resume / drain. Used to (a) eagerly trim the
    # per-participant buffer of events the client has confirmed it
    # consumed, and (b) seed buffer-replay filters when the caller
    # didn't pass an explicit ``last_acked_seq`` (so clients that ACK
    # promptly don't need to track a watermark themselves).
    last_acked_seq: int = 0

    @staticmethod
    def create(
        role: str | None = None,
        objective: str | None = None,
        capabilities: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        delivery_mode: str = "push",
    ) -> Participant:
        return Participant(
            participant_id=str(uuid.uuid4()),
            role=role,
            objective=objective,
            capabilities=capabilities or [],
            metadata=metadata or {},
            joined_at=_utcnow(),
            delivery_mode=delivery_mode,
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

    def to_snapshot(self) -> dict[str, object]:
        """JSON-safe snapshot used by PersistenceStore."""
        return {
            "command_id": self.command_id,
            "sender_id": self.sender_id,
            "dispatch_mode": self.dispatch_mode.value,
            "dispatched_to": list(self.dispatched_to),
            "deadline_ms": int(self.deadline.timestamp() * 1000),
            "received_count": self.received_count,
            "expected_count": self.expected_count,
            "responses_received": list(self.responses_received),
            "created_at_ms": int(self.created_at.timestamp() * 1000),
        }

    @classmethod
    def from_snapshot(cls, d: dict[str, Any]) -> CommandTracker:
        from datetime import UTC, datetime
        return cls(
            command_id=d["command_id"],
            sender_id=d["sender_id"],
            dispatch_mode=DispatchMode(d["dispatch_mode"]),
            dispatched_to=list(d.get("dispatched_to", [])),
            created_at=datetime.fromtimestamp(
                d.get("created_at_ms", 0) / 1000, tz=UTC),
            deadline=datetime.fromtimestamp(d["deadline_ms"] / 1000, tz=UTC),
            responses_received=set(d.get("responses_received", [])),
        )
