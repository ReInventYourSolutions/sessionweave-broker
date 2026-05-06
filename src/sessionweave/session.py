from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionHandle, ConnectionManager
from sessionweave.dispatcher import RoundRobinPicker
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.models import CommandTracker, Participant
from sessionweave.schemas import ParticipantPublic

_UNSET: Any = object()


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _isoformat(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


class Session:
    """Single-session state machine. All public methods acquire `self.lock`
    so per-session operations are linearizable.

    Tasks 10-13 add membership, command, resume, cancellation methods.
    """

    __slots__ = (
        "_connections",
        "_created_at",
        "_inflight",
        "_last_activity_at",
        "_participants",
        "_picker",
        "config",
        "lock",
        "session_id",
    )

    def __init__(
        self,
        session_id: str,
        config: SessionConfig,
        connections: ConnectionManager,
    ) -> None:
        now = _utcnow()
        self.session_id = session_id
        self.config = config
        self._participants: dict[str, Participant] = {}
        self._inflight: dict[str, CommandTracker] = {}
        self._picker = RoundRobinPicker()
        self._last_activity_at: datetime = now
        self._created_at: datetime = now
        self.lock = asyncio.Lock()
        self._connections = connections

    # --- read-only helpers ---

    def expires_at(self, *, at: datetime | None = None) -> datetime:
        return self._last_activity_at + timedelta(seconds=self.config.idle_ttl_seconds)

    def is_idle_expired(self, *, at: datetime | None = None) -> bool:
        now = at or _utcnow()
        return now >= self.expires_at()

    def roster_snapshot(self) -> list[ParticipantPublic]:
        return [p.to_public() for p in self._participants.values()]

    def expires_at_iso(self) -> str:
        return _isoformat(self.expires_at())

    # --- activity tracking ---

    def bump_activity(self, *, at: datetime | None = None) -> None:
        self._last_activity_at = at or _utcnow()

    # --- membership operations ---

    async def join(
        self,
        handle: ConnectionHandle,
        *,
        role: str | None,
        objective: str | None = None,
        capabilities: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Participant:
        async with self.lock:
            if len(self._participants) >= self.config.max_participants:
                raise ToolError(ErrorCode.SESSION_FULL, "max participants reached")
            p = Participant.create(
                role=role,
                objective=objective,
                capabilities=capabilities or [],
                metadata=metadata or {},
            )
            p.online = True
            p.connection_ref = handle
            self._connections.bind(handle, self.session_id, p.participant_id)
            self._participants[p.participant_id] = p
            self.bump_activity()
            self._notify_others(
                exclude_pid=p.participant_id,
                name="participant.joined",
                payload={"participant": p.to_public().model_dump()},
            )
            return p

    async def leave(self, participant_id: str) -> None:
        async with self.lock:
            p = self._participants.pop(participant_id, None)
            if p is None:
                raise ToolError(ErrorCode.NOT_IN_SESSION, "participant not in session")
            handle = p.connection_ref
            if handle is not None:
                self._connections.unbind(handle)  # type: ignore[arg-type]
            self.bump_activity()
            self._notify_others(
                exclude_pid=participant_id,
                name="participant.left",
                payload={"participant_id": participant_id, "reason": "left"},
            )

    async def update_self(
        self,
        participant_id: str,
        *,
        role: Any = _UNSET,
        objective: Any = _UNSET,
        capabilities: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ParticipantPublic:
        async with self.lock:
            p = self._participants.get(participant_id)
            if p is None:
                raise ToolError(ErrorCode.NOT_IN_SESSION, "participant not in session")
            if role is not _UNSET:
                p.role = role
            if objective is not _UNSET:
                p.objective = objective
            if capabilities is not None:
                p.capabilities = list(capabilities)
            if metadata is not None:
                p.metadata = dict(metadata)
            self.bump_activity()
            self._notify_others(
                exclude_pid=participant_id,
                name="participant.updated",
                payload={"participant": p.to_public().model_dump()},
            )
            return p.to_public()

    def _notify_others(
        self,
        *,
        exclude_pid: str,
        name: str,
        payload: dict[str, Any],
    ) -> None:
        for pid, p in self._participants.items():
            if pid == exclude_pid:
                continue
            if p.online:
                self._connections.send(self.session_id, pid, name, payload)
            # offline participants will be handled in Task 12 with the buffer
