from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.dispatcher import RoundRobinPicker
from sessionweave.models import CommandTracker, Participant
from sessionweave.schemas import ParticipantPublic


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
