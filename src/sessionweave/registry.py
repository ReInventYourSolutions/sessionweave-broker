from __future__ import annotations

import asyncio
import uuid
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from sessionweave.config import DeploymentDefaults, apply_overrides
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.session import Session


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _isoformat(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


class SessionRegistry:
    """Owns the session map and runs the idle-TTL housekeeping loop."""

    __slots__ = ("_connections", "_defaults", "_lock", "_loop_task", "_sessions")

    def __init__(
        self,
        defaults: DeploymentDefaults,
        connections: ConnectionManager,
    ) -> None:
        self._defaults = defaults
        self._sessions: dict[str, Session] = {}
        self._connections = connections
        self._lock = asyncio.Lock()
        self._loop_task: asyncio.Task[None] | None = None

    @property
    def connections(self) -> ConnectionManager:
        return self._connections

    async def create(
        self, overrides: dict[str, Any]
    ) -> tuple[str, str, dict[str, int]]:
        cfg = apply_overrides(
            self._defaults.defaults, self._defaults.bounds, overrides
        )
        async with self._lock:
            sid = str(uuid.uuid4())
            sess = Session(sid, cfg, self._connections)
            self._sessions[sid] = sess
            return sid, _isoformat(sess.expires_at()), asdict(cfg)

    def get_or_raise(self, session_id: str) -> Session:
        sess = self._sessions.get(session_id)
        if sess is None:
            raise ToolError(ErrorCode.SESSION_NOT_FOUND, "session not found")
        return sess

    def tick(self, *, now: datetime | None = None) -> None:
        moment = now or _utcnow()
        expired: list[str] = []
        for sid, sess in self._sessions.items():
            if sess.is_idle_expired(at=moment):
                expired.append(sid)
            else:
                sess.tick(now=moment)
        for sid in expired:
            sess = self._sessions.pop(sid)
            sess.broadcast_session_ended(reason="idle_ttl")

    async def start(self, *, interval_seconds: float = 1.0) -> None:
        async def _runner() -> None:
            try:
                while True:
                    await asyncio.sleep(interval_seconds)
                    self.tick()
            except asyncio.CancelledError:
                pass

        self._loop_task = asyncio.create_task(_runner(), name="sessionweave.registry.tick")

    async def shutdown(self, reason: str = "server_shutdown") -> None:
        if self._loop_task is not None:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        for sess in list(self._sessions.values()):
            sess.broadcast_session_ended(reason=reason)
        self._sessions.clear()
