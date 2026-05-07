from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from sessionweave.config import DeploymentDefaults, apply_overrides
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.metrics import Metrics
from sessionweave.session import MAX_PURPOSE_BYTES, Session

_MAX_PURPOSE_BYTES = MAX_PURPOSE_BYTES


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _isoformat(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


class SessionRegistry:
    """Owns the session map and runs the idle-TTL housekeeping loop."""

    __slots__ = (
        "_connections",
        "_defaults",
        "_lock",
        "_loop_task",
        "_metrics",
        "_sessions",
    )

    def __init__(
        self,
        defaults: DeploymentDefaults,
        connections: ConnectionManager,
        metrics: Metrics | None = None,
    ) -> None:
        self._defaults = defaults
        self._sessions: dict[str, Session] = {}
        self._connections = connections
        self._lock = asyncio.Lock()
        self._loop_task: asyncio.Task[None] | None = None
        self._metrics = metrics

    @property
    def connections(self) -> ConnectionManager:
        return self._connections

    async def create(
        self,
        overrides: dict[str, Any],
        purpose: dict[str, Any] | None = None,
    ) -> tuple[str, str, dict[str, int], dict[str, Any] | None]:
        cfg = apply_overrides(
            self._defaults.defaults, self._defaults.bounds, overrides
        )
        if purpose is not None:
            size = len(json.dumps(purpose, separators=(",", ":")).encode("utf-8"))
            if size > _MAX_PURPOSE_BYTES:
                raise ToolError(
                    ErrorCode.INVALID_CONFIG,
                    f"purpose {size} bytes exceeds cap of {_MAX_PURPOSE_BYTES}",
                )
        async with self._lock:
            sid = str(uuid.uuid4())
            sess = Session(
                sid,
                cfg,
                self._connections,
                metrics=self._metrics,
                purpose=purpose,
            )
            self._sessions[sid] = sess
            if self._metrics is not None:
                self._metrics.sessions_active.inc()
            return sid, _isoformat(sess.expires_at()), asdict(cfg), purpose

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
            if self._metrics is not None:
                self._metrics.sessions_active.dec()

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
        ended = list(self._sessions.values())
        for sess in ended:
            sess.broadcast_session_ended(reason=reason)
        if self._metrics is not None and ended:
            for _ in ended:
                self._metrics.sessions_active.dec()
        self._sessions.clear()
