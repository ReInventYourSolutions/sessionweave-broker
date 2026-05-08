from __future__ import annotations

import asyncio
import heapq
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

# Cap on how long the housekeeping loop will sleep before re-checking
# the heap. The min-heap normally drives precise wake-ups, but
# in-session events (a new command, a participant going offline) can
# pull a deadline forward without the heap entry being updated. Capping
# the sleep keeps that lateness bounded without forcing a per-event
# heap re-push.
_MAX_SLEEP_SECONDS = 1.0


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _isoformat(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


class SessionRegistry:
    """Owns the session map and runs the idle-TTL housekeeping loop.

    Eviction is driven by a min-heap of ``(deadline_epoch, sid)`` entries
    so the loop only inspects sessions whose next-event-time is due,
    instead of scanning the full session map every tick. The heap is
    lazily-invalidated: stale entries (sid no longer present, or the
    session's actual deadline moved later) are skipped on pop and the
    correct deadline is re-inserted.
    """

    __slots__ = (
        "_connections",
        "_defaults",
        "_heap",
        "_lock",
        "_loop_task",
        "_metrics",
        "_sessions",
        "_wake",
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
        # heap entries are (deadline_epoch_seconds, sid). A session may
        # appear multiple times after re-pushes; lazy invalidation drops
        # stale entries on pop.
        self._heap: list[tuple[float, str]] = []
        # Set lazily once the housekeeping loop is started.
        self._wake: asyncio.Event | None = None

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
            self._schedule(sess)
            if self._metrics is not None:
                self._metrics.sessions_active.inc()
            return sid, _isoformat(sess.expires_at()), asdict(cfg), purpose

    def get_or_raise(self, session_id: str) -> Session:
        sess = self._sessions.get(session_id)
        if sess is None:
            raise ToolError(ErrorCode.SESSION_NOT_FOUND, "session not found")
        return sess

    def _schedule(self, sess: Session) -> None:
        """Push the session's current next-deadline onto the heap and
        wake the loop so it can recompute its sleep target."""
        deadline = sess.next_deadline().timestamp()
        heapq.heappush(self._heap, (deadline, sess.session_id))
        if self._wake is not None:
            self._wake.set()

    def tick(self, *, now: datetime | None = None) -> None:
        """Process every heap entry whose deadline has come due.

        Pops from the heap while the top entry is past, skipping stale
        ones (sid removed, session actually due later) and re-pushing
        the freshly-computed deadline for sessions that are still alive
        after the per-session ``Session.tick`` runs.

        Before draining the heap we run a cheap presence sweep on every
        session to catch handles that closed without an explicit
        ``leave``. Marking a participant offline shortens
        ``next_deadline`` to the offline-grace deadline, so we re-push
        the affected session so the heap path can pick it up promptly.
        """
        moment = now or _utcnow()
        moment_epoch = moment.timestamp()
        for sid, sess in list(self._sessions.items()):
            if sess.detect_disconnects():
                heapq.heappush(self._heap, (sess.next_deadline().timestamp(), sid))
        while self._heap and self._heap[0][0] <= moment_epoch:
            _deadline, sid = heapq.heappop(self._heap)
            sess_opt = self._sessions.get(sid)
            if sess_opt is None:
                # Session already evicted/ended — stale entry, drop.
                continue
            sess = sess_opt
            real_deadline = sess.next_deadline().timestamp()
            if real_deadline > moment_epoch:
                # Deadline shifted later (e.g. activity bumped expiry).
                # Re-push and stop — heap is sorted, anything later is also not due.
                heapq.heappush(self._heap, (real_deadline, sid))
                continue
            if sess.is_idle_expired(at=moment):
                self._sessions.pop(sid, None)
                sess.broadcast_session_ended(reason="idle_ttl")
                if self._metrics is not None:
                    self._metrics.sessions_active.dec()
                continue
            sess.tick(now=moment)
            # Compute the post-tick deadline. The session might now be
            # idle-expired on the next tick, or have new in-flight TTLs;
            # push whichever comes first.
            heapq.heappush(self._heap, (sess.next_deadline().timestamp(), sid))

    def _next_sleep_seconds(self, *, now: datetime | None = None) -> float:
        """How long the loop should sleep before the next ``tick``.

        Capped at ``_MAX_SLEEP_SECONDS`` so an in-session event that
        shortened a deadline (without re-pushing) is picked up within
        one tick interval at worst.
        """
        if not self._heap:
            return _MAX_SLEEP_SECONDS
        delta = self._heap[0][0] - (now or _utcnow()).timestamp()
        if delta <= 0:
            return 0.0
        return min(delta, _MAX_SLEEP_SECONDS)

    async def start(self, *, interval_seconds: float | None = None) -> None:
        """Launch the housekeeping loop.

        ``interval_seconds`` is honoured as an *upper* bound on how
        long the loop will sleep — present so callers (and tests) can
        force a faster cadence. The default is ``_MAX_SLEEP_SECONDS``
        which matches the legacy 1 Hz behaviour.
        """
        cap = interval_seconds if interval_seconds is not None else _MAX_SLEEP_SECONDS
        self._wake = asyncio.Event()

        async def _runner() -> None:
            assert self._wake is not None
            try:
                while True:
                    sleep_for = min(self._next_sleep_seconds(), cap)
                    if sleep_for > 0:
                        try:
                            await asyncio.wait_for(self._wake.wait(), timeout=sleep_for)
                        except TimeoutError:
                            pass
                    self._wake.clear()
                    self.tick()
            except asyncio.CancelledError:
                pass

        self._loop_task = asyncio.create_task(
            _runner(), name="sessionweave.registry.tick"
        )

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
        self._heap.clear()
