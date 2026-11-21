from __future__ import annotations

import asyncio
import heapq
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sessionweave.access import SessionAuth
from sessionweave.config import DeploymentDefaults, apply_overrides
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.firehose import FirehoseRegistry
from sessionweave.metrics import Metrics
from sessionweave.session import MAX_PURPOSE_BYTES, Session

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from sessionweave.persistence import PersistenceStore
    from sessionweave.schemas import PersistenceConfig, PersistenceStatePublic

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
        "_background_tasks",
        "_connections",
        "_defaults",
        "_firehose",
        "_heap",
        "_hydrate_locks",
        "_lock",
        "_loop_task",
        "_metrics",
        "_sessions",
        "_store",
        "_wake",
    )

    def __init__(
        self,
        defaults: DeploymentDefaults,
        connections: ConnectionManager,
        metrics: Metrics | None = None,
        firehose: FirehoseRegistry | None = None,
        store: PersistenceStore | None = None,
    ) -> None:
        self._defaults = defaults
        self._sessions: dict[str, Session] = {}
        self._connections = connections
        self._lock = asyncio.Lock()
        self._loop_task: asyncio.Task[None] | None = None
        self._metrics = metrics
        self._firehose = firehose
        self._store = store
        # heap entries are (deadline_epoch_seconds, sid). A session may
        # appear multiple times after re-pushes; lazy invalidation drops
        # stale entries on pop.
        self._heap: list[tuple[float, str]] = []
        # Strong refs to fire-and-forget tasks so the event loop doesn't GC
        # them while they're still in flight (RUF006).
        self._background_tasks: set[asyncio.Task[None]] = set()
        # Set lazily once the housekeeping loop is started.
        self._wake: asyncio.Event | None = None
        # Per-sid locks to dedup concurrent hydrate calls for the same
        # cold session.
        self._hydrate_locks: dict[str, asyncio.Lock] = {}

    @property
    def connections(self) -> ConnectionManager:
        return self._connections

    async def create(
        self,
        overrides: dict[str, Any],
        purpose: dict[str, Any] | None = None,
        *,
        persistence: PersistenceConfig | None = None,
        creator_token_hash: bytes | None = None,
        relay_epoch: int = 0,
        owner_account_id: str | None = None,
    ) -> tuple[str, str, dict[str, int], dict[str, Any] | None, PersistenceStatePublic | None]:
        from sessionweave.schemas import PersistenceStatePublic

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
        # Validate persistence TTL bounds
        if persistence is not None and persistence.mode == "ttl":
            ttl = persistence.ttl_seconds
            lo = self._defaults.bounds.persistence_ttl_seconds_min
            hi = self._defaults.bounds.persistence_ttl_seconds_max
            if ttl is None or ttl < lo or ttl > hi:
                raise ToolError(
                    ErrorCode.INVALID_CONFIG,
                    f"persistence.ttl_seconds {ttl} outside bounds [{lo}, {hi}]",
                )
        async with self._lock:
            now = _utcnow()
            sid = str(uuid.uuid4())
            # Persist metadata BEFORE allocating in-RAM Session
            if persistence is not None and self._store is not None:
                from sessionweave.persistence import SessionMetadata
                purpose_json = json.dumps(purpose) if purpose is not None else None
                cfg_json = json.dumps(asdict(cfg))
                created_at_ms = int(now.timestamp() * 1000)
                meta = SessionMetadata(
                    session_id=sid,
                    config_json=cfg_json,
                    purpose_json=purpose_json,
                    creator_token_hash=creator_token_hash or b"",
                    persistence_mode=persistence.mode,
                    persistence_ttl_seconds=persistence.ttl_seconds,
                    created_at=created_at_ms,
                    last_activity_at=created_at_ms,
                    relay_epoch_at_create=relay_epoch,
                    owner_account_id=owner_account_id,
                )
                try:
                    await self._store.save_session_metadata(meta)
                except Exception as e:
                    raise ToolError(ErrorCode.PERSISTENCE_FAILED, str(e)) from e
            sess = Session(
                sid,
                cfg,
                self._connections,
                metrics=self._metrics,
                purpose=purpose,
                # Tell the session how to bump our heap when something
                # internal (a new inflight tracker, a participant going
                # offline) shortens its next_deadline. Without this the
                # heap top sits on the original idle_ttl entry and the
                # tick that should fire command.cancelled / command.completed
                # never runs until the session itself idles out.
                on_deadline_shortened=self._schedule,
                firehose=self._firehose,
                store=self._store if persistence is not None else None,
                persistence_mode=persistence.mode if persistence is not None else None,
            )
            sess.auth = SessionAuth(creator_token_hash, owner_account_id)
            self._sessions[sid] = sess
            self._schedule(sess)
            if self._metrics is not None:
                self._metrics.sessions_active.inc()
            # Compute persistence_state
            persistence_state: PersistenceStatePublic | None
            if persistence is None:
                persistence_state = None
            elif persistence.mode == "ttl":
                expires_at_iso = _isoformat(
                    now + timedelta(seconds=persistence.ttl_seconds)  # type: ignore[arg-type]
                )
                persistence_state = PersistenceStatePublic(
                    mode="ttl", expires_at=expires_at_iso
                )
            else:  # until_deleted
                persistence_state = PersistenceStatePublic(
                    mode="until_deleted", expires_at=None
                )
            return sid, _isoformat(sess.expires_at()), asdict(cfg), purpose, persistence_state

    async def get_or_raise(self, session_id: str) -> Session:
        sess = self._sessions.get(session_id)
        if sess is not None:
            return sess
        if self._store is None:
            raise ToolError(ErrorCode.SESSION_NOT_FOUND, "session not found")
        if not await self._store.exists(session_id):
            raise ToolError(ErrorCode.SESSION_NOT_FOUND, "session not found")
        lock = self._hydrate_locks.setdefault(session_id, asyncio.Lock())
        async with lock:
            sess = self._sessions.get(session_id)
            if sess is not None:
                return sess
            import time as _time
            t0 = _time.perf_counter()
            snap = await self._store.load_session(session_id)
            if snap is None:
                raise ToolError(ErrorCode.SESSION_NOT_FOUND, "session not found")
            if snap.ended_at is not None:
                raise ToolError(
                    ErrorCode.SESSION_NOT_FOUND,
                    f"session ended ({snap.ended_reason})"
                )
            # Reconstruct SessionConfig from snap.config_json
            # We use dataclasses.replace to merge the stored values with
            # the current defaults (for any keys added since the session
            # was created).  apply_overrides rejects non-int fields, and
            # SessionConfig now has str fields (persistence_db_path), so
            # we reconstruct directly instead.
            cfg_dict = json.loads(snap.config_json) if snap.config_json else {}
            if cfg_dict:
                from dataclasses import fields as _dc_fields
                from dataclasses import replace as _dc_replace

                from sessionweave.config import SessionConfig
                # Filter to known field names to tolerate removed fields.
                known = {f.name for f in _dc_fields(SessionConfig)}
                filtered = {k: v for k, v in cfg_dict.items() if k in known}
                cfg = _dc_replace(self._defaults.defaults, **filtered)
            else:
                cfg = self._defaults.defaults
            sess = Session.from_snapshot(
                snap, config=cfg,
                connections=self._connections,
                store=self._store,
                metrics=self._metrics,
                firehose=self._firehose,
                on_deadline_shortened=self._schedule,
            )
            self._sessions[session_id] = sess
            if self._metrics is not None:
                # Cold → hot: balance the dec() the eviction path issued
                # when this session left RAM. Without this inc(), every
                # evict+hydrate (or evict+delete, since delete hydrates
                # internally) net-decrements the gauge once, producing a
                # steady negative drift over time on relays with
                # persistence + idle eviction.
                self._metrics.sessions_active.inc()
                self._metrics.session_hydrate_total.inc()
                self._metrics.session_hydrate_seconds.observe(
                    _time.perf_counter() - t0
                )
            self._schedule(sess)
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
                if sess._persistence_mode is not None and self._store is not None:
                    # Hot → cold: keep DB row, kick subscribers
                    try:
                        last_activity_ms = int(sess._last_activity_at.timestamp() * 1000)
                        _t = asyncio.create_task(self._store.update_last_activity(
                            sid, last_activity_ms))
                        self._background_tasks.add(_t)
                        _t.add_done_callback(self._background_tasks.discard)
                    except Exception:
                        pass
                    sess.broadcast_to_subscribers("session.cold", {})
                    sess.close_all_subscribers()
                    sess.release_participant_metrics()
                    self._sessions.pop(sid, None)
                    if self._metrics is not None:
                        self._metrics.session_cold_total.inc()
                        self._metrics.sessions_active.dec()
                    continue
                # Ephemeral path (unchanged)
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

    def get(self, session_id: str) -> Session | None:
        """Return the session or ``None`` if not found."""
        return self._sessions.get(session_id)

    async def end_session(self, sid: str, reason: str) -> None:
        """Explicitly end a session, broadcasting ``session.ended`` to all participants.

        Mirrors the idle-TTL eviction path in ``tick()``.  If the session is
        not found this is a no-op (idempotent).
        """
        async with self._lock:
            sess = self._sessions.pop(sid, None)
        if sess is None:
            return
        sess.broadcast_session_ended(reason=reason)
        if self._metrics is not None:
            self._metrics.sessions_active.dec()
        if self._store is not None:
            try:
                await self._store.delete_session(sid)
            except Exception:
                pass

    async def delete(self, sid: str, *, reason: str) -> str:
        """Primitive shared by tool / HTTP delete entry points.

        Broadcasts session_ended, closes subscribers, cancels pending
        wait_for_event, store.delete_session (if persisted), removes
        from RAM. Returns ISO timestamp.
        """
        sess = await self.get_or_raise(sid)
        async with sess.lock:
            sess.broadcast_session_ended(reason=reason)
            sess.close_all_subscribers()
            # Cancel pending wait_for_event futures, if any
            try:
                sess.cancel_all_wait_for_event()
            except AttributeError:
                pass  # method may not exist in current Session API
        if self._store is not None:
            try:
                await self._store.delete_session(sid)
            except Exception:
                if self._metrics is not None:
                    self._metrics.persistence_failures_total.labels(
                        op="delete_session").inc()
                raise
        self._sessions.pop(sid, None)
        if self._metrics is not None:
            try:
                self._metrics.session_purge_total.labels(reason=reason).inc()
                self._metrics.sessions_active.dec()
            except Exception:
                pass
        return _isoformat(_utcnow())

    async def owned_session_ids(self, account_id: str) -> list[str]:
        """Live sessions owned by the account: hot in RAM plus persisted."""
        ids = [
            sid for sid, sess in self._sessions.items()
            if sess.auth.owner_account_id == account_id
        ]
        if self._store is not None:
            ids += [s for s in await self._store.find_owned(account_id) if s not in ids]
        return ids

    async def owned_session_summaries(self, account_id: str) -> list[dict[str, Any]]:
        """``{session_id, suspended, persisted, purpose}`` per live owned session.

        Hot sessions come from RAM; persisted sessions that are not in RAM
        (cold) are read from the store. No session appears twice.
        """
        out: list[dict[str, Any]] = []
        hot: set[str] = set()
        for sid, sess in list(self._sessions.items()):
            if sess.auth.owner_account_id == account_id:
                hot.add(sid)
                out.append({
                    "session_id": sid,
                    "suspended": sess.suspended,
                    "persisted": sess.persisted,
                    "purpose": sess.purpose,
                })
        if self._store is not None:
            for sid, suspended, purpose_json in await self._store.owned_summaries(account_id):
                if sid not in hot:
                    try:
                        purpose = json.loads(purpose_json) if purpose_json else None
                    except ValueError:
                        purpose = None  # corrupt stored purpose: list the session anyway
                    out.append({
                        "session_id": sid,
                        "suspended": suspended,
                        "persisted": True,
                        "purpose": purpose,
                    })
        return out

    async def _for_each_owned(
        self, account_id: str, what: str, action: Callable[[str], Awaitable[None]]
    ) -> int:
        """Run ``action`` on each owned session; one failure never aborts the rest.

        A session that vanished meanwhile counts as done. Other failures are
        logged and, after every session was tried, raised as one error.
        """
        sids = await self.owned_session_ids(account_id)
        done = failed = 0
        for sid in sids:
            try:
                await action(sid)
            except ToolError as e:
                if e.code != ErrorCode.SESSION_NOT_FOUND:
                    logger.exception("%s failed for session %s", what, sid)
                    failed += 1
                    continue
            except Exception:
                logger.exception("%s failed for session %s", what, sid)
                failed += 1
                continue
            done += 1
        if failed:
            raise ToolError(
                ErrorCode.PERSISTENCE_FAILED,
                f"{what} failed for {failed} of {len(sids)} sessions",
            )
        return done

    async def suspend_owned(self, account_id: str) -> int:
        async def act(sid: str) -> None:
            await (await self.get_or_raise(sid)).suspend()

        return await self._for_each_owned(account_id, "suspend", act)

    async def resume_owned(self, account_id: str) -> int:
        async def act(sid: str) -> None:
            await (await self.get_or_raise(sid)).unsuspend()

        return await self._for_each_owned(account_id, "resume", act)

    async def delete_owned(self, account_id: str) -> int:
        """Delete every session the account owns, ended ones included."""
        async def act(sid: str) -> None:
            await self.delete(sid, reason="account_deleted")

        removed = await self._for_each_owned(account_id, "delete", act)
        if self._store is not None:
            for sid in await self._store.find_owned_all(account_id):
                await self._store.delete_session(sid)
                removed += 1
        return removed

    async def sweep_expired_once(self, *, now_ms: int, safety_seconds: int) -> int:
        """One pass over the store: purge sessions whose persistence has expired.

        Returns the number of sessions actually purged. Errors on
        individual sessions are isolated; the loop continues for the rest.
        """
        if self._store is None:
            return 0
        try:
            sids = await self._store.find_expired_sessions(
                now_ms=now_ms, safety_seconds=safety_seconds,
            )
        except Exception:
            if self._metrics is not None:
                self._metrics.persistence_failures_total.labels(op="sweep").inc()
            return 0
        purged = 0
        for sid in sids:
            try:
                sess = self._sessions.get(sid)
                if sess is not None:
                    async with sess.lock:
                        sess.broadcast_session_ended(reason="persistence_expired")
                        sess.close_all_subscribers()
                    self._sessions.pop(sid, None)
                    if self._metrics is not None:
                        self._metrics.sessions_active.dec()
                await self._store.delete_session(sid)
                if self._metrics is not None:
                    self._metrics.session_purge_total.labels(
                        reason="persistence_expired"
                    ).inc()
                purged += 1
            except Exception:
                if self._metrics is not None:
                    self._metrics.persistence_failures_total.labels(op="sweep").inc()
        return purged

    async def sweep_expired_loop(self, *, interval_seconds: int,
                                  safety_seconds: int,
                                  on_tick: Callable[[int], Awaitable[object]] | None = None,
                                  ) -> None:
        """Periodic loop scheduled at server startup; runs until cancelled."""
        import time as _time
        while True:
            try:
                await self.sweep_expired_once(
                    now_ms=int(_time.time() * 1000),
                    safety_seconds=safety_seconds,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            if on_tick is not None:
                try:
                    await on_tick(int(_time.time() * 1000))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("periodic tick failed: %s", type(exc).__name__)
            try:
                await asyncio.sleep(interval_seconds)
            except asyncio.CancelledError:
                return

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
