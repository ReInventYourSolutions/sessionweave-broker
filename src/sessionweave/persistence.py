"""SQLite-backed persistence store for sessions.

Single writer connection + a read pool. WAL mode for concurrent reads
during writes. Schema migrations are plain SQL files in
``persistence_migrations/``. All public methods are async.
"""

from __future__ import annotations

import asyncio
import importlib.resources as ir
import time
from dataclasses import dataclass
from typing import Any

import aiosqlite


@dataclass(slots=True, frozen=True)
class SessionMetadata:
    session_id: str
    config_json: str
    purpose_json: str | None
    creator_token_hash: bytes
    persistence_mode: str  # 'ttl' | 'until_deleted'
    persistence_ttl_seconds: int | None
    created_at: int  # unix ms
    last_activity_at: int  # unix ms
    relay_epoch_at_create: int


@dataclass(slots=True, frozen=True)
class EventsPage:
    events: list[dict[str, Any]]
    next_cursor: int | None
    has_more: bool


@dataclass(slots=True, frozen=True)
class SessionSnapshot:
    session_id: str
    config_json: str
    purpose_json: str | None
    creator_token_hash: bytes
    persistence_mode: str
    persistence_ttl_seconds: int | None
    created_at: int
    last_activity_at: int
    ended_at: int | None
    ended_reason: str | None
    relay_epoch_at_create: int
    participants: list[dict[str, Any]]
    inflight: dict[str, dict[str, Any]]
    buffered: list[dict[str, Any]]
    # Highest observed seq in `events` for this session. Used by
    # ``Session.from_snapshot`` to resume ``_event_seq`` so the next
    # event dispatched after hydrate doesn't collide with an existing
    # row (which would raise SQLITE_CONSTRAINT on the (session_id, seq)
    # PK). For sessions that never recorded any event this is 0.
    max_event_seq: int


class PersistenceStore:
    def __init__(self, path: str) -> None:
        self._path = path
        self._write_conn: aiosqlite.Connection | None = None
        self._read_pool: list[aiosqlite.Connection] = []
        # SQLite refuses nested transactions on a single connection: a
        # second BEGIN while the first transaction is still open raises
        # ``sqlite3.OperationalError: cannot start a transaction within
        # a transaction``. Because every relay write goes through the
        # one ``_write_conn``, two coroutines that call
        # ``persist_event_atomic`` (or one ``persist_event_atomic``
        # racing ``delete_session``) at the same time trigger exactly
        # that error. This lock serialises the BEGIN/COMMIT/ROLLBACK
        # sequences; non-transactional single-statement writes
        # (autocommit because we opened the connection with
        # ``isolation_level=None``) don't take the lock and run in
        # whatever interleaving aiosqlite's worker thread allows.
        self._write_tx_lock: asyncio.Lock = asyncio.Lock()

    async def init(self, pool_size: int = 4) -> None:
        self._write_conn = await aiosqlite.connect(self._path, isolation_level=None)
        await self._apply_pragmas(self._write_conn)
        await self._apply_migrations(self._write_conn)
        for _ in range(pool_size):
            conn = await aiosqlite.connect(self._path)
            await self._apply_pragmas(conn)
            self._read_pool.append(conn)

    async def close(self) -> None:
        if self._write_conn is not None:
            try:
                await self._write_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except aiosqlite.Error:
                pass
            await self._write_conn.close()
            self._write_conn = None
        for conn in self._read_pool:
            await conn.close()
        self._read_pool = []

    async def _apply_pragmas(self, conn: aiosqlite.Connection) -> None:
        await conn.execute("PRAGMA journal_mode = WAL")
        await conn.execute("PRAGMA synchronous = NORMAL")
        await conn.execute("PRAGMA temp_store = MEMORY")
        await conn.execute("PRAGMA mmap_size = 268435456")
        await conn.execute("PRAGMA foreign_keys = ON")

    async def _apply_migrations(self, conn: aiosqlite.Connection) -> None:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS _migrations ("
            "  version    INTEGER PRIMARY KEY,"
            "  applied_at INTEGER NOT NULL"
            ") STRICT"
        )
        async with conn.execute("SELECT version FROM _migrations") as cur:
            applied = {row[0] async for row in cur}

        pkg = ir.files("sessionweave.persistence_migrations")
        files = sorted(
            f.name for f in pkg.iterdir()
            if f.name.endswith(".sql") and f.name[0].isdigit()
        )
        for fname in files:
            version = int(fname.split("_", 1)[0])
            if version in applied:
                continue
            sql = (pkg / fname).read_text(encoding="utf-8")
            await conn.executescript(sql)
            await conn.execute(
                "INSERT INTO _migrations(version, applied_at) VALUES (?, ?)",
                (version, int(time.time() * 1000)),
            )

    # ----- sessions table CRUD -----

    async def save_session_metadata(self, meta: SessionMetadata) -> None:
        assert self._write_conn is not None
        await self._write_conn.execute(
            "INSERT INTO sessions ("
            " session_id, config_json, purpose_json, creator_token_hash,"
            " persistence_mode, persistence_ttl_seconds,"
            " created_at, last_activity_at,"
            " ended_at, ended_reason, relay_epoch_at_create"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)",
            (
                meta.session_id, meta.config_json, meta.purpose_json,
                meta.creator_token_hash, meta.persistence_mode,
                meta.persistence_ttl_seconds,
                meta.created_at, meta.last_activity_at,
                meta.relay_epoch_at_create,
            ),
        )

    async def exists(self, sid: str) -> bool:
        if not self._read_pool:
            return False
        conn = self._read_pool[0]
        async with conn.execute(
            "SELECT 1 FROM sessions WHERE session_id = ? LIMIT 1", (sid,)
        ) as cur:
            row = await cur.fetchone()
            return row is not None

    async def mark_session_ended(self, sid: str, reason: str, ended_at: int) -> None:
        assert self._write_conn is not None
        await self._write_conn.execute(
            "UPDATE sessions SET ended_at = ?, ended_reason = ? WHERE session_id = ?",
            (ended_at, reason, sid),
        )

    async def delete_session(self, sid: str) -> None:
        assert self._write_conn is not None
        async with self._write_tx_lock:
            await self._write_conn.execute("BEGIN")
            try:
                await self._write_conn.execute(
                    "DELETE FROM buffered_notifications WHERE session_id = ?", (sid,))
                await self._write_conn.execute(
                    "DELETE FROM inflight_commands WHERE session_id = ?", (sid,))
                await self._write_conn.execute(
                    "DELETE FROM participants WHERE session_id = ?", (sid,))
                await self._write_conn.execute(
                    "DELETE FROM events WHERE session_id = ?", (sid,))
                await self._write_conn.execute(
                    "DELETE FROM sessions WHERE session_id = ?", (sid,))
                await self._write_conn.execute("COMMIT")
            except Exception:
                await self._write_conn.execute("ROLLBACK")
                raise

    async def update_last_activity(self, sid: str, last_activity_at: int) -> None:
        assert self._write_conn is not None
        await self._write_conn.execute(
            "UPDATE sessions SET last_activity_at = ? WHERE session_id = ?",
            (last_activity_at, sid),
        )

    # ----- events table -----

    async def persist_event_atomic(
        self, *, sid: str, seq: int, kind: str, payload: dict[str, Any], ts: int,
        inflight_upsert: tuple[str, dict[str, Any]] | None = None,
        inflight_complete: str | None = None,
        buffered_pids: tuple[str, ...] | list[str] = (),
    ) -> None:
        import json as _json
        assert self._write_conn is not None
        async with self._write_tx_lock:
            await self._write_conn.execute("BEGIN")
            try:
                await self._write_conn.execute(
                    "INSERT INTO events (session_id, seq, kind, payload_json, ts)"
                    " VALUES (?, ?, ?, ?, ?)",
                    (sid, seq, kind, _json.dumps(payload, separators=(",", ":")), ts),
                )
                if inflight_upsert is not None:
                    cid, tracker = inflight_upsert
                    await self._write_conn.execute(
                        "INSERT INTO inflight_commands"
                        " (session_id, command_id, tracker_json, created_at)"
                        " VALUES (?, ?, ?, ?)"
                        " ON CONFLICT(session_id, command_id)"
                        " DO UPDATE SET tracker_json = excluded.tracker_json",
                        (sid, cid, _json.dumps(tracker, separators=(",", ":")), ts),
                    )
                if inflight_complete is not None:
                    await self._write_conn.execute(
                        "DELETE FROM inflight_commands WHERE session_id = ? AND command_id = ?",
                        (sid, inflight_complete),
                    )
                for pid in buffered_pids:
                    await self._write_conn.execute(
                        "INSERT OR IGNORE INTO buffered_notifications"
                        " (session_id, participant_id, seq) VALUES (?, ?, ?)",
                        (sid, pid, seq),
                    )
                await self._write_conn.execute(
                    "UPDATE sessions SET last_activity_at = ? WHERE session_id = ?",
                    (ts, sid),
                )
                await self._write_conn.execute("COMMIT")
            except Exception:
                await self._write_conn.execute("ROLLBACK")
                raise

    async def lookup_seq_by_command_id(self, sid: str, command_id: str) -> int | None:
        if not self._read_pool:
            return None
        async with self._read_pool[0].execute(
            "SELECT seq FROM events WHERE session_id = ?"
            " AND json_extract(payload_json, '$.command_id') = ?"
            " AND kind = 'command.received' LIMIT 1",
            (sid, command_id),
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else None

    async def fetch_events(
        self, *, sid: str,
        after_seq: int | None = None,
        before_seq: int | None = None,
        kinds: tuple[str, ...] | list[str] | None = None,
        participant_ids: tuple[str, ...] | list[str] | None = None,
        limit: int = 200, order: str = "asc",
    ) -> EventsPage:
        import json as _json
        clauses = ["session_id = ?"]
        params: list[Any] = [sid]
        if after_seq is not None:
            clauses.append("seq > ?")
            params.append(after_seq)
        if before_seq is not None:
            clauses.append("seq < ?")
            params.append(before_seq)
        if kinds:
            placeholders = ",".join("?" for _ in kinds)
            clauses.append(f"kind IN ({placeholders})")
            params.extend(kinds)
        order_sql = "ASC" if order == "asc" else "DESC"
        query = (
            f"SELECT seq, kind, payload_json, ts FROM events"
            f" WHERE {' AND '.join(clauses)} ORDER BY seq {order_sql} LIMIT ?"
        )
        params.append(limit + 1)
        conn = self._read_pool[0] if self._read_pool else self._write_conn
        assert conn is not None
        events: list[dict[str, Any]] = []
        async with conn.execute(query, tuple(params)) as cur:
            async for seq, kind, payload_json, ts in cur:
                events.append({
                    "seq": seq, "kind": kind, "ts": ts,
                    "payload": _json.loads(payload_json),
                })
        has_more = len(events) > limit
        if has_more:
            events = events[:limit]
        if participant_ids:
            allowed = set(participant_ids)
            def _matches(e: dict[str, Any]) -> bool:
                p = e["payload"]
                if p.get("sender_id") in allowed:
                    return True
                recipients = p.get("recipients") or p.get("responders") or []
                return any(r in allowed for r in recipients)
            events = [e for e in events if _matches(e)]
        next_cursor = events[-1]["seq"] if events else None
        return EventsPage(events=events, next_cursor=next_cursor, has_more=has_more)

    # ----- participants table -----

    async def upsert_participant(self, sid: str, pid: str, snapshot: dict[str, Any],
                                  last_seen_seq: int, joined_at: int) -> None:
        import json as _json
        assert self._write_conn is not None
        await self._write_conn.execute(
            "INSERT INTO participants (session_id, participant_id, snapshot_json,"
            " last_seen_seq, joined_at, left_at) VALUES (?, ?, ?, ?, ?, NULL)"
            " ON CONFLICT(session_id, participant_id) DO UPDATE SET"
            " snapshot_json = excluded.snapshot_json,"
            " last_seen_seq = excluded.last_seen_seq,"
            " left_at = NULL",
            (sid, pid, _json.dumps(snapshot, separators=(",", ":")),
             last_seen_seq, joined_at),
        )

    async def soft_delete_participant(self, sid: str, pid: str, left_at: int) -> None:
        assert self._write_conn is not None
        await self._write_conn.execute(
            "UPDATE participants SET left_at = ? WHERE session_id = ? AND participant_id = ?",
            (left_at, sid, pid),
        )

    async def ack_participant(self, sid: str, pid: str, last_seen_seq: int) -> None:
        assert self._write_conn is not None
        await self._write_conn.execute(
            "UPDATE participants SET last_seen_seq = ?"
            " WHERE session_id = ? AND participant_id = ?",
            (last_seen_seq, sid, pid),
        )

    async def update_session_purpose(self, sid: str, purpose_json: str | None) -> None:
        assert self._write_conn is not None
        await self._write_conn.execute(
            "UPDATE sessions SET purpose_json = ? WHERE session_id = ?",
            (purpose_json, sid),
        )

    async def remove_buffered_through(self, sid: str, pid: str,
                                       max_seq_inclusive: int) -> int:
        assert self._write_conn is not None
        cur = await self._write_conn.execute(
            "DELETE FROM buffered_notifications WHERE session_id = ?"
            " AND participant_id = ? AND seq <= ?",
            (sid, pid, max_seq_inclusive),
        )
        return cur.rowcount or 0

    # ----- load_session -----

    async def load_session(self, sid: str) -> SessionSnapshot | None:
        import json as _json
        if not self._read_pool:
            return None
        conn = self._read_pool[0]
        async with conn.execute(
            "SELECT config_json, purpose_json, creator_token_hash, persistence_mode,"
            " persistence_ttl_seconds, created_at, last_activity_at, ended_at,"
            " ended_reason, relay_epoch_at_create FROM sessions WHERE session_id = ?",
            (sid,),
        ) as cur:
            srow = await cur.fetchone()
        if srow is None:
            return None
        participants: list[dict[str, Any]] = []
        async with conn.execute(
            "SELECT participant_id, snapshot_json, last_seen_seq, joined_at"
            " FROM participants WHERE session_id = ? AND left_at IS NULL",
            (sid,),
        ) as cur:
            async for pid, snap_json, last_seen_seq, joined_at in cur:
                participants.append({
                    "participant_id": pid,
                    "snapshot": _json.loads(snap_json),
                    "last_seen_seq": last_seen_seq,
                    "joined_at": joined_at,
                })
        inflight: dict[str, dict[str, Any]] = {}
        async with conn.execute(
            "SELECT command_id, tracker_json FROM inflight_commands WHERE session_id = ?",
            (sid,),
        ) as cur:
            async for cid, tj in cur:
                inflight[cid] = _json.loads(tj)
        buffered: list[dict[str, Any]] = []
        async with conn.execute(
            "SELECT b.participant_id, b.seq, e.kind, e.payload_json"
            " FROM buffered_notifications b"
            " JOIN events e ON e.session_id = b.session_id AND e.seq = b.seq"
            " WHERE b.session_id = ? ORDER BY b.participant_id, b.seq",
            (sid,),
        ) as cur:
            async for pid, seq, kind, payload_json in cur:
                buffered.append({
                    "participant_id": pid, "seq": seq, "kind": kind,
                    "payload": _json.loads(payload_json),
                })
        # Highest seq in `events` for this session — used by
        # ``Session.from_snapshot`` to resume ``_event_seq`` even when
        # ``buffered`` is empty (push-only sessions that went cold with
        # all subscribers caught up still have rows in `events`).
        async with conn.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM events WHERE session_id = ?",
            (sid,),
        ) as cur:
            _max_row = await cur.fetchone()
            assert _max_row is not None
            (max_event_seq,) = _max_row
        return SessionSnapshot(
            session_id=sid,
            config_json=srow[0], purpose_json=srow[1],
            creator_token_hash=srow[2], persistence_mode=srow[3],
            persistence_ttl_seconds=srow[4],
            created_at=srow[5], last_activity_at=srow[6],
            ended_at=srow[7], ended_reason=srow[8],
            relay_epoch_at_create=srow[9],
            participants=participants, inflight=inflight, buffered=buffered,
            max_event_seq=int(max_event_seq or 0),
        )

    # ----- sweep helpers -----

    async def find_expired_sessions(self, *, now_ms: int, safety_seconds: int,
                                     batch: int = 1000) -> list[str]:
        if not self._read_pool:
            return []
        sids: list[str] = []
        async with self._read_pool[0].execute(
            "SELECT session_id FROM sessions"
            " WHERE ended_at IS NULL AND ("
            "  (persistence_mode = 'ttl'"
            "   AND last_activity_at + persistence_ttl_seconds * 1000 < ?)"
            "  OR (persistence_mode = 'until_deleted'"
            "   AND last_activity_at + ? * 1000 < ?)"
            " ) LIMIT ?",
            (now_ms, safety_seconds, now_ms, batch),
        ) as cur:
            async for (sid,) in cur:
                sids.append(sid)
        return sids

    async def find_stranded(self, *, now_ms: int, idle_threshold_hours: int = 24,
                             batch: int = 1000) -> list[str]:
        if not self._read_pool:
            return []
        threshold_ms = idle_threshold_hours * 3600 * 1000
        sids: list[str] = []
        async with self._read_pool[0].execute(
            "SELECT session_id FROM sessions"
            " WHERE ended_at IS NULL AND last_activity_at < ? LIMIT ?",
            (now_ms - threshold_ms, batch),
        ) as cur:
            async for (sid,) in cur:
                sids.append(sid)
        return sids
