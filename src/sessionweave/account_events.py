"""Append-only account event log with a long-poll friendly wake-up.

Events are stored in ``account_events`` and never carry token values.
``event_id`` is globally monotonic (AUTOINCREMENT) and ``ts`` grows with it,
so pruning by age only ever removes a prefix of the id space.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any

from sessionweave.persistence import PersistenceStore

_DAY_MS = 86_400_000


@dataclass(frozen=True)
class Origin:
    """Where a request came from: source IP and optional client label."""

    ip: str
    client: str | None


def _event(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "event_id": row[0],
        "account_id": row[1],
        "kind": row[2],
        "ts": row[3],
        "data": json.loads(row[4]),
    }


class EventLog:
    def __init__(self, store: PersistenceStore) -> None:
        self._store = store
        self._cond = asyncio.Condition()
        self._last_any = 0
        self._last_by_account: dict[str, int] = {}

    async def emit(self, account_id: str, kind: str, data: dict[str, Any]) -> int:
        conn = self._store._write_conn
        assert conn is not None
        async with self._store._write_tx_lock:
            cur = await conn.execute(
                "INSERT INTO account_events (account_id, kind, ts, data_json)"
                " VALUES (?, ?, ?, ?)",
                (account_id, kind, int(time.time() * 1000), json.dumps(data)),
            )
            event_id = cur.lastrowid
        assert event_id is not None
        async with self._cond:
            self._last_any = max(self._last_any, event_id)
            self._last_by_account[account_id] = max(
                self._last_by_account.get(account_id, 0), event_id
            )
            self._cond.notify_all()
        return event_id

    async def list_events(
        self, after: int, *, account_id: str | None, limit: int
    ) -> tuple[list[dict[str, Any]], int, bool]:
        """Events with ``event_id > after`` (oldest first), ``next`` cursor, ``truncated``.

        ``truncated`` means events of this scope with id > ``after`` were pruned
        before they could be read.
        """
        sql = (
            "SELECT event_id, account_id, kind, ts, data_json FROM account_events"
            " WHERE event_id > ?"
        )
        params: list[Any] = [after]
        if account_id is not None:
            sql += " AND account_id = ?"
            params.append(account_id)
        sql += " ORDER BY event_id LIMIT ?"
        params.append(limit)
        read = self._store._read_pool[0]
        async with read.execute(sql, params) as cur:
            events = [_event(tuple(row)) async for row in cur]
        qp: tuple[str, ...] = ()
        if account_id is None:
            q = "SELECT MAX(max_pruned_id) FROM account_event_prune"
        else:
            q = "SELECT max_pruned_id FROM account_event_prune WHERE account_id = ?"
            qp = (account_id,)
        async with read.execute(q, qp) as cur:
            row = await cur.fetchone()
        truncated = row is not None and row[0] is not None and row[0] > after
        nxt = events[-1]["event_id"] if events else after
        return events, nxt, truncated

    def _newer(self, after: int, account_id: str | None) -> bool:
        last = self._last_any if account_id is None else self._last_by_account.get(account_id, 0)
        return last > after

    async def wait_for(self, after: int, *, account_id: str | None, timeout: float) -> None:
        """Return once an event newer than ``after`` exists for the scope, or on timeout.

        The initial DB check covers events written before this process started
        (the in-memory last-id map only knows events emitted since startup).
        """
        events, _, _ = await self.list_events(after, account_id=account_id, limit=1)
        if events:
            return
        async with self._cond:
            try:
                await asyncio.wait_for(
                    self._cond.wait_for(lambda: self._newer(after, account_id)), timeout
                )
            except TimeoutError:
                return

    async def prune(self, now_ms: int, retention_days: int, *, batch: int = 1000) -> int:
        """Delete events older than the retention window, ``batch`` rows at a time.

        The write lock is released between batches. The highest pruned id per
        account is recorded first, in the same locked block as the delete.
        """
        conn = self._store._write_conn
        assert conn is not None
        cutoff = now_ms - retention_days * _DAY_MS
        total = 0
        while True:
            async with self._store._write_tx_lock:
                async with conn.execute(
                    "SELECT event_id, account_id FROM account_events WHERE ts < ?"
                    " ORDER BY event_id LIMIT ?",
                    (cutoff, batch),
                ) as cur:
                    rows = [tuple(r) async for r in cur]
                if not rows:
                    return total
                per_account: dict[str, int] = {}
                for event_id, acc in rows:
                    per_account[acc] = max(per_account.get(acc, 0), event_id)
                await conn.executemany(
                    "INSERT INTO account_event_prune (account_id, max_pruned_id) VALUES (?, ?)"
                    " ON CONFLICT(account_id) DO UPDATE SET"
                    " max_pruned_id = MAX(max_pruned_id, excluded.max_pruned_id)",
                    list(per_account.items()),
                )
                await conn.execute(
                    "DELETE FROM account_events WHERE event_id <= ? AND ts < ?",
                    (rows[-1][0], cutoff),
                )
                total += len(rows)
            if len(rows) < batch:
                return total
