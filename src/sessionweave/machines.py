"""Machines: the persisted descriptor of each protocol-v2 launcher.

A machine is identified by ``(account_id, launcher_key)``; ``account_id`` is
the owner account, or ``OPEN_OWNER`` (``""``) for open-mode launchers. The
row survives the launcher's disconnects and broker restarts, so the Cockpit
can list offline machines and the broker can queue definitions for them
(``agent_definitions``, same key). Nothing a launcher produces is stored here.
"""
from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from sessionweave.access import hash_token
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.launchers import (
    MAX_MACHINES_OPEN,
    MAX_MACHINES_PER_ACCOUNT,
    OPEN_OWNER,
    HelloV2,
    LauncherKeyInUseError,
    key_secret_matches,
    new_key_secret,
)
from sessionweave.persistence import PersistenceStore

_COLUMNS = (
    "account_id, launcher_key, machine, mode, ceiling, capabilities_json, sandbox,"
    " run_as, platform, prereqs_json, first_seen, last_seen"
)


_DEF_COLUMNS = (
    "def_id, revision, definition_json, state, applied_revision, problems_json,"
    " warnings_json, deleted, updated_at"
)
_T = TypeVar("_T")
# Everything stored per machine, deleted together when it is forgotten.
_MACHINE_TABLES = (
    "agent_definitions", "agent_definition_revisions", "launcher_machines",
)


class MachineLimitError(Exception):
    """The owner already has the maximum number of machines."""


@dataclass
class Machine:
    account_id: str
    launcher_key: str
    machine: str
    mode: str
    ceiling: str | None
    capabilities: list[str]
    sandbox: str | None
    run_as: bool
    platform: str
    prereqs: list[dict[str, Any]]
    first_seen: int
    last_seen: int


@dataclass
class StoredDefinition:
    """A deployed definition (desired state) and the launcher's last status.

    ``revision`` grows with every put and delete; ``state`` is one of
    queued, sent, pending_in_use, applied, rejected, removing;
    ``applied_revision`` is the last revision the launcher applied;
    ``deleted`` marks a removal waiting for the launcher's ``removed``."""

    def_id: str
    revision: int
    definition: dict[str, Any]
    state: str
    applied_revision: int | None
    problems: list[str]
    warnings: list[str]
    deleted: bool
    updated_at: int

    def deploy(self) -> dict[str, Any]:
        """The ``deploy`` object of the API's Machine agents."""
        return {
            "revision": self.revision, "applied_revision": self.applied_revision,
            "state": self.state, "problems": list(self.problems),
            "warnings": list(self.warnings),
        }


def _def_row(r: Any) -> StoredDefinition:
    return StoredDefinition(
        def_id=r[0], revision=r[1], definition=json.loads(r[2]), state=r[3],
        applied_revision=r[4], problems=list(json.loads(r[5])),
        warnings=list(json.loads(r[6])), deleted=bool(r[7]), updated_at=r[8],
    )


def _row(r: Any) -> Machine:
    return Machine(
        account_id=r[0], launcher_key=r[1], machine=r[2], mode=r[3], ceiling=r[4],
        capabilities=list(json.loads(r[5])), sandbox=r[6], run_as=bool(r[7]),
        platform=r[8], prereqs=list(json.loads(r[9])), first_seen=r[10], last_seen=r[11],
    )


class MachineStore:
    def __init__(self, store: PersistenceStore) -> None:
        self._store = store

    def _write(self) -> Any:
        assert self._store._write_conn is not None
        return self._store._write_conn

    def _read(self) -> Any:
        return self._store._read_pool[0]

    async def upsert_machine(
        self, account_id: str, machine: str, hello: HelloV2, now_ms: int,
        *, key_secret: str | None = None, token_id: str | None = None,
    ) -> Machine:
        """``claim_machine`` without the issued secret (see there)."""
        m, _issued = await self.claim_machine(
            account_id, machine, hello, now_ms, key_secret=key_secret, token_id=token_id
        )
        return m

    async def claim_machine(
        self, account_id: str, machine: str, hello: HelloV2, now_ms: int,
        *, key_secret: str | None = None, token_id: str | None = None,
    ) -> tuple[Machine, str | None]:
        """Record (or refresh) the machine of a v2 hello; ``first_seen`` is kept.

        A new key gets a secret, returned once (only its sha256 is stored,
        with ``token_id``, the account token that registered it). An existing
        key is refreshed only when ``key_secret`` matches its stored hash,
        else ``LauncherKeyInUseError`` and nothing changes (reset: forget the
        machine). Raises ``MachineLimitError`` for a new key over the owner's
        limit (``MAX_MACHINES_OPEN`` for ``OPEN_OWNER``).
        """
        key = hello.launcher_key
        if key is None:
            raise ValueError("a machine needs a launcher_key")
        limit = MAX_MACHINES_OPEN if account_id == OPEN_OWNER else MAX_MACHINES_PER_ACCOUNT
        conn = self._write()
        issued: str | None = None
        async with self._store._write_tx_lock:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                async with conn.execute(
                    "SELECT launcher_key, key_secret_hash FROM launcher_machines"
                    " WHERE account_id = ?",
                    (account_id,),
                ) as cur:
                    rows = {r[0]: r[1] async for r in cur}
                if rows.get(key):
                    if not key_secret_matches(key_secret, rows[key]):
                        raise LauncherKeyInUseError("launcher_key_in_use")
                    secret_hash = rows[key]
                else:
                    if key not in rows and len(rows) >= limit:
                        raise MachineLimitError(f"at most {limit} machines")
                    issued = new_key_secret()
                    secret_hash = hash_token(issued)
                await conn.execute(
                    f"INSERT INTO launcher_machines ({_COLUMNS}, key_secret_hash, token_id)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    " ON CONFLICT(account_id, launcher_key) DO UPDATE SET"
                    " machine = excluded.machine, mode = excluded.mode,"
                    " ceiling = excluded.ceiling,"
                    " capabilities_json = excluded.capabilities_json,"
                    " sandbox = excluded.sandbox, run_as = excluded.run_as,"
                    " platform = excluded.platform, prereqs_json = excluded.prereqs_json,"
                    " last_seen = excluded.last_seen,"
                    " key_secret_hash = excluded.key_secret_hash,"
                    " token_id = COALESCE(launcher_machines.token_id, excluded.token_id)",
                    (
                        account_id, key, machine, hello.mode, hello.ceiling,
                        json.dumps(sorted(hello.capabilities)), hello.sandbox,
                        int(hello.run_as), hello.platform, json.dumps(list(hello.prereqs)),
                        now_ms, now_ms, secret_hash, token_id,
                    ),
                )
                async with conn.execute(
                    f"SELECT {_COLUMNS} FROM launcher_machines"
                    " WHERE account_id = ? AND launcher_key = ?",
                    (account_id, key),
                ) as cur:
                    row = await cur.fetchone()
                await conn.execute("COMMIT")
            except BaseException:
                await conn.execute("ROLLBACK")
                raise
        assert row is not None
        return _row(row), issued

    async def set_prereqs(
        self, account_id: str, launcher_key: str, prereqs: list[dict[str, Any]]
    ) -> None:
        """Replace the machine's prerequisite report (no-op for an unknown one)."""
        async with self._store._write_tx_lock:
            await self._write().execute(
                "UPDATE launcher_machines SET prereqs_json = ?"
                " WHERE account_id = ? AND launcher_key = ?",
                (json.dumps(prereqs), account_id, launcher_key),
            )

    async def touch(self, account_id: str, launcher_key: str, now_ms: int) -> None:
        """Set ``last_seen`` (the launcher went offline); no-op for an unknown one."""
        async with self._store._write_tx_lock:
            await self._write().execute(
                "UPDATE launcher_machines SET last_seen = ?"
                " WHERE account_id = ? AND launcher_key = ?",
                (now_ms, account_id, launcher_key),
            )

    async def prune_open(
        self, now_ms: int, retention_days: int, *, online_keys: set[str]
    ) -> int:
        """Forget open-mode machines (and their definitions) offline for more
        than ``retention_days`` (``last_seen`` older; ``online_keys`` are
        always kept). Account machines are never pruned. Returns the count."""
        cutoff = now_ms - retention_days * 86_400_000
        conn = self._write()
        async with self._store._write_tx_lock:
            async with conn.execute(
                "SELECT launcher_key FROM launcher_machines"
                " WHERE account_id = ? AND last_seen < ?",
                (OPEN_OWNER, cutoff),
            ) as cur:
                stale = [r[0] async for r in cur if r[0] not in online_keys]
            if not stale:
                return 0
            await conn.execute("BEGIN IMMEDIATE")
            try:
                for key in stale:
                    for table in _MACHINE_TABLES:
                        await conn.execute(
                            f"DELETE FROM {table} WHERE account_id = ? AND launcher_key = ?",
                            (OPEN_OWNER, key),
                        )
                await conn.execute("COMMIT")
            except BaseException:
                await conn.execute("ROLLBACK")
                raise
        return len(stale)

    async def list_machines(self, account_id: str) -> list[Machine]:
        async with self._read().execute(
            f"SELECT {_COLUMNS} FROM launcher_machines WHERE account_id = ?"
            " ORDER BY first_seen, launcher_key",
            (account_id,),
        ) as cur:
            return [_row(r) async for r in cur]

    async def get_machine(self, account_id: str, launcher_key: str) -> Machine:
        async with self._read().execute(
            f"SELECT {_COLUMNS} FROM launcher_machines"
            " WHERE account_id = ? AND launcher_key = ?",
            (account_id, launcher_key),
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            raise ToolError(ErrorCode.LAUNCHER_NOT_FOUND, "no such machine")
        return _row(row)

    async def _delete(self, where: str, params: tuple[str, ...]) -> None:
        conn = self._write()
        async with self._store._write_tx_lock:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                for table in _MACHINE_TABLES:
                    await conn.execute(f"DELETE FROM {table} WHERE {where}", params)
                await conn.execute("COMMIT")
            except BaseException:
                await conn.execute("ROLLBACK")
                raise

    async def forget_machine(self, account_id: str, launcher_key: str) -> None:
        """Delete a machine and its deployed definitions (idempotent)."""
        await self._delete("account_id = ? AND launcher_key = ?", (account_id, launcher_key))

    async def forget_account(self, account_id: str) -> None:
        """Delete every machine and definition of an owner (idempotent)."""
        await self._delete("account_id = ?", (account_id,))

    # --- deployed definitions (agent_definitions) ---------------------------

    async def _in_tx(self, fn: Callable[[Any], Awaitable[_T]]) -> _T:
        conn = self._write()
        async with self._store._write_tx_lock:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                result = await fn(conn)
                await conn.execute("COMMIT")
            except BaseException:
                await conn.execute("ROLLBACK")
                raise
        return result

    @staticmethod
    async def _fetch_def(
        conn: Any, account_id: str, launcher_key: str, def_id: str
    ) -> StoredDefinition | None:
        async with conn.execute(
            f"SELECT {_DEF_COLUMNS} FROM agent_definitions"
            " WHERE account_id = ? AND launcher_key = ? AND def_id = ?",
            (account_id, launcher_key, def_id),
        ) as cur:
            row = await cur.fetchone()
        return None if row is None else _def_row(row)

    async def put_definition(
        self, account_id: str, launcher_key: str, definition: dict[str, Any],
        state: str, now_ms: int,
    ) -> StoredDefinition:
        """Store a (validated) definition as the desired state under its ``id``:
        revision = previous + 1 (1 for a new one), ``deleted`` cleared,
        problems/warnings reset; ``applied_revision`` is kept."""
        def_id = str(definition["id"])
        key = (account_id, launcher_key, def_id)

        async def put(conn: Any) -> StoredDefinition:
            prev = await self._fetch_def(conn, account_id, launcher_key, def_id)
            last = prev.revision if prev is not None else 0
            if prev is None:  # purged before? continue its revisions
                async with conn.execute(
                    "SELECT revision FROM agent_definition_revisions"
                    " WHERE account_id = ? AND launcher_key = ? AND def_id = ?",
                    key,
                ) as cur:
                    hwm = await cur.fetchone()
                last = hwm[0] if hwm is not None else 0
            await conn.execute(
                f"INSERT INTO agent_definitions ({_DEF_COLUMNS}, account_id, launcher_key)"
                " VALUES (?, ?, ?, ?, ?, '[]', '[]', 0, ?, ?, ?)"
                " ON CONFLICT(account_id, launcher_key, def_id) DO UPDATE SET"
                " revision = excluded.revision, definition_json = excluded.definition_json,"
                " state = excluded.state, problems_json = '[]', warnings_json = '[]',"
                " deleted = 0, updated_at = excluded.updated_at",
                (
                    def_id, last + 1, json.dumps(definition),
                    state, None, now_ms, account_id, launcher_key,
                ),
            )
            out = await self._fetch_def(conn, account_id, launcher_key, def_id)
            assert out is not None
            return out

        return await self._in_tx(put)

    async def list_definitions(
        self, account_id: str, launcher_key: str, *, include_deleted: bool = False
    ) -> list[StoredDefinition]:
        """The machine's definitions by id (rows being removed only when asked)."""
        async with self._read().execute(
            f"SELECT {_DEF_COLUMNS} FROM agent_definitions"
            " WHERE account_id = ? AND launcher_key = ?"
            + ("" if include_deleted else " AND deleted = 0")
            + " ORDER BY def_id",
            (account_id, launcher_key),
        ) as cur:
            return [_def_row(r) async for r in cur]

    async def get_definition(
        self, account_id: str, launcher_key: str, def_id: str
    ) -> StoredDefinition:
        """One definition, also while being removed (see ``deleted``); raises
        ``ToolError(DEFINITION_NOT_FOUND)``."""
        found = await self._fetch_def(self._read(), account_id, launcher_key, def_id)
        if found is None:
            raise ToolError(ErrorCode.DEFINITION_NOT_FOUND, "no such definition")
        return found

    async def set_status(
        self, account_id: str, launcher_key: str, def_id: str, revision: int,
        state: str, problems: list[str], warnings: list[str], now_ms: int,
    ) -> StoredDefinition | None:
        """Record the launcher's status for ``revision``; ignored (``None``)
        when that is not the stored revision (stale) or the row is unknown.
        ``applied`` also sets ``applied_revision``."""

        async def update(conn: Any) -> StoredDefinition | None:
            prev = await self._fetch_def(conn, account_id, launcher_key, def_id)
            if prev is None or prev.revision != revision:
                return None
            await conn.execute(
                "UPDATE agent_definitions SET state = ?, problems_json = ?,"
                " warnings_json = ?, updated_at = ?,"
                " applied_revision = CASE WHEN ? THEN revision ELSE applied_revision END"
                " WHERE account_id = ? AND launcher_key = ? AND def_id = ?",
                (
                    state, json.dumps(problems), json.dumps(warnings), now_ms,
                    int(state == "applied"), account_id, launcher_key, def_id,
                ),
            )
            return await self._fetch_def(conn, account_id, launcher_key, def_id)

        return await self._in_tx(update)

    async def mark_deleted(
        self, account_id: str, launcher_key: str, def_id: str, now_ms: int
    ) -> StoredDefinition:
        """Ask for the definition's removal: revision + 1, ``deleted``, state
        ``removing`` (the row goes on ``purge_definition``). Raises
        ``ToolError(DEFINITION_NOT_FOUND)``."""

        async def delete(conn: Any) -> StoredDefinition:
            prev = await self._fetch_def(conn, account_id, launcher_key, def_id)
            if prev is None:
                raise ToolError(ErrorCode.DEFINITION_NOT_FOUND, "no such definition")
            await conn.execute(
                "UPDATE agent_definitions SET revision = revision + 1, deleted = 1,"
                " state = 'removing', problems_json = '[]', warnings_json = '[]',"
                " updated_at = ?"
                " WHERE account_id = ? AND launcher_key = ? AND def_id = ?",
                (now_ms, account_id, launcher_key, def_id),
            )
            out = await self._fetch_def(conn, account_id, launcher_key, def_id)
            assert out is not None
            return out

        return await self._in_tx(delete)

    async def purge_definition(self, account_id: str, launcher_key: str, def_id: str) -> None:
        """Delete the definition's row (idempotent). Its revision is kept as a
        high-water mark: a later put of the same id continues from it."""
        key = (account_id, launcher_key, def_id)

        async def purge(conn: Any) -> None:
            prev = await self._fetch_def(conn, *key)
            if prev is None:
                return
            await conn.execute(
                "INSERT INTO agent_definition_revisions"
                " (account_id, launcher_key, def_id, revision) VALUES (?, ?, ?, ?)"
                " ON CONFLICT(account_id, launcher_key, def_id)"
                " DO UPDATE SET revision = excluded.revision",
                (*key, prev.revision),
            )
            await conn.execute(
                "DELETE FROM agent_definitions"
                " WHERE account_id = ? AND launcher_key = ? AND def_id = ?",
                key,
            )

        await self._in_tx(purge)
