"""Machines: the persisted descriptor of each protocol-v2 launcher.

A machine is identified by ``(account_id, launcher_key)``; ``account_id`` is
the owner account, or ``OPEN_OWNER`` (``""``) for open-mode launchers. The
row survives the launcher's disconnects and broker restarts, so the Cockpit
can list offline machines and the broker can queue definitions for them
(``agent_definitions``, same key). Nothing a launcher produces is stored here.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

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
                await conn.execute(f"DELETE FROM agent_definitions WHERE {where}", params)
                await conn.execute(f"DELETE FROM launcher_machines WHERE {where}", params)
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
