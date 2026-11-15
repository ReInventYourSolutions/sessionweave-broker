"""Broker accounts and their API tokens.

An account's token authorises session creation. Plaintext tokens are
returned once, at mint time; only SHA-256 digests are stored. A token is
usable only while it is not revoked, not expired, and its account is
``active``. Times are integer milliseconds.
"""

from __future__ import annotations

import secrets
import sqlite3
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from sessionweave.access import hash_token
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.persistence import PersistenceStore

ACCOUNT_STATUSES: frozenset[str] = frozenset({"pending", "active", "disabled"})
TOKEN_PREFIX = "swa_"
LABEL_MAX_LEN = 64

def session_creation_mode(env: Mapping[str, str]) -> Literal["open", "account"]:
    """Deployment mode from ``SW_SESSION_CREATION`` (default ``open``).

    ``account`` needs a persistence DB (accounts live there) and an admin token
    (the only way to mint accounts), so both are validated up front.
    """
    raw = (env.get("SW_SESSION_CREATION") or "open").strip().lower()
    if raw == "open":
        return "open"
    if raw != "account":
        raise ToolError(
            ErrorCode.INVALID_CONFIG,
            f"SW_SESSION_CREATION={raw!r} must be 'open' or 'account'",
        )
    if not env.get("SW_PERSISTENCE_DB_PATH"):
        raise ToolError(
            ErrorCode.INVALID_CONFIG,
            "SW_SESSION_CREATION=account requires SW_PERSISTENCE_DB_PATH",
        )
    if not env.get("SW_ADMIN_TOKEN"):
        raise ToolError(
            ErrorCode.INVALID_CONFIG,
            "SW_SESSION_CREATION=account requires SW_ADMIN_TOKEN",
        )
    return "account"


_ACCOUNT_COLS = "account_id, name, status, contact, created_by, created_at"
_TOKEN_COLS = "token_id, account_id, label, created_at, expires_at, revoked_at"


@dataclass(frozen=True)
class Account:
    account_id: str
    name: str
    status: str
    contact: str | None
    created_by: str
    created_at: int

    def to_public(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "name": self.name,
            "status": self.status,
            "contact": self.contact,
            "created_by": self.created_by,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class AccountToken:
    token_id: str
    account_id: str
    label: str
    created_at: int
    expires_at: int | None
    revoked_at: int | None

    def to_public(self) -> dict[str, Any]:
        return {
            "token_id": self.token_id,
            "account_id": self.account_id,
            "label": self.label,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "revoked_at": self.revoked_at,
        }


def _name_taken(name: str) -> ToolError:
    return ToolError(ErrorCode.ACCOUNT_NAME_TAKEN, f"account name already in use: {name}")


def _not_found(account_id: str) -> ToolError:
    return ToolError(ErrorCode.ACCOUNT_NOT_FOUND, f"unknown account: {account_id}")


def _check_status(status: str) -> None:
    if status not in ACCOUNT_STATUSES:
        raise ToolError(ErrorCode.INVALID_REQUEST, f"invalid account status: {status!r}")


class AccountStore:
    def __init__(self, store: PersistenceStore) -> None:
        self._store = store

    # ----- accounts -----

    async def create(
        self,
        name: str,
        *,
        status: str = "active",
        contact: str | None = None,
        created_by: str = "admin",
    ) -> Account:
        _check_status(status)
        acc = Account(
            account_id="acc_" + secrets.token_hex(8),
            name=name,
            status=status,
            contact=contact,
            created_by=created_by,
            created_at=int(time.time() * 1000),
        )
        conn = self._write_conn()
        async with self._store._write_tx_lock:
            try:
                await conn.execute(
                    f"INSERT INTO accounts ({_ACCOUNT_COLS}) VALUES (?, ?, ?, ?, ?, ?)",
                    (acc.account_id, acc.name, acc.status, acc.contact,
                     acc.created_by, acc.created_at),
                )
            except sqlite3.IntegrityError as exc:
                raise _name_taken(name) from exc
        return acc

    async def get(self, account_id: str) -> Account:
        async with self._read().execute(
            f"SELECT {_ACCOUNT_COLS} FROM accounts WHERE account_id = ?", (account_id,)
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            raise _not_found(account_id)
        return Account(*row)

    async def list_accounts(self) -> list[Account]:
        async with self._read().execute(
            f"SELECT {_ACCOUNT_COLS} FROM accounts ORDER BY created_at, account_id"
        ) as cur:
            return [Account(*row) async for row in cur]

    async def update(
        self,
        account_id: str,
        *,
        status: str | None = None,
        name: str | None = None,
        contact: str | None = None,
    ) -> tuple[Account, Account]:
        """Apply the given changes; return (before, after)."""
        if status is not None:
            _check_status(status)
        before = await self.get(account_id)
        sets: list[str] = []
        params: list[Any] = []
        for column, value in (("status", status), ("name", name), ("contact", contact)):
            if value is not None:
                sets.append(f"{column} = ?")
                params.append(value)
        if not sets:
            return before, before
        conn = self._write_conn()
        async with self._store._write_tx_lock:
            try:
                cur = await conn.execute(
                    f"UPDATE accounts SET {', '.join(sets)} WHERE account_id = ?",
                    (*params, account_id),
                )
            except sqlite3.IntegrityError as exc:
                raise _name_taken(name or "") from exc
            if cur.rowcount == 0:
                raise _not_found(account_id)
        return before, await self.get(account_id)

    async def delete(self, account_id: str) -> None:
        """Delete an account; its tokens go with it (ON DELETE CASCADE)."""
        conn = self._write_conn()
        async with self._store._write_tx_lock:
            await conn.execute("DELETE FROM accounts WHERE account_id = ?", (account_id,))

    # ----- tokens -----

    async def create_token(
        self, account_id: str, label: str, *, expires_at_ms: int | None = None
    ) -> tuple[str, AccountToken]:
        """Mint a token; the plaintext is returned here and never stored."""
        if not 1 <= len(label.strip()) <= LABEL_MAX_LEN or len(label) > LABEL_MAX_LEN:
            raise ToolError(
                ErrorCode.INVALID_REQUEST,
                f"token label must be 1-{LABEL_MAX_LEN} characters",
            )
        await self.get(account_id)
        plaintext = TOKEN_PREFIX + secrets.token_urlsafe(32)
        meta = AccountToken(
            token_id=secrets.token_hex(8),
            account_id=account_id,
            label=label,
            created_at=int(time.time() * 1000),
            expires_at=expires_at_ms,
            revoked_at=None,
        )
        conn = self._write_conn()
        async with self._store._write_tx_lock:
            try:
                await conn.execute(
                    "INSERT INTO account_tokens (token_id, account_id, token_hash,"
                    " label, created_at, expires_at, revoked_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, NULL)",
                    (meta.token_id, account_id, hash_token(plaintext), label,
                     meta.created_at, expires_at_ms),
                )
            except sqlite3.IntegrityError as exc:  # account deleted concurrently
                raise _not_found(account_id) from exc
        return plaintext, meta

    async def list_tokens(self, account_id: str) -> list[AccountToken]:
        async with self._read().execute(
            f"SELECT {_TOKEN_COLS} FROM account_tokens WHERE account_id = ?"
            " ORDER BY created_at, token_id",
            (account_id,),
        ) as cur:
            return [AccountToken(*row) async for row in cur]

    async def revoke_token(self, account_id: str, token_id: str) -> AccountToken:
        """Revoke a token. Idempotent: a revoked token keeps its first revoke time."""
        conn = self._write_conn()
        async with self._store._write_tx_lock:
            await conn.execute(
                "UPDATE account_tokens SET revoked_at = ?"
                " WHERE account_id = ? AND token_id = ? AND revoked_at IS NULL",
                (int(time.time() * 1000), account_id, token_id),
            )
        async with self._read().execute(
            f"SELECT {_TOKEN_COLS} FROM account_tokens"
            " WHERE account_id = ? AND token_id = ?",
            (account_id, token_id),
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            raise ToolError(ErrorCode.TOKEN_NOT_FOUND, f"unknown token: {token_id}")
        return AccountToken(*row)

    async def match_token(self, token: str, now_ms: int) -> Account | None:
        """The active account owning a usable token, else None."""
        async with self._read().execute(
            "SELECT a.account_id, a.name, a.status, a.contact, a.created_by,"
            " a.created_at FROM account_tokens t JOIN accounts a"
            " ON a.account_id = t.account_id"
            " WHERE t.token_hash = ? AND t.revoked_at IS NULL"
            " AND (t.expires_at IS NULL OR t.expires_at > ?)"
            " AND a.status = 'active'",
            (hash_token(token), now_ms),
        ) as cur:
            row = await cur.fetchone()
        return Account(*row) if row is not None else None

    # ----- connections -----

    def _write_conn(self) -> Any:
        assert self._store._write_conn is not None
        return self._store._write_conn

    def _read(self) -> Any:
        return self._store._read_pool[0]
