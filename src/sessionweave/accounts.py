"""Broker accounts and their API tokens.

An account's token authorises session creation. Plaintext tokens are
returned once, at mint time; only SHA-256 digests are stored. A token is
usable only while it is not revoked, not expired, and its account is
``active``. Times are integer milliseconds.
"""

from __future__ import annotations

import ipaddress
import json
import secrets
import sqlite3
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from sessionweave.access import hash_token
from sessionweave.account_events import EventLog, Origin
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
_TOKEN_COLS = (
    "token_id, account_id, label, created_at, expires_at, revoked_at,"
    " network_policy, approved_networks, last_used_at, last_used_ip"
)
NETWORK_POLICIES: frozenset[str] = frozenset({"any", "first_use", "approved_only"})
APPROVAL_TTL_MS = 24 * 3_600_000
MAX_PENDING_PER_TOKEN = 10
REJECTED_EVENT_INTERVAL_MS = 10 * 60_000
LAST_USED_INTERVAL_MS = 60_000
APPROVALS_PRUNE_INTERVAL_MS = 60_000


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
    network_policy: str = "any"
    approved_networks: tuple[str, ...] = ()
    last_used_at: int | None = None
    last_used_ip: str | None = None

    @classmethod
    def from_row(cls, row: Any) -> AccountToken:
        return cls(
            row[0], row[1], row[2], row[3], row[4], row[5],
            row[6], tuple(json.loads(row[7])), row[8], row[9],
        )

    def to_public(self) -> dict[str, Any]:
        return {
            "network_policy": self.network_policy,
            "approved_networks": list(self.approved_networks),
            "last_used_at": self.last_used_at,
            "last_used_ip": self.last_used_ip,
            "token_id": self.token_id,
            "account_id": self.account_id,
            "label": self.label,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "revoked_at": self.revoked_at,
        }


@dataclass
class AuthResult:
    account: Account | None
    token_id: str | None
    refusal: Literal["network"] | None = None
    approval_id: str | None = None
    token_label: str | None = None
    account_id: str | None = None
    # When the token expires (ms since the epoch), if it does.
    expires_at: int | None = None


def _parse_ip(raw: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    ip = ipaddress.ip_address(raw.strip())
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        return ip.ipv4_mapped
    return ip


def _normalize_cidr(raw: Any) -> str:
    if not isinstance(raw, str):
        raise ToolError(ErrorCode.INVALID_REQUEST, "networks must be strings")
    try:
        net = ipaddress.ip_network(raw.strip(), strict=False)
    except ValueError as exc:
        raise ToolError(ErrorCode.INVALID_REQUEST, f"invalid network: {raw!r}") from exc
    if (
        isinstance(net, ipaddress.IPv6Network)
        and net.prefixlen >= 96
        and net.network_address.ipv4_mapped is not None
    ):
        # ::ffff:a.b.c.d/N is the IPv4 network a.b.c.d/(N-96): _parse_ip maps
        # client addresses the same way, so store the IPv4 form.
        net = ipaddress.IPv4Network(
            (net.network_address.ipv4_mapped, net.prefixlen - 96), strict=False
        )
    return str(net)


def _normalize_networks(raw: Any) -> list[str]:
    if not isinstance(raw, (list, tuple)):
        raise ToolError(ErrorCode.INVALID_REQUEST, "approved_networks must be a list")
    out: list[str] = []
    for item in raw:
        cidr = _normalize_cidr(item)
        if cidr not in out:
            out.append(cidr)
    return out


def _pending_key(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> str:
    """Pending approvals are one per address (IPv4) or per /64 (IPv6)."""
    if isinstance(ip, ipaddress.IPv6Address):
        return str(ipaddress.ip_network(f"{ip}/64", strict=False))
    return str(ip)


def _check_policy(policy: str) -> None:
    if policy not in NETWORK_POLICIES:
        raise ToolError(ErrorCode.INVALID_REQUEST, f"invalid network policy: {policy!r}")


def _ip_in(ip: ipaddress.IPv4Address | ipaddress.IPv6Address, cidrs: list[str]) -> bool:
    for c in cidrs:
        net = ipaddress.ip_network(c, strict=False)
        if net.version == ip.version and ip in net:
            return True
    return False


def _host_cidr(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> str:
    return f"{ip}/{ip.max_prefixlen}"


def _approval_not_found(approval_id: str) -> ToolError:
    return ToolError(ErrorCode.APPROVAL_NOT_FOUND, f"unknown approval: {approval_id}")


def _name_taken(name: str) -> ToolError:
    return ToolError(ErrorCode.ACCOUNT_NAME_TAKEN, f"account name already in use: {name}")


def _not_found(account_id: str) -> ToolError:
    return ToolError(ErrorCode.ACCOUNT_NOT_FOUND, f"unknown account: {account_id}")


def _check_status(status: str) -> None:
    if status not in ACCOUNT_STATUSES:
        raise ToolError(ErrorCode.INVALID_REQUEST, f"invalid account status: {status!r}")


class AccountStore:
    def __init__(self, store: PersistenceStore, events: EventLog | None = None) -> None:
        self._store = store
        self.events = events
        self._rejected: dict[tuple[str, str], int] = {}
        self._last_used: dict[str, int] = {}
        self._approvals_pruned_at = 0

    async def _emit(self, account_id: str, kind: str, data: dict[str, Any]) -> None:
        if self.events is not None:
            await self.events.emit(account_id, kind, data)

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
        """Delete an account; its tokens go with it (ON DELETE CASCADE), and
        its machines and deployed agent definitions are deleted too."""
        conn = self._write_conn()
        async with self._store._write_tx_lock:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                for table in (
                    "agent_definitions", "agent_definition_revisions",
                    "launcher_machines", "accounts",
                ):
                    await conn.execute(
                        f"DELETE FROM {table} WHERE account_id = ?", (account_id,)
                    )
                await conn.execute("COMMIT")
            except BaseException:
                await conn.execute("ROLLBACK")
                raise

    # ----- tokens -----

    async def create_token(
        self,
        account_id: str,
        label: str,
        *,
        expires_at_ms: int | None = None,
        max_tokens: int | None = None,
        network_policy: str = "any",
        approved_networks: list[str] | None = None,
    ) -> tuple[str, AccountToken]:
        """Mint a token; the plaintext is returned here and never stored.

        With ``max_tokens``, refuses (``account_token_limit``) when the account
        already has that many tokens that are neither revoked nor expired.
        """
        if not 1 <= len(label.strip()) <= LABEL_MAX_LEN or len(label) > LABEL_MAX_LEN:
            raise ToolError(
                ErrorCode.INVALID_REQUEST,
                f"token label must be 1-{LABEL_MAX_LEN} characters",
            )
        _check_policy(network_policy)
        networks = _normalize_networks(
            approved_networks if approved_networks is not None else []
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
            network_policy=network_policy,
            approved_networks=tuple(networks),
        )
        conn = self._write_conn()
        async with self._store._write_tx_lock:
            if max_tokens is not None:
                async with conn.execute(
                    "SELECT COUNT(*) FROM account_tokens WHERE account_id = ?"
                    " AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > ?)",
                    (account_id, meta.created_at),
                ) as cur:
                    row = await cur.fetchone()
                if row is not None and row[0] >= max_tokens:
                    raise ToolError(
                        ErrorCode.ACCOUNT_TOKEN_LIMIT,
                        f"this account already has {row[0]} active tokens (limit "
                        f"{max_tokens}); revoke one before minting another",
                    )
            try:
                await conn.execute(
                    "INSERT INTO account_tokens (token_id, account_id, token_hash,"
                    " label, created_at, expires_at, revoked_at, network_policy,"
                    " approved_networks) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)",
                    (meta.token_id, account_id, hash_token(plaintext), label,
                     meta.created_at, expires_at_ms, network_policy,
                     json.dumps(networks)),
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
            return [AccountToken.from_row(row) async for row in cur]

    async def revoke_token(self, account_id: str, token_id: str) -> AccountToken:
        """Revoke a token. Idempotent: a revoked token keeps its first revoke time."""
        return (await self.revoke_token_changed(account_id, token_id))[0]

    async def revoke_token_changed(
        self, account_id: str, token_id: str
    ) -> tuple[AccountToken, bool]:
        """Revoke a token; also report whether this call changed the row, so a
        caller emits ``token.revoked`` exactly once under concurrent revokes."""
        conn = self._write_conn()
        async with self._store._write_tx_lock:
            cur = await conn.execute(
                "UPDATE account_tokens SET revoked_at = ?"
                " WHERE account_id = ? AND token_id = ? AND revoked_at IS NULL",
                (int(time.time() * 1000), account_id, token_id),
            )
            changed = cur.rowcount > 0
            await cur.close()
        return await self._get_token(account_id, token_id), changed

    async def _get_token(self, account_id: str, token_id: str) -> AccountToken:
        async with self._read().execute(
            f"SELECT {_TOKEN_COLS} FROM account_tokens"
            " WHERE account_id = ? AND token_id = ?",
            (account_id, token_id),
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            raise ToolError(ErrorCode.TOKEN_NOT_FOUND, f"unknown token: {token_id}")
        return AccountToken.from_row(row)

    async def get_token(self, account_id: str, token_id: str) -> AccountToken:
        return await self._get_token(account_id, token_id)

    @staticmethod
    def network_allows(token: AccountToken, ip: str) -> bool:
        """Whether ``ip`` may use ``token`` under its current policy and networks.

        A pure check (no approvals, no first-use binding): used to decide which
        live streams to close after a policy change.
        """
        if token.network_policy == "any":
            return True
        try:
            return _ip_in(_parse_ip(ip), list(token.approved_networks))
        except ValueError:
            return False

    async def set_token_networks(
        self,
        account_id: str,
        token_id: str,
        *,
        network_policy: str | None,
        approved_networks: list[str] | None,
    ) -> AccountToken:
        """Change a token's policy and/or replace its approved networks."""
        if network_policy is not None:
            _check_policy(network_policy)
        networks = (
            _normalize_networks(approved_networks) if approved_networks is not None else None
        )
        await self._get_token(account_id, token_id)
        sets: list[str] = []
        params: list[Any] = []
        if network_policy is not None:
            sets.append("network_policy = ?")
            params.append(network_policy)
        if networks is not None:
            sets.append("approved_networks = ?")
            params.append(json.dumps(networks))
        if sets:
            async with self._store._write_tx_lock:
                await self._write_conn().execute(
                    f"UPDATE account_tokens SET {', '.join(sets)}"
                    " WHERE account_id = ? AND token_id = ?",
                    (*params, account_id, token_id),
                )
        return await self._get_token(account_id, token_id)

    async def match_token(self, token: str, now_ms: int) -> Account | None:
        """The active account owning a usable token, else None (no network check)."""
        return (await self.authenticate(token, now_ms, None)).account

    async def match_token_with_id(
        self, token: str, now_ms: int
    ) -> tuple[Account, str] | None:
        """The active account owning a usable token, with that token's id."""
        res = await self.authenticate(token, now_ms, None)
        if res.account is None or res.token_id is None:
            return None
        return res.account, res.token_id

    async def authenticate(
        self,
        token: str,
        now_ms: int,
        origin: Origin | None,
        *,
        reject_origin: Origin | None = None,
    ) -> AuthResult:
        """Resolve an account token, applying its network policy to ``origin``.

        ``origin=None`` is a trusted internal call: the network check is skipped.
        ``reject_origin`` only labels a ``token.rejected`` event of such a call.

        A revoked, expired or inactive-account token always records
        ``token.rejected`` on the token's own account, even when presented on
        another account's session; it never creates a pending approval or a
        network event.
        """
        async with self._read().execute(
            "SELECT a.account_id, a.name, a.status, a.contact, a.created_by,"
            " a.created_at, t.token_id, t.label, t.revoked_at, t.expires_at,"
            " t.network_policy, t.approved_networks, t.last_used_at"
            " FROM account_tokens t JOIN accounts a ON a.account_id = t.account_id"
            " WHERE t.token_hash = ?",
            (hash_token(token),),
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return AuthResult(None, None)
        account = Account(*row[:6])
        token_id, label = row[6], row[7]
        reason: str | None = None
        if row[8] is not None:
            reason = "revoked"
        elif row[9] is not None and row[9] <= now_ms:
            reason = "expired"
        elif account.status != "active":
            reason = f"account_{account.status}"
        if reason is not None:
            await self._reject(
                account.account_id, token_id, label, reason, now_ms, origin or reject_origin
            )
            return AuthResult(None, None)
        policy, networks = row[10], json.loads(row[11])
        if origin is not None and policy != "any":
            refusal = await self._check_network(
                account, token_id, label, policy, networks, now_ms, origin
            )
            if refusal is not None:
                return refusal
        if origin is not None:
            await self._touch(token_id, row[12], now_ms, origin)
        return AuthResult(
            account, token_id, token_label=label, account_id=account.account_id,
            expires_at=row[9],
        )

    async def _reject(
        self,
        account_id: str,
        token_id: str,
        label: str,
        reason: str,
        now_ms: int,
        origin: Origin | None,
    ) -> None:
        key = (token_id, reason)
        last = self._rejected.get(key)
        if last is not None and now_ms - last < REJECTED_EVENT_INTERVAL_MS:
            return
        self._rejected[key] = now_ms
        await self._emit(
            account_id,
            "token.rejected",
            {
                "token_id": token_id,
                "label": label,
                "reason": reason,
                "ip": origin.ip if origin else None,
                "client": origin.client if origin else None,
            },
        )

    async def _check_network(
        self,
        account: Account,
        token_id: str,
        label: str,
        policy: str,
        networks: list[str],
        now_ms: int,
        origin: Origin,
    ) -> AuthResult | None:
        """None when the origin is allowed, else the refusal result."""
        try:
            ip = _parse_ip(origin.ip)
        except ValueError:
            ip = None
        if ip is not None:
            if _ip_in(ip, networks):
                return None
            if policy == "first_use" and not networks:
                bound: str | None = None
                async with self._store._write_tx_lock:
                    async with self._write_conn().execute(
                        "SELECT approved_networks, network_policy FROM account_tokens"
                        " WHERE token_id = ?",
                        (token_id,),
                    ) as cur:
                        cur_row = await cur.fetchone()
                    current = json.loads(cur_row[0]) if cur_row is not None else None
                    cur_policy = cur_row[1] if cur_row is not None else None
                    if current == [] and cur_policy == "first_use":
                        bound = _host_cidr(ip)
                        await self._write_conn().execute(
                            "UPDATE account_tokens SET approved_networks = ?"
                            " WHERE token_id = ?",
                            (json.dumps([bound]), token_id),
                        )
                    elif current is not None and _ip_in(ip, current):
                        return None
                if bound is not None:
                    await self._emit(
                        account.account_id,
                        "token.network_approved",
                        {
                            "token": {"id": token_id, "label": label},
                            "token_id": token_id,
                            "label": label,
                            "ip": str(ip),
                            "cidr": bound,
                            "by": "first_use",
                            "client": origin.client,
                        },
                    )
                    return None
        if ip is None:
            # No usable address to approve: refuse, but record nothing.
            return AuthResult(
                None, token_id, refusal="network",
                token_label=label, account_id=account.account_id,
            )
        ip_text = str(ip)
        approval_id, created = await self._pending(
            account.account_id, token_id, ip, origin.client, now_ms
        )
        if created:
            await self._emit(
                account.account_id,
                "token.network_pending",
                {
                    "token_id": token_id,
                    "label": label,
                    "approval_id": approval_id,
                    "ip": ip_text,
                    "client": origin.client,
                    "expires_at": now_ms + APPROVAL_TTL_MS,
                },
            )
        return AuthResult(
            None, token_id, refusal="network", approval_id=approval_id,
            token_label=label, account_id=account.account_id,
        )

    async def _find_pending(
        self, conn: Any, token_id: str, key: str, now_ms: int
    ) -> str | None:
        async with conn.execute(
            "SELECT approval_id, ip FROM network_approvals"
            " WHERE token_id = ? AND expires_at > ? ORDER BY created_at, rowid",
            (token_id, now_ms),
        ) as cur:
            for approval_id, ip_text in await cur.fetchall():
                try:
                    if _pending_key(_parse_ip(ip_text)) == key:
                        return str(approval_id)
                except ValueError:
                    continue
        return None

    async def _pending(
        self,
        account_id: str,
        token_id: str,
        ip: ipaddress.IPv4Address | ipaddress.IPv6Address,
        client: str | None,
        now_ms: int,
    ) -> tuple[str, bool]:
        key = _pending_key(ip)
        # Repeats (the common case under attack) are answered from a read
        # connection: no write transaction per refused request.
        found = await self._find_pending(self._read(), token_id, key, now_ms)
        if found is not None:
            return found, False
        conn = self._write_conn()
        async with self._store._write_tx_lock:
            found = await self._find_pending(conn, token_id, key, now_ms)
            if found is not None:
                return found, False
            if now_ms - self._approvals_pruned_at >= APPROVALS_PRUNE_INTERVAL_MS:
                self._approvals_pruned_at = now_ms
                await conn.execute(
                    "DELETE FROM network_approvals WHERE expires_at <= ?", (now_ms,)
                )
            approval_id = "apr_" + secrets.token_hex(8)
            await conn.execute(
                "INSERT INTO network_approvals (approval_id, token_id, account_id, ip,"
                " client, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (approval_id, token_id, account_id, str(ip), client, now_ms,
                 now_ms + APPROVAL_TTL_MS),
            )
            await conn.execute(
                "DELETE FROM network_approvals WHERE token_id = ? AND approval_id NOT IN"
                " (SELECT approval_id FROM network_approvals WHERE token_id = ?"
                "  ORDER BY created_at DESC, rowid DESC LIMIT ?)",
                (token_id, token_id, MAX_PENDING_PER_TOKEN),
            )
        return approval_id, True

    async def _touch(
        self, token_id: str, db_last: int | None, now_ms: int, origin: Origin | None
    ) -> None:
        last = self._last_used.get(token_id, db_last)
        if last is not None and now_ms - last < LAST_USED_INTERVAL_MS:
            return
        self._last_used[token_id] = now_ms
        async with self._store._write_tx_lock:
            await self._write_conn().execute(
                "UPDATE account_tokens SET last_used_at = ?, last_used_ip = ?"
                " WHERE token_id = ?",
                (now_ms, origin.ip if origin else None, token_id),
            )

    # ----- approvals -----

    async def list_approvals(self, account_id: str) -> list[dict[str, Any]]:
        now_ms = int(time.time() * 1000)
        async with self._read().execute(
            "SELECT a.approval_id, a.token_id, t.label, a.ip, a.client, a.created_at,"
            " a.expires_at FROM network_approvals a JOIN account_tokens t"
            " ON t.token_id = a.token_id WHERE a.account_id = ? AND a.expires_at > ?"
            " ORDER BY a.created_at, a.approval_id",
            (account_id, now_ms),
        ) as cur:
            return [
                {
                    "approval_id": r[0],
                    "token_id": r[1],
                    "label": r[2],
                    "ip": r[3],
                    "client": r[4],
                    "created_at": r[5],
                    "expires_at": r[6],
                }
                async for r in cur
            ]

    async def _live_approval(
        self, account_id: str, approval_id: str, now_ms: int
    ) -> tuple[str, str, str]:
        """(token_id, label, ip) of a live approval; caller holds the write lock."""
        async with self._write_conn().execute(
            "SELECT a.token_id, t.label, a.ip FROM network_approvals a"
            " JOIN account_tokens t ON t.token_id = a.token_id"
            " WHERE a.approval_id = ? AND a.account_id = ? AND a.expires_at > ?",
            (approval_id, account_id, now_ms),
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            raise _approval_not_found(approval_id)
        return row[0], row[1], row[2]

    async def approve(
        self,
        account_id: str,
        approval_id: str,
        scope: Literal["ip", "network"],
        *,
        by: str = "self",
        client: str | None = None,
    ) -> AccountToken:
        if scope not in ("ip", "network"):
            raise ToolError(ErrorCode.INVALID_REQUEST, f"invalid approval scope: {scope!r}")
        # Lookup and mutation share one locked section, so a concurrent
        # approve/deny of the same approval cannot both succeed.
        async with self._store._write_tx_lock:
            conn = self._write_conn()
            token_id, label, ip_text = await self._live_approval(
                account_id, approval_id, int(time.time() * 1000)
            )
            try:
                ip = _parse_ip(ip_text)
            except ValueError as exc:
                raise ToolError(
                    ErrorCode.INVALID_REQUEST, "approval has an invalid address"
                ) from exc
            if scope == "ip":
                cidr = _host_cidr(ip)
            else:
                prefix = 24 if ip.version == 4 else 64
                cidr = str(ipaddress.ip_network(f"{ip}/{prefix}", strict=False))
            async with conn.execute(
                "SELECT approved_networks FROM account_tokens WHERE token_id = ?",
                (token_id,),
            ) as cur:
                trow = await cur.fetchone()
            if trow is None:
                raise _approval_not_found(approval_id)
            networks = json.loads(trow[0])
            if cidr not in networks:
                networks.append(cidr)
            await conn.execute(
                "UPDATE account_tokens SET approved_networks = ? WHERE token_id = ?",
                (json.dumps(networks), token_id),
            )
            await conn.execute(
                "DELETE FROM network_approvals WHERE approval_id = ?", (approval_id,)
            )
        await self._emit(
            account_id,
            "token.network_approved",
            {
                "token_id": token_id,
                "label": label,
                "ip": ip_text,
                "cidr": cidr,
                "by": by,
                "client": client,
            },
        )
        return await self._get_token(account_id, token_id)

    async def deny(
        self,
        account_id: str,
        approval_id: str,
        *,
        by: str = "self",
        client: str | None = None,
    ) -> None:
        async with self._store._write_tx_lock:
            token_id, label, ip_text = await self._live_approval(
                account_id, approval_id, int(time.time() * 1000)
            )
            await self._write_conn().execute(
                "DELETE FROM network_approvals WHERE approval_id = ?", (approval_id,)
            )
        await self._emit(
            account_id,
            "token.network_denied",
            {
                "token_id": token_id,
                "label": label,
                "ip": ip_text,
                "by": by,
                "client": client,
            },
        )

    # ----- connections -----

    def _write_conn(self) -> Any:
        assert self._store._write_conn is not None
        return self._store._write_conn

    def _read(self) -> Any:
        return self._store._read_pool[0]
