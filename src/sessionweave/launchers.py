"""Registry of connected agent launchers and the WebSocket they connect on.

A launcher is a small always-on service on a machine that declares which agent
profiles it can start. It connects to ``GET /v1/launchers/connect`` (WebSocket),
authenticates, sends a ``hello`` with its profiles and stays connected; it is
offline the moment the socket closes. The registry is in memory only and is
rebuilt from live launchers.

Workdir and start prompt never reach the broker: the launcher keeps them.

Protocol v2 (``"protocol": 2`` in ``hello``) adds a stable ``launcher_key``,
the launcher's start ``mode`` and ``ceiling``, the remote ``capabilities`` it
offers, its ``sandbox``, ``platform`` and ``prereqs``. A v2 launcher is
indexed by ``(owner key, launcher_key)``: a newer connection with the same key
replaces the old one (closed 4409), and its machine descriptor is kept in the
persistence DB (``machines.MachineStore``) when there is one.
"""
from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
import logging
import os
import posixpath
import re
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Protocol

from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect

from sessionweave.access import admin_matches, hash_token
from sessionweave.account_events import Origin, request_client
from sessionweave.errors import ErrorCode, ToolError

if TYPE_CHECKING:
    from sessionweave.launches import LaunchManager
    from sessionweave.machines import MachineStore
    from sessionweave.session import Session

log = logging.getLogger(__name__)

PING_INTERVAL_S = 20.0
HELLO_TIMEOUT_S = 10.0
_CLOSE_BAD_HELLO = 4400
_CLOSE_UNAUTHORIZED = 4401
_CLOSE_FORBIDDEN = 4403
_CLOSE_REPLACED = 4409
_CLOSE_LIMITED = 4429
_CLOSE_TOO_BIG = 1009
_CLOSE_INTERNAL = 1011
# Limits: launchers per owner account (account mode) and in all (open mode),
# and the largest frame a launcher may send.
MAX_LAUNCHERS_PER_ACCOUNT = 5
MAX_LAUNCHERS_OPEN = 50
MAX_FRAME_BYTES = 256 * 1024
# Failed launcher authentications are counted on the shared auth limiter
# under this pseudo session id, per source IP.
AUTH_LIMIT_KEY = "launcher"

_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_RUNTIMES = {"claude", "opencode", "cline"}
_PRESETS = {"reader", "operator"}
_VISIBILITIES = {"personal", "general"}
_MAX_PROFILES = 200
_MAX_ACCOUNTS = 100
_SOURCES = {"local", "remote"}
_KINDS = {"observer", "operator"}

# Launcher protocol v2: what the broker speaks (sent in ``welcome``).
PROTOCOL_VERSION = 2
# Persisted machines (v2 launchers with a key) per owner account, and in all
# for the open-mode owner key.
MAX_MACHINES_PER_ACCOUNT = 20
MAX_MACHINES_OPEN = 100
# Remote capabilities a launcher may advertise; unknown names are dropped.
KNOWN_CAPABILITIES = frozenset({"definitions", "browse", "processes"})
# Owner key of open-mode (ownerless) launchers, machines and definitions.
OPEN_OWNER = ""

_MODES = {"config", "local", "root"}
_KEY_RE = re.compile(r"^lk_[0-9a-f]{32}$")
_SANDBOX_RE = re.compile(r"^[a-z0-9_-]{1,32}$")
_PREREQ_STATES = {"ok", "installed", "missing", "wrong_version", "not_logged_in"}
_MAX_PREREQS = 32
_MAX_PREREQ_NAME = 32
_MAX_PREREQ_TEXT = 300
_MAX_CEILING = 4096
_MAX_PLATFORM = 32
_MAX_CAPABILITY_NAMES = 32


class LauncherConn(Protocol):
    async def send_json(self, data: Any) -> None: ...


class HelloError(ValueError):
    """The hello message as a whole is unusable."""


class LauncherLimitError(Exception):
    """Too many launchers connected for this owner (or in all)."""


class LauncherKeyInUseError(Exception):
    """A launcher key is already claimed and the hello did not present its
    ``key_secret``."""


# Close reason when a hello reuses a claimed launcher key without its secret.
KEY_IN_USE_REASON = "launcher_key_in_use"
_MAX_KEY_SECRET = 200


def new_key_secret() -> str:
    """A fresh per-key secret (sent once in ``welcome``; only its hash is kept)."""
    return "lks_" + secrets.token_urlsafe(32)


def key_secret_matches(presented: str | None, stored_hash: bytes | None) -> bool:
    """Constant-time check of a presented ``key_secret`` against a stored hash."""
    if presented is None or not stored_hash:
        return False
    return hmac.compare_digest(hash_token(presented), stored_hash)


def hello_key_secret(hello: dict[str, Any]) -> str | None:
    """The ``key_secret`` of a hello (None when absent); raises HelloError."""
    v = hello.get("key_secret")
    if v is not None and (not isinstance(v, str) or not 0 < len(v) <= _MAX_KEY_SECRET):
        raise HelloError("key_secret must be a non-empty string")
    return v


@dataclass(frozen=True)
class HelloV2:
    """The protocol-v2 part of a launcher's ``hello`` (defaults for v1)."""

    protocol: int
    launcher_key: str | None
    mode: str
    ceiling: str | None
    capabilities: frozenset[str]
    sandbox: str | None
    run_as: bool
    platform: str
    prereqs: tuple[dict[str, Any], ...]


LEGACY_HELLO = HelloV2(1, None, "config", None, frozenset(), None, False, "", ())


def _short_text(v: Any, limit: int = _MAX_PREREQ_TEXT) -> bool:
    return isinstance(v, str) and len(v) <= limit and v.isprintable()


def parse_prereqs(raw: Any) -> tuple[dict[str, Any], ...]:
    """Validate a launcher's prerequisite list; raise ValueError with the reason."""
    if not isinstance(raw, list) or len(raw) > _MAX_PREREQS:
        raise ValueError(f"prereqs must be a list of at most {_MAX_PREREQS} entries")
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("each prereq must be an object")
        name = item.get("name")
        if not isinstance(name, str) or not name or not _short_text(name, _MAX_PREREQ_NAME):
            raise ValueError(f"prereq name must be 1-{_MAX_PREREQ_NAME} printable chars")
        state = item.get("state")
        if state not in _PREREQ_STATES:
            raise ValueError(f"prereq state must be one of {sorted(_PREREQ_STATES)}")
        version, command = item.get("version"), item.get("command")
        for label, v in (("version", version), ("command", command)):
            if v is not None and not _short_text(v):
                raise ValueError(
                    f"prereq {label} must be null or at most {_MAX_PREREQ_TEXT} printable chars"
                )
        required = item.get("required", False)
        if not isinstance(required, bool):
            raise ValueError("prereq required must be a boolean")
        needed_by = item.get("needed_by", [])
        if (
            not isinstance(needed_by, list)
            or len(needed_by) > _MAX_PREREQS
            or not all(_short_text(n) for n in needed_by)
        ):
            raise ValueError("prereq needed_by must be a list of short strings")
        out.append({"name": name, "state": state, "version": version, "required": required,
                    "needed_by": list(needed_by), "command": command})
    return tuple(out)


def _ceiling(mode: str, raw: Any) -> str | None:
    if mode == "config":
        if raw is not None:
            raise HelloError("ceiling must be null in config mode")
        return None
    if mode == "root":
        if raw is not None and raw != "/":
            raise HelloError("ceiling must be '/' in root mode")
        return "/"
    if (
        not isinstance(raw, str)
        or not 0 < len(raw) <= _MAX_CEILING
        or "\x00" in raw
        or not raw.startswith("/")
        or raw.startswith("//")
        or posixpath.normpath(raw) != raw
    ):
        raise HelloError("ceiling must be a normalized absolute path in local mode")
    return raw


def parse_hello_v2(hello: dict[str, Any], *, deploy_allowed: bool) -> HelloV2:
    """Parse the v2 fields of ``hello`` (a hello without ``protocol`` >= 2 is
    a v1 launcher: config mode, no key, no capability).

    ``deploy_allowed=False`` strips the capabilities (open mode without a
    persistence DB). Raises ``HelloError``.
    """
    protocol = hello.get("protocol", 1)
    if not _is_int(protocol) or protocol < 1:
        raise HelloError("protocol must be a positive integer")
    if protocol < 2:
        return LEGACY_HELLO
    key = hello.get("launcher_key")
    if not isinstance(key, str) or not _KEY_RE.match(key):
        raise HelloError("launcher_key must be 'lk_' and 32 lowercase hex chars")
    mode = hello.get("mode", "config")
    if mode not in _MODES:
        raise HelloError(f"mode must be one of {sorted(_MODES)}")
    ceiling = _ceiling(mode, hello.get("ceiling"))
    caps_raw = hello.get("capabilities", [])
    if (
        not isinstance(caps_raw, list)
        or len(caps_raw) > _MAX_CAPABILITY_NAMES
        or not all(isinstance(c, str) and len(c) <= 64 for c in caps_raw)
    ):
        raise HelloError("capabilities must be a list of names")
    caps = frozenset(caps_raw) & KNOWN_CAPABILITIES
    if mode == "config" or not deploy_allowed:
        caps = frozenset()
    sandbox = hello.get("sandbox")
    if sandbox is not None and (not isinstance(sandbox, str) or not _SANDBOX_RE.match(sandbox)):
        raise HelloError("sandbox must be null or a short name")
    run_as = hello.get("run_as", False)
    if not isinstance(run_as, bool):
        raise HelloError("run_as must be a boolean")
    if run_as and mode != "root":
        raise HelloError("run_as is only valid in root mode")
    platform = hello.get("platform", "")
    if not _short_text(platform, _MAX_PLATFORM):
        raise HelloError(f"platform must be at most {_MAX_PLATFORM} printable chars")
    try:
        prereqs = parse_prereqs(hello.get("prereqs", []))
    except ValueError as exc:
        raise HelloError(str(exc)) from exc
    return HelloV2(protocol, key, mode, ceiling, caps, sandbox, run_as, platform, prereqs)


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def validate_profile(raw: Any) -> dict[str, Any]:
    """Return the normalized profile or raise ValueError with the reason."""
    if not isinstance(raw, dict):
        raise ValueError("profile must be an object")
    out: dict[str, Any] = {}
    for key in ("id", "domain", "role"):
        v = raw.get(key)
        if not isinstance(v, str) or not _ID_RE.match(v):
            raise ValueError(f"{key} must be 1-64 chars of [A-Za-z0-9._-]")
        out[key] = v
    caps = raw.get("capabilities", [])
    if (
        not isinstance(caps, list)
        or len(caps) > 32
        or not all(isinstance(c, str) and 0 < len(c) <= 64 for c in caps)
    ):
        raise ValueError("capabilities must be at most 32 non-empty strings of at most 64 chars")
    out["capabilities"] = list(caps)
    desc = raw.get("description", "")
    if not isinstance(desc, str) or len(desc) > 1000:
        raise ValueError("description must be a string of at most 1000 chars")
    out["description"] = desc
    if raw.get("runtime") not in _RUNTIMES:
        raise ValueError(f"runtime must be one of {sorted(_RUNTIMES)}")
    out["runtime"] = raw["runtime"]
    model = raw.get("model", "")
    if not isinstance(model, str) or len(model) > 200:
        raise ValueError("model must be a string of at most 200 chars")
    out["model"] = model
    access = raw.get("access")
    if not isinstance(access, dict) or access.get("preset") not in _PRESETS:
        raise ValueError(f"access.preset must be one of {sorted(_PRESETS)}")
    files, network = access.get("files"), access.get("network")
    if files is not None and not isinstance(files, dict):
        raise ValueError("access.files must be an object")
    if network is not None and not (
        network in ("none", "any")
        or (isinstance(network, list) and len(network) <= 100
            and all(isinstance(h, str) and 0 < len(h) <= 253 for h in network))
    ):
        raise ValueError("access.network must be 'none', 'any' or a list of hosts")
    out["access_preset"] = access["preset"]
    limits = raw.get("limits", {})
    if (
        not isinstance(limits, dict)
        or len(limits) > 10
        or not all(
            isinstance(k, str) and len(k) <= 64
            and (v is None or _is_int(v) or isinstance(v, float))
            for k, v in limits.items()
        )
    ):
        raise ValueError("limits must be an object of at most 10 numeric values")
    out["limits"] = dict(limits)
    mi = raw.get("max_instances")
    if not isinstance(mi, int) or isinstance(mi, bool) or not 1 <= mi <= 32:
        raise ValueError("max_instances must be an integer from 1 to 32")
    out["max_instances"] = mi
    # Per-profile visibility (account mode): overrides the launcher's when given.
    vis = raw.get("visibility")
    if vis is not None and vis not in _VISIBILITIES:
        raise ValueError("visibility must be 'personal' or 'general'")
    out["visibility"] = vis
    allowed = raw.get("allowed_accounts")
    if allowed is not None and not _account_names(allowed):
        raise ValueError("allowed_accounts must be a list of account names")
    out["allowed_accounts"] = (
        None if allowed is None else frozenset(a.casefold() for a in allowed)
    )
    # v2: where the profile comes from (the launcher's TOML or a remote
    # definition), its kind and whether it can coordinate a session.
    source = raw.get("source", "local")
    if source not in _SOURCES:
        raise ValueError("source must be 'local' or 'remote'")
    out["source"] = source
    kind = raw.get("kind")
    if kind is not None and kind not in _KINDS:
        raise ValueError("kind must be 'observer', 'operator' or null")
    out["kind"] = kind
    coord = raw.get("coordinator_capable", False)
    if not isinstance(coord, bool):
        raise ValueError("coordinator_capable must be a boolean")
    out["coordinator_capable"] = coord
    return out


def _account_names(v: Any) -> bool:
    return (
        isinstance(v, list)
        and len(v) <= _MAX_ACCOUNTS
        and all(isinstance(a, str) and 0 < len(a) <= 200 for a in v)
    )


@dataclass(frozen=True)
class _Auth:
    """Who a launcher connection authenticated as (account mode)."""

    owner_account_id: str | None = None
    owner_name: str | None = None
    token_id: str | None = None
    expires_at_ms: int | None = None


@dataclass
class _Launcher:
    launcher_id: str
    conn: LauncherConn
    owner_account_id: str | None
    machine: str
    visibility: str
    allowed_accounts: frozenset[str]  # casefolded account names
    profiles: dict[str, dict[str, Any]] = field(default_factory=dict)
    running: dict[str, int] = field(default_factory=dict)
    # The account token it authenticated with and its address (account
    # mode), so it can be dropped when that token or account dies.
    token_id: str | None = None
    ip: str = ""
    # sha256 of the resume secret sent in welcome (the secret is not kept).
    resume_hash: bytes = b""
    # The owner account's name (account mode), shown in discovery.
    owner_name: str | None = None
    # When the account token it connected with expires (ms), if it does.
    expires_at_ms: int | None = None
    # Protocol v2 fields of its hello (LEGACY_HELLO for a v1 launcher).
    v2: HelloV2 = LEGACY_HELLO
    # sha256 of its launcher key's secret (used without a persistence DB).
    key_hash: bytes = b""


def owner_key(lnc: _Launcher) -> str:
    """The key machines and definitions are stored under: the owner account,
    or ``OPEN_OWNER`` for an open-mode launcher."""
    return lnc.owner_account_id or OPEN_OWNER


def _load_profiles(
    raw_profiles: list[Any],
) -> tuple[dict[str, dict[str, Any]], list[dict[str, str]]]:
    profiles: dict[str, dict[str, Any]] = {}
    rejected: list[dict[str, str]] = []
    for raw in raw_profiles:
        pid = raw.get("id") if isinstance(raw, dict) else None
        label = pid if isinstance(pid, str) else ""
        try:
            prof = validate_profile(raw)
            if prof["id"] in profiles:
                raise ValueError("duplicate profile id")
        except ValueError as exc:
            rejected.append({"id": label[:64], "reason": str(exc)})
            continue
        profiles[prof["id"]] = prof
    return profiles, rejected


class LauncherRegistry:
    def __init__(self) -> None:
        self._launchers: dict[str, _Launcher] = {}
        # Set by ToolDeps: told when a launcher goes away.
        self.launches: LaunchManager | None = None

    def register(
        self,
        conn: LauncherConn,
        owner_account_id: str | None,
        hello: dict[str, Any],
        *,
        token_id: str | None = None,
        ip: str = "",
        resume_hash: bytes = b"",
        owner_name: str | None = None,
        expires_at_ms: int | None = None,
        v2: HelloV2 | None = None,
    ) -> tuple[str, list[dict[str, str]]]:
        """Register a launcher from a validated-shape hello.

        ``v2`` is the parsed v2 part of the hello; when omitted it is parsed
        here, with capabilities kept only for an account-mode launcher.

        Returns ``(launcher_id, rejected)`` where ``rejected`` lists the
        invalid or duplicate profiles as ``{"id", "reason"}``. Raises
        ``LauncherLimitError`` when the owner (or, in open mode, the broker)
        already has the maximum number of launchers. A connection holding
        the same launcher key is not counted: the caller replaces it.
        """
        if v2 is None:
            v2 = parse_hello_v2(hello, deploy_allowed=owner_account_id is not None)
        owner = owner_account_id or OPEN_OWNER
        others = [
            lnc for lnc in self._launchers.values()
            if v2.launcher_key is None
            or lnc.v2.launcher_key != v2.launcher_key
            or owner_key(lnc) != owner
        ]
        if owner_account_id is not None:
            owned = sum(1 for lnc in others if lnc.owner_account_id == owner_account_id)
            if owned >= MAX_LAUNCHERS_PER_ACCOUNT:
                raise LauncherLimitError(
                    f"at most {MAX_LAUNCHERS_PER_ACCOUNT} launchers per account"
                )
        elif len(others) >= MAX_LAUNCHERS_OPEN:
            raise LauncherLimitError(f"at most {MAX_LAUNCHERS_OPEN} launchers on this broker")
        machine = hello.get("machine")
        if not isinstance(machine, str) or not machine.strip() or len(machine) > 100:
            raise HelloError("machine must be a non-empty string of at most 100 chars")
        visibility = hello.get("visibility", "personal")
        if visibility not in _VISIBILITIES:
            raise HelloError("visibility must be 'personal' or 'general'")
        allowed = hello.get("allowed_accounts", [])
        if not _account_names(allowed):
            raise HelloError("allowed_accounts must be a list of account names")
        running = hello.get("running", [])
        if (
            not isinstance(running, list)
            or len(running) > 1000
            or not all(isinstance(r, str) and 0 < len(r) <= 64 for r in running)
        ):
            raise HelloError("running must be a list of launch ids")
        secret = hello.get("resume_secret")
        if secret is not None and (not isinstance(secret, str) or len(secret) > 200):
            raise HelloError("resume_secret must be a string")
        raw_profiles = hello.get("profiles", [])
        if not isinstance(raw_profiles, list) or len(raw_profiles) > _MAX_PROFILES:
            raise HelloError(f"profiles must be a list of at most {_MAX_PROFILES} entries")
        launcher_id = "lnc_" + secrets.token_hex(8)
        lnc = _Launcher(
            launcher_id, conn, owner_account_id, machine.strip(), visibility,
            frozenset(a.casefold() for a in allowed), token_id=token_id, ip=ip,
            resume_hash=resume_hash, owner_name=owner_name, expires_at_ms=expires_at_ms,
            v2=v2,
        )
        lnc.profiles, rejected = _load_profiles(raw_profiles)
        self._launchers[launcher_id] = lnc
        return launcher_id, rejected

    def get(self, launcher_id: str) -> _Launcher | None:
        return self._launchers.get(launcher_id)

    def by_key(self, owner: str, launcher_key: str) -> _Launcher | None:
        """The connected launcher of ``owner`` (an account id or
        ``OPEN_OWNER``) with this launcher key, if any (the newest one)."""
        found: _Launcher | None = None
        for lnc in self._launchers.values():
            if lnc.v2.launcher_key == launcher_key and owner_key(lnc) == owner:
                found = lnc
        return found

    def for_account(self, owner: str) -> list[_Launcher]:
        """Connected launchers of ``owner``; ``OPEN_OWNER`` = every ownerless one."""
        return [lnc for lnc in self._launchers.values() if owner_key(lnc) == owner]

    def replace_profiles(self, launcher_id: str, raw: Any) -> list[dict[str, str]]:
        """Replace the published profiles of a launcher (a ``profiles``
        frame); returns the rejected ones. A list that is not a list of at
        most ``_MAX_PROFILES`` entries changes nothing."""
        lnc = self._launchers.get(launcher_id)
        if lnc is None:
            return []
        if not isinstance(raw, list) or len(raw) > _MAX_PROFILES:
            return [{"id": "", "reason":
                     f"profiles must be a list of at most {_MAX_PROFILES} entries"}]
        lnc.profiles, rejected = _load_profiles(raw)
        return rejected

    def claim_key(self, launcher_id: str, presented: str | None) -> str | None:
        """Claim the launcher key of ``launcher_id`` without a persistence DB.

        While another connection of the same owner holds the key, the hello
        must present that connection's ``key_secret`` (else
        ``LauncherKeyInUseError``, nothing changed); the secret is kept and
        None returned. A key nobody holds gets a new secret, returned so it
        is sent in ``welcome`` (only its hash is kept).
        """
        lnc = self._launchers[launcher_id]
        key, owner = lnc.v2.launcher_key, owner_key(lnc)
        holders = [
            o for o in self._launchers.values()
            if o.launcher_id != launcher_id and o.v2.launcher_key == key
            and owner_key(o) == owner
        ]
        if holders:
            if not all(key_secret_matches(presented, o.key_hash) for o in holders):
                raise LauncherKeyInUseError(KEY_IN_USE_REASON)
            lnc.key_hash = holders[0].key_hash
            return None
        secret = new_key_secret()
        lnc.key_hash = hash_token(secret)
        return secret

    def set_prereqs(self, launcher_id: str, prereqs: tuple[dict[str, Any], ...]) -> None:
        lnc = self._launchers.get(launcher_id)
        if lnc is not None:
            lnc.v2 = replace(lnc.v2, prereqs=prereqs)

    def unregister(self, launcher_id: str) -> None:
        """Forget a launcher; its launches not yet joined fail (idempotent)."""
        if self._launchers.pop(launcher_id, None) is not None and self.launches is not None:
            self.launches.launcher_gone(launcher_id)

    def resume_hash(self, launcher_id: str) -> bytes | None:
        lnc = self._launchers.get(launcher_id)
        return None if lnc is None else lnc.resume_hash

    def hold_slot(self, launcher_id: str, profile_id: str) -> bool:
        """Count one more running instance (a re-associated agent)."""
        lnc = self._launchers.get(launcher_id)
        if lnc is None:
            return False
        lnc.running[profile_id] = lnc.running.get(profile_id, 0) + 1
        return True

    def release_slot(self, launcher_id: str, profile_id: str) -> None:
        lnc = self._launchers.get(launcher_id)
        if lnc is not None and lnc.running.get(profile_id, 0) > 0:
            lnc.running[profile_id] -= 1

    def select(
        self,
        session: Session,
        owner_name: str | None,
        *,
        launcher_id: str | None = None,
        profile_id: str | None = None,
        domain: str | None = None,
        role: str | None = None,
    ) -> tuple[_Launcher, dict[str, Any]]:
        """The launcher and profile to launch: the named one, or the first
        online launcher with a free slot for ``domain`` + ``role``.

        Without ``launcher_id`` only launchers of the session owner's account
        (or open-mode launchers) are candidates: another account's ``general``
        launcher is used only when named explicitly.

        Raises PROFILE_NOT_FOUND (nothing visible matches) or NO_FREE_SLOT.
        """
        session_owner = session.auth.owner_account_id
        matches: list[tuple[_Launcher, dict[str, Any]]] = []
        for lnc in self._launchers.values():
            if launcher_id is not None and lnc.launcher_id != launcher_id:
                continue
            if (
                launcher_id is None
                and lnc.owner_account_id is not None
                and lnc.owner_account_id != session_owner
            ):
                continue
            for p in lnc.profiles.values():
                if profile_id is not None and p["id"] != profile_id:
                    continue
                if domain is not None and p["domain"].casefold() != domain.casefold():
                    continue
                if role is not None and p["role"].casefold() != role.casefold():
                    continue
                if not self._visible(lnc, session, owner_name, p):
                    continue
                matches.append((lnc, p))
        if not matches:
            raise ToolError(
                ErrorCode.PROFILE_NOT_FOUND,
                "no online launcher visible to this session offers that profile "
                "(see agents_discover)",
            )
        for lnc, p in matches:
            if lnc.running.get(p["id"], 0) < p["max_instances"]:
                return lnc, p
        raise ToolError(
            ErrorCode.NO_FREE_SLOT, "every matching profile is running its max_instances"
        )

    async def close_for_account(
        self,
        account_id: str,
        token_id: str | None = None,
        ip_filter: Callable[[str], bool] | None = None,
        *,
        reason: str = "account token revoked",
    ) -> int:
        """Drop the launchers of ``account_id`` (only those connected with
        ``token_id``, and only from addresses ``ip_filter`` accepts): close
        their socket with 4403 and unregister them, failing their launches
        not yet joined with ``launcher_offline``."""
        hits = [
            lnc for lnc in self._launchers.values()
            if lnc.owner_account_id == account_id
            and (token_id is None or lnc.token_id == token_id)
            and (ip_filter is None or ip_filter(lnc.ip))
        ]
        for lnc in hits:
            self.unregister(lnc.launcher_id)
            close = getattr(lnc.conn, "close", None)
            if close is not None:
                with contextlib.suppress(Exception):
                    await close(code=_CLOSE_FORBIDDEN, reason=reason[:100])
        return len(hits)

    def get_conn(self, launcher_id: str) -> LauncherConn | None:
        lnc = self._launchers.get(launcher_id)
        return lnc.conn if lnc is not None else None

    @staticmethod
    def _visible(
        lnc: _Launcher, session: Session, owner_name: str | None, prof: dict[str, Any]
    ) -> bool:
        """Whether ``session`` may see profile ``prof`` of ``lnc``: the
        profile's own visibility/allowed_accounts when set, else the
        launcher's."""
        if lnc.owner_account_id is None:
            return True  # open mode: the private network is the protection
        owner_id = session.auth.owner_account_id
        if owner_id is not None and owner_id == lnc.owner_account_id:
            return True
        visibility = prof.get("visibility") or lnc.visibility
        if visibility == "personal":
            return False
        allowed = prof.get("allowed_accounts")
        if allowed is None:
            allowed = lnc.allowed_accounts
        if not allowed:
            return True  # general, open to every account (and to ownerless sessions)
        # A session without an owner account (admin-created) matches no list.
        return (owner_id is not None and owner_name is not None
                and owner_name.casefold() in allowed)

    @staticmethod
    def owner_fields(lnc_owner: str | None, lnc_owner_name: str | None,
                     session: Session) -> dict[str, Any]:
        """``owner`` (the launcher owner's account name, None in open mode)
        and ``own`` (it belongs to the session owner's account, or it is an
        ownerless open-mode launcher, which auto-select also treats as own)."""
        return {
            "owner": lnc_owner_name,
            "own": lnc_owner is None or lnc_owner == session.auth.owner_account_id,
        }

    def profiles_for(
        self,
        session: Session,
        domain: str,
        role: str | None = None,
        capability: str | None = None,
        *,
        owner_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """Profiles of ``domain`` visible to ``session`` (``owner_name`` is the
        session owner's account name, used for ``allowed_accounts``)."""
        out: list[dict[str, Any]] = []
        for lnc in self._launchers.values():
            for p in lnc.profiles.values():
                if p["domain"].casefold() != domain.casefold():
                    continue
                if not self._visible(lnc, session, owner_name, p):
                    continue
                if role is not None and p["role"].casefold() != role.casefold():
                    continue
                if capability is not None and capability.casefold() not in (
                    c.casefold() for c in p["capabilities"]
                ):
                    continue
                out.append({
                    "launcher_id": lnc.launcher_id,
                    "machine": lnc.machine,
                    **self.owner_fields(lnc.owner_account_id, lnc.owner_name, session),
                    "profile_id": p["id"],
                    "domain": p["domain"],
                    "role": p["role"],
                    "capabilities": list(p["capabilities"]),
                    "description": p["description"],
                    "runtime": p["runtime"],
                    "model": p["model"],
                    "access_preset": p["access_preset"],
                    "limits": dict(p["limits"]),
                    "source": p["source"],
                    "kind": p["kind"],
                    "coordinator_capable": p["coordinator_capable"],
                    "online": True,
                    "free_slots": max(0, p["max_instances"] - lnc.running.get(p["id"], 0)),
                })
        # The session owner's own launchers first (stable otherwise).
        out.sort(key=lambda item: not item["own"])
        return out

    def domains_for(self, session: Session, *, owner_name: str | None = None) -> list[str]:
        seen: dict[str, str] = {}
        for lnc in self._launchers.values():
            for p in lnc.profiles.values():
                if self._visible(lnc, session, owner_name, p):
                    seen.setdefault(p["domain"].casefold(), p["domain"])
        return sorted(seen.values(), key=str.casefold)


class FrameTooLargeError(Exception):
    """A launcher frame over MAX_FRAME_BYTES."""


def _frame_bytes(msg: Any) -> bytes:
    text = msg.get("text")
    if text is not None:
        return str(text).encode("utf-8", "surrogatepass")
    return bytes(msg.get("bytes") or b"")


async def _read_hello(websocket: WebSocket) -> dict[str, Any]:
    try:
        msg = await asyncio.wait_for(websocket.receive(), HELLO_TIMEOUT_S)
    except (TimeoutError, WebSocketDisconnect, RuntimeError) as exc:
        raise HelloError("expected a hello message") from exc
    if msg.get("type") == "websocket.disconnect":
        raise HelloError("expected a hello message")
    raw = _frame_bytes(msg)
    if len(raw) > MAX_FRAME_BYTES:
        raise FrameTooLargeError
    try:
        body = json.loads(raw)
    except ValueError as exc:
        raise HelloError("expected a hello message") from exc
    if not isinstance(body, dict) or body.get("type") != "hello":
        raise HelloError("first message must be type 'hello'")
    return body


async def _ping_loop(websocket: WebSocket, expires_at_ms: int | None = None) -> None:
    """Ping every PING_INTERVAL_S; close with 4401 once the account token the
    launcher connected with expires (checked at least every ping)."""
    while True:
        delay = PING_INTERVAL_S
        if expires_at_ms is not None:
            left = expires_at_ms / 1000 - time.time()
            if left <= 0:
                await websocket.close(code=_CLOSE_UNAUTHORIZED, reason="token expired")
                return
            delay = min(delay, left)
        await asyncio.sleep(delay)
        if expires_at_ms is not None and time.time() * 1000 >= expires_at_ms:
            continue  # closed at the top of the loop
        await websocket.send_json({"type": "ping"})


async def _authenticate(websocket: WebSocket, deps: Any) -> _Auth | None:
    """Check the launcher's credential; on refusal the socket is closed and
    None is returned."""
    header = websocket.headers.get("authorization") or ""
    token = header[7:].strip() if header.lower().startswith("bearer ") else ""
    source_ip = (websocket.client.host if websocket.client is not None else "") or ""
    limiter = deps.auth_limiter

    async def refuse(reason: str) -> None:
        # A valid credential always passes; failures are counted per IP.
        if limiter.is_throttled(AUTH_LIMIT_KEY, source_ip):
            await websocket.close(code=_CLOSE_LIMITED, reason="too many failed attempts")
        else:
            limiter.record_failure(AUTH_LIMIT_KEY, source_ip)
            await websocket.close(code=_CLOSE_UNAUTHORIZED, reason=reason)
        return None

    if deps.session_creation == "account":
        if deps.accounts is None:  # fail closed
            await websocket.close(code=_CLOSE_INTERNAL, reason="accounts unavailable")
            return None
        if not token:
            await refuse("missing token")
            return None
        res = await deps.accounts.authenticate(
            token, int(time.time() * 1000), Origin(source_ip, request_client.get())
        )
        if res.account is None:
            if res.refusal == "network":
                await websocket.close(code=_CLOSE_FORBIDDEN, reason="network not approved")
                return None
            await refuse("invalid token")
            return None
        return _Auth(res.account.account_id, res.account.name, res.token_id, res.expires_at)
    required = websocket.app.state.launcher_token
    if required is not None and not admin_matches(token, required):
        await refuse("invalid token")
        return None
    return _Auth()


async def launcher_ws(websocket: WebSocket) -> None:
    deps = websocket.app.state.tool_deps
    registry: LauncherRegistry = deps.launchers
    await websocket.accept()
    auth = await _authenticate(websocket, deps)
    if auth is None:
        return
    owner_account_id, token_id = auth.owner_account_id, auth.token_id
    source_ip = (websocket.client.host if websocket.client is not None else "") or ""

    machines: MachineStore | None = deps.machines
    try:
        hello = await _read_hello(websocket)
        # Remote capabilities need the DB that keeps the deployed definitions:
        # always there in account mode, optional in open mode.
        v2 = parse_hello_v2(
            hello, deploy_allowed=owner_account_id is not None or machines is not None
        )
        presented_secret = hello_key_secret(hello)
        # Lets this launcher reclaim its running agents after a reconnect;
        # only its hash is kept, and it is never logged.
        resume_secret = secrets.token_urlsafe(32)
        launcher_id, rejected = registry.register(
            websocket, owner_account_id, hello, token_id=token_id, ip=source_ip,
            resume_hash=hash_token(resume_secret), owner_name=auth.owner_name,
            expires_at_ms=auth.expires_at_ms, v2=v2,
        )
    except FrameTooLargeError:
        await websocket.close(code=_CLOSE_TOO_BIG, reason="frame too large")
        return
    except HelloError as exc:
        await websocket.close(code=_CLOSE_BAD_HELLO, reason=str(exc)[:100])
        return
    except LauncherLimitError as exc:
        await websocket.close(code=_CLOSE_LIMITED, reason=str(exc)[:100])
        return
    lnc = registry.get(launcher_id)
    assert lnc is not None
    owner = owner_key(lnc)
    # Proof of possession of the launcher key: its first registration gets a
    # secret (sent once in welcome); a later hello with the key must present
    # it, else nothing is replaced or updated. Never logged.
    issued_secret: str | None = None
    if v2.launcher_key is not None:
        from sessionweave.machines import MachineLimitError

        try:
            if machines is not None:
                _machine, issued_secret = await machines.claim_machine(
                    owner, lnc.machine, v2, int(time.time() * 1000),
                    key_secret=presented_secret, token_id=token_id,
                )
            else:
                issued_secret = registry.claim_key(launcher_id, presented_secret)
        except LauncherKeyInUseError:
            registry.unregister(launcher_id)
            log.info("launcher key in use: refused a connection without its secret")
            await websocket.close(code=_CLOSE_REPLACED, reason=KEY_IN_USE_REASON)
            return
        except MachineLimitError:
            registry.unregister(launcher_id)
            await websocket.close(
                code=_CLOSE_LIMITED, reason="too many machines; forget one in the Cockpit"
            )
            return
        except Exception:
            registry.unregister(launcher_id)
            log.exception("could not record the launcher's machine")
            await websocket.close(code=_CLOSE_INTERNAL, reason="internal error")
            return
    stops: list[dict[str, Any]] = []
    if deps.launches is not None and hello.get("running"):
        stops = deps.launches.reattach(
            launcher_id, owner_account_id, token_id, hello["machine"].strip(),
            list(hello["running"]), hello.get("resume_secret"),
        )
    if v2.launcher_key is not None:
        # The same launcher reconnected (its old socket may be half-open):
        # its running agents moved over above; drop the old connection.
        for old in registry.for_account(owner):
            if old.launcher_id != launcher_id and old.v2.launcher_key == v2.launcher_key:
                registry.unregister(old.launcher_id)
                close = getattr(old.conn, "close", None)
                if close is not None:
                    with contextlib.suppress(Exception):
                        await close(code=_CLOSE_REPLACED, reason="replaced by a newer connection")

    pinger: asyncio.Task[None] | None = None
    receiver: asyncio.Task[None] | None = None
    try:
        # ``account``: the owner account's name (None in open mode), so the
        # launcher can double-check the visibility of each order.
        await websocket.send_json(
            {"type": "welcome", "launcher_id": launcher_id, "rejected": rejected,
             "resume_secret": resume_secret, "account": auth.owner_name,
             "protocol": PROTOCOL_VERSION, "key_secret": issued_secret}
        )
        for stop in stops:
            await websocket.send_json(stop)
        pinger = asyncio.create_task(_ping_loop(websocket, auth.expires_at_ms))
        receiver = asyncio.create_task(_receive(websocket, launcher_id, deps))
        await asyncio.wait({pinger, receiver}, return_when=asyncio.FIRST_COMPLETED)
    except (WebSocketDisconnect, RuntimeError):
        pass
    except Exception:
        log.exception("launcher connection crashed")
    finally:
        for task in (pinger, receiver):
            if task is None:
                continue
            if not task.done():
                task.cancel()
            with contextlib.suppress(BaseException):
                # Retrieve the outcome (a failed ping send is the normal way a
                # dead socket is noticed) so it is never reported unretrieved.
                await task
        registry.unregister(launcher_id)
        with contextlib.suppress(Exception):
            await websocket.close()


async def _receive(websocket: WebSocket, launcher_id: str, deps: Any) -> None:
    """Handle launcher frames (launch results, stops, profiles, prereqs)
    until the socket closes."""
    launches: LaunchManager | None = deps.launches
    registry: LauncherRegistry = deps.launchers
    with contextlib.suppress(WebSocketDisconnect, RuntimeError):
        while True:
            msg = await websocket.receive()
            if msg.get("type") == "websocket.disconnect":
                return
            raw = _frame_bytes(msg)
            if len(raw) > MAX_FRAME_BYTES:
                await websocket.close(code=_CLOSE_TOO_BIG, reason="frame too large")
                return
            try:
                body = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(body, dict):
                continue
            kind = body.get("type")
            if kind == "profiles":
                rejected = registry.replace_profiles(launcher_id, body.get("profiles"))
                if rejected:
                    log.info("launcher %s: profiles rejected: %s", launcher_id, rejected)
            elif kind == "prereqs":
                await _on_prereqs(registry, deps.machines, launcher_id, body.get("prereqs"))
            elif launches is None:
                continue
            elif kind == "launch_result":
                launches.on_result(launcher_id, body)
            elif kind == "stopped":
                launches.on_stopped(launcher_id, body)


async def _on_prereqs(
    registry: LauncherRegistry, machines: MachineStore | None, launcher_id: str, raw: Any
) -> None:
    """A later prerequisite report (e.g. after an install): an invalid list
    is ignored; a valid one updates the registry and the machine row."""
    try:
        prereqs = parse_prereqs(raw)
    except ValueError as exc:
        log.info("launcher %s: prereqs ignored: %s", launcher_id, exc)
        return
    registry.set_prereqs(launcher_id, prereqs)
    lnc = registry.get(launcher_id)
    if machines is None or lnc is None or lnc.v2.launcher_key is None:
        return
    try:
        await machines.set_prereqs(owner_key(lnc), lnc.v2.launcher_key, list(prereqs))
    except Exception:
        log.exception("could not record the launcher's prereqs")


def launcher_token_from_env() -> str | None:
    return os.environ.get("SW_LAUNCHER_TOKEN", "").strip() or None


def routes() -> list[WebSocketRoute]:
    return [WebSocketRoute("/v1/launchers/connect", launcher_ws)]
