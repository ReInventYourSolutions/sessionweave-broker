"""Registry of connected agent launchers and the WebSocket they connect on.

A launcher is a small always-on service on a machine that declares which agent
profiles it can start. It connects to ``GET /v1/launchers/connect`` (WebSocket),
authenticates, sends a ``hello`` with its profiles and stays connected; it is
offline the moment the socket closes. The registry is in memory only and is
rebuilt from live launchers.

Workdir and start prompt never reach the broker: the launcher keeps them.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect

from sessionweave.access import admin_matches, hash_token
from sessionweave.account_events import Origin, request_client
from sessionweave.errors import ErrorCode, ToolError

if TYPE_CHECKING:
    from sessionweave.launches import LaunchManager
    from sessionweave.session import Session

log = logging.getLogger(__name__)

PING_INTERVAL_S = 20.0
HELLO_TIMEOUT_S = 10.0
_CLOSE_BAD_HELLO = 4400
_CLOSE_UNAUTHORIZED = 4401
_CLOSE_FORBIDDEN = 4403
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


class LauncherConn(Protocol):
    async def send_json(self, data: Any) -> None: ...


class HelloError(ValueError):
    """The hello message as a whole is unusable."""


class LauncherLimitError(Exception):
    """Too many launchers connected for this owner (or in all)."""


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
    return out


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
    ) -> tuple[str, list[dict[str, str]]]:
        """Register a launcher from a validated-shape hello.

        Returns ``(launcher_id, rejected)`` where ``rejected`` lists the
        invalid or duplicate profiles as ``{"id", "reason"}``. Raises
        ``LauncherLimitError`` when the owner (or, in open mode, the broker)
        already has the maximum number of launchers.
        """
        if owner_account_id is not None:
            owned = sum(
                1 for lnc in self._launchers.values() if lnc.owner_account_id == owner_account_id
            )
            if owned >= MAX_LAUNCHERS_PER_ACCOUNT:
                raise LauncherLimitError(
                    f"at most {MAX_LAUNCHERS_PER_ACCOUNT} launchers per account"
                )
        elif len(self._launchers) >= MAX_LAUNCHERS_OPEN:
            raise LauncherLimitError(f"at most {MAX_LAUNCHERS_OPEN} launchers on this broker")
        machine = hello.get("machine")
        if not isinstance(machine, str) or not machine.strip() or len(machine) > 100:
            raise HelloError("machine must be a non-empty string of at most 100 chars")
        visibility = hello.get("visibility", "personal")
        if visibility not in _VISIBILITIES:
            raise HelloError("visibility must be 'personal' or 'general'")
        allowed = hello.get("allowed_accounts", [])
        if (
            not isinstance(allowed, list)
            or len(allowed) > _MAX_ACCOUNTS
            or not all(isinstance(a, str) and 0 < len(a) <= 200 for a in allowed)
        ):
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
            resume_hash=resume_hash,
        )
        rejected: list[dict[str, str]] = []
        for raw in raw_profiles:
            pid = raw.get("id") if isinstance(raw, dict) else None
            label = pid if isinstance(pid, str) else ""
            try:
                prof = validate_profile(raw)
                if prof["id"] in lnc.profiles:
                    raise ValueError("duplicate profile id")
            except ValueError as exc:
                rejected.append({"id": label[:64], "reason": str(exc)})
                continue
            lnc.profiles[prof["id"]] = prof
        self._launchers[launcher_id] = lnc
        return launcher_id, rejected

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
            if not self._visible(lnc, session, owner_name):
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

    def _visible(self, lnc: _Launcher, session: Session, owner_name: str | None) -> bool:
        if lnc.owner_account_id is None:
            return True  # open mode: the private network is the protection
        owner_id = session.auth.owner_account_id
        if owner_id is None:
            return False
        if lnc.visibility == "personal":
            return owner_id == lnc.owner_account_id
        if owner_id == lnc.owner_account_id:
            return True
        if not lnc.allowed_accounts:
            return True
        return owner_name is not None and owner_name.casefold() in lnc.allowed_accounts

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
            if not self._visible(lnc, session, owner_name):
                continue
            for p in lnc.profiles.values():
                if p["domain"].casefold() != domain.casefold():
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
                    "profile_id": p["id"],
                    "domain": p["domain"],
                    "role": p["role"],
                    "capabilities": list(p["capabilities"]),
                    "description": p["description"],
                    "runtime": p["runtime"],
                    "model": p["model"],
                    "access_preset": p["access_preset"],
                    "limits": dict(p["limits"]),
                    "online": True,
                    "free_slots": max(0, p["max_instances"] - lnc.running.get(p["id"], 0)),
                })
        return out

    def domains_for(self, session: Session, *, owner_name: str | None = None) -> list[str]:
        seen: dict[str, str] = {}
        for lnc in self._launchers.values():
            if self._visible(lnc, session, owner_name):
                for p in lnc.profiles.values():
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


async def _ping_loop(websocket: WebSocket) -> None:
    while True:
        await asyncio.sleep(PING_INTERVAL_S)
        await websocket.send_json({"type": "ping"})


async def _authenticate(websocket: WebSocket, deps: Any) -> tuple[bool, str | None, str | None]:
    """Check the launcher's credential; on refusal the socket is closed.

    Returns ``(ok, owner_account_id, account_token_id)``.
    """
    header = websocket.headers.get("authorization") or ""
    token = header[7:].strip() if header.lower().startswith("bearer ") else ""
    source_ip = (websocket.client.host if websocket.client is not None else "") or ""
    limiter = deps.auth_limiter

    async def refuse(reason: str) -> tuple[bool, None, None]:
        # A valid credential always passes; failures are counted per IP.
        if limiter.is_throttled(AUTH_LIMIT_KEY, source_ip):
            await websocket.close(code=_CLOSE_LIMITED, reason="too many failed attempts")
        else:
            limiter.record_failure(AUTH_LIMIT_KEY, source_ip)
            await websocket.close(code=_CLOSE_UNAUTHORIZED, reason=reason)
        return False, None, None

    if deps.session_creation == "account":
        if deps.accounts is None:  # fail closed
            await websocket.close(code=_CLOSE_INTERNAL, reason="accounts unavailable")
            return False, None, None
        if not token:
            return await refuse("missing token")
        res = await deps.accounts.authenticate(
            token, int(time.time() * 1000), Origin(source_ip, request_client.get())
        )
        if res.account is None:
            if res.refusal == "network":
                await websocket.close(code=_CLOSE_FORBIDDEN, reason="network not approved")
                return False, None, None
            return await refuse("invalid token")
        return True, res.account.account_id, res.token_id
    required = websocket.app.state.launcher_token
    if required is not None and not admin_matches(token, required):
        return await refuse("invalid token")
    return True, None, None


async def launcher_ws(websocket: WebSocket) -> None:
    deps = websocket.app.state.tool_deps
    registry: LauncherRegistry = deps.launchers
    await websocket.accept()
    ok, owner_account_id, token_id = await _authenticate(websocket, deps)
    if not ok:
        return
    source_ip = (websocket.client.host if websocket.client is not None else "") or ""

    try:
        hello = await _read_hello(websocket)
        # Lets this launcher reclaim its running agents after a reconnect;
        # only its hash is kept, and it is never logged.
        resume_secret = secrets.token_urlsafe(32)
        launcher_id, rejected = registry.register(
            websocket, owner_account_id, hello, token_id=token_id, ip=source_ip,
            resume_hash=hash_token(resume_secret),
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
    stops: list[dict[str, Any]] = []
    if deps.launches is not None and hello.get("running"):
        stops = deps.launches.reattach(
            launcher_id, owner_account_id, token_id, hello["machine"].strip(),
            list(hello["running"]), hello.get("resume_secret"),
        )

    pinger: asyncio.Task[None] | None = None
    receiver: asyncio.Task[None] | None = None
    try:
        await websocket.send_json(
            {"type": "welcome", "launcher_id": launcher_id, "rejected": rejected,
             "resume_secret": resume_secret}
        )
        for stop in stops:
            await websocket.send_json(stop)
        pinger = asyncio.create_task(_ping_loop(websocket))
        receiver = asyncio.create_task(_receive(websocket, launcher_id, deps.launches))
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


async def _receive(websocket: WebSocket, launcher_id: str, launches: LaunchManager | None) -> None:
    """Handle launcher frames (launch results, stops) until the socket closes."""
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
            if not isinstance(body, dict) or launches is None:
                continue
            kind = body.get("type")
            if kind == "launch_result":
                launches.on_result(launcher_id, body)
            elif kind == "stopped":
                launches.on_stopped(launcher_id, body)


def launcher_token_from_env() -> str | None:
    return os.environ.get("SW_LAUNCHER_TOKEN", "").strip() or None


def routes() -> list[WebSocketRoute]:
    return [WebSocketRoute("/v1/launchers/connect", launcher_ws)]
