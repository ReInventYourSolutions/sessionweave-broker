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
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect

from sessionweave.access import admin_matches
from sessionweave.account_events import Origin, request_client

if TYPE_CHECKING:
    from sessionweave.session import Session

log = logging.getLogger(__name__)

PING_INTERVAL_S = 20.0
HELLO_TIMEOUT_S = 10.0
_CLOSE_BAD_HELLO = 4400
_CLOSE_UNAUTHORIZED = 4401
_CLOSE_FORBIDDEN = 4403

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


class LauncherRegistry:
    def __init__(self) -> None:
        self._launchers: dict[str, _Launcher] = {}

    def register(
        self, conn: LauncherConn, owner_account_id: str | None, hello: dict[str, Any]
    ) -> tuple[str, list[dict[str, str]]]:
        """Register a launcher from a validated-shape hello.

        Returns ``(launcher_id, rejected)`` where ``rejected`` lists the
        invalid or duplicate profiles as ``{"id", "reason"}``.
        """
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
        raw_profiles = hello.get("profiles", [])
        if not isinstance(raw_profiles, list) or len(raw_profiles) > _MAX_PROFILES:
            raise HelloError(f"profiles must be a list of at most {_MAX_PROFILES} entries")
        launcher_id = "lnc_" + secrets.token_hex(8)
        lnc = _Launcher(
            launcher_id, conn, owner_account_id, machine.strip(), visibility,
            frozenset(a.casefold() for a in allowed),
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
        self._launchers.pop(launcher_id, None)

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


async def _read_hello(websocket: WebSocket) -> dict[str, Any]:
    try:
        msg = await asyncio.wait_for(websocket.receive_text(), HELLO_TIMEOUT_S)
        body = json.loads(msg)
    except (TimeoutError, ValueError, WebSocketDisconnect, RuntimeError) as exc:
        raise HelloError("expected a hello message") from exc
    if not isinstance(body, dict) or body.get("type") != "hello":
        raise HelloError("first message must be type 'hello'")
    return body


async def _ping_loop(websocket: WebSocket) -> None:
    while True:
        await asyncio.sleep(PING_INTERVAL_S)
        await websocket.send_json({"type": "ping"})


async def launcher_ws(websocket: WebSocket) -> None:
    deps = websocket.app.state.tool_deps
    registry: LauncherRegistry = deps.launchers
    await websocket.accept()

    header = websocket.headers.get("authorization") or ""
    token = header[7:].strip() if header.lower().startswith("bearer ") else ""
    owner_account_id: str | None = None
    if deps.session_creation == "account" and deps.accounts is not None:
        if not token:
            await websocket.close(code=_CLOSE_UNAUTHORIZED, reason="missing token")
            return
        source_ip = (websocket.client.host if websocket.client is not None else "") or ""
        res = await deps.accounts.authenticate(
            token, int(time.time() * 1000), Origin(source_ip, request_client.get())
        )
        if res.account is None:
            if res.refusal == "network":
                await websocket.close(code=_CLOSE_FORBIDDEN, reason="network not approved")
            else:
                await websocket.close(code=_CLOSE_UNAUTHORIZED, reason="invalid token")
            return
        owner_account_id = res.account.account_id
    else:
        required = websocket.app.state.launcher_token
        if required is not None and not admin_matches(token, required):
            await websocket.close(code=_CLOSE_UNAUTHORIZED, reason="invalid token")
            return

    try:
        hello = await _read_hello(websocket)
        launcher_id, rejected = registry.register(websocket, owner_account_id, hello)
    except HelloError as exc:
        await websocket.close(code=_CLOSE_BAD_HELLO, reason=str(exc)[:100])
        return

    pinger: asyncio.Task[None] | None = None
    try:
        await websocket.send_json(
            {"type": "welcome", "launcher_id": launcher_id, "rejected": rejected}
        )
        pinger = asyncio.create_task(_ping_loop(websocket))
        receiver = asyncio.ensure_future(_drain(websocket))
        try:
            await asyncio.wait({pinger, receiver}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            receiver.cancel()
    except (WebSocketDisconnect, RuntimeError):
        pass
    except Exception:
        log.exception("launcher connection crashed")
    finally:
        if pinger is not None and not pinger.done():
            pinger.cancel()
        registry.unregister(launcher_id)
        with contextlib.suppress(Exception):
            await websocket.close()


async def _drain(websocket: WebSocket) -> None:
    """Consume launcher frames (pongs, later results) until the socket closes."""
    with contextlib.suppress(WebSocketDisconnect, RuntimeError):
        while True:
            msg = await websocket.receive()
            if msg.get("type") == "websocket.disconnect":
                return


def launcher_token_from_env() -> str | None:
    return os.environ.get("SW_LAUNCHER_TOKEN", "").strip() or None


def routes() -> list[WebSocketRoute]:
    return [WebSocketRoute("/v1/launchers/connect", launcher_ws)]
