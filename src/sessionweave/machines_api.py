"""REST routes for machines (v2 launchers) and their deployed agent definitions.

``/v1/me/launchers...`` (account mode): the caller's account token; only the
machines its own launchers registered. The relay admin token is not an
account token and is refused.

``/v1/launchers...`` (open mode): no credential, every machine of the
broker. This is LAN-only trust: any client that reaches the broker may list
machines, deploy to ``--local``/``--root`` launchers and forget an offline
machine (which also frees its launcher key). ``SW_LAUNCHER_TOKEN``, when set,
still gates the launchers' own connections. In account mode these routes
answer 403; without a persistence DB the write, browse and process routes
answer 503 ``definitions_unavailable`` and the list shows live launchers only.

``.../{key}/browse`` and ``.../{key}/processes`` relay a request to the
machine's connected launcher and return its (validated) answer. The results
are never stored, and they are logged without paths or process data.

The machine MCP tools (``machines_list``, ``machine_get``, ``definition_deploy``,
``definition_remove``, ``machine_forget``, ``machine_browse``,
``machine_processes``) call the same shared operations as these routes.
"""
from __future__ import annotations

import logging
import math
import re
from collections.abc import Mapping
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from sessionweave.account_events import Origin
from sessionweave.accounts_api import _body, _store, self_account
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.http_api import (
    _bearer,
    _deps,
    _error_response,
    _request_source_ip,
    origin_from_request,
)
from sessionweave.launchers import (
    _KEY_RE,
    BROWSE_MAX_LIMIT,
    OPEN_OWNER,
    PROCESSES_MAX_LIMIT,
    _Launcher,
)
from sessionweave.tools import ToolDeps

log = logging.getLogger(__name__)

DEFAULT_PAGE_LIMIT = 100
_MAX_PATH = 4096
_MAX_CURSOR = 512
_MAX_QUERY = 100
_MAX_NAME = 255
_ENTRY_KINDS = frozenset({"dir", "file", "symlink", "other"})
_MAX_USER = 64
_MAX_PROC_NAME = 255
# The Runner truncates redacted command lines to CMDLINE_MAX = 400.
_MAX_CMDLINE = 400
_LIMIT_RE = re.compile(r"^[0-9]{1,9}$")
# A process-list cursor is the last pid returned (sent to the launcher as an int).
_PID_CURSOR_RE = re.compile(r"^[0-9]{1,10}$")
# A launcher's ``ok: false`` error -> what the client gets.
_LAUNCHER_ERRORS: dict[str, tuple[ErrorCode, str]] = {
    "outside_ceiling": (ErrorCode.OUTSIDE_CEILING, "the path is outside the machine's ceiling"),
    "not_found": (ErrorCode.PATH_NOT_FOUND, "no such path on the machine"),
    "not_a_directory": (ErrorCode.INVALID_REQUEST, "the path is not a directory"),
    "bad_request": (ErrorCode.INVALID_REQUEST, "the machine refused the request"),
    "permission_denied": (ErrorCode.PERMISSION_DENIED, "the machine denied access"),
}


def _me(handler: Any) -> Any:
    """Wrap ``handler(request, deps, owner)`` as a ``/v1/me/launchers...``
    route (owner = the caller's account id)."""

    async def route(request: Request) -> Response:
        deps = _deps(request)
        try:
            owner = await account_owner(
                deps, _bearer(request), origin_from_request(request)
            )
            return await handler(request, deps, owner)  # type: ignore[no-any-return]
        except ToolError as exc:
            return _error_response(exc)

    return route


def _open_launchers(handler: Any) -> Any:
    """Wrap ``handler(request, deps, owner)`` as an open-mode
    ``/v1/launchers...`` route (owner = ``OPEN_OWNER``; no credential read)."""

    async def route(request: Request) -> Response:
        deps = _deps(request)
        try:
            owner = open_owner(deps, "use /v1/me/launchers with your account token")
            return await handler(request, deps, owner)  # type: ignore[no-any-return]
        except ToolError as exc:
            return _error_response(exc)

    return route


# --- shared operations (REST routes and MCP tools) ---------------------------
#
# Request-free, so ``/v1/me/launchers``, ``/v1/launchers`` and the machine MCP
# tools run the same owner resolution, validation and relay code. Query-style
# inputs (``path``, ``cursor``, ``limit``, ``hidden``, ``q``) are validated as
# the strings a query string carries; the MCP tools hand theirs over in that
# form.


async def account_owner(deps: ToolDeps, token: str | None, origin: Origin) -> str:
    """Owner of the caller's machines in account mode: its account id."""
    _store(deps)
    return (await self_account(deps, token, origin)).account_id


def open_owner(deps: ToolDeps, refusal: str) -> str:
    """Owner of every machine of an open-mode broker; refused (403
    ``permission_denied``, with ``refusal``) in account mode."""
    if deps.session_creation == "account":
        raise ToolError(ErrorCode.PERMISSION_DENIED, refusal)
    return OPEN_OWNER


def check_key(key: str) -> str:
    if not _KEY_RE.match(key):
        raise ToolError(ErrorCode.LAUNCHER_NOT_FOUND, "no such machine")
    return key


def _key(request: Request) -> str:
    return check_key(str(request.path_params["key"]))


async def list_machines(deps: ToolDeps, owner: str) -> dict[str, Any]:
    return {"launchers": await deps.deploys.machines(owner)}


async def machine_detail(deps: ToolDeps, owner: str, key: str) -> dict[str, Any]:
    return await deps.deploys.machine_detail(owner, check_key(key))


async def forget_machine(deps: ToolDeps, owner: str, key: str, *, client: str) -> None:
    await deps.deploys.forget(owner, check_key(key), client=client)


def _deploy_body_definition(body: dict[str, Any]) -> Any:
    """The definition of a deploy body ``{"definition": {...}}``."""
    extra = sorted(k for k in body if k != "definition")
    if extra:
        raise ToolError(ErrorCode.INVALID_REQUEST, f"unknown keys {extra}")
    if "definition" not in body:
        raise ToolError(ErrorCode.INVALID_REQUEST, "definition is required")
    return body["definition"]


async def deploy_definition(
    deps: ToolDeps, owner: str, key: str, def_id: str, body: dict[str, Any], *, client: str
) -> dict[str, Any]:
    """Deploy (or redeploy) ``body["definition"]`` as ``def_id`` on the machine."""
    key = check_key(key)
    deps.deploys.require_store()
    return await deps.deploys.deploy(
        owner, key, def_id, _deploy_body_definition(body), client=client
    )


async def remove_definition(
    deps: ToolDeps, owner: str, key: str, def_id: str, *, client: str
) -> dict[str, Any]:
    return await deps.deploys.remove(owner, check_key(key), def_id, client=client)


async def _list(request: Request, deps: ToolDeps, owner: str) -> Response:
    return JSONResponse(await list_machines(deps, owner))


async def _detail(request: Request, deps: ToolDeps, owner: str) -> Response:
    return JSONResponse(await machine_detail(deps, owner, _key(request)))


async def _forget(request: Request, deps: ToolDeps, owner: str) -> Response:
    await forget_machine(deps, owner, _key(request), client=_request_source_ip(request))
    return Response(status_code=204)


async def _put(request: Request, deps: ToolDeps, owner: str) -> Response:
    key = _key(request)
    deps.deploys.require_store()  # 503 before reading the body
    return JSONResponse(await deploy_definition(
        deps, owner, key, str(request.path_params["def_id"]), await _body(request),
        client=_request_source_ip(request),
    ))


async def _remove(request: Request, deps: ToolDeps, owner: str) -> Response:
    return JSONResponse(await remove_definition(
        deps, owner, _key(request), str(request.path_params["def_id"]),
        client=_request_source_ip(request),
    ))


# --- remote browse and process list (relayed, never stored) -----------------


def _bad(message: str) -> ToolError:
    return ToolError(ErrorCode.INVALID_REQUEST, message)


def _bad_reply(what: str) -> ToolError:
    return ToolError(ErrorCode.LAUNCHER_BAD_REPLY, f"the machine sent an invalid {what}")


def _query_text(params: Mapping[str, str], name: str, limit: int) -> str | None:
    value = params.get(name)
    if value is None or value == "":
        return None
    if len(value) > limit or "\x00" in value:
        raise _bad(f"{name} must be at most {limit} characters")
    return value


def _query_limit(params: Mapping[str, str], maximum: int) -> int:
    """``limit`` (default 100); a value above ``maximum`` is capped."""
    raw = params.get("limit")
    if raw is None or raw == "":
        return min(DEFAULT_PAGE_LIMIT, maximum)
    # Digits only: int() would also take "1_000", " 5", "+5" and non-ASCII digits.
    if not _LIMIT_RE.fullmatch(raw):
        raise _bad("limit must be a positive integer")
    value = int(raw)
    if value < 1:
        raise _bad("limit must be at least 1")
    return min(value, maximum)


async def _relay_target(deps: ToolDeps, owner: str, key: str, capability: str) -> _Launcher:
    """The connected launcher of ``owner``'s machine ``key`` offering
    ``capability``: 503 without a DB, 404 for a machine that is not the
    owner's, 409 ``launcher_offline`` / ``launcher_capability_missing``."""
    store = deps.deploys.require_store()
    await store.get_machine(owner, key)  # 404 launcher_not_found
    lnc = deps.launchers.by_key(owner, key)
    if lnc is None:
        raise ToolError(ErrorCode.LAUNCHER_OFFLINE, "the machine is offline")
    if capability not in lnc.v2.capabilities:
        raise ToolError(
            ErrorCode.LAUNCHER_CAPABILITY_MISSING,
            f"this machine's launcher does not offer {capability}"
            " (start it with --local or --root)",
        )
    return lnc


def _reply_ok(reply: dict[str, Any]) -> None:
    """Raise the client error for an ``ok: false`` reply."""
    if reply.get("ok") is True:
        return
    error = reply.get("error")
    mapped = _LAUNCHER_ERRORS.get(error) if isinstance(error, str) else None
    if mapped is None:
        raise _bad_reply("error")
    raise ToolError(*mapped)


def _cursor_out(raw: Any, *, numeric: bool = False) -> str | None:
    """A reply's ``next_cursor`` as the string a client passes back.
    ``numeric`` (process lists: the last pid) also accepts an integer."""
    if raw is None:
        return None
    if numeric and isinstance(raw, int) and not isinstance(raw, bool) and raw >= 0:
        raw = str(raw)
    if not isinstance(raw, str) or len(raw) > _MAX_CURSOR:
        raise _bad_reply("cursor")
    if numeric and not _PID_CURSOR_RE.fullmatch(raw):
        raise _bad_reply("cursor")
    return raw


def _num(v: Any) -> bool:
    return (isinstance(v, (int, float)) and not isinstance(v, bool)
            and math.isfinite(v) and v >= 0)


def _entry(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise _bad_reply("entry")
    name, kind = raw.get("name"), raw.get("kind")
    if (not isinstance(name, str) or not 1 <= len(name) <= _MAX_NAME
            or name in (".", "..") or "/" in name or "\x00" in name
            or kind not in _ENTRY_KINDS):
        raise _bad_reply("entry")
    out: dict[str, Any] = {"name": name, "kind": kind}
    for opt in ("size", "mtime"):
        if raw.get(opt) is not None:
            if not _num(raw[opt]) or (opt == "size" and not isinstance(raw[opt], int)):
                raise _bad_reply("entry")
            out[opt] = raw[opt]
    if raw.get("inside") is not None:
        if not isinstance(raw["inside"], bool):
            raise _bad_reply("entry")
        out["inside"] = raw["inside"]
    return out


def _proc_text(v: Any, limit: int, *, optional: bool = False) -> bool:
    if v is None:
        return optional
    return isinstance(v, str) and len(v) <= limit


def _process(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise _bad_reply("process")
    pid, ppid = raw.get("pid"), raw.get("ppid")
    if (not isinstance(pid, int) or isinstance(pid, bool) or pid < 0
            or not (ppid is None or (isinstance(ppid, int) and not isinstance(ppid, bool)
                                     and ppid >= 0))
            or not _proc_text(raw.get("user"), _MAX_USER, optional=True)
            or not _proc_text(raw.get("name"), _MAX_PROC_NAME)
            or not _proc_text(raw.get("cmdline"), _MAX_CMDLINE, optional=True)
            or not (raw.get("started_at") is None or _num(raw.get("started_at")))):
        raise _bad_reply("process")
    return {"pid": pid, "ppid": ppid, "user": raw.get("user"), "name": raw["name"],
            "cmdline": raw.get("cmdline"), "started_at": raw.get("started_at")}


async def browse(
    deps: ToolDeps, owner: str, key: str, params: Mapping[str, str], *, client: str
) -> dict[str, Any]:
    """List a directory of the machine (relayed, validated, never stored).
    ``params``: optional ``path``, ``cursor``, ``limit``, ``hidden`` ("0"/"1")."""
    key = check_key(key)
    path = _query_text(params, "path", _MAX_PATH)
    if path is not None and not path.startswith("/"):
        raise _bad("path must be absolute")
    cursor = _query_text(params, "cursor", _MAX_CURSOR)
    limit = _query_limit(params, BROWSE_MAX_LIMIT)
    hidden = params.get("hidden", "0")
    if hidden not in ("0", "1"):
        raise _bad("hidden must be 0 or 1")
    lnc = await _relay_target(deps, owner, key, "browse")
    if path is None:
        path = lnc.v2.ceiling or "/"
    log.info("browse relayed to machine %s (client %s)", key, client)
    reply = await deps.launchers.request(
        lnc.launcher_id, "browse",
        {"path": path, "cursor": cursor, "limit": limit, "hidden": hidden == "1"},
        timeout=deps.launchers.relay_timeout,
    )
    _reply_ok(reply)
    out_path, entries = reply.get("path"), reply.get("entries")
    if not isinstance(out_path, str) or not out_path.startswith("/") \
            or len(out_path) > _MAX_PATH or "\x00" in out_path:
        raise _bad_reply("path")
    if not isinstance(entries, list) or len(entries) > limit:
        raise _bad_reply("entry list")
    return {"path": out_path, "entries": [_entry(e) for e in entries],
            "next_cursor": _cursor_out(reply.get("next_cursor"))}


async def processes(
    deps: ToolDeps, owner: str, key: str, params: Mapping[str, str], *, client: str
) -> dict[str, Any]:
    """List the machine's processes (relayed, validated, never stored).
    ``params``: optional ``cursor``, ``limit``, ``q``."""
    key = check_key(key)
    raw_cursor = _query_text(params, "cursor", _MAX_CURSOR)
    if raw_cursor is not None and not _PID_CURSOR_RE.fullmatch(raw_cursor):
        raise _bad("cursor must be the next_cursor of the previous page")
    cursor = None if raw_cursor is None else int(raw_cursor)
    limit = _query_limit(params, PROCESSES_MAX_LIMIT)
    query = _query_text(params, "q", _MAX_QUERY)
    lnc = await _relay_target(deps, owner, key, "processes")
    log.info("process list relayed to machine %s (client %s)", key, client)
    reply = await deps.launchers.request(
        lnc.launcher_id, "processes", {"cursor": cursor, "limit": limit, "query": query},
        timeout=deps.launchers.relay_timeout,
    )
    _reply_ok(reply)
    procs = reply.get("processes")
    if not isinstance(procs, list) or len(procs) > limit:
        raise _bad_reply("process list")
    return {"processes": [_process(p) for p in procs],
            "next_cursor": _cursor_out(reply.get("next_cursor"), numeric=True)}


async def _browse(request: Request, deps: ToolDeps, owner: str) -> Response:
    return JSONResponse(await browse(
        deps, owner, _key(request), request.query_params,
        client=_request_source_ip(request),
    ))


async def _processes(request: Request, deps: ToolDeps, owner: str) -> Response:
    return JSONResponse(await processes(
        deps, owner, _key(request), request.query_params,
        client=_request_source_ip(request),
    ))


def routes() -> list[Route]:
    out: list[Route] = []
    for prefix, wrap in (("/v1/me/launchers", _me), ("/v1/launchers", _open_launchers)):
        out += [
            Route(prefix, wrap(_list), methods=["GET"]),
            Route(prefix + "/{key}", wrap(_detail), methods=["GET"]),
            Route(prefix + "/{key}", wrap(_forget), methods=["DELETE"]),
            Route(prefix + "/{key}/definitions/{def_id}", wrap(_put), methods=["PUT"]),
            Route(prefix + "/{key}/definitions/{def_id}", wrap(_remove), methods=["DELETE"]),
            Route(prefix + "/{key}/browse", wrap(_browse), methods=["GET"]),
            Route(prefix + "/{key}/processes", wrap(_processes), methods=["GET"]),
        ]
    return out
