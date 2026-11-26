"""REST routes for machines (v2 launchers) and their deployed agent definitions.

``/v1/me/launchers...`` (account mode): the caller's account token; only the
machines its own launchers registered. The relay admin token is not an
account token and is refused.

``/v1/launchers...`` (open mode): no credential, every machine of the
broker. This is LAN-only trust: any client that reaches the broker may list
machines, deploy to ``--local``/``--root`` launchers and forget an offline
machine (which also frees its launcher key). ``SW_LAUNCHER_TOKEN``, when set,
still gates the launchers' own connections. In account mode these routes
answer 403; without a persistence DB the write routes answer 503
``definitions_unavailable`` and the list shows live launchers only.
"""
from __future__ import annotations

from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from sessionweave.accounts_api import _body, _require_self, _store
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.http_api import _deps, _error_response, _request_source_ip
from sessionweave.launchers import _KEY_RE, OPEN_OWNER
from sessionweave.tools import ToolDeps


def _me(handler: Any) -> Any:
    """Wrap ``handler(request, deps, owner)`` as a ``/v1/me/launchers...``
    route (owner = the caller's account id)."""

    async def route(request: Request) -> Response:
        deps = _deps(request)
        try:
            _store(deps)
            account = await _require_self(request, deps)
            return await handler(request, deps, account.account_id)  # type: ignore[no-any-return]
        except ToolError as exc:
            return _error_response(exc)

    return route


def _open_launchers(handler: Any) -> Any:
    """Wrap ``handler(request, deps, owner)`` as an open-mode
    ``/v1/launchers...`` route (owner = ``OPEN_OWNER``; no credential read)."""

    async def route(request: Request) -> Response:
        deps = _deps(request)
        try:
            if deps.session_creation == "account":
                raise ToolError(
                    ErrorCode.PERMISSION_DENIED,
                    "use /v1/me/launchers with your account token",
                )
            return await handler(request, deps, OPEN_OWNER)  # type: ignore[no-any-return]
        except ToolError as exc:
            return _error_response(exc)

    return route


def _key(request: Request) -> str:
    key = str(request.path_params["key"])
    if not _KEY_RE.match(key):
        raise ToolError(ErrorCode.LAUNCHER_NOT_FOUND, "no such machine")
    return key


async def _list(request: Request, deps: ToolDeps, owner: str) -> Response:
    return JSONResponse({"launchers": await deps.deploys.machines(owner)})


async def _detail(request: Request, deps: ToolDeps, owner: str) -> Response:
    return JSONResponse(await deps.deploys.machine_detail(owner, _key(request)))


async def _forget(request: Request, deps: ToolDeps, owner: str) -> Response:
    await deps.deploys.forget(owner, _key(request), client=_request_source_ip(request))
    return Response(status_code=204)


async def _put(request: Request, deps: ToolDeps, owner: str) -> Response:
    key = _key(request)
    deps.deploys.require_store()  # 503 before reading the body
    body = await _body(request)
    extra = sorted(k for k in body if k != "definition")
    if extra:
        raise ToolError(ErrorCode.INVALID_REQUEST, f"unknown keys {extra}")
    if "definition" not in body:
        raise ToolError(ErrorCode.INVALID_REQUEST, "definition is required")
    return JSONResponse(await deps.deploys.deploy(
        owner, key, str(request.path_params["def_id"]), body["definition"],
        client=_request_source_ip(request),
    ))


async def _remove(request: Request, deps: ToolDeps, owner: str) -> Response:
    return JSONResponse(await deps.deploys.remove(
        owner, _key(request), str(request.path_params["def_id"]),
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
        ]
    return out
