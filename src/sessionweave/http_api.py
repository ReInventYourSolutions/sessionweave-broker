"""HTTP REST surface for the relay.

Mirrors the MCP tool catalogue under ``/v1/...`` so a non-MCP client
(plain HTTP, language SDKs, agents whose runtime hasn't refreshed its
MCP catalogue yet) can drive the same backend.

Auth shape (sub-project #1): the Bearer token IS the participant_id.
Sub-project #3 will swap that for a JWT. The token is returned by
``POST /v1/sessions/{sid}/participants`` (alongside ``participant_id``)
and required on every authenticated route.

HTTP is request/response — there is no persistent transport for push.
HTTP-joined participants therefore always operate in pull mode and
drain via ``POST /v1/sessions/{sid}/participants/me/inbox/drain``.
"""
from __future__ import annotations

from typing import Any

from pydantic import ValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.supervisor import SupervisorRequestError, parse_supervisor_request
from sessionweave.tools import (
    ToolDeps,
    session_check_inbox,
    session_create,
    session_describe,
    session_join,
    session_leave,
    session_list_participants,
    session_respond,
    session_send,
    session_update_purpose,
    session_update_self,
    session_wait_for_event,
)


class HTTPParticipantHandle:
    """ConnectionHandle stub for participants connected via HTTP REST.

    HTTP is request/response — there's no persistent transport for
    server-pushed notifications. ``send`` is a no-op (events accumulate
    in the per-participant buffer and are drained by the inbox endpoint
    or by the WebSocket events stream once that lands).
    ``is_alive`` always returns True because the relay can't observe
    HTTP connection lifecycle the way it does MCP transports.
    """

    __slots__ = ()

    def send(self, name: str, payload: dict[str, Any]) -> None:
        return None

    def is_alive(self) -> bool:
        return True


# A fresh ``HTTPParticipantHandle()`` is created per HTTP request and
# bound for the duration of that request only. A module-level singleton
# would serialise concurrent requests on ``ConnectionManager.bind``
# (which rejects re-binding a handle to a different participant): two
# parallel HTTP clients on the same relay process would collide. The
# handle is stateless (``__slots__ = ()``), so per-request instances
# are essentially free.


# ---------- error mapping ----------

_STATUS_BY_CODE: dict[ErrorCode, int] = {
    ErrorCode.SESSION_NOT_FOUND: 404,
    ErrorCode.PARTICIPANT_NOT_FOUND: 404,
    ErrorCode.COMMAND_NOT_FOUND: 404,
    ErrorCode.NOT_IN_SESSION: 401,
    ErrorCode.ALREADY_IN_SESSION: 409,
    ErrorCode.SESSION_FULL: 409,
    ErrorCode.ALREADY_RESPONDED: 409,
    ErrorCode.INVALID_DELIVERY_MODE: 409,
    ErrorCode.WAIT_ALREADY_IN_FLIGHT: 409,
    ErrorCode.PARTICIPANT_EVICTED: 409,
    ErrorCode.NOT_A_RECIPIENT: 403,
    ErrorCode.PAYLOAD_TOO_LARGE: 413,
    ErrorCode.TOO_MANY_WAITS: 429,
    ErrorCode.INVALID_DISPATCH_FILTERS: 422,
    ErrorCode.INVALID_TARGETS: 422,
    ErrorCode.INVALID_CONFIG: 422,
}


def _error_response(exc: ToolError) -> JSONResponse:
    status = _STATUS_BY_CODE.get(exc.code, 400)
    return JSONResponse(
        {"error": {"code": exc.code.value, "message": exc.message}},
        status_code=status,
    )


def _validation_error(exc: ValidationError) -> JSONResponse:
    return JSONResponse(
        {"error": {"code": "validation_error", "message": str(exc)}},
        status_code=400,
    )


# ---------- helpers ----------


async def _read_json_body(request: Request) -> dict[str, Any]:
    if not (await request.body()):
        return {}
    try:
        body = await request.json()
    except Exception as exc:
        raise ToolError(
            ErrorCode.INVALID_CONFIG, f"invalid JSON body: {exc}"
        ) from exc
    if not isinstance(body, dict):
        raise ToolError(
            ErrorCode.INVALID_CONFIG,
            f"request body must be a JSON object, got {type(body).__name__}",
        )
    return body


def _bind_participant(
    deps: ToolDeps, session_id: str, participant_id: str
) -> HTTPParticipantHandle:
    """Validate the Bearer token then create + bind a fresh
    ``HTTPParticipantHandle`` to that pair for the duration of one tool
    call. The caller MUST pass the returned handle to
    ``_unbind_participant`` in a ``finally`` block.

    Raises ``ToolError(NOT_IN_SESSION)`` if the token doesn't correspond
    to an active participant of the named session — that's how unknown
    or stale Bearer tokens get rejected uniformly.
    """
    sess = deps.registry.get_or_raise(session_id)
    if participant_id not in sess._participants:
        raise ToolError(
            ErrorCode.NOT_IN_SESSION, "unknown or stale participant token"
        )
    handle = HTTPParticipantHandle()
    deps.registry.connections.bind(handle, session_id, participant_id)
    return handle


def _unbind_participant(
    connections: ConnectionManager, handle: HTTPParticipantHandle
) -> None:
    connections.unbind(handle)


def _bearer(request: Request) -> str | None:
    auth = request.headers.get("authorization") or ""
    if auth.lower().startswith("bearer "):
        return auth[len("bearer ") :].strip() or None
    return None


def _require_bearer(request: Request) -> str:
    token = _bearer(request)
    if not token:
        raise ToolError(
            ErrorCode.NOT_IN_SESSION, "missing Authorization: Bearer header"
        )
    return token


def _deps(request: Request) -> ToolDeps:
    return request.app.state.tool_deps  # type: ignore[no-any-return]


# ---------- handlers ----------


async def create_session(request: Request) -> JSONResponse:
    deps = _deps(request)
    try:
        body = await _read_json_body(request)
        out = await session_create(deps, body)
    except ToolError as exc:
        return _error_response(exc)
    except ValidationError as exc:
        return _validation_error(exc)
    return JSONResponse(out, status_code=201)


async def get_session(request: Request) -> JSONResponse:
    sid = request.path_params["sid"]
    try:
        out = await session_describe(_deps(request), {"session_id": sid})
    except ToolError as exc:
        return _error_response(exc)
    return JSONResponse(out)


async def patch_session_purpose(request: Request) -> JSONResponse:
    sid = request.path_params["sid"]
    deps = _deps(request)
    try:
        token = _require_bearer(request)
        body = await _read_json_body(request)
        handle = _bind_participant(deps, sid, token)
        try:
            out = await session_update_purpose(
                deps, handle, {"purpose": body.get("purpose")}
            )
        finally:
            _unbind_participant(deps.registry.connections, handle)
    except ToolError as exc:
        return _error_response(exc)
    except ValidationError as exc:
        return _validation_error(exc)
    return JSONResponse(out)


async def post_participants(request: Request) -> JSONResponse:
    """Join — body shape mirrors session_join. Returns participant_id; the
    caller MUST keep that as the Bearer token for subsequent calls."""
    sid = request.path_params["sid"]
    deps = _deps(request)
    # session_join itself binds the handle to the new participant; we
    # provide a fresh per-request handle and release it before returning.
    handle = HTTPParticipantHandle()
    try:
        body = await _read_json_body(request)
        body = {**body, "session_id": sid}
        # Force pull delivery — HTTP has no push transport.
        body["delivery_mode"] = "pull"
        out = await session_join(deps, handle, body)
    except ToolError as exc:
        return _error_response(exc)
    except ValidationError as exc:
        return _validation_error(exc)
    finally:
        _unbind_participant(deps.registry.connections, handle)
    response = {**out, "participant_token": out["participant_id"]}
    return JSONResponse(response, status_code=201)


async def get_participants(request: Request) -> JSONResponse:
    sid = request.path_params["sid"]
    deps = _deps(request)
    try:
        token = _require_bearer(request)
        handle = _bind_participant(deps, sid, token)
        try:
            out = await session_list_participants(deps, handle)
        finally:
            _unbind_participant(deps.registry.connections, handle)
    except ToolError as exc:
        return _error_response(exc)
    return JSONResponse(out)


async def patch_self(request: Request) -> JSONResponse:
    sid = request.path_params["sid"]
    deps = _deps(request)
    try:
        token = _require_bearer(request)
        body = await _read_json_body(request)
        handle = _bind_participant(deps, sid, token)
        try:
            out = await session_update_self(deps, handle, body)
        finally:
            _unbind_participant(deps.registry.connections, handle)
    except ToolError as exc:
        return _error_response(exc)
    except ValidationError as exc:
        return _validation_error(exc)
    return JSONResponse(out)


async def delete_self(request: Request) -> Response:
    """Leave the session.

    Optionally accepts a JSON body with a ``feedback`` field — see
    ``Session.leave`` and skill v0.11.0 for the convention. RFC 7230
    permits a body on DELETE; clients without that flexibility can
    send the feedback on a final ``session_send`` before deleting.
    """
    sid = request.path_params["sid"]
    deps = _deps(request)
    try:
        token = _require_bearer(request)
        body = await _read_json_body(request)
        handle = _bind_participant(deps, sid, token)
        try:
            await session_leave(deps, handle, body if body else None)
        finally:
            _unbind_participant(deps.registry.connections, handle)
    except ToolError as exc:
        return _error_response(exc)
    # 204 No Content: deliberately empty body per RFC 7230.
    return Response(status_code=204)


async def post_inbox_drain(request: Request) -> JSONResponse:
    sid = request.path_params["sid"]
    deps = _deps(request)
    try:
        token = _require_bearer(request)
        handle = _bind_participant(deps, sid, token)
        try:
            out = await session_check_inbox(deps, handle)
        finally:
            _unbind_participant(deps.registry.connections, handle)
    except ToolError as exc:
        return _error_response(exc)
    return JSONResponse(out)


async def post_inbox_wait(request: Request) -> JSONResponse:
    sid = request.path_params["sid"]
    deps = _deps(request)
    try:
        token = _require_bearer(request)
        body = await _read_json_body(request)
        # session_id is mandatory in the MCP schema but path-derived here;
        # populate from the path for parity with the other endpoints.
        body = {**body, "session_id": sid}
        handle = _bind_participant(deps, sid, token)
        try:
            out = await session_wait_for_event(deps, handle, body)
        finally:
            _unbind_participant(deps.registry.connections, handle)
    except ToolError as exc:
        return _error_response(exc)
    except ValidationError as exc:
        return _validation_error(exc)
    return JSONResponse(out)


async def post_commands(request: Request) -> JSONResponse:
    sid = request.path_params["sid"]
    deps = _deps(request)
    try:
        token = _require_bearer(request)
        body = await _read_json_body(request)
        handle = _bind_participant(deps, sid, token)
        try:
            out = await session_send(deps, handle, body)
        finally:
            _unbind_participant(deps.registry.connections, handle)
    except ToolError as exc:
        return _error_response(exc)
    except ValidationError as exc:
        return _validation_error(exc)
    return JSONResponse(out, status_code=201)


async def post_command_response(request: Request) -> JSONResponse:
    sid = request.path_params["sid"]
    cid = request.path_params["cid"]
    deps = _deps(request)
    try:
        token = _require_bearer(request)
        body = await _read_json_body(request)
        body = {**body, "command_id": cid}
        handle = _bind_participant(deps, sid, token)
        try:
            await session_respond(deps, handle, body)
        finally:
            _unbind_participant(deps.registry.connections, handle)
    except ToolError as exc:
        return _error_response(exc)
    except ValidationError as exc:
        return _validation_error(exc)
    return JSONResponse({}, status_code=201)


async def _supervisor_notes_handler(request: Request) -> JSONResponse:
    sid = request.path_params["sid"]
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    if auth[7:].strip() != sid:
        return JSONResponse({"error": "forbidden"}, status_code=403)

    registry = _deps(request).registry
    session = registry.get(sid)
    if session is None:
        return JSONResponse({"error": "not_found"}, status_code=404)

    limiter = request.app.state.supervisor_rate
    if not limiter.try_acquire(sid):
        return JSONResponse({"error": "rate_limited"}, status_code=429)

    try:
        body = await request.json()
    except ValueError:
        return JSONResponse({"error": "invalid_json"}, status_code=400)

    try:
        note = parse_supervisor_request(body)
    except SupervisorRequestError as e:
        return JSONResponse({"error": "invalid_request", "detail": str(e)}, status_code=400)

    payload = {
        "note_id": note.note_id,
        "kind": note.kind,
        "target": note.target,
        "message": note.message,
        "ts_ms": note.ts_ms,
    }

    # Fan out to firehose subscribers (lock-free by design: firehose is a
    # separate publish path and never touches participant state).
    firehose = request.app.state.firehose
    await firehose.publish(sid, {"name": "supervisor.note", "payload": payload})

    # Fan out to participants under session.lock — _deliver_or_buffer touches
    # _participants / _subscribers / _buffers, and the invariant across the
    # rest of session.py is that those are only accessed while the lock is
    # held. If target is a participant id that is not in the session, return
    # 404 instead of a silent success.
    async with session.lock:
        if note.target == "all":
            targets = list(session._participants.keys())
        elif note.target in session._participants:
            targets = [note.target]
        else:
            return JSONResponse(
                {
                    "error": "participant_not_found",
                    "detail": f"target {note.target!r} is not a participant of this session",
                },
                status_code=404,
            )
        for pid in targets:
            session._deliver_or_buffer(pid, "supervisor.note", payload)

    return JSONResponse(payload, status_code=200)


async def _delete_session_handler(request: Request) -> Response:
    """End a session. The bearer token must equal the session_id."""
    sid = request.path_params["sid"]
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    token = auth[7:].strip()
    if token != sid:
        return JSONResponse({"error": "forbidden"}, status_code=403)

    registry = _deps(request).registry
    session = registry.get(sid)
    if session is None:
        return JSONResponse({"error": "not_found"}, status_code=404)

    await registry.end_session(sid, reason="deleted_by_cockpit")
    return Response(status_code=204)


# ---------- routing ----------


def routes() -> list[Route]:
    """Return the list of `/v1/...` routes to mount on the main app."""
    return [
        Route("/v1/sessions", create_session, methods=["POST"]),
        Route("/v1/sessions/{sid}", get_session, methods=["GET"]),
        Route("/v1/sessions/{sid}", _delete_session_handler, methods=["DELETE"]),
        Route(
            "/v1/sessions/{sid}/purpose",
            patch_session_purpose,
            methods=["PATCH"],
        ),
        Route(
            "/v1/sessions/{sid}/participants",
            post_participants,
            methods=["POST"],
        ),
        Route(
            "/v1/sessions/{sid}/participants",
            get_participants,
            methods=["GET"],
        ),
        Route(
            "/v1/sessions/{sid}/participants/me",
            patch_self,
            methods=["PATCH"],
        ),
        Route(
            "/v1/sessions/{sid}/participants/me",
            delete_self,
            methods=["DELETE"],
        ),
        Route(
            "/v1/sessions/{sid}/participants/me/inbox/drain",
            post_inbox_drain,
            methods=["POST"],
        ),
        Route(
            "/v1/sessions/{sid}/participants/me/inbox/wait",
            post_inbox_wait,
            methods=["POST"],
        ),
        Route(
            "/v1/sessions/{sid}/commands",
            post_commands,
            methods=["POST"],
        ),
        Route(
            "/v1/sessions/{sid}/commands/{cid}/responses",
            post_command_response,
            methods=["POST"],
        ),
        Route(
            "/v1/sessions/{sid}/supervisor/notes",
            _supervisor_notes_handler,
            methods=["POST"],
        ),
    ]


__all__ = ["HTTPParticipantHandle", "routes"]
