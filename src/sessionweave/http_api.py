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

import asyncio
import logging
from typing import Any

from pydantic import ValidationError
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Route

from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.supervisor import SupervisorRequestError, parse_supervisor_request
from sessionweave.tools import (
    ToolDeps,
    session_check_inbox,
    session_create,
    session_describe,
    session_get_events,
    session_join,
    session_leave,
    session_list_participants,
    session_respond,
    session_send,
    session_update_purpose,
    session_update_self,
    session_wait_for_event,
)

logger = logging.getLogger(__name__)


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

    def send(
        self, name: str, payload: dict[str, Any], *, seq: int | None = None
    ) -> None:
        # No-op: REST clients are pull-mode by definition (they drain
        # via ``inbox/drain`` or upgrade to the WS events endpoint).
        # The ``seq`` parameter is accepted to satisfy the
        # ``ConnectionHandle`` protocol but ignored.
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
    ErrorCode.PERMISSION_DENIED: 403,
    ErrorCode.NO_HISTORY: 409,
    ErrorCode.PERSISTENCE_FAILED: 503,
    ErrorCode.PERSISTENCE_CORRUPTED: 500,
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


async def _bind_participant(
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
    sess = await deps.registry.get_or_raise(session_id)
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


def _public_base_url(request: Request) -> str:
    configured = getattr(request.app.state, "public_base_url", None)
    if configured:
        return str(configured).rstrip("/")
    # uvicorn runs with proxy_headers=True, so request.base_url reflects
    # X-Forwarded-Proto/Host behind the configured reverse proxy.
    return str(request.base_url).rstrip("/")


def _effective_upgrades(request: Request, base: str) -> Any:
    """Brief upgrade pointers with empty (unconfigured) fields defaulted to the
    broker's own native onboarding routes. Operator-set env values still win."""
    from sessionweave.brief import BriefUpgrades

    configured = request.app.state.brief_upgrades
    return BriefUpgrades(
        skill_url=configured.skill_url or f"{base}/skill.md",
        mcp_snippet_url=configured.mcp_snippet_url or f"{base}/connect.mcp.json",
        runner_install_url=configured.runner_install_url,
    )


async def invite_brief(request: Request) -> Response:
    sid = request.path_params["sid"]
    try:
        info = await session_describe(_deps(request), {"session_id": sid})
    except ToolError as exc:
        return _error_response(exc)
    from sessionweave.brief import render_brief
    base = _public_base_url(request)
    md = render_brief(
        base_url=base,
        session_id=sid,
        purpose=info.get("purpose"),
        upgrades=_effective_upgrades(request, base),
    )
    return Response(md, media_type="text/markdown; charset=utf-8")


async def serve_skill(request: Request) -> Response:
    """The participant skill, parametrized on this broker's reachable address.

    ``?download=1`` returns it as a file attachment rather than inline.
    """
    from sessionweave.skill_serve import load_canonical_skill, render_skill

    try:
        canonical = load_canonical_skill()
    except (FileNotFoundError, ModuleNotFoundError, OSError):
        return Response(
            "skill resource missing from this broker build",
            status_code=500,
            media_type="text/plain; charset=utf-8",
        )
    md = render_skill(canonical, _public_base_url(request))
    headers = {}
    if request.query_params.get("download"):
        headers["content-disposition"] = (
            'attachment; filename="sessionweave-participant.md"'
        )
    return Response(md, media_type="text/markdown; charset=utf-8", headers=headers)


async def serve_mcp_snippet(request: Request) -> JSONResponse:
    """A ready-to-paste ``.mcp.json`` registration snippet for this broker."""
    from sessionweave.skill_serve import render_mcp_snippet

    return JSONResponse(render_mcp_snippet(_public_base_url(request)))


async def serve_api_docs(request: Request) -> HTMLResponse:
    """Swagger UI over this broker's own openapi.yaml, fully offline."""
    from sessionweave.docs_hub import render_api_page

    return HTMLResponse(render_api_page())


async def serve_sdk_docs(request: Request) -> Response:
    """SDK docs page: offline pip-install line + rendered README."""
    from sessionweave.docs_hub import load_sdk_readme, render_sdk_page

    try:
        readme = load_sdk_readme()
    except (FileNotFoundError, ModuleNotFoundError, OSError):
        return Response(
            "SDK docs missing from this broker build",
            status_code=500,
            media_type="text/plain; charset=utf-8",
        )
    return HTMLResponse(render_sdk_page(_public_base_url(request), readme))


async def serve_sdk_readme(request: Request) -> Response:
    """Raw SDK README (markdown), consistent with /skill.md."""
    from sessionweave.docs_hub import load_sdk_readme

    try:
        readme = load_sdk_readme()
    except (FileNotFoundError, ModuleNotFoundError, OSError):
        return Response("not found", status_code=500, media_type="text/plain")
    return Response(readme, media_type="text/markdown; charset=utf-8")


async def serve_sdk_tarball(request: Request) -> Response:
    """Pip-installable .tar.gz of the vendored SDK source."""
    from sessionweave.docs_hub import SDK_TARBALL_ROOT, build_sdk_tarball

    try:
        blob = build_sdk_tarball()
    except (FileNotFoundError, ModuleNotFoundError, OSError):
        return Response(
            "SDK source missing from this broker build",
            status_code=500,
            media_type="text/plain; charset=utf-8",
        )
    return Response(
        blob,
        media_type="application/gzip",
        headers={
            "content-disposition": (
                f'attachment; filename="{SDK_TARBALL_ROOT}.tar.gz"'
            )
        },
    )


async def serve_static(request: Request) -> Response:
    """Bundled static assets (Swagger UI, markdown renderer) under /_static."""
    from sessionweave.docs_hub import static_asset

    try:
        body, ctype = static_asset(request.path_params["asset"])
    except (FileNotFoundError, PermissionError, ValueError):
        return Response("not found", status_code=404, media_type="text/plain")
    return Response(body, media_type=ctype)


async def landing(request: Request) -> HTMLResponse:
    """Self-serve onboarding page: status + skill / MCP / SDK pointers, all
    keyed off the broker's reachable address."""
    from sessionweave import __version__
    from sessionweave.landing import render_landing

    relay_epoch = getattr(
        getattr(request.app.state, "tool_deps", None), "relay_epoch", 0
    )
    html = render_landing(
        base_url=_public_base_url(request),
        version=__version__,
        relay_epoch=relay_epoch,
    )
    return HTMLResponse(html)


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
        handle = await _bind_participant(deps, sid, token)
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
        handle = await _bind_participant(deps, sid, token)
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
        handle = await _bind_participant(deps, sid, token)
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
        handle = await _bind_participant(deps, sid, token)
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
    # Optional ``?last_acked_seq=N`` lets a reconnecting client tell the
    # relay it already consumed events up to N (typically via an earlier
    # WS session whose socket died). Invalid integers are silently
    # ignored to keep the endpoint forgiving — a missing param falls back
    # to the original full-drain behaviour.
    raw_acked = request.query_params.get("last_acked_seq")
    last_acked_seq: int | None
    if raw_acked is None:
        last_acked_seq = None
    else:
        try:
            last_acked_seq = int(raw_acked)
        except ValueError:
            last_acked_seq = None
    try:
        token = _require_bearer(request)
        handle = await _bind_participant(deps, sid, token)
        try:
            out = await session_check_inbox(
                deps, handle, last_acked_seq=last_acked_seq
            )
        finally:
            _unbind_participant(deps.registry.connections, handle)
    except ToolError as exc:
        return _error_response(exc)
    return JSONResponse(out)


async def _watch_client_disconnect(
    request: Request, target_task: asyncio.Task[Any]
) -> None:
    """Poll the ASGI receive channel for an ``http.disconnect`` event
    and cancel ``target_task`` when the client gives up.

    Starlette exposes :meth:`Request.is_disconnected` as a one-shot
    check that drains the receive queue for a disconnect message. The
    relay's long-poll endpoint can block on the server for up to
    ``max_wait_seconds`` (default 600 s) while a typical HTTP client
    has a much shorter read timeout (httpx defaults to 5 s, the
    SessionWeave SDK to 30 s). Without disconnect-driven cancellation
    the server keeps the wait alive, ``_wait_in_flight`` stays set for
    the participant, and every retry the client issues races back with
    a 409 ``wait_already_in_flight`` until the original 90-second-ish
    wait expires on its own.
    """
    try:
        while not target_task.done():
            if await request.is_disconnected():
                if not target_task.done():
                    target_task.cancel()
                return
            await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        # Watcher itself was cancelled because the wait finished
        # normally — that's the happy path, not an error.
        raise


async def post_inbox_wait(request: Request) -> JSONResponse:
    sid = request.path_params["sid"]
    deps = _deps(request)
    try:
        token = _require_bearer(request)
        body = await _read_json_body(request)
        # session_id is mandatory in the MCP schema but path-derived here;
        # populate from the path for parity with the other endpoints.
        body = {**body, "session_id": sid}
        handle = await _bind_participant(deps, sid, token)
        try:
            # Race the wait against a disconnect watcher: whichever
            # finishes first wins. If the watcher fires it cancels the
            # wait task, the wait's outer finally clears
            # _wait_in_flight, and we return a 499 sentinel that the
            # client never sees (the socket is gone). The crucial
            # invariant is that _wait_in_flight is cleared by the time
            # we return — without this, a participant whose HTTP
            # client times out shorter than the server-side wait gets
            # locked out for the remainder of the wait window.
            wait_task: asyncio.Task[Any] = asyncio.create_task(
                session_wait_for_event(deps, handle, body)
            )
            watch_task: asyncio.Task[None] = asyncio.create_task(
                _watch_client_disconnect(request, wait_task)
            )
            try:
                await asyncio.wait(
                    {wait_task, watch_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                # Whatever happened, the watcher has no further job.
                if not watch_task.done():
                    watch_task.cancel()
                    try:
                        await watch_task
                    except (asyncio.CancelledError, Exception):
                        pass

            if wait_task.cancelled():
                # Disconnect path: wait was cancelled by the watcher.
                # The wait coroutine's outer finally cleared
                # _wait_in_flight, so a subsequent reconnection works.
                # 499 is the conventional "client closed request"
                # status (nginx); the client won't see this body, but
                # it makes the log line accurate.
                logger.info(
                    "wait_for_event cancelled by client disconnect "
                    "(sid=%s)",
                    sid,
                )
                return JSONResponse(
                    {
                        "error": {
                            "code": "client_disconnect",
                            "message": (
                                "client closed the request before the "
                                "wait completed"
                            ),
                        }
                    },
                    status_code=499,
                )
            # Re-raise any exception the wait raised so the existing
            # ToolError / ValidationError handlers below catch it.
            out = wait_task.result()
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
        handle = await _bind_participant(deps, sid, token)
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
        handle = await _bind_participant(deps, sid, token)
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
    token = auth[7:].strip()
    # Four authentication modes (priority order):
    #   1. admin-token mode (``SW_SUPERVISOR_ADMIN_TOKEN`` set): token ==
    #      admin_token → relay-wide override.
    #   2. in-session participant: token matches the participant_id of an
    #      active participant in THIS session → accept. Lets an SDK caller
    #      broadcast supervisor notes (e.g. kind=note target=all "session
    #      summary") without requiring the admin token. An orchestrator/
    #      supervisor role can also issue kind=halt targeting peers — that's
    #      the intended power-of-an-in-session-role model.
    #   3. per-session creator: token matches the session's creator_token
    #      (when persistence + creator_token are set at create). Symmetric
    #      with the firehose WS auth (events_ws.py) so the cockpit's
    #      creator-role surface has matching supervisor power on both
    #      transports.
    #   4. legacy: admin token unset AND token == sid. Local-dev cockpit
    #      backwards-compat (startup logger warns operators to set the env
    #      var in prod).
    registry = _deps(request).registry
    session = registry.get(sid)
    if session is None:
        return JSONResponse({"error": "not_found"}, status_code=404)

    import hmac as _hmac

    from sessionweave.persistence_token import verify_creator_token

    admin_token = getattr(request.app.state, "supervisor_admin_token", None)
    accepted = False
    # Constant-time comparison on the admin_token path; falling back to
    # plain `==` would let a local attacker recover the admin token via
    # timing side-channel on this hot endpoint.
    if admin_token is not None and _hmac.compare_digest(token, admin_token):
        accepted = True
    elif token in session._participants:
        # participant_id is a uuid4 lookup — dict membership is O(1) and
        # already constant-time for any fixed key size.
        accepted = True
    elif session.creator_token_hash is not None and verify_creator_token(
        token, session.creator_token_hash
    ):
        accepted = True
    elif admin_token is None and _hmac.compare_digest(token, sid):
        accepted = True
    if not accepted:
        return JSONResponse({"error": "forbidden"}, status_code=403)

    limiter = request.app.state.supervisor_rate
    source_ip = request.headers.get("x-forwarded-for", "").split(",")[0].strip()
    if not source_ip and request.client is not None:
        source_ip = request.client.host or ""
    if not limiter.try_acquire(sid, source_ip):
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


async def _get_session_events_handler(request: Request) -> Response:
    """Browse the timeline of a persisted session without hydrating."""
    sid = request.path_params["sid"]
    qp = request.query_params

    def _opt_int(name: str) -> int | None:
        v = qp.get(name)
        return int(v) if v is not None and v != "" else None

    try:
        limit = int(qp.get("limit") or 200)
    except ValueError:
        limit = 200
    payload = {
        "session_id": sid,
        "after_seq": _opt_int("after_seq"),
        "before_seq": _opt_int("before_seq"),
        "kind": qp.get("kind") or None,
        "participant_id": qp.get("participant_id") or None,
        "limit": limit,
        "order": qp.get("order") or "asc",
    }
    deps = _deps(request)
    try:
        result = await session_get_events(deps, payload)
    except ToolError as e:
        return JSONResponse(
            {"error": e.code.value, "message": e.message},
            status_code=_STATUS_BY_CODE.get(e.code, 500),
        )
    return JSONResponse(result, status_code=200)


async def _delete_session_handler(request: Request) -> Response:
    """End a session. Accepts the per-session creator_token or the
    relay-wide SW_SUPERVISOR_ADMIN_TOKEN as Bearer."""
    import asyncio as _asyncio
    import hmac as _hmac

    from sessionweave.errors import ErrorCode, ToolError
    from sessionweave.persistence_token import verify_creator_token

    sid = request.path_params["sid"]
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    token = auth[7:].strip()

    state = request.app.state
    deps = _deps(request)
    rl = deps.delete_rate_limiter
    if rl is not None and rl.is_banned(sid):
        await _asyncio.sleep(0.5)
        if deps.metrics is not None:
            try:
                deps.metrics.session_delete_failed_total.labels(
                    reason="rate_limited").inc()
            except Exception:
                pass
        return JSONResponse({"error": "permission_denied"}, status_code=403)

    try:
        sess = await deps.registry.get_or_raise(sid)
    except ToolError as e:
        if e.code is ErrorCode.SESSION_NOT_FOUND:
            return JSONResponse({"error": "not_found"}, status_code=404)
        raise

    reason: str | None = None
    if sess.creator_token_hash is not None and verify_creator_token(
        token, sess.creator_token_hash
    ):
        reason = "host_deleted"
    else:
        admin = getattr(state, "supervisor_admin_token", None)
        if admin is not None and _hmac.compare_digest(token, admin):
            reason = "admin_deleted"

    if reason is None:
        if rl is not None:
            rl.record_failure(sid)
        if deps.metrics is not None:
            try:
                deps.metrics.session_delete_failed_total.labels(
                    reason="invalid_token").inc()
            except Exception:
                pass
        # Uniform 0.5 s tax matches the rate-limited path so callers
        # can't distinguish "rate-limited" from "invalid token" via
        # response latency. See session_delete in tools.py for rationale.
        await _asyncio.sleep(0.5)
        return JSONResponse({"error": "permission_denied"}, status_code=403)

    deleted_at = await deps.registry.delete(sid, reason=reason)
    if rl is not None:
        rl.reset(sid)
    return JSONResponse({"deleted_at": deleted_at}, status_code=200)


# ---------- routing ----------


def routes() -> list[Route]:
    """Return the list of `/v1/...` routes to mount on the main app."""
    return [
        Route("/", landing, methods=["GET"]),
        Route("/skill.md", serve_skill, methods=["GET"]),
        Route("/connect.mcp.json", serve_mcp_snippet, methods=["GET"]),
        Route("/api", serve_api_docs, methods=["GET"]),
        Route("/sdk", serve_sdk_docs, methods=["GET"]),
        Route("/sdk.md", serve_sdk_readme, methods=["GET"]),
        Route("/sdk/sessionweave-client.tar.gz", serve_sdk_tarball, methods=["GET"]),
        Route("/_static/{asset:path}", serve_static, methods=["GET"]),
        Route("/invite/{sid}", invite_brief, methods=["GET"]),
        Route("/v1/sessions", create_session, methods=["POST"]),
        Route("/v1/sessions/{sid}", get_session, methods=["GET"]),
        Route("/v1/sessions/{sid}", _delete_session_handler, methods=["DELETE"]),
        Route("/v1/sessions/{sid}/events", _get_session_events_handler, methods=["GET"]),
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
