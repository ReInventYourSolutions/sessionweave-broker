"""HTTP REST surface for the relay.

Mirrors the MCP tool catalogue under ``/v1/...`` so a non-MCP client
(plain HTTP, language SDKs, agents whose runtime hasn't refreshed its
MCP catalogue yet) can drive the same backend.

Auth shape: every authenticated route takes ``Authorization: Bearer
<token>``. Joining (``POST /v1/sessions/{sid}/participants``) needs a join
token (or the creator/admin token) and returns the participant's secret
``participant_token``, which is the Bearer for every later participant
route. The participant_id is an identifier, never a credential.

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
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from starlette.routing import Route

from sessionweave.access import participant_id_for_token
from sessionweave.account_events import Origin, client_label
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.supervisor import SupervisorRequestError, parse_supervisor_request
from sessionweave.tools import (
    ToolDeps,
    check_creation_credential,
    session_check_inbox,
    session_create,
    session_describe,
    session_get_events,
    session_join,
    session_leave,
    session_reply_to_note,
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
    ErrorCode.TOKEN_NOT_FOUND: 404,
    ErrorCode.NOTE_NOT_FOUND: 404,
    ErrorCode.NOT_IN_SESSION: 401,
    ErrorCode.ALREADY_IN_SESSION: 409,
    ErrorCode.SESSION_FULL: 409,
    ErrorCode.ALREADY_RESPONDED: 409,
    ErrorCode.INVALID_DELIVERY_MODE: 409,
    ErrorCode.WAIT_ALREADY_IN_FLIGHT: 409,
    ErrorCode.PARTICIPANT_EVICTED: 409,
    ErrorCode.PARTICIPANT_PAUSED: 409,
    ErrorCode.PARTICIPANT_REMOVED: 403,
    ErrorCode.NOT_A_RECIPIENT: 403,
    ErrorCode.PAYLOAD_TOO_LARGE: 413,
    ErrorCode.TOO_MANY_WAITS: 429,
    ErrorCode.RATE_LIMITED: 429,
    ErrorCode.INVALID_DISPATCH_FILTERS: 422,
    ErrorCode.INVALID_TARGETS: 422,
    ErrorCode.INVALID_CONFIG: 422,
    ErrorCode.PERMISSION_DENIED: 403,
    ErrorCode.NO_HISTORY: 409,
    ErrorCode.PERSISTENCE_FAILED: 503,
    ErrorCode.PERSISTENCE_CORRUPTED: 500,
    ErrorCode.INVALID_REQUEST: 400,
    ErrorCode.ACCOUNT_TOKEN_REQUIRED: 401,
    ErrorCode.INVALID_ACCOUNT_TOKEN: 401,
    ErrorCode.ACCOUNT_NOT_FOUND: 404,
    ErrorCode.ACCOUNT_NAME_TAKEN: 409,
    ErrorCode.ACCOUNTS_UNAVAILABLE: 503,
    ErrorCode.SESSION_SUSPENDED: 409,
    ErrorCode.ACCOUNT_SESSION_LIMIT: 429,
    ErrorCode.ACCOUNT_TOKEN_LIMIT: 409,
    ErrorCode.NETWORK_NOT_APPROVED: 403,
    ErrorCode.APPROVAL_NOT_FOUND: 404,
    ErrorCode.PROFILE_NOT_FOUND: 404,
    ErrorCode.NO_FREE_SLOT: 409,
    ErrorCode.LAUNCH_LIMIT: 409,
    ErrorCode.LAUNCH_NOT_FOUND: 404,
    ErrorCode.LAUNCHER_NOT_FOUND: 404,
    ErrorCode.DEFINITION_NOT_FOUND: 404,
    ErrorCode.OUTSIDE_CEILING: 400,
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
    deps: ToolDeps, session_id: str, token: str, *, source_ip: str = ""
) -> HTTPParticipantHandle:
    """Resolve the secret participant token then create + bind a fresh
    ``HTTPParticipantHandle`` to that pair for the duration of one tool
    call. The caller MUST pass the returned handle to
    ``_unbind_participant`` in a ``finally`` block.

    Raises ``ToolError(NOT_IN_SESSION)`` if the token isn't a participant
    token of the named session (a participant_id, join, supervisor or
    creator token all land here) — that's how unknown or stale Bearer
    tokens get rejected uniformly.
    """
    sess = await deps.registry.get_or_raise(session_id)
    participant_id = participant_id_for_token(
        sess, token, limiter=deps.auth_limiter, source_ip=source_ip
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
    bearer = _bearer(request)
    origin = origin_from_request(request)
    source_ip = origin.ip
    try:
        # Authenticate before touching the (possibly huge) body.
        await check_creation_credential(deps, bearer, source_ip, origin=origin)
        body = await _read_json_body(request)
        out = await session_create(
            deps,
            body,
            public_base_url=_public_base_url(request),
            bearer=bearer,
            source_ip=source_ip,
            origin=origin,
        )
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


_INVITE_EXPIRED_PAGE = (
    "# Invite link not valid\n\n"
    "This invite link is missing its join token or has expired. "
    "Ask the session creator for a new invite link.\n"
)

_INVITE_NOT_FOUND_PAGE = (
    "# Session not found\n\n"
    "This invite link points to a session that does not exist on this relay "
    "(it may have ended or been deleted). Ask the session creator for a new "
    "invite link.\n"
)

_INVITE_RATE_LIMITED_PAGE = (
    "# Too many invalid invite links\n\n"
    "Too many invalid tokens were tried for this session from your address. "
    "Wait a minute, then open the invite link again exactly as you received it.\n"
)


async def invite_brief(request: Request) -> Response:
    from sessionweave.access import require, resolve_principal
    from sessionweave.brief import render_brief

    sid = request.path_params["sid"]
    deps = _deps(request)
    jt = (request.query_params.get("jt") or "").strip()
    try:
        principal = await resolve_principal(
            deps, sid, jt, source_ip=_request_source_ip(request)
        )
        require(principal, "joiner", "participant", "supervisor", "creator", "admin")
        purpose = (await deps.registry.get_or_raise(sid)).purpose
    except ToolError as exc:
        pages = {
            ErrorCode.PERMISSION_DENIED: (_INVITE_EXPIRED_PAGE, 403),
            ErrorCode.SESSION_NOT_FOUND: (_INVITE_NOT_FOUND_PAGE, 404),
            ErrorCode.RATE_LIMITED: (_INVITE_RATE_LIMITED_PAGE, 429),
        }
        if exc.code in pages:
            page, status_code = pages[exc.code]
            return Response(
                page,
                status_code=status_code,
                media_type="text/markdown; charset=utf-8",
            )
        return _error_response(exc)
    base = _public_base_url(request)
    md = render_brief(
        base_url=base,
        session_id=sid,
        purpose=purpose,
        upgrades=_effective_upgrades(request, base),
        # Only a join token is meant to be shared; never echo a stronger credential.
        join_token=jt if principal.role == "joiner" else "<join_token>",
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

    account_mode = _deps(request).session_creation == "account"
    return JSONResponse(
        render_mcp_snippet(_public_base_url(request), account_mode=account_mode)
    )


async def serve_api_docs(request: Request) -> HTMLResponse:
    """Swagger UI over this broker's own openapi.yaml, fully offline."""
    from sessionweave.docs_hub import render_api_page

    return HTMLResponse(render_api_page())


async def serve_sdk_docs(request: Request) -> Response:
    """SDK docs page: pinned ``pip install sessionweave-sdk==<v>`` + README."""
    from sessionweave.docs_hub import load_sdk_readme, render_sdk_page, sdk_version

    try:
        version = sdk_version()
        readme = load_sdk_readme()
    except (LookupError, OSError):  # PackageNotFoundError is a LookupError
        return Response(
            "SDK docs missing: the sessionweave-sdk package is not installed",
            status_code=500,
            media_type="text/plain; charset=utf-8",
        )
    return HTMLResponse(render_sdk_page(_public_base_url(request), readme, version))


async def serve_sdk_readme(request: Request) -> Response:
    """Raw SDK README (markdown), consistent with /skill.md."""
    from sessionweave.docs_hub import load_sdk_readme

    try:
        readme = load_sdk_readme()
    except (LookupError, OSError):
        return Response("not found", status_code=500, media_type="text/plain")
    return Response(readme, media_type="text/markdown; charset=utf-8")


async def serve_sdk_tarball(request: Request) -> Response:
    """Legacy SDK download: redirect to the matching sdist on PyPI.

    Brokers before 0.35 served a vendored copy of the SDK source here; pip
    follows the redirect, so ``pip install <broker>/sdk/sessionweave-client.tar.gz``
    keeps working (it now needs access to PyPI)."""
    from sessionweave.docs_hub import sdk_sdist_url, sdk_version

    try:
        version = sdk_version()
    except LookupError:
        return Response(
            "SDK missing: the sessionweave-sdk package is not installed",
            status_code=500,
            media_type="text/plain; charset=utf-8",
        )
    return RedirectResponse(sdk_sdist_url(version), status_code=307)


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
        out = await session_describe(
            _deps(request),
            {"session_id": sid, "access_token": _require_bearer(request)},
            source_ip=_request_source_ip(request),
        )
    except ToolError as exc:
        return _error_response(exc)
    return JSONResponse(out)


async def patch_session_purpose(request: Request) -> JSONResponse:
    """Replace or clear the purpose. Bearer is the creator token (or admin)."""
    sid = request.path_params["sid"]
    deps = _deps(request)
    try:
        token = _require_bearer(request)
        body = await _read_json_body(request)
        out = await session_update_purpose(
            deps,
            {"session_id": sid, "creator_token": token, "purpose": body.get("purpose")},
            source_ip=_request_source_ip(request),
        )
    except ToolError as exc:
        return _error_response(exc)
    except ValidationError as exc:
        return _validation_error(exc)
    return JSONResponse(out)


async def post_participants(request: Request) -> JSONResponse:
    """Join — body shape mirrors session_join. The Bearer is a join token
    (or the creator/admin token). Returns participant_id and the secret
    participant_token, which the caller MUST keep as the Bearer token for
    subsequent calls."""
    sid = request.path_params["sid"]
    deps = _deps(request)
    # session_join itself binds the handle to the new participant; we
    # provide a fresh per-request handle and release it before returning.
    handle = HTTPParticipantHandle()
    try:
        token = _require_bearer(request)
        body = await _read_json_body(request)
        body = {**body, "session_id": sid, "join_token": token}
        # Force pull delivery — HTTP has no push transport.
        body["delivery_mode"] = "pull"
        out = await session_join(
            deps, handle, body, source_ip=_request_source_ip(request)
        )
    except ToolError as exc:
        return _error_response(exc)
    except ValidationError as exc:
        return _validation_error(exc)
    finally:
        _unbind_participant(deps.registry.connections, handle)
    return JSONResponse(out, status_code=201)


async def get_participants(request: Request) -> JSONResponse:
    from sessionweave import tools

    sid = request.path_params["sid"]
    try:
        out = await tools.session_roster(
            _deps(request), sid, _require_bearer(request),
            source_ip=_request_source_ip(request),
        )
    except ToolError as exc:
        return _error_response(exc)
    return JSONResponse(out)


async def patch_self(request: Request) -> JSONResponse:
    sid = request.path_params["sid"]
    deps = _deps(request)
    try:
        token = _require_bearer(request)
        body = await _read_json_body(request)
        handle = await _bind_participant(
            deps, sid, token, source_ip=_request_source_ip(request)
        )
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
        handle = await _bind_participant(
            deps, sid, token, source_ip=_request_source_ip(request)
        )
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
        handle = await _bind_participant(
            deps, sid, token, source_ip=_request_source_ip(request)
        )
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
        handle = await _bind_participant(
            deps, sid, token, source_ip=_request_source_ip(request)
        )
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
        handle = await _bind_participant(
            deps, sid, token, source_ip=_request_source_ip(request)
        )
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
        handle = await _bind_participant(
            deps, sid, token, source_ip=_request_source_ip(request)
        )
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
    from sessionweave.access import require, resolve_principal

    sid = request.path_params["sid"]
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    token = auth[7:].strip()
    deps = _deps(request)
    source_ip = _request_source_ip(request)
    try:
        principal = await resolve_principal(deps, sid, token, source_ip=source_ip)
        require(principal, "supervisor", "creator", "admin")
        # A cold persisted session is hydrated, so an order always reaches
        # the live roster rather than answering 404.
        session = await deps.registry.get_or_raise(sid)
    except ToolError as exc:
        if exc.code is ErrorCode.SESSION_NOT_FOUND:
            return JSONResponse({"error": "not_found"}, status_code=404)
        if exc.code is ErrorCode.RATE_LIMITED:
            return JSONResponse({"error": "rate_limited"}, status_code=429)
        if exc.code in (ErrorCode.PERSISTENCE_FAILED, ErrorCode.PERSISTENCE_CORRUPTED):
            return _error_response(exc)
        return JSONResponse({"error": "forbidden"}, status_code=403)

    limiter = request.app.state.supervisor_rate
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

    # Session.post_order applies the state change, records and persists the
    # note, delivers it to the addressed participants and mirrors it to the
    # firehose (exactly once).
    try:
        out = await session.post_order(note)
    except ToolError as exc:
        return _error_response(exc)
    return JSONResponse(out, status_code=200)


async def post_note_reply(request: Request) -> JSONResponse:
    sid = request.path_params["sid"]
    note_id = request.path_params["note_id"]
    deps = _deps(request)
    try:
        token = _require_bearer(request)
        body = await _read_json_body(request)
        handle = await _bind_participant(
            deps, sid, token, source_ip=_request_source_ip(request)
        )
        try:
            out = await session_reply_to_note(
                deps, handle, {
                    "note_id": note_id,
                    "message": body.get("message"),
                    "outcome": body.get("outcome"),
                },
            )
        finally:
            _unbind_participant(deps.registry.connections, handle)
    except ToolError as exc:
        return _error_response(exc)
    return JSONResponse(out, status_code=201)


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
        payload["access_token"] = _require_bearer(request)
        result = await session_get_events(
            deps, payload, source_ip=_request_source_ip(request)
        )
    except ToolError as e:
        return JSONResponse(
            {"error": e.code.value, "message": e.message},
            status_code=_STATUS_BY_CODE.get(e.code, 500),
        )
    return JSONResponse(result, status_code=200)


def origin_from_request(request: Request) -> Origin:
    """Where an HTTP request came from: trusted client IP plus its User-Agent."""
    return Origin(
        _request_source_ip(request), client_label(request.headers.get("user-agent"))
    )


def _request_source_ip(request: Request) -> str:
    # Trusted client IP only: uvicorn (proxy_headers + forwarded_allow_ips)
    # already rewrites request.client from X-Forwarded-For for trusted
    # proxies. Never read the header here: it is attacker-controlled.
    return (request.client.host if request.client is not None else "") or ""


async def _delete_session_handler(request: Request) -> Response:
    """End a session. Accepts the session creator token or the relay-wide
    SW_ADMIN_TOKEN as Bearer."""
    from sessionweave.access import require, resolve_principal
    from sessionweave.errors import ErrorCode, ToolError

    sid = request.path_params["sid"]
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    token = auth[7:].strip()

    deps = _deps(request)
    try:
        principal = await resolve_principal(
            deps, sid, token, source_ip=_request_source_ip(request)
        )
        require(principal, "creator", "admin")
    except ToolError as e:
        if e.code is ErrorCode.SESSION_NOT_FOUND:
            return JSONResponse({"error": "not_found"}, status_code=404)
        if e.code in (ErrorCode.PERMISSION_DENIED, ErrorCode.RATE_LIMITED):
            if deps.metrics is not None:
                try:
                    deps.metrics.session_delete_failed_total.labels(
                        reason="rate_limited"
                        if e.code is ErrorCode.RATE_LIMITED
                        else "invalid_token"
                    ).inc()
                except Exception:
                    pass
        return _error_response(e)

    reason = "admin_deleted" if principal.role == "admin" else "host_deleted"
    deleted_at = await deps.registry.delete(sid, reason=reason)
    return JSONResponse({"deleted_at": deleted_at}, status_code=200)


async def _run_credential_call(request: Request, call: Any, status: int = 200) -> Response:
    """Run a tools.session_* credential call; map ToolError to the error body."""
    try:
        out = await call(
            _deps(request),
            request.path_params["sid"],
            _bearer(request) or "",
            source_ip=_request_source_ip(request),
        )
    except ToolError as exc:
        return _error_response(exc)
    if status == 204:
        return Response(status_code=204)
    return JSONResponse(out, status_code=status)


async def _mint_join_token_handler(request: Request) -> Response:
    from sessionweave import tools

    base = _public_base_url(request)

    async def call(deps: ToolDeps, sid: str, token: str, *, source_ip: str) -> Any:
        return await tools.session_mint_join_token(
            deps, sid, token, public_base_url=base, source_ip=source_ip
        )

    return await _run_credential_call(request, call, 201)


async def _mint_supervisor_token_handler(request: Request) -> Response:
    from sessionweave import tools

    return await _run_credential_call(request, tools.session_mint_supervisor_token, 201)


async def _list_supervisor_tokens_handler(request: Request) -> Response:
    from sessionweave import tools

    return await _run_credential_call(request, tools.session_list_supervisor_tokens)


async def _revoke_supervisor_token_handler(request: Request) -> Response:
    from sessionweave import tools

    token_id = request.path_params["token_id"]

    async def call(deps: ToolDeps, sid: str, token: str, *, source_ip: str) -> Any:
        await tools.session_revoke_supervisor_token(
            deps, sid, token, token_id, source_ip=source_ip
        )

    return await _run_credential_call(request, call, 204)


async def _remove_participant_handler(request: Request) -> Response:
    from sessionweave import tools

    participant_id = request.path_params["pid"]

    async def call(deps: ToolDeps, sid: str, token: str, *, source_ip: str) -> Any:
        return await tools.session_remove_participant(
            deps, sid, token, participant_id, source_ip=source_ip
        )

    return await _run_credential_call(request, call)


async def _set_coordinator_handler(request: Request) -> Response:
    from sessionweave import tools

    try:
        body = await _read_json_body(request)
    except ToolError as exc:
        return _error_response(exc)
    pid = body.get("participant_id")
    if pid is not None and not isinstance(pid, str):
        return _error_response(
            ToolError(ErrorCode.INVALID_REQUEST, "participant_id must be a string or null")
        )

    async def call(deps: ToolDeps, sid: str, token: str, *, source_ip: str) -> Any:
        return await tools.session_set_coordinator(deps, sid, token, pid, source_ip=source_ip)

    return await _run_credential_call(request, call)


async def _agents_handler(request: Request) -> Response:
    from sessionweave import tools

    q = request.query_params
    sid = q.get("session_id")
    domain = q.get("domain")
    if not sid:
        return _error_response(ToolError(ErrorCode.INVALID_REQUEST, "session_id is required"))
    if request.url.path.endswith("/domains"):
        domain = None
    elif not domain:
        return _error_response(
            ToolError(ErrorCode.INVALID_REQUEST, "domain is required (see /v1/agents/domains)")
        )
    try:
        out = await tools.agents_discover(
            _deps(request), sid, _bearer(request) or "", domain,
            q.get("role") or None, q.get("capability") or None,
            source_ip=_request_source_ip(request),
        )
    except ToolError as exc:
        return _error_response(exc)
    return JSONResponse(out)


async def _launch_agent_handler(request: Request) -> Response:
    from sessionweave import tools

    try:
        body = await _read_json_body(request)
    except ToolError as exc:
        return _error_response(exc)
    # Only the configured SW_PUBLIC_BASE_URL: never the request's Host header.
    base = getattr(request.app.state, "public_base_url", None)

    async def call(deps: ToolDeps, sid: str, token: str, *, source_ip: str) -> Any:
        return await tools.agents_launch(
            deps, sid, token, body, broker_url=base, source_ip=source_ip
        )

    return await _run_credential_call(request, call, 202)


async def _launch_status_handler(request: Request) -> Response:
    from sessionweave import tools

    launch_id = request.path_params["launch_id"]

    async def call(deps: ToolDeps, sid: str, token: str, *, source_ip: str) -> Any:
        return await tools.agents_launch_status(deps, sid, token, launch_id, source_ip=source_ip)

    return await _run_credential_call(request, call)


async def _access_handler(request: Request) -> Response:
    from sessionweave import tools

    return await _run_credential_call(request, tools.session_access)


# ---------- routing ----------


def routes() -> list[Route]:
    """Return the list of `/v1/...` routes to mount on the main app."""
    from sessionweave import accounts_api

    return [
        *accounts_api.routes(),
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
        Route("/v1/agents", _agents_handler, methods=["GET"]),
        Route("/v1/agents/domains", _agents_handler, methods=["GET"]),
        Route(
            "/v1/sessions/{sid}/coordinator",
            _set_coordinator_handler,
            methods=["PUT"],
        ),
        Route("/v1/sessions/{sid}/agents", _launch_agent_handler, methods=["POST"]),
        Route(
            "/v1/sessions/{sid}/agents/{launch_id}",
            _launch_status_handler,
            methods=["GET"],
        ),
        Route(
            "/v1/sessions/{sid}/participants/{pid}",
            _remove_participant_handler,
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
            "/v1/sessions/{sid}/join-tokens",
            _mint_join_token_handler, methods=["POST"],
        ),
        Route(
            "/v1/sessions/{sid}/supervisor-tokens",
            _mint_supervisor_token_handler, methods=["POST"],
        ),
        Route(
            "/v1/sessions/{sid}/supervisor-tokens",
            _list_supervisor_tokens_handler, methods=["GET"],
        ),
        Route(
            "/v1/sessions/{sid}/supervisor-tokens/{token_id}",
            _revoke_supervisor_token_handler, methods=["DELETE"],
        ),
        Route(
            "/v1/sessions/{sid}/access",
            _access_handler, methods=["GET"],
        ),
        Route(
            "/v1/sessions/{sid}/supervisor/notes",
            _supervisor_notes_handler,
            methods=["POST"],
        ),
        Route(
            "/v1/sessions/{sid}/supervisor/notes/{note_id}/replies",
            post_note_reply,
            methods=["POST"],
        ),
    ]


__all__ = ["HTTPParticipantHandle", "routes"]
