from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import math
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from typing import Any, TypeVar, cast

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError as MCPToolError
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import ValidationError
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, Response
from starlette.routing import Mount, Route

from sessionweave import __version__
from sessionweave.brief import BriefUpgrades
from sessionweave.config import load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.cors import CORSConfig
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.events_ws import routes as events_ws_routes
from sessionweave.firehose import FirehoseRegistry
from sessionweave.http_api import routes as http_api_routes
from sessionweave.mcp_adapter import (
    HandleRegistry,
    connection_of,
    require_bindable_connection,
)
from sessionweave.metrics import METRICS_CONTENT_TYPE, Metrics
from sessionweave.persistence import PersistenceStore
from sessionweave.persistence_token import DeleteRateLimiter
from sessionweave.registry import SessionRegistry
from sessionweave.supervisor import NoteRateLimiter
from sessionweave.tools import (
    ToolDeps,
    session_check_inbox,
    session_create,
    session_delete,
    session_describe,
    session_get_events,
    session_join,
    session_leave,
    session_list_participants,
    session_respond,
    session_resume,
    session_send,
    session_update_purpose,
    session_update_self,
    session_wait_for_event,
)

_LOOPBACK_HOSTS: tuple[str, ...] = ("127.0.0.1:*", "localhost:*", "[::1]:*")


def _allowed_hosts() -> list[str]:
    """Build the DNS-rebinding ``allowed_hosts`` list for the MCP transport.

    Defaults to loopback only (matches the SDK's stock policy). Extra
    hosts come from ``SW_ALLOWED_HOSTS`` (comma-separated, e.g.
    ``relay.sessionweave.com,relay.sessionweave.com:443``). Wildcard
    port patterns (``host:*``) are honoured by the upstream middleware.
    """
    extra = os.environ.get("SW_ALLOWED_HOSTS", "").strip()
    parsed: list[str] = []
    if extra:
        parsed = [tok.strip() for tok in extra.split(",") if tok.strip()]
    # De-duplicate while keeping insertion order: loopback first, custom next.
    seen: set[str] = set()
    out: list[str] = []
    for host in [*_LOOPBACK_HOSTS, *parsed]:
        if host not in seen:
            seen.add(host)
            out.append(host)
    return out


_ToolFn = TypeVar("_ToolFn", bound=Callable[..., Awaitable[dict[str, Any]]])

def _anticipated_failures[F: Callable[..., Awaitable[dict[str, Any]]]](
    fn: F,
) -> F:
    """Re-raise the relay's ``ToolError`` as the SDK's, so its code reaches the client.

    mcp 2.x sorts tool failures into two kinds: its own ``ToolError`` is an
    *anticipated* failure and keeps its message, while anything else is a
    crash whose text is deliberately withheld so internals cannot leak
    ("An anticipated failure keeps its own text after the prefix. A crash
    does not" — ``mcpserver/tools/base.py``).

    The relay's ``ToolError`` is the anticipated kind: ``session_not_found``,
    ``not_in_session``, ``payload_too_large`` and the rest are a documented
    contract that callers branch on. Without this translation every one of
    them reaches an MCP client as "Error executing tool <name>" and the
    error contract silently disappears — which is precisely what the
    integration suite caught during the 2.x migration.

    Only the relay's own errors are translated. A genuine crash keeps the
    SDK's masking, which is the behaviour we want.
    """

    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> dict[str, Any]:
        try:
            return await fn(*args, **kwargs)
        except ToolError as exc:
            # str(exc) is "<code>: <message>", e.g.
            # "not_in_session: call session_join first".
            raise MCPToolError(str(exc)) from exc
        except ValidationError as exc:
            # The relay validates tool bodies against its own pydantic
            # models (e.g. dispatch="all" requires target.role or
            # target.capability), which the JSON schema cannot express.
            # That is the caller's mistake to read and correct — the same
            # judgement the SDK makes about its own schema validation, which
            # it reports as a ToolError rather than a crash.
            raise MCPToolError(str(exc)) from exc

    return cast("F", wrapper)


_SERVER_INSTRUCTIONS = """SessionWeave relays messages between agents inside named sessions. The
relay brokers; it holds no agent's history.

Two rules govern every tool here.

1. One MCP connection is in at most one session at a time. session_join
   (or session_resume) binds this connection to a session; session_leave
   unbinds it. A second join on the same connection returns
   already_in_session — leave first to move elsewhere. To be in two
   sessions at once you need two MCP connections, or the HTTP API, which
   has no such limit.

2. Because the connection *is* the session context, most tools take no
   session_id. They act on the session this connection joined and return
   not_in_session until session_join has run.

The usual order: session_describe to look without committing,
session_join to bind, then session_send / session_respond and
session_wait_for_event to do the work, then session_leave. Open a new
session with session_create, which returns a one-time creator_token that
session_delete later requires."""


# mcp 2.x transport defaults, mirrored here so they are visible in the relay's
# own configuration instead of being inherited silently from the SDK.
_DEFAULT_MCP_SESSION_IDLE_TIMEOUT = 1800.0
_DEFAULT_MCP_MAX_SESSIONS = 10_000

def _mcp_session_idle_timeout() -> float | None:
    """How long the SDK keeps an idle MCP transport before terminating it.

    New in mcp 2.x and on by default (1800 s). It is what finally reclaims a
    transport whose client vanished without a DELETE — under 1.x those
    leaked for the life of the process. Set explicitly rather than inherited
    silently, because it changes a client-visible failure: past the timeout
    the session id answers 404 and the client must re-initialize onto a new
    connection, which means calling ``session_resume`` rather than finding
    itself mysteriously ``not_in_session``.

    ``SW_MCP_SESSION_IDLE_TIMEOUT`` overrides it; ``0`` disables the timeout,
    which is not recommended — it restores the 1.x leak.
    """
    raw = os.environ.get("SW_MCP_SESSION_IDLE_TIMEOUT", "").strip()
    if not raw:
        return _DEFAULT_MCP_SESSION_IDLE_TIMEOUT
    try:
        value = float(raw)
    except ValueError:
        logging.getLogger(__name__).warning(
            "SW_MCP_SESSION_IDLE_TIMEOUT=%r is not a number; using %s",
            raw, _DEFAULT_MCP_SESSION_IDLE_TIMEOUT,
        )
        return _DEFAULT_MCP_SESSION_IDLE_TIMEOUT
    if not math.isfinite(value) or value <= 0:
        # The SDK rejects non-finite values outright, which would take the
        # relay down at startup rather than at the misconfigured setting.
        return None
    return value


def _mcp_max_sessions() -> int:
    """Cap on concurrent MCP transports; beyond it the SDK answers 503.

    Also new in 2.x. Explicit for the same reason as the idle timeout: it is
    a new refusal path, and an operator hitting it should find the number in
    the relay's own configuration rather than in the SDK's defaults.
    """
    raw = os.environ.get("SW_MCP_MAX_SESSIONS", "").strip()
    if not raw:
        return _DEFAULT_MCP_MAX_SESSIONS
    try:
        return max(1, int(raw))
    except ValueError:
        logging.getLogger(__name__).warning(
            "SW_MCP_MAX_SESSIONS=%r is not an integer; using %s",
            raw, _DEFAULT_MCP_MAX_SESSIONS,
        )
        return _DEFAULT_MCP_MAX_SESSIONS


def _transport_security() -> TransportSecuritySettings:
    """DNS-rebinding protection for the MCP endpoint.

    In the mcp 2.x SDK this is no longer a server-constructor argument —
    it is passed to ``streamable_http_app()`` at mount time, so it lives
    in its own helper rather than inline at either call site.
    """
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=_allowed_hosts(),
    )


def _build_fastmcp(
    deps: ToolDeps,
    handles: HandleRegistry,
    connections: ConnectionManager | None = None,
    registry: SessionRegistry | None = None,
) -> MCPServer:
    mcp = MCPServer(
        "sessionweave",
        instructions=_SERVER_INSTRUCTIONS,
    )

    def _tool(name: str) -> Callable[[_ToolFn], _ToolFn]:
        """Register a tool, using its docstring — dedented — as the description.

        MCPServer takes ``fn.__doc__`` verbatim (``description or fn.__doc__
        or ""``), so an indented docstring reaches the client with the
        source indentation baked in. Cleaning it here keeps the docstrings
        readable in this file and flush left on the wire.
        """

        def deco(fn: _ToolFn) -> _ToolFn:
            mcp.tool(name=name, description=inspect.cleandoc(fn.__doc__ or ""))(
                _anticipated_failures(fn)
            )
            return fn

        return deco

    def _handle_for(ctx: Context) -> Any:
        return handles.for_connection(
            require_bindable_connection(connection_of(ctx)),
            connections=connections,
            registry=registry,
        )

    @asynccontextmanager
    async def _participant(
        ctx: Context,
        session_id: str | None,
        participant_token: str | None,
    ) -> AsyncIterator[Any]:
        """The handle a connection-bound tool should act through.

        Normally the connection *is* the identity: the participant is
        bound to it by ``session_join`` and every later tool call on that
        connection finds it. Some MCP runtimes cannot work that way —
        they open a fresh connection per tool call, so a binding never
        survives the call that created it. Measured: `claude -p` does
        exactly this, and so does anything on the 2026-07-28
        single-exchange protocol era.

        For those, the caller presents the identity explicitly instead,
        exactly as a REST client does with its Bearer token: the pair is
        validated against the live session and bound to an ephemeral
        handle for the duration of this one call
        (``http_api._bind_participant``, the same mechanism). Nothing is
        remembered afterwards, so it works no matter how the transport
        behaves.

        The connection binding wins when there is one, so nothing changes
        for a client that holds its connection open.
        """
        if connections is not None:
            try:
                bound = connections.session_for(_handle_for(ctx))
            except Exception:
                bound = None
            if bound is not None:
                yield _handle_for(ctx)
                return
        if not (session_id and participant_token):
            # No binding and no explicit identity: let the normal
            # not_in_session error come from the tool itself.
            yield _handle_for(ctx)
            return
        from sessionweave.http_api import _bind_participant, _unbind_participant
        handle = await _bind_participant(deps, session_id, participant_token)
        try:
            yield handle
        finally:
            _unbind_participant(deps.registry.connections, handle)

    @_tool("session_create")
    async def session_create_tool(
        config: dict[str, Any] | None = None,
        purpose: dict[str, Any] | None = None,
        persistence: dict[str, Any] | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Open a new session and return its session_id plus a one-time creator_token.

    Save the creator_token: it is returned exactly once, nothing can
    recover it, and session_delete requires it. Creating a session does
    not put you in it — call session_join next.

    persistence is opt-in. Omit it and the session lives in RAM only and
    is discarded once it goes idle, so nothing touches disk. Pass
    {"mode": "ttl", "ttl_seconds": N} or {"mode": "until_deleted"} to
    persist the roster, command timeline and buffered events.

    purpose is free-form JSON echoed to everyone who joins, so a joiner
    can act without a separate briefing.
    """
        return await session_create(
            deps, {"config": config, "purpose": purpose, "persistence": persistence}
        )

    @_tool("session_describe")
    async def session_describe_tool(
        session_id: str,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Read-only snapshot of a session: config, purpose, roster, persistence state.

    Allocates no participant and does not bind this connection, so it is
    the safe pre-flight before deciding whether to join.
    """
        return await session_describe(deps, {"session_id": session_id})

    @_tool("session_update_purpose")
    async def session_update_purpose_tool(
        purpose: dict[str, Any] | None = None,
        session_id: str | None = None,
        participant_token: str | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Replace the purpose of the session this connection joined, or clear it.

    Pass nothing to clear. Broadcasts session.purpose_updated to the
    other participants. Requires session_join on this connection first.
    """
        assert ctx is not None
        async with _participant(ctx, session_id, participant_token) as h:
            return await session_update_purpose(deps, h, {"purpose": purpose})

    @_tool("session_join")
    async def session_join_tool(
        session_id: str,
        role: str | None = None,
        objective: str | None = None,
        capabilities: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        delivery_mode: str | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Join a session as a new participant and bind this connection to it.

    Returns your participant_id, the current roster, the session purpose
    and relay_epoch — remember the epoch: a different value on a later
    resume means the relay restarted underneath you.

    One MCP connection is in at most one session at a time. A second join
    returns already_in_session; call session_leave first to move
    elsewhere.

    delivery_mode is "push" (default — events arrive as MCP
    notifications) or "pull" (events are buffered until you call
    session_wait_for_event or session_check_inbox). Prefer "push" unless
    your runtime cannot act on an incoming notification.
    """
        assert ctx is not None
        payload: dict[str, Any] = {"session_id": session_id}
        if role is not None:
            payload["role"] = role
        if objective is not None:
            payload["objective"] = objective
        if capabilities is not None:
            payload["capabilities"] = capabilities
        if metadata is not None:
            payload["metadata"] = metadata
        if delivery_mode is not None:
            payload["delivery_mode"] = delivery_mode
        return await session_join(deps, _handle_for(ctx), payload)

    @_tool("session_resume")
    async def session_resume_tool(
        session_id: str,
        participant_id: str,
        last_acked_seq: int | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Re-attach to a session you already joined, keeping the same participant_id.

    Use this after a disconnect rather than joining again, which would
    make you a second participant. Buffered events are replayed; pass
    last_acked_seq to skip the ones you already handled. Compare the
    returned relay_epoch against the one from your join — a different
    value means the relay restarted.

    Binds this connection just as session_join does, so it returns
    already_in_session if this connection is already in a session.
    """
        assert ctx is not None
        return await session_resume(
            deps,
            _handle_for(ctx),
            {
                "session_id": session_id,
                "participant_id": participant_id,
                "last_acked_seq": last_acked_seq,
            },
        )

    @_tool("session_leave")
    async def session_leave_tool(
        feedback: dict[str, Any] | None = None,
        session_id: str | None = None,
        participant_token: str | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Leave the session this connection joined and unbind the connection.

    After leaving, this connection is free to join another session.
    Broadcasts participant.left to the others; the optional feedback
    object travels with that event as a close-out note. Requires
    session_join on this connection first.
    """
        assert ctx is not None
        body = {"feedback": feedback} if feedback is not None else None
        async with _participant(ctx, session_id, participant_token) as h:
            return await session_leave(deps, h, body)

    @_tool("session_update_self")
    async def session_update_self_tool(
        role: str | None = None,
        objective: str | None = None,
        capabilities: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        session_id: str | None = None,
        participant_token: str | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Replace your own role, objective, capabilities or metadata.

    Applies to the session this connection joined. Fields you omit stay
    unchanged, but a field you do pass replaces its previous value
    outright — lists are not merged. Requires session_join on this
    connection first.
    """
        assert ctx is not None
        payload: dict[str, Any] = {}
        if role is not None:
            payload["role"] = role
        if objective is not None:
            payload["objective"] = objective
        if capabilities is not None:
            payload["capabilities"] = capabilities
        if metadata is not None:
            payload["metadata"] = metadata
        async with _participant(ctx, session_id, participant_token) as h:
            return await session_update_self(deps, h, payload)

    @_tool("session_list_participants")
    async def session_list_participants_tool(
        session_id: str | None = None,
        participant_token: str | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Return the roster of the session this connection joined.

    Each entry carries the participant's role, capabilities, objective
    and whether it is currently online. Requires session_join on this
    connection first.
    """
        assert ctx is not None
        async with _participant(ctx, session_id, participant_token) as h:
            return await session_list_participants(deps, h)

    @_tool("session_check_inbox")
    async def session_check_inbox_tool(
        session_id: str | None = None,
        participant_token: str | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Drain the events buffered for you right now and return them, empty or not.

    Returns immediately — use session_wait_for_event to block until
    something arrives instead of polling this in a loop. Needed when you
    joined with delivery_mode="pull"; push-mode participants receive
    events as notifications instead. Requires session_join on this
    connection first.
    """
        assert ctx is not None
        async with _participant(ctx, session_id, participant_token) as h:
            return await session_check_inbox(deps, h)

    @_tool("session_wait_for_event")
    async def session_wait_for_event_tool(
        session_id: str,
        names: list[str] | None = None,
        timeout_seconds: float = 60.0,
        participant_token: str | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Block until at least one buffered event matches, then return every match.

    The long-poll that keeps a turn alive without busy-polling
    session_check_inbox. names filters by event name (for example
    ["command.received"]); omit it to accept anything. A response with
    timed_out=true means the budget elapsed with nothing matching — that
    is normal, loop and call again. Requires session_join on this
    connection first.
    """
        assert ctx is not None
        body: dict[str, Any] = {
            "session_id": session_id,
            "timeout_seconds": timeout_seconds,
        }
        if names is not None:
            body["names"] = names
        async with _participant(ctx, session_id, participant_token) as h:
            return await session_wait_for_event(deps, h, body)

    @_tool("session_send")
    async def session_send_tool(
        dispatch: str,
        target: dict[str, Any],
        payload: dict[str, Any],
        timeout_seconds: int | None = None,
        session_id: str | None = None,
        participant_token: str | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Dispatch a command to other participants of the session this connection joined.

    Returns as soon as the command reaches its recipients
    ({command_id, delivered_to}). It does NOT wait for their answers:
    results arrive later as result.received and command.completed events,
    which you collect with session_wait_for_event or session_check_inbox.

    dispatch chooses the recipients:
      "specific" - requires target.participant_ids; exactly those.
      "all"      - requires target.role or target.capability; every
                   online match.
      "any"      - requires target.role or target.capability; one match,
                   picked round-robin.

    payload is your free-form command body. Pass a stable command_id you
    generate yourself to make retries idempotent — a repeat with the same
    id returns the original dispatch's response instead of sending twice.
    Requires session_join on this connection first.
    """
        assert ctx is not None
        body: dict[str, Any] = {
            "dispatch": dispatch,
            "target": target,
            "payload": payload,
        }
        if timeout_seconds is not None:
            body["timeout_seconds"] = timeout_seconds
        async with _participant(ctx, session_id, participant_token) as h:
            return await session_send(deps, h, body)

    @_tool("session_respond")
    async def session_respond_tool(
        command_id: str,
        payload: dict[str, Any],
        status: str = "ok",
        session_id: str | None = None,
        participant_token: str | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Answer a command you received.

    command_id comes from the command.received event that delivered the
    command. status is "ok" or "error" (default "ok"); payload is your
    free-form answer. Requires session_join on this connection first.
    """
        assert ctx is not None
        async with _participant(ctx, session_id, participant_token) as h:
            return await session_respond(
                deps, h,
                {"command_id": command_id, "payload": payload, "status": status},
            )

    @_tool("session_delete")
    async def session_delete_tool(
        session_id: str,
        creator_token: str,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Delete a session permanently, for everyone in it.

    Requires the creator_token returned once by session_create. Every
    participant is disconnected with session.ended, and a persisted
    session's stored timeline is removed. Needs no session_join, and
    cannot be undone.
    """
        return await session_delete(
            deps, {"session_id": session_id, "creator_token": creator_token}
        )

    @_tool("session_get_events")
    async def session_get_events_tool(
        session_id: str,
        after_seq: int | None = None,
        before_seq: int | None = None,
        kind: str | None = None,
        participant_id: str | None = None,
        limit: int = 200,
        order: str = "asc",
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Browse the timeline of a persisted session without bringing it back into RAM.

    Returns paginated events plus session_state ("hot", "cold" or
    "ended"). Page with after_seq / before_seq and limit; order is "asc"
    (default) or "desc". Narrow with kind and participant_id. An
    ephemeral session stores no timeline and returns no_history. Needs no
    session_join.
    """
        payload = {
            "session_id": session_id,
            "after_seq": after_seq,
            "before_seq": before_seq,
            "kind": kind,
            "participant_id": participant_id,
            "limit": limit,
            "order": order,
        }
        return await session_get_events(deps, payload)

    return mcp


def build_app() -> Starlette:
    """Build the Starlette app exposing the MCP endpoint and health probes."""
    deps_cfg = load_deployment_defaults(os.environ)
    connections = ConnectionManager()
    metrics = Metrics()
    firehose = FirehoseRegistry()
    registry = SessionRegistry(deps_cfg, connections, metrics=metrics, firehose=firehose)
    handles = HandleRegistry()
    # Per-process startup epoch. Surfaced on /v1/info, session_join,
    # and session_resume so SDK clients can detect relay restart and
    # invalidate their local seq cursor. See #65.
    relay_epoch = int(time.time())
    delete_rate_limiter = DeleteRateLimiter()
    tool_deps = ToolDeps(
        registry,
        relay_epoch=relay_epoch,
        bounds=deps_cfg.bounds,
        metrics=metrics,
        delete_rate_limiter=delete_rate_limiter,
    )

    mcp = _build_fastmcp(tool_deps, handles, connections=connections, registry=registry)
    mcp_app = mcp.streamable_http_app(
        transport_security=_transport_security(),
        session_idle_timeout=_mcp_session_idle_timeout(),
        max_sessions=_mcp_max_sessions(),
    )

    state: dict[str, bool] = {"draining": False}

    async def healthz(request: Request) -> JSONResponse:
        if state["draining"]:
            return JSONResponse(
                {"status": "draining", "version": __version__},
                status_code=503,
            )
        return JSONResponse({"status": "ok", "version": __version__})

    async def readyz(request: Request) -> JSONResponse:
        return JSONResponse({"status": "ready", "version": __version__})

    async def info(request: Request) -> JSONResponse:
        # ``auth_model`` lets SDK clients detect they're talking to the
        # sub-project #1 deployment (bearer == participant_id, no
        # rotation / expiry / scope). New SDK versions can surface a
        # one-shot warning when this value is "shared-bearer-sub-project-1"
        # so users don't unwittingly assume production-grade auth.
        # Sub-project #3 (JWT scoped tokens) will return a different
        # string here.
        #
        # ``relay_epoch`` is the UNIX-seconds startup time. External
        # monitors and SDK clients can poll this; a change means the
        # relay has restarted and any in-memory state they remember
        # (seq cursors, participant_ids) is gone. See #65.
        return JSONResponse(
            {
                "name": "sessionweave",
                "version": __version__,
                "subproject": "#1 (anonymous, transient, in-memory)",
                "auth_model": "shared-bearer-sub-project-1",
                "relay_epoch": relay_epoch,
                "transports": ["mcp", "http", "ws"],
                "capabilities": {
                    "firehose": True,
                    "supervisor_notes": True,
                    "session_delete": True,
                },
            }
        )

    metrics_token = os.environ.get("SW_METRICS_TOKEN", "").strip() or None

    async def metrics_endpoint(request: Request) -> Response:
        # When SW_METRICS_TOKEN is set, require a matching bearer token.
        # Unset → endpoint is open (backwards-compatible default).
        if metrics_token is not None:
            auth = request.headers.get("authorization", "")
            if not auth.lower().startswith("bearer "):
                return Response("unauthorized", status_code=401)
            if auth[7:].strip() != metrics_token:
                return Response("forbidden", status_code=403)
        return Response(metrics.render(), media_type=METRICS_CONTENT_TYPE)

    _openapi_path = Path(__file__).parent / "openapi.yaml"

    async def openapi_yaml(request: Request) -> Response:
        """Canonical OpenAPI spec, shipped alongside the package.

        Served as ``application/yaml``; clients that need JSON can pipe
        through any YAML→JSON converter. CORS is handled by the
        global middleware.
        """
        return FileResponse(_openapi_path, media_type="application/yaml")

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        _log = logging.getLogger(__name__)
        db_path = deps_cfg.defaults.persistence_db_path
        store: PersistenceStore | None = None
        sweep_task: asyncio.Task | None = None
        if db_path:
            store = PersistenceStore(db_path)
            await store.init(pool_size=deps_cfg.defaults.persistence_db_pool_size)
            # Integrity check
            assert store._write_conn is not None
            async with store._write_conn.execute("PRAGMA integrity_check") as cur:
                row = await cur.fetchone()
            val = row[0] if row else "unknown"
            if val != "ok":
                _log.critical("persistence integrity check failed: %r", val)
                raise SystemExit(2)
            # Stranded recovery
            stranded = await store.find_stranded(
                now_ms=int(time.time() * 1000),
                idle_threshold_hours=24,
            )
            for sid in stranded:
                await store.mark_session_ended(
                    sid, "startup_stranded", int(time.time() * 1000)
                )
                if metrics is not None:
                    metrics.session_stranded_recovered_total.inc()
            # Hand store to registry
            registry._store = store
            # Sweep loop
            sweep_task = asyncio.create_task(
                registry.sweep_expired_loop(
                    interval_seconds=deps_cfg.defaults.persistence_sweep_interval_seconds,
                    safety_seconds=deps_cfg.defaults.persistence_until_deleted_safety_seconds,
                )
            )
        app.state.persistence_store = store
        async with AsyncExitStack() as stack:
            await registry.start()
            stack.push_async_callback(_drain_registry, registry, state)
            await stack.enter_async_context(mcp_app.router.lifespan_context(mcp_app))
            try:
                yield
            finally:
                if sweep_task is not None:
                    sweep_task.cancel()
                    try:
                        await sweep_task
                    except (asyncio.CancelledError, Exception):
                        pass
                if store is not None:
                    await store.close()

    cors_cfg = CORSConfig.from_env(os.environ)
    app = Starlette(
        routes=[
            Route("/healthz", healthz),
            Route("/readyz", readyz),
            Route("/v1/info", info),
            Route("/openapi.yaml", openapi_yaml),
            Route("/metrics", metrics_endpoint),
            *http_api_routes(),
            *events_ws_routes(),
            Mount("/", app=mcp_app),
        ],
        middleware=[cors_cfg.as_middleware()],
        lifespan=lifespan,
    )
    app.state.registry = registry
    app.state.connections = connections
    app.state.handles = handles
    app.state.metrics = metrics
    app.state.tool_deps = tool_deps
    app.state.delete_rate_limiter = delete_rate_limiter
    app.state.state = state
    app.state.firehose = firehose
    # Optional relay-wide admin token gating the firehose endpoint.
    # When set, the firehose WS upgrade requires ``?token=<this value>``
    # — knowing the session_id is no longer sufficient. When unset, the
    # legacy ``token == session_id`` check remains active so existing
    # dev clients keep working (with a startup warning).
    # Operators in multi-tenant production should set this.
    app.state.firehose_admin_token = os.environ.get("SW_FIREHOSE_ADMIN_TOKEN", "") or None
    if app.state.firehose_admin_token is None:
        logging.getLogger(__name__).warning(
            "SW_FIREHOSE_ADMIN_TOKEN not set; firehose authenticates with "
            "token==session_id (LEGACY mode). Any session participant can "
            "subscribe and observe the full event stream. Set the env var "
            "in production deployments."
        )
    # Optional relay-wide admin token gating the supervisor-notes
    # endpoint. Mirrors the firehose-admin-token pattern: when set, the
    # supervisor /notes POST requires this token instead of accepting
    # ``token == session_id``. Knowing the session_id alone is no longer
    # enough to halt or redirect participants, so an XSS on a session
    # roster page cannot escalate to "kill all agents".
    app.state.supervisor_admin_token = (
        os.environ.get("SW_SUPERVISOR_ADMIN_TOKEN", "") or None
    )
    if app.state.supervisor_admin_token is None:
        logging.getLogger(__name__).warning(
            "SW_SUPERVISOR_ADMIN_TOKEN not set; supervisor /notes endpoint "
            "authenticates with token==session_id (LEGACY mode). Any holder "
            "of the session_id (every session participant inherently) can "
            "POST supervisor notes targeting any participant, including "
            "kind=halt. Set the env var in multi-tenant production "
            "deployments."
        )
    app.state.supervisor_rate = NoteRateLimiter(
        per_minute=_env_int("SW_SUPERVISOR_NOTES_PER_MINUTE", 30, lo=1, hi=10_000),
        per_hour=_env_int("SW_SUPERVISOR_NOTES_PER_HOUR", 200, lo=1, hi=1_000_000),
    )
    app.state.public_base_url = os.environ.get("SW_PUBLIC_BASE_URL", "").rstrip("/") or None
    app.state.brief_upgrades = BriefUpgrades(
        skill_url=os.environ.get("SW_SKILL_URL", ""),
        mcp_snippet_url=os.environ.get("SW_MCP_SNIPPET_URL", ""),
        runner_install_url=os.environ.get("SW_RUNNER_INSTALL_URL", ""),
    )
    return app


def _env_int(name: str, default: int, *, lo: int, hi: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as e:
        raise ToolError(
            ErrorCode.INVALID_CONFIG,
            f"{name}={raw!r} is not an integer: {e}",
        ) from e
    if value < lo or value > hi:
        raise ToolError(
            ErrorCode.INVALID_CONFIG,
            f"{name}={value} outside bounds [{lo}, {hi}]",
        )
    return value


async def _drain_registry(registry: SessionRegistry, state: dict[str, bool]) -> None:
    state["draining"] = True
    await registry.shutdown(reason="server_shutdown")


__all__ = ["ToolError", "build_app"]
