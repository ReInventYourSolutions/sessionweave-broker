from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import math
import os
import re
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from typing import Annotated, Any, Literal, TypeVar, cast

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError as MCPToolError
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import Field, ValidationError
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Mount, Route

from sessionweave import __version__, accounts_api, machines_api
from sessionweave.access import AuthFailureLimiter, admin_matches
from sessionweave.account_events import (
    EventLog,
    RequestClientMiddleware,
    client_label,
    request_client,
)
from sessionweave.accounts import TOKEN_PREFIX as ACCOUNT_TOKEN_PREFIX
from sessionweave.accounts import AccountStore, session_creation_mode
from sessionweave.brief import BriefUpgrades
from sessionweave.config import load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.cors import CORSConfig
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.events_ws import routes as events_ws_routes
from sessionweave.firehose import FirehoseRegistry
from sessionweave.http_api import routes as http_api_routes
from sessionweave.launchers import OPEN_OWNER, PROTOCOL_VERSION, launcher_token_from_env
from sessionweave.launchers import routes as launcher_routes
from sessionweave.limits import (
    McpNewSessionLimitMiddleware,
    RequestSizeLimitMiddleware,
    account_events_retention_days,
    account_max_sessions,
    account_max_tokens,
    open_machines_retention_days,
)
from sessionweave.machines import MachineStore
from sessionweave.machines_api import routes as machines_routes
from sessionweave.mcp_adapter import (
    HandleRegistry,
    connection_of,
    require_bindable_connection,
)
from sessionweave.metrics import METRICS_CONTENT_TYPE, Metrics
from sessionweave.persistence import PersistenceStore
from sessionweave.registry import SessionRegistry
from sessionweave.schemas import ReplyOutcome
from sessionweave.supervisor import NoteRateLimiter
from sessionweave.tools import (
    ToolDeps,
    agents_discover,
    agents_launch,
    agents_launch_status,
    current_origin,
    session_check_inbox,
    session_create,
    session_delete,
    session_describe,
    session_get_events,
    session_join,
    session_leave,
    session_list_participants,
    session_list_supervisor_tokens,
    session_mint_join_token,
    session_mint_supervisor_token,
    session_post_order,
    session_remove_participant,
    session_reply_to_note,
    session_respond,
    session_resume,
    session_revoke_supervisor_token,
    session_send,
    session_set_coordinator,
    session_update_purpose,
    session_update_self,
    session_wait_for_event,
)

_LOOPBACK_HOSTS: tuple[str, ...] = ("127.0.0.1:*", "localhost:*", "[::1]:*")


def _allowed_hosts() -> list[str]:
    """Build the DNS-rebinding ``allowed_hosts`` list for the MCP transport.

    Defaults to loopback only (matches the SDK's stock policy). Extra
    hosts come from ``SW_ALLOWED_HOSTS`` (comma-separated, e.g.
    ``broker.sessionweave.com,broker.sessionweave.com:443``). Wildcard
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

The usual order: session_describe (with an access_token) to look without
committing, session_join (with the join_token from the invite link) to bind,
then session_send / session_respond and session_wait_for_event to do the work,
then session_leave. Joining needs a credential: the join_token carried by an
invite link (swv://host/<session_id>?jt=<join_token>), the creator_token, or
the relay admin token. The session id alone grants nothing, and neither does
a participant_id: session_join returns a secret participant_token, and that is
the credential for a client that has no stable connection (pass session_id +
participant_token explicitly, or to session_resume).

Open a new session with session_create. It returns the invite link to hand to
the agents you want to involve, a creator_token (delete the session, change its
purpose, mint tokens) and a supervisor_token (observe and post supervisor
notes), plus the first join_token. Hand each back to whoever asked, labelled
with its purpose; the plaintext is shown once."""


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


# ---- Parameter descriptions -------------------------------------------------
#
# Every MCP tool parameter carries a description in the JSON schema, and the
# string-valued enums are real ``enum``s. Parameters that recur across tools
# are defined once here, so the same argument cannot drift into two
# different explanations in two tools.

SessionIdArg = Annotated[str, Field(
    description="The session to act on: the id from the invite link, or the "
                "session_id returned by session_create.")]

OptSessionIdArg = Annotated[str | None, Field(
    description="Only together with participant_token (or a participant "
                "token sent as the Authorization: Bearer header), for a "
                "runtime that opens a new connection per tool call. Leave "
                "unset if you joined on this same connection.")]

ParticipantTokenArg = Annotated[str | None, Field(
    description="The participant_token session_join returned — not your "
                "participant_id — passed with session_id. Needed only if a "
                "call right after joining said not_in_session — your runtime "
                "opens a new connection per call. Otherwise leave unset. It "
                "is a credential: keep it secret.")]

ManagerTokenArg = Annotated[str | None, Field(
    description="The one-time creator_token session_create returned (it cannot "
                "be recovered), the token of the account that owns the session, "
                "or the relay operator's admin token. May be omitted when it "
                "arrives as the MCP Authorization: Bearer header.")]

RemoveTokenArg = Annotated[str | None, Field(
    description="Named creator_token for compatibility; it takes any credential that "
                "may remove participants: the one-time creator_token, the token of the "
                "account that owns the session or the relay admin token (anyone), or "
                "the session coordinator's participant_token (launched agents only). "
                "May be omitted when it arrives as the MCP Authorization: Bearer "
                "header.")]

AccessTokenArg = Annotated[str | None, Field(
    description="Any credential that may read the session: the join token "
                "from the invite link, your participant_token, a "
                "supervisor_token, the creator_token, the owning account's "
                "token, or the relay admin token. Not needed if you already joined on this same "
                "connection. It is a credential: keep it secret.")]

AccountTokenArg = Annotated[str | None, Field(
    description="One of your account's API tokens (starts with swa_). May be omitted "
                "when it arrives as the MCP Authorization: Bearer header. It is a "
                "credential: keep it secret.")]

AdminTokenArg = Annotated[str | None, Field(
    description="The relay operator's admin token (SW_ADMIN_TOKEN). Required: admin "
                "tools never read it from the MCP Authorization header. Wrong values "
                "are counted and throttled per address. Keep it secret.")]

AdminAccountIdArg = Annotated[str, Field(
    description="The account to act on: an account_id from admin_accounts.")]

TokenLabelArg = Annotated[str, Field(
    description="A name for the new token, 1-64 characters, e.g. the machine or tool "
                "that will use it.")]

TokenExpiresArg = Annotated[str | None, Field(
    description="Optional expiry, an ISO 8601 timestamp in the future, e.g. "
                "2030-01-01T00:00:00Z (no timezone means UTC). Omit for no expiry.")]

TokenPolicyArg = Annotated[str | None, Field(
    description='Network policy: "any" (default; usable from anywhere), "first_use" '
                "(bound to the network of its first use) or \"approved_only\" (only "
                "from approved_networks; other addresses wait for your approval).")]

TokenNetworksArg = Annotated[list[str] | None, Field(
    description="Approved networks for the token, as CIDRs or IPs, e.g. "
                '["10.0.0.0/8", "192.0.2.7"]. Omit for none.')]

RevokeTokenIdArg = Annotated[str | None, Field(
    description="Replace a token: the token_id of a token of the same account to revoke "
                "once the new one exists (checked first: an unknown id mints nothing).")]


MachinesTokenArg = Annotated[str | None, Field(
    description="One of your account's API tokens (starts with swa_): you see and act "
                "on your own machines only. Omit on an open-mode broker (every machine, "
                "no credential), or when it arrives as the MCP Authorization: Bearer "
                "header. Keep it secret.")]

MachineKeyArg = Annotated[str, Field(
    description="The machine's launcher_key (lk_ followed by 32 hex characters), "
                "from machines_list.")]

DefinitionIdArg = Annotated[str, Field(
    description="The agent definition's id (its profile id on the machine), e.g. obs.")]

NetworksTokenIdArg = Annotated[str, Field(
    description="The token_id whose networks to show or change (from account_tokens "
                "or admin_account).")]

NetworksPolicyArg = Annotated[str | None, Field(
    description='New network policy: "any", "first_use" or "approved_only". Omit to '
                "keep it (and to only read when approved_networks is omitted too).")]

NetworksListArg = Annotated[list[str] | None, Field(
    description="New approved networks (CIDRs or IPs), replacing the whole list; [] "
                "clears it. Omit to keep them.")]

ApprovalIdArg = Annotated[str, Field(
    description="The approval_id of a pending approval (from account_approvals or "
                "admin_approvals).")]

ApprovalScopeArg = Annotated[str, Field(
    description='What to approve: "ip" (default; that exact address) or "network" '
                "(its /24 for IPv4, /64 for IPv6).")]

PurposeArg = Annotated[dict[str, Any] | None, Field(
    description="Free-form JSON telling joiners what the session is for; "
                "echoed to everyone who joins. Recognised keys: kind, "
                "your_role, your_capabilities, available_roles, instructions, "
                "delivery_mode. 16 KiB cap.")]


def _note_rate_limiter() -> NoteRateLimiter:
    return NoteRateLimiter(
        per_minute=_env_int("SW_SUPERVISOR_NOTES_PER_MINUTE", 30, lo=1, hi=10_000),
        per_hour=_env_int("SW_SUPERVISOR_NOTES_PER_HOUR", 200, lo=1, hi=1_000_000),
    )


def _build_fastmcp(
    deps: ToolDeps,
    handles: HandleRegistry,
    connections: ConnectionManager | None = None,
    registry: SessionRegistry | None = None,
    note_limiter: NoteRateLimiter | None = None,
    firehose: FirehoseRegistry | None = None,
) -> MCPServer:
    # Shared with the HTTP notes route (``app.state.supervisor_rate``) so both
    # surfaces draw on one per-(session, source ip) budget.
    order_limiter = note_limiter if note_limiter is not None else _note_rate_limiter()
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

    def _request_of(ctx: Context | None) -> Any:
        return getattr(getattr(ctx, "request_context", None), "request", None)

    def _client_ip(ctx: Context | None) -> str:
        """The request's client IP; also records this tool call's client label
        (User-Agent plus MCP clientInfo) for the account-token origin."""
        request_client.set(_client_label(ctx))
        client = getattr(_request_of(ctx), "client", None)
        return getattr(client, "host", "") or ""

    def _client_label(ctx: Context | None) -> str | None:
        headers = getattr(_request_of(ctx), "headers", None)
        ua = headers.get("user-agent") if headers is not None else None
        info = None
        try:
            params = ctx.session.client_params if ctx is not None else None
            info = getattr(params, "client_info", None) or getattr(params, "clientInfo", None)
        except Exception:  # no session (stateless call) or an older SDK shape
            info = None
        return client_label(
            ua, getattr(info, "name", None), getattr(info, "version", None)
        )

    def _bearer_of(ctx: Context | None) -> str | None:
        """Bearer token of the MCP HTTP request's Authorization header, if any."""
        headers = getattr(_request_of(ctx), "headers", None)
        auth = (headers.get("authorization") if headers is not None else "") or ""
        if auth.lower().startswith("bearer "):
            return auth[len("bearer "):].strip() or None
        return None

    def _creator_cred(explicit: str | None, ctx: Context | None) -> str:
        """The creator-level credential: explicit argument, else Bearer header."""
        return explicit or _bearer_of(ctx) or ""

    def _public_base_url(ctx: Context | None) -> str | None:
        """Configured public base URL, else the MCP HTTP request's own base URL."""
        configured = os.environ.get("SW_PUBLIC_BASE_URL", "").rstrip("/")
        if configured:
            return configured
        req = _request_of(ctx)
        base = getattr(req, "base_url", None)
        return str(base).rstrip("/") if base is not None else None

    def _handle_for(ctx: Context) -> Any:
        return handles.for_connection(
            require_bindable_connection(connection_of(ctx)),
            connections=connections,
            registry=registry,
        )

    def _optional_handle(ctx: Context | None) -> Any:
        """This connection's handle, or None when there is none to bind."""
        if ctx is None:
            return None
        try:
            return _handle_for(ctx)
        except Exception:
            return None

    async def _may_be_participant_token(bearer: str, session_id: str) -> bool:
        """False for a Bearer that is another kind of credential (admin, account,
        or a creator/supervisor/join/launch token of the session, even revoked
        or expired): it is the documented header for session_create and
        manager tools, so it is never tried as a participant token (which would
        count an auth failure and change the error the caller gets). A cold
        (persisted) session is loaded to tell."""
        if admin_matches(bearer, deps.admin_token) or bearer.startswith(ACCOUNT_TOKEN_PREFIX):
            return False
        try:
            sess = await deps.registry.get_or_raise(session_id)
        except ToolError:
            return True  # the bind reports the unknown session
        return not sess.auth.is_session_credential(bearer)

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
        # A launched agent's runtime carries its participant token only as
        # the Authorization: Bearer header of the MCP request (it never
        # appears in the agent's prompt), so the header stands in for an
        # absent explicit token. The session still has to be named: the
        # relay keeps no token -> session index.
        token = participant_token
        if not token and session_id:
            bearer = _bearer_of(ctx)
            if bearer and await _may_be_participant_token(bearer, session_id):
                token = bearer
        if not (session_id and token):
            # No binding and no explicit identity: let the normal
            # not_in_session error come from the tool itself.
            yield _handle_for(ctx)
            return
        from sessionweave.http_api import _bind_participant, _unbind_participant
        handle = await _bind_participant(
            deps, session_id, token, source_ip=_client_ip(ctx)
        )
        try:
            yield handle
        finally:
            _unbind_participant(deps.registry.connections, handle)

    @_tool("session_create")
    async def session_create_tool(
        config: Annotated[dict[str, Any] | None, Field(description=
            'Per-session overrides of relay defaults, e.g. {"idle_ttl_seconds": 3600}. Values '
            'outside the operator bounds are refused with invalid_config. Omit to use the '
            'defaults.'
        )] = None,
        purpose: PurposeArg = None,
        persistence: Annotated[dict[str, Any] | None, Field(description=
            'Omit for RAM-only, zero data retention (the default). Pass {"mode": "ttl", '
            '"ttl_seconds": N} or {"mode": "until_deleted"} to persist to disk. Refused with '
            'persistence_failed if this relay has persistence disabled.'
        )] = None,
        account_token: Annotated[str | None, Field(description=
            'Account API token authorising session creation. Used only when the MCP HTTP '
            'request carries no Authorization: Bearer header. Required on relays running in '
            'account mode; omit on open relays.'
        )] = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Open a new session; returns session_id, invite_link and the one-time tokens.

    Save creator_token and supervisor_token: each is returned exactly once
    and nothing can recover it. creator_token manages the session
    (session_delete, minting and revoking tokens); supervisor_token lets a
    supervisor observe it. invite_link (swv:// or swvs://) and invite_url
    (HTTP) carry join_token, a credential valid for 15 minutes that anyone
    can use to join; mint a fresh one with session_mint_join_token.
    Creating a session does not put you in it — call session_join next.

    persistence is opt-in. Omit it and the session lives in RAM only and
    is discarded once it goes idle, so nothing touches disk. Pass
    {"mode": "ttl", "ttl_seconds": N} or {"mode": "until_deleted"} to
    persist the roster, command timeline and buffered events.

    purpose is free-form JSON echoed to everyone who joins, so a joiner
    can act without a separate briefing.
    """
        return await session_create(
            deps,
            {"config": config, "purpose": purpose, "persistence": persistence},
            public_base_url=_public_base_url(ctx),
            bearer=_bearer_of(ctx) or account_token or None,
            source_ip=_client_ip(ctx),
        )

    @_tool("session_describe")
    async def session_describe_tool(
        session_id: SessionIdArg,
        access_token: AccessTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Read-only snapshot of a session: config, purpose, roster, persistence state.

    Allocates no participant and does not bind this connection, so it is
    the safe pre-flight before deciding whether to join. Needs an
    access_token (the join token from the invite link will do) unless this
    connection already joined the session.
    """
        return await session_describe(
            deps,
            {"session_id": session_id, "access_token": access_token},
            handle=_optional_handle(ctx),
            source_ip=_client_ip(ctx),
        )

    @_tool("session_update_purpose")
    async def session_update_purpose_tool(
        session_id: SessionIdArg,
        purpose: PurposeArg = None,
        creator_token: ManagerTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Replace the purpose of a session, or clear it.

    Pass no purpose to clear. Requires the creator_token (or the relay
    admin token, or the owning account's token, also accepted from the MCP
    Authorization: Bearer header); participants cannot change the purpose. Broadcasts
    session.purpose_updated to the participants. Needs no session_join.
    """
        return await session_update_purpose(
            deps,
            {
                "session_id": session_id,
                "creator_token": _creator_cred(creator_token, ctx),
                "purpose": purpose,
            },
            source_ip=_client_ip(ctx),
        )

    @_tool("session_join")
    async def session_join_tool(
        session_id: SessionIdArg,
        role: Annotated[str | None, Field(description=
            'Who you are in this session, e.g. "worker". Take purpose.your_role if the session '
            'prescribes one. Always pass it: dispatch by role relies on it.'
        )] = None,
        objective: Annotated[str | None, Field(description=
            "One line on what you are here to do, shown to the other participants in the roster."
        )] = None,
        capabilities: Annotated[list[str] | None, Field(description=
            'Strings other participants target with dispatch "all"/"any", e.g. ["chat"]. Always '
            'pass them: a participant without capabilities is invisible to capability-filtered '
            'dispatch.'
        )] = None,
        metadata: Annotated[dict[str, Any] | None, Field(description=
            "Free-form JSON attached to your roster entry."
        )] = None,
        delivery_mode: Annotated[Literal["push", "pull"] | None, Field(description=
            '"push" (default): events arrive as MCP notifications. "pull": events are buffered for'
            ' session_check_inbox / session_wait_for_event. Choose pull if you intend to long-poll'
            ' — session_wait_for_event refuses push participants.'
        )] = None,
        join_token: Annotated[str | None, Field(description=
            "The join token from your invite link (the jt value). The session creator's token "
            "or the relay admin token also works. Required: without a valid one the join is "
            "refused with permission_denied; an expired link means ask the creator for a new one."
        )] = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Join a session as a new participant and bind this connection to it.

    Needs a join token (see join_token). Returns your participant_id and
    your secret participant_token (keep it: it is your credential, shown
    once — pass it to session_resume, or as participant_token with
    session_id if your runtime opens a new connection per call), the
    current roster, the session purpose
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
        if join_token is not None:
            payload["join_token"] = join_token
        return await session_join(
            deps, _handle_for(ctx), payload, source_ip=_client_ip(ctx)
        )

    @_tool("session_resume")
    async def session_resume_tool(
        session_id: SessionIdArg,
        participant_token: Annotated[str, Field(description=
            "The participant_token session_join returned (not your participant_id). Resuming "
            "keeps that identity; joining again would make you a second participant."
        )],
        last_acked_seq: Annotated[int | None, Field(description=
            "Highest event seq you already processed; buffered events up to it are not replayed. "
            "Omit to replay everything buffered."
        )] = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Re-attach to a session you already joined, keeping the same participant.

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
                "participant_token": participant_token,
                "last_acked_seq": last_acked_seq,
            },
            source_ip=_client_ip(ctx),
        )

    @_tool("session_leave")
    async def session_leave_tool(
        feedback: Annotated[dict[str, Any] | None, Field(description=
            'Exit report broadcast to the others on participant.left: {"summary": "...", "rating":'
            ' 1-5, "issues": [...], "metrics": {...}}. Always send one, even {"summary": "no '
            'issues", "rating": 5, "issues": []}. 4 KiB cap.'
        )] = None,
        session_id: OptSessionIdArg = None,
        participant_token: ParticipantTokenArg = None,
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
        role: Annotated[str | None, Field(description=
            "New role; replaces the old one. Omit to keep it."
        )] = None,
        objective: Annotated[str | None, Field(description=
            "New objective line; replaces the old one. Omit to keep it."
        )] = None,
        capabilities: Annotated[list[str] | None, Field(description=
            "New capability list. REPLACES the old list outright — pass the full set you want, not"
            " just additions. Omit to keep it."
        )] = None,
        metadata: Annotated[dict[str, Any] | None, Field(description=
            "New metadata object; replaces the old one, no merge. Omit to keep it."
        )] = None,
        session_id: OptSessionIdArg = None,
        participant_token: ParticipantTokenArg = None,
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
        session_id: OptSessionIdArg = None,
        participant_token: ParticipantTokenArg = None,
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
        session_id: OptSessionIdArg = None,
        participant_token: ParticipantTokenArg = None,
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
        session_id: SessionIdArg,
        names: Annotated[list[str] | None, Field(description=
            'Event names to wait for, e.g. ["command.received", "supervisor.note"]. Omit to return'
            ' on any event.'
        )] = None,
        timeout_seconds: Annotated[float, Field(description=
            "How long to block, in seconds. Clamped to the relay MCP cap (default 20 s); "
            "effective_timeout_seconds in the response says what applied. timed_out=true is normal"
            " — just call again."
        )] = 60.0,
        participant_token: ParticipantTokenArg = None,
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
        dispatch: Annotated[Literal["specific", "all", "any"], Field(description=
            '"specific": exactly target.participant_ids. "all": every match of target.role / '
            'target.capability, including offline participants (their copy is buffered). "any": '
            'ONE match among participants online right now, round-robin — an offline participant '
            'is never picked.'
        )],
        target: Annotated[dict[str, Any], Field(description=
            'Who receives it: {"participant_ids": [...]} for specific, or {"role": "..."} / '
            '{"capability": "..."} for all and any.'
        )],
        payload: Annotated[dict[str, Any], Field(description=
            'Your command body, free-form JSON, e.g. {"body": "..."}. 256 KiB cap.'
        )],
        timeout_seconds: Annotated[int | None, Field(description=
            "How long recipients have to answer, in seconds (default 60, max 300). Recipients that"
            " have not answered get command.cancelled."
        )] = None,
        session_id: OptSessionIdArg = None,
        participant_token: ParticipantTokenArg = None,
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
        command_id: Annotated[str, Field(description=
            "The command_id from the command.received event you are answering. Answer before its "
            "expires_at, and not at all if command.cancelled arrived for it."
        )],
        payload: Annotated[dict[str, Any], Field(description=
            'Your answer, free-form JSON, e.g. {"body": "..."}.'
        )],
        status: Annotated[Literal["ok", "error"], Field(description=
            '"ok" (default), or "error" if you could not do the work.'
        )] = "ok",
        session_id: OptSessionIdArg = None,
        participant_token: ParticipantTokenArg = None,
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

    @_tool("session_reply_to_note")
    async def session_reply_to_note_tool(
        note_id: Annotated[str, Field(description=
            "The note_id from the supervisor.note event you are answering."
        )],
        message: Annotated[str, Field(description=
            "Your reply, non-empty, at most 2048 bytes."
        )],
        outcome: Annotated[ReplyOutcome | None, Field(description=
            "Optional structured result, used when answering an undo order: status "
            "(done, partial or impossible), undone (what was reverted) and "
            "not_reversible (what could not be). The message stays required."
        )] = None,
        session_id: OptSessionIdArg = None,
        participant_token: ParticipantTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Reply in the thread under a supervisor note.

    note_id comes from the supervisor.note event that delivered the note.
    A note sent to all is answered in the open: every other participant and
    the supervision holders see the reply. A note sent to you alone can only
    be answered by you, and only supervision holders see the reply. The
    thread is flat: reply to the note, not to another reply. Requires
    session_join on this connection first. Returns reply_id, note_id,
    participant_id, message and ts_ms (plus outcome when given).
    """
        assert ctx is not None
        async with _participant(ctx, session_id, participant_token) as h:
            return await session_reply_to_note(
                deps, h,
                {
                    "note_id": note_id,
                    "message": message,
                    "outcome": outcome.model_dump() if outcome is not None else None,
                },
            )

    @_tool("session_delete")
    async def session_delete_tool(
        session_id: SessionIdArg,
        creator_token: ManagerTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Delete a session permanently, for everyone in it.

    Requires the creator_token returned once by session_create (or the relay
    admin token, or the owning account's token, also accepted from the MCP
    Authorization: Bearer header). Every
    participant is disconnected with session.ended, and a persisted
    session's stored timeline is removed. Needs no session_join, and
    cannot be undone.
    """
        return await session_delete(
            deps,
            {"session_id": session_id, "creator_token": _creator_cred(creator_token, ctx)},
            source_ip=_client_ip(ctx),
        )

    @_tool("session_mint_join_token")
    async def session_mint_join_token_tool(
        session_id: SessionIdArg,
        creator_token: ManagerTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Mint a fresh join token and invite link for a session.

    Requires the creator_token (or the relay admin token, or
    the owning account's token, also accepted from the MCP
    Authorization: Bearer header). The join token
    is valid for 15 minutes and can be used by several joiners within that
    time; participants who already joined are unaffected when it expires.
    Returns join_token, expires_at, invite_url and invite_link.
    """
        return await session_mint_join_token(
            deps, session_id, _creator_cred(creator_token, ctx),
            public_base_url=_public_base_url(ctx), source_ip=_client_ip(ctx),
        )

    @_tool("session_mint_supervisor_token")
    async def session_mint_supervisor_token_tool(
        session_id: SessionIdArg,
        creator_token: ManagerTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Mint an additional supervisor token for a session.

    Requires the creator_token (or the relay admin token, or
    the owning account's token, also accepted from the MCP
    Authorization: Bearer header). The token is
    returned exactly once as supervisor_token, with a token_id you can
    pass to session_revoke_supervisor_token. A supervisor token cannot
    mint or revoke tokens.
    """
        return await session_mint_supervisor_token(
            deps, session_id, _creator_cred(creator_token, ctx), source_ip=_client_ip(ctx)
        )

    @_tool("session_list_supervisor_tokens")
    async def session_list_supervisor_tokens_tool(
        session_id: SessionIdArg,
        creator_token: ManagerTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """List a session's supervisor tokens (ids and timestamps, never the secrets).

    Requires the creator_token (or the relay admin token, or
    the owning account's token, also accepted from the MCP
    Authorization: Bearer header). Each entry has
    token_id, created_at and revoked_at (null while the token is active).
    """
        return {
            "supervisor_tokens": await session_list_supervisor_tokens(
                deps, session_id, _creator_cred(creator_token, ctx), source_ip=_client_ip(ctx)
            )
        }

    @_tool("session_revoke_supervisor_token")
    async def session_revoke_supervisor_token_tool(
        session_id: SessionIdArg,
        token_id: Annotated[str, Field(description=
            "The token_id of the supervisor token to revoke, from "
            "session_mint_supervisor_token, session_create or "
            "session_list_supervisor_tokens."
        )],
        creator_token: ManagerTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Revoke a supervisor token so it stops working immediately.

    Requires the creator_token (or the relay admin token, or
    the owning account's token, also accepted from the MCP
    Authorization: Bearer header). Fails with
    token_not_found if token_id is unknown. Returns {"revoked": true}.
    """
        await session_revoke_supervisor_token(
            deps, session_id, _creator_cred(creator_token, ctx), token_id, source_ip=_client_ip(ctx)
        )
        return {"revoked": True}

    @_tool("session_remove_participant")
    async def session_remove_participant_tool(
        session_id: SessionIdArg,
        participant_id: Annotated[str, Field(description=
            "The participant_id of the agent to remove, as shown in the roster."
        )],
        creator_token: RemoveTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Remove one agent from a session.

    Requires the creator_token (or the relay admin token, or
    the owning account's token, also accepted from the MCP
    Authorization: Bearer header), or the coordinator's participant token
    for launched agents (pass it in creator_token); a supervisor
    token is refused. The agent first receives a halt order, then leaves
    the session: its token stops working and the others see
    participant.left with reason removed. Every still-valid join token of
    the session is invalidated, so mint a new one to invite again. Only
    the named participant is affected. Fails with participant_not_found
    for an unknown participant_id. The session's coordinator (its participant
    token) may remove launched agents (metadata.launch_id set, whoever
    launched them), and only those. Returns removed
    (the removed ids) and join_tokens_invalidated.
    """
        return await session_remove_participant(
            deps, session_id, _creator_cred(creator_token, ctx), participant_id,
            source_ip=_client_ip(ctx),
        )

    @_tool("session_set_coordinator")
    async def session_set_coordinator_tool(
        session_id: SessionIdArg,
        participant_id: Annotated[str | None, Field(description=
            "The participant_id (from the roster) to make the session's coordinator, "
            "or null to clear the role."
        )],
        creator_token: ManagerTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Assign, move or clear the session's coordinator.

    The coordinator is the one participant allowed to discover and launch
    agents; it is whoever first joined with the creator credential (or the
    relay admin token) while the role was vacant, until you change it here.
    Requires the creator_token (or the relay admin token, or
    the owning account's token, also accepted from the MCP Authorization:
    Bearer header). The target must be a current participant, else
    participant_not_found. Returns coordinator_id.
    """
        return await session_set_coordinator(
            deps, session_id, _creator_cred(creator_token, ctx), participant_id,
            source_ip=_client_ip(ctx),
        )

    @_tool("agents_discover")
    async def agents_discover_tool(
        session_id: SessionIdArg,
        domain: Annotated[str | None, Field(description=
            "Application domain to list agent profiles for. Omit to list the "
            "domains available to this session."
        )] = None,
        role: Annotated[str | None, Field(description=
            "Only profiles with this role."
        )] = None,
        capability: Annotated[str | None, Field(description=
            "Only profiles declaring this capability tag."
        )] = None,
        participant_token: Annotated[str | None, Field(description=
            "The coordinator's participant token, or the creator token / owning "
            "account's token / admin token acting for the session. May be omitted "
            "when it arrives as the MCP Authorization: Bearer header."
        )] = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Find the agents you may start in this session.

    Only the session's coordinator (its participant token) or the creator/admin
    may call this; anyone else gets permission_denied and should ask the
    coordinator. Without a domain, returns {"domains": [...]}; with one,
    {"agents": [...]} where each item has launcher_id, machine, owner (the
    launcher owner's account name, null in open mode), own, profile_id,
    domain, role, capabilities, description, runtime, model, access_preset,
    limits, online and free_slots. Own launchers (own: true, the session
    owner's account) come first: prefer them. The machine name and
    description are chosen by the launcher's owner; launch a foreign launcher
    (own: false) only when the user asked for it — it receives the task and
    its agent reads the whole session.
    """
        return await agents_discover(
            deps, session_id, _creator_cred(participant_token, ctx), domain, role,
            capability,
            source_ip=_client_ip(ctx),
        )

    @_tool("agents_launch")
    async def agents_launch_tool(
        session_id: SessionIdArg,
        task: Annotated[str, Field(description=
            "What the new agent should do (at most 16 KiB); added to the profile's "
            "start prompt."
        )],
        launcher_id: Annotated[str | None, Field(description=
            "Launcher of the profile to start (from agents_discover); use with profile_id."
        )] = None,
        profile_id: Annotated[str | None, Field(description=
            "Profile to start on launcher_id."
        )] = None,
        domain: Annotated[str | None, Field(description=
            "Instead of launcher_id + profile_id: the domain, with role, letting the "
            "broker pick the first online launcher with a free slot."
        )] = None,
        role: Annotated[str | None, Field(description=
            "The profile role to start, with domain."
        )] = None,
        participant_token: Annotated[str | None, Field(description=
            "The coordinator's participant token, or the creator token / owning "
            "account's token / admin token acting for the session. May be omitted "
            "when it arrives as the MCP Authorization: Bearer header."
        )] = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Start an agent profile in this session through its launcher.

    Only the session's coordinator (or the creator/admin) may call this;
    anyone else gets permission_denied and should ask the coordinator. Give
    launcher_id + profile_id, or domain + role. Returns {"launch_id",
    "status": "pending"}; the agent then joins by itself and you see a normal
    participant.joined. When the task is done the agent sends its result
    with session_send to the coordinator that launched it (else the
    session's coordinator), and then stays in the session for more work.
    Read the launch with agents_launch_status; a failure also arrives as
    an agent.launch_failed event. The response says owner and own (see
    agents_discover). Errors: launch_limit (the session's
    max_launched_agents reached), no_free_slot, profile_not_found,
    session_suspended. Remove the agent with session_remove_participant
    when done.
    """
        body = {"task": task, "launcher_id": launcher_id, "profile_id": profile_id,
                "domain": domain, "role": role}
        return await agents_launch(
            deps, session_id, _creator_cred(participant_token, ctx), body,
            # Only the configured public URL, never one derived from headers.
            broker_url=os.environ.get("SW_PUBLIC_BASE_URL", "").rstrip("/") or None,
            source_ip=_client_ip(ctx),
        )

    @_tool("agents_launch_status")
    async def agents_launch_status_tool(
        session_id: SessionIdArg,
        launch_id: Annotated[str, Field(description="The launch_id agents_launch returned.")],
        participant_token: Annotated[str | None, Field(description=
            "The coordinator's participant token, or the creator token / owning "
            "account's token / admin token acting for the session. May be omitted "
            "when it arrives as the MCP Authorization: Bearer header."
        )] = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Read the result of one launch.

    Returns launch_id, status (pending | started | joined | failed | stopped),
    profile_id, launcher_id, machine, owner, own, and when known
    participant_id, error (why it failed: launcher_offline, join_timeout,
    cancelled, or the launcher's error such as profile_unknown, no_free_slot,
    visibility_refused, profile_rejected, runtime_unavailable, start_failed,
    join_failed*), message (the launcher's explanation, e.g. which runtime
    version is required) and reason (why it stopped: removed, left, evicted,
    session_ended, session_cold, launcher_lost, or the launcher's reason such
    as idle_limit, cost_limit, duration_limit or runtime_failed). Coordinator
    or creator/admin only.
    """
        return await agents_launch_status(
            deps, session_id, _creator_cred(participant_token, ctx), launch_id,
            source_ip=_client_ip(ctx),
        )

    @_tool("session_post_order")
    async def session_post_order_tool(
        session_id: SessionIdArg,
        kind: Annotated[Literal["note", "halt", "continue", "redirect", "undo"], Field(description=
            "note: guidance for the work in progress (state unchanged). "
            "halt: stop all work now and wait, paused. continue: resume where "
            "stopped. redirect: follow the new instructions in message from now on. "
            "undo: stop, revert what message says (or everything you did) using your "
            "journal, report the result, and stay paused until continue or redirect."
        )],
        target: Annotated[str, Field(description=
            "'all' for every participant, or one participant_id."
        )],
        message: Annotated[str | None, Field(description=
            "The order's text, at most 2048 bytes. Required for note, "
            "redirect and undo; optional for halt and continue."
        )] = None,
        token: Annotated[str | None, Field(description=
            "A supervisor_token, the creator_token, the token of the account that "
            "owns the session, or the relay admin token. May be omitted when it "
            "arrives as the MCP Authorization: Bearer header. A participant or "
            "join token is refused with permission_denied."
        )] = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Send a supervisor order to one participant or to all of them.

    Requires a supervisor_token, the creator_token or the relay admin token;
    participants receive orders, they do not send them. kind is one of note,
    halt, continue, redirect, undo. halt and undo pause the addressed participants: they
    stay in the session but commands to them are refused with
    participant_paused until a continue or redirect. Only the addressed
    participants are affected. Rate-limited per session and source address.
    Returns note_id, kind, target, message, ts_ms and affected (the
    participants whose supervision state changed).
    """
        body: dict[str, Any] = {"kind": kind, "target": target}
        if message is not None:
            body["message"] = message
        return await session_post_order(
            deps, session_id, _creator_cred(token, ctx), body,
            limiter=order_limiter, source_ip=_client_ip(ctx),
        )

    @_tool("session_get_events")
    async def session_get_events_tool(
        session_id: Annotated[str, Field(description=
            "A persisted session. An ephemeral one keeps no timeline and returns no_history."
        )],
        after_seq: Annotated[int | None, Field(description=
            "Only events with seq strictly greater than this. Pass the last seq you saw to page "
            "forward."
        )] = None,
        before_seq: Annotated[int | None, Field(description=
            "Only events with seq strictly less than this, to page backward."
        )] = None,
        kind: Annotated[str | None, Field(description=
            'Comma-separated event names to keep, e.g. "command.received,result.received". Omit '
            'for all kinds.'
        )] = None,
        participant_id: Annotated[str | None, Field(description=
            "Comma-separated participant ids; keep only events involving them."
        )] = None,
        limit: Annotated[int, Field(description=
            "Maximum events to return, 1 to 1000 (default 200)."
        )] = 200,
        order: Annotated[Literal["asc", "desc"], Field(description=
            '"asc" (default) oldest first, or "desc" newest first.'
        )] = "asc",
        access_token: AccessTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Browse the timeline of a persisted session without bringing it back into RAM.

    Returns paginated events plus session_state ("hot", "cold" or
    "ended"). Page with after_seq / before_seq and limit; order is "asc"
    (default) or "desc". Narrow with kind and participant_id. An
    ephemeral session stores no timeline and returns no_history. Needs no
    session_join, but an access_token unless this connection joined the session.
    """
        payload = {
            "session_id": session_id,
            "after_seq": after_seq,
            "before_seq": before_seq,
            "kind": kind,
            "participant_id": participant_id,
            "limit": limit,
            "order": order,
            "access_token": access_token,
        }
        return await session_get_events(
            deps, payload, handle=_optional_handle(ctx), source_ip=_client_ip(ctx)
        )

    # ---------- accounts: self-service and admin ----------
    #
    # Each tool runs the same shared operation as its REST route
    # (``accounts_api``): same validation, authorisation, auth-failure
    # throttling and account events. Credentials are never logged.

    def _account_token_of(explicit: str | None, ctx: Context | None) -> str:
        """The account token: the explicit argument, else the Bearer header when
        it is an account token (a launched agent's Bearer is its participant
        token, which is never tried as an account token)."""
        if explicit:
            return explicit
        bearer = _bearer_of(ctx)
        if bearer and bearer.startswith(ACCOUNT_TOKEN_PREFIX):
            return bearer
        raise ToolError(
            ErrorCode.INVALID_ACCOUNT_TOKEN,
            "an account token is required: pass account_token (or send it as the "
            "Authorization: Bearer header)",
        )

    async def _self_auth(explicit: str | None, ctx: Context | None) -> Any:
        """(store, auth) for an account tool; 503 first, like REST."""
        store = accounts_api._store(deps)
        token = _account_token_of(explicit, ctx)
        auth = await accounts_api.self_auth(deps, token, current_origin(_client_ip(ctx)))
        return store, auth

    async def _self(explicit: str | None, ctx: Context | None) -> Any:
        """(store, account) for an account tool."""
        store, auth = await _self_auth(explicit, ctx)
        return store, auth.account

    def _admin(explicit: str | None, ctx: Context | None) -> AccountStore:
        """The account store, once the caller proved the relay admin token.

        The admin token is taken from the ``admin_token`` argument only, never
        from the MCP Bearer header (often another credential of the caller).
        Every wrong value is counted and throttled per address exactly like the
        REST admin routes' Bearer; a missing one is refused without counting."""
        store = accounts_api._store(deps)
        if not explicit:
            raise ToolError(
                ErrorCode.PERMISSION_DENIED,
                "the relay admin token is required: pass it as admin_token "
                "(the Authorization header is not used by admin tools)",
            )
        accounts_api.check_admin(deps, explicit, _client_ip(ctx))
        return store

    def _mint_body(
        label: str, expires_at: str | None, network_policy: str | None,
        approved_networks: list[str] | None, revoke_token_id: str | None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"label": label}
        for key, value in (
            ("expires_at", expires_at), ("network_policy", network_policy),
            ("approved_networks", approved_networks), ("revoke_token_id", revoke_token_id),
        ):
            if value is not None:
                body[key] = value
        return body

    @_tool("account_me")
    async def account_me_tool(
        account_token: AccountTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Show the account an account token belongs to, with all its tokens.

    Returns account_id, name, status, contact, created_by, created_at and
    tokens: every token of the account (token_id, label, created_at,
    expires_at, revoked_at, last_used_at, last_used_ip, network_policy,
    approved_networks) — never a token's secret value — and current_token:
    the token this call authenticated with (same fields) plus
    first_use_bound, the network this very call bound a fresh first_use
    token to (else null). Same as REST GET /v1/me. Errors:
    invalid_account_token (missing, unknown, revoked or expired token, or an
    account that is not active), network_not_approved,
    rate_limited, accounts_unavailable (relay without accounts).
    """
        store, auth = await _self_auth(account_token, ctx)
        return await accounts_api.account_view(store, auth.account, auth)

    @_tool("account_tokens")
    async def account_tokens_tool(
        account_token: AccountTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """List the tokens of your account (metadata only, never the secrets).

    Returns {"tokens": [...]} with token_id, label, created_at, expires_at,
    revoked_at, last_used_at, last_used_ip, network_policy and
    approved_networks for each. Use a token_id with account_token_revoke or
    as revoke_token_id of account_token_create. Same as REST GET
    /v1/me/tokens; errors as account_me.
    """
        store, account = await _self(account_token, ctx)
        return await accounts_api.tokens_view(store, account.account_id)

    @_tool("account_token_create")
    async def account_token_create_tool(
        label: TokenLabelArg,
        expires_at: TokenExpiresArg = None,
        network_policy: TokenPolicyArg = None,
        approved_networks: TokenNetworksArg = None,
        revoke_token_id: RevokeTokenIdArg = None,
        account_token: AccountTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Mint a new token for your own account; its secret is returned once.

    Tokens are stored hashed: a lost token cannot be shown again, so
    "recovering" one means minting a new one here (and revoking the old one
    with revoke_token_id, or later with account_token_revoke). Returns
    token (the secret, shown only in this answer: hand it to whoever asked,
    never log it), token_id, label, created_at, expires_at, network_policy,
    approved_networks; with revoke_token_id also revoked_token_id (or
    revoke_error if the revoke failed — the new token is valid either way).
    Same as REST POST /v1/me/tokens. Errors: invalid_request (bad label,
    expires_at, network_policy or approved_networks),
    account_token_limit, token_not_found (revoke_token_id is not a token of
    this account; nothing is minted), plus those of account_me.
    """
        store, account = await _self(account_token, ctx)
        return await accounts_api.mint_token(
            deps, store, account.account_id,
            _mint_body(label, expires_at, network_policy, approved_networks, revoke_token_id),
            by="self", origin=current_origin(_client_ip(ctx)), firehose=firehose,
        )

    @_tool("account_token_revoke")
    async def account_token_revoke_tool(
        token_id: Annotated[str, Field(description=
            "The token_id to revoke (from account_tokens). Revoking the token you "
            "authenticate with is allowed; it stops working at once."
        )],
        account_token: AccountTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Revoke one of your account's tokens; it stops working at once.

    Idempotent: revoking an already revoked token succeeds again. Live
    firehose streams and launchers opened with it are cut. Returns
    {"token_id": ..., "revoked": true}. Same as REST DELETE
    /v1/me/tokens/{token_id}. Errors: token_not_found (no such token on
    this account), plus those of account_me.
    """
        store, account = await _self(account_token, ctx)
        await accounts_api.revoke_token(
            deps, store, firehose, account.account_id, token_id,
            by="self", origin=current_origin(_client_ip(ctx)),
        )
        return {"token_id": token_id, "revoked": True}

    def _networks_changes(
        network_policy: str | None, approved_networks: list[str] | None
    ) -> dict[str, Any] | None:
        """The REST PUT body for a networks change, or None for a read."""
        body: dict[str, Any] = {}
        if network_policy is not None:
            body["network_policy"] = network_policy
        if approved_networks is not None:
            body["approved_networks"] = approved_networks
        return body or None

    @_tool("account_token_networks")
    async def account_token_networks_tool(
        token_id: NetworksTokenIdArg,
        network_policy: NetworksPolicyArg = None,
        approved_networks: NetworksListArg = None,
        account_token: AccountTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Show or change where one of your account's tokens may be used from.

    Without network_policy and approved_networks it only reads; with either
    it changes that setting (approved_networks replaces the whole list; []
    clears it). Live streams opened from addresses no longer allowed are
    cut. Returns {"network_policy", "approved_networks"}. Same as REST
    GET/PUT /v1/me/tokens/{token_id}/networks. Errors: token_not_found,
    invalid_request (bad policy or network), plus those of account_me.
    """
        store, account = await _self(account_token, ctx)
        body = _networks_changes(network_policy, approved_networks)
        if body is None:
            return await accounts_api.token_networks(store, account.account_id, token_id)
        return await accounts_api.set_token_networks(
            deps, store, firehose, account.account_id, token_id, body
        )

    @_tool("account_approvals")
    async def account_approvals_tool(
        account_token: AccountTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """List your account's pending network approvals.

    A token whose policy is first_use or approved_only that is used from a
    new address is refused with network_not_approved and leaves a pending
    approval here (approval_id, token, ip, network, client, created_at...).
    Approve it with account_approve or refuse it with account_deny. Returns
    {"approvals": [...]}. Same as REST GET /v1/me/network-approvals.
    Errors: those of account_me.
    """
        store, account = await _self(account_token, ctx)
        return await accounts_api.approvals_view(store, account.account_id)

    @_tool("account_approve")
    async def account_approve_tool(
        approval_id: ApprovalIdArg,
        scope: ApprovalScopeArg = "ip",
        account_token: AccountTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Approve a pending network approval of your account: the token may then
    be used from that address (scope "ip") or its whole network (scope
    "network": its /24 or /64).

    Returns the token's {"network_policy", "approved_networks"} after the
    change. Same as REST POST /v1/me/network-approvals/{id}/approve.
    Errors: approval_not_found, invalid_request (bad scope), plus those of
    account_me.
    """
        store, account = await _self(account_token, ctx)
        return await accounts_api.approve(
            store, account.account_id, approval_id, {"scope": scope},
            by="self", origin=current_origin(_client_ip(ctx)),
        )

    @_tool("account_deny")
    async def account_deny_tool(
        approval_id: ApprovalIdArg,
        account_token: AccountTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Deny a pending network approval of your account; the address stays refused.

    Returns {"approval_id": ..., "denied": true}. Same as REST POST
    /v1/me/network-approvals/{id}/deny. Errors: approval_not_found, plus
    those of account_me.
    """
        store, account = await _self(account_token, ctx)
        await accounts_api.deny(
            store, account.account_id, approval_id,
            by="self", origin=current_origin(_client_ip(ctx)),
        )
        return {"approval_id": approval_id, "denied": True}

    @_tool("admin_accounts")
    async def admin_accounts_tool(
        admin_token: AdminTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """List every broker account (relay admin only).

    Returns {"accounts": [...]} with account_id, name, status (pending,
    active or disabled), contact, created_by and created_at. Same as REST
    GET /v1/accounts. Errors: permission_denied (missing or wrong admin
    token; wrong tokens are counted per address), rate_limited (too many
    wrong attempts from this address), accounts_unavailable.
    """
        store = _admin(admin_token, ctx)
        return await accounts_api.accounts_view(store)

    @_tool("admin_account")
    async def admin_account_tool(
        account_id: AdminAccountIdArg,
        admin_token: AdminTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Show one broker account with all its tokens (relay admin only).

    Returns the account (as in admin_accounts) plus tokens: metadata of each
    token, never a secret. Same as REST GET /v1/accounts/{account_id}.
    Errors: account_not_found, plus those of admin_accounts.
    """
        store = _admin(admin_token, ctx)
        return await accounts_api.admin_account_view(store, account_id)

    @_tool("admin_token_create")
    async def admin_token_create_tool(
        account_id: AdminAccountIdArg,
        label: TokenLabelArg,
        expires_at: TokenExpiresArg = None,
        network_policy: TokenPolicyArg = None,
        approved_networks: TokenNetworksArg = None,
        revoke_token_id: RevokeTokenIdArg = None,
        admin_token: AdminTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Mint a token for any account (relay admin only); its secret is returned once.

    The way to give a user who lost their token a new one: mint here and
    pass the lost token's id as revoke_token_id to retire it in the same
    call. Returns the same fields as account_token_create (token is the
    secret, shown only in this answer). Recorded as a token.created account
    event by "admin". Same as REST POST /v1/accounts/{account_id}/tokens.
    Errors: account_not_found, token_not_found (revoke_token_id; nothing is
    minted), invalid_request, account_token_limit, plus those of
    admin_accounts.
    """
        store = _admin(admin_token, ctx)
        return await accounts_api.mint_token(
            deps, store, account_id,
            _mint_body(label, expires_at, network_policy, approved_networks, revoke_token_id),
            by="admin", origin=current_origin(_client_ip(ctx)), firehose=firehose,
        )

    @_tool("admin_token_revoke")
    async def admin_token_revoke_tool(
        account_id: AdminAccountIdArg,
        token_id: Annotated[str, Field(description=
            "The token_id to revoke (from admin_account)."
        )],
        admin_token: AdminTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Revoke a token of any account (relay admin only); it stops working at once.

    Idempotent. Live streams and launchers opened with the token are cut.
    Returns {"token_id": ..., "revoked": true}. Same as REST DELETE
    /v1/accounts/{account_id}/tokens/{token_id}. Errors: account_not_found,
    token_not_found, plus those of admin_accounts.
    """
        store = _admin(admin_token, ctx)
        await accounts_api.admin_revoke_token(
            deps, store, firehose, account_id, token_id,
            origin=current_origin(_client_ip(ctx)),
        )
        return {"token_id": token_id, "revoked": True}

    async def _admin_on(explicit: str | None, ctx: Context | None, account_id: str) -> Any:
        """The store, once the caller is the admin and ``account_id`` exists."""
        store = _admin(explicit, ctx)
        await store.get(account_id)  # account_not_found
        return store

    @_tool("admin_token_networks")
    async def admin_token_networks_tool(
        account_id: AdminAccountIdArg,
        token_id: NetworksTokenIdArg,
        network_policy: NetworksPolicyArg = None,
        approved_networks: NetworksListArg = None,
        admin_token: AdminTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Show or change where a token of any account may be used from (relay admin only).

    As account_token_networks, for account_id. Same as REST GET/PUT
    /v1/accounts/{account_id}/tokens/{token_id}/networks. Errors:
    account_not_found, token_not_found, invalid_request, plus those of
    admin_accounts.
    """
        store = await _admin_on(admin_token, ctx, account_id)
        body = _networks_changes(network_policy, approved_networks)
        if body is None:
            return await accounts_api.token_networks(store, account_id, token_id)
        return await accounts_api.set_token_networks(
            deps, store, firehose, account_id, token_id, body
        )

    @_tool("admin_approvals")
    async def admin_approvals_tool(
        account_id: AdminAccountIdArg,
        admin_token: AdminTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """List an account's pending network approvals (relay admin only).

    As account_approvals, for account_id. Same as REST GET
    /v1/accounts/{account_id}/network-approvals. Errors: account_not_found,
    plus those of admin_accounts.
    """
        store = await _admin_on(admin_token, ctx, account_id)
        return await accounts_api.approvals_view(store, account_id)

    @_tool("admin_approve")
    async def admin_approve_tool(
        account_id: AdminAccountIdArg,
        approval_id: ApprovalIdArg,
        scope: ApprovalScopeArg = "ip",
        admin_token: AdminTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Approve a pending network approval of any account (relay admin only).

    As account_approve, for account_id; recorded as approved by "admin".
    Same as REST POST /v1/accounts/{account_id}/network-approvals/{id}/approve.
    Errors: account_not_found, approval_not_found, invalid_request, plus
    those of admin_accounts.
    """
        store = await _admin_on(admin_token, ctx, account_id)
        return await accounts_api.approve(
            store, account_id, approval_id, {"scope": scope},
            by="admin", origin=current_origin(_client_ip(ctx)),
        )

    @_tool("admin_deny")
    async def admin_deny_tool(
        account_id: AdminAccountIdArg,
        approval_id: ApprovalIdArg,
        admin_token: AdminTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Deny a pending network approval of any account (relay admin only).

    Returns {"approval_id": ..., "denied": true}. Same as REST POST
    /v1/accounts/{account_id}/network-approvals/{id}/deny. Errors:
    account_not_found, approval_not_found, plus those of admin_accounts.
    """
        store = await _admin_on(admin_token, ctx, account_id)
        await accounts_api.deny(
            store, account_id, approval_id,
            by="admin", origin=current_origin(_client_ip(ctx)),
        )
        return {"approval_id": approval_id, "denied": True}

    # ---------- machines (v2 launchers) and their agent definitions ----------
    #
    # The same shared operations as /v1/me/launchers (account token: only the
    # caller's own machines) and /v1/launchers (open mode, no credential).

    async def _machines_owner(explicit: str | None, ctx: Context | None) -> str:
        token = explicit
        if not token:
            bearer = _bearer_of(ctx)
            if bearer and bearer.startswith(ACCOUNT_TOKEN_PREFIX):
                token = bearer
        if token:
            return await machines_api.account_owner(
                deps, token, current_origin(_client_ip(ctx))
            )
        if deps.session_creation == "account":
            raise ToolError(
                ErrorCode.PERMISSION_DENIED,
                "this broker is in account mode: pass account_token (one of your "
                "account's tokens) or send it as the Authorization: Bearer header",
            )
        return machines_api.open_owner(deps, "")

    @_tool("machines_list")
    async def machines_list_tool(
        account_token: MachinesTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """List the machines (v2 launchers) you can deploy agents to, with their agents.

    With an account token: the machines your account's launchers registered.
    On an open-mode broker without a token: every machine of the broker
    (in account mode, no token is permission_denied, as REST /v1/launchers).
    Returns {"launchers": [...]}: per machine launcher_key (the key the
    other machine tools take), machine, online, mode (config, local or
    root), ceiling, capabilities (definitions, browse, processes),
    platform, last_seen, launcher_id and agents (each profile with its
    status, source and deploy state). Same as REST GET /v1/me/launchers
    (or /v1/launchers). Errors: invalid_account_token, network_not_approved,
    rate_limited, accounts_unavailable.
    """
        owner = await _machines_owner(account_token, ctx)
        return await machines_api.list_machines(deps, owner)

    @_tool("machine_get")
    async def machine_get_tool(
        key: MachineKeyArg,
        account_token: MachinesTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Show one machine with its deployed agent definitions.

    Returns the machine (as in machines_list) plus definitions: each
    stored definition with its deploy state (queued, sent, applied,
    pending, rejected, removing) and revision. Same as REST GET
    /v1/me/launchers/{key}. Errors: launcher_not_found (unknown key, or
    another account's machine), plus those of machines_list.
    """
        owner = await _machines_owner(account_token, ctx)
        return await machines_api.machine_detail(deps, owner, key)

    @_tool("definition_deploy")
    async def definition_deploy_tool(
        key: MachineKeyArg,
        definition_id: DefinitionIdArg,
        definition: Annotated[dict[str, Any], Field(description=
            "The agent definition, e.g. {\"id\": \"obs\", \"domain\": \"ops\", "
            "\"role\": \"watcher\", \"runtime\": \"claude\", \"kind\": \"observer\", "
            "\"workdir\": \"/srv/app\", \"max_instances\": 1}. Its id must equal "
            "definition_id; workdir must be inside the machine's ceiling."
        )],
        account_token: MachinesTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Deploy (or update) an agent definition on a machine.

    The broker validates and stores it, then sends it to the machine's
    launcher, now if it is online, else when it reconnects. Returns
    definition_id, revision, state ("sent" or "queued", "pending" while
    the agent runs) and warnings. Needs a machine started with --local or
    --root. Same as REST PUT /v1/me/launchers/{key}/definitions/{id}.
    Errors: invalid_request (invalid definition, with the reason),
    outside_ceiling, launcher_not_found, launcher_capability_missing,
    definition_id_conflict (a local profile of the machine has that id),
    definition_limit, definitions_budget, definitions_unavailable (broker without a
    persistence DB), plus those of machines_list.
    """
        owner = await _machines_owner(account_token, ctx)
        return await machines_api.deploy_definition(
            deps, owner, key, definition_id, {"definition": definition},
            client=_client_ip(ctx),
        )

    @_tool("definition_remove")
    async def definition_remove_tool(
        key: MachineKeyArg,
        definition_id: DefinitionIdArg,
        account_token: MachinesTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Remove a deployed agent definition from a machine.

    Returns definition_id, revision and state ("removing" until the
    launcher confirms). Refused while one of its agents runs. Same as REST
    DELETE /v1/me/launchers/{key}/definitions/{id}. Errors:
    definition_not_found, definition_in_use, launcher_not_found,
    definitions_unavailable, plus those of machines_list.
    """
        owner = await _machines_owner(account_token, ctx)
        return await machines_api.remove_definition(
            deps, owner, key, definition_id, client=_client_ip(ctx)
        )

    @_tool("machine_forget")
    async def machine_forget_tool(
        key: MachineKeyArg,
        account_token: MachinesTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Forget an offline machine: its record, definitions and launcher key go.

    Refused while the machine is online. A launcher that later connects
    with that key registers as a new machine. Returns {"key": ...,
    "forgotten": true}. Same as REST DELETE /v1/me/launchers/{key}.
    Errors: launcher_online, launcher_not_found, definitions_unavailable,
    plus those of machines_list.
    """
        owner = await _machines_owner(account_token, ctx)
        await machines_api.forget_machine(deps, owner, key, client=_client_ip(ctx))
        return {"key": key, "forgotten": True}

    @_tool("machine_browse")
    async def machine_browse_tool(
        key: MachineKeyArg,
        path: Annotated[str | None, Field(description=
            "Absolute directory path to list; omit for the machine's ceiling."
        )] = None,
        cursor: Annotated[str | None, Field(description=
            "next_cursor of the previous page, to continue a listing."
        )] = None,
        limit: Annotated[int | None, Field(description=
            "Entries per page, at least 1 (default 100; larger values are capped)."
        )] = None,
        hidden: Annotated[bool, Field(description=
            "Include hidden (dot) entries. Default false."
        )] = False,
        account_token: MachinesTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """List a directory on a machine (inside its ceiling), e.g. to pick a workdir.

    Relayed to the machine's launcher; nothing is stored. Returns path,
    entries (name, kind: dir, file, symlink or other, optional size,
    mtime, inside) and next_cursor (null on the last page). Same as REST
    GET /v1/me/launchers/{key}/browse. Errors: invalid_request,
    outside_ceiling, path_not_found, permission_denied, launcher_offline,
    launcher_capability_missing, launcher_timeout, launcher_bad_reply,
    launcher_not_found, definitions_unavailable, plus those of
    machines_list.
    """
        owner = await _machines_owner(account_token, ctx)
        params = _query_params(path=path, cursor=cursor, limit=limit,
                               hidden="1" if hidden else "0")
        return await machines_api.browse(deps, owner, key, params, client=_client_ip(ctx))

    @_tool("machine_processes")
    async def machine_processes_tool(
        key: MachineKeyArg,
        query: Annotated[str | None, Field(description=
            "Only processes whose name or command line contains this text (max 100)."
        )] = None,
        cursor: Annotated[str | None, Field(description=
            "next_cursor of the previous page, to continue a listing."
        )] = None,
        limit: Annotated[int | None, Field(description=
            "Processes per page, at least 1 (default 100; larger values are capped)."
        )] = None,
        account_token: MachinesTokenArg = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """List the processes running on a machine (command lines redacted by the launcher).

    Relayed to the machine's launcher; nothing is stored. Returns
    processes (pid, ppid, user, name, cmdline, started_at) and
    next_cursor. Same as REST GET /v1/me/launchers/{key}/processes.
    Errors: as machine_browse.
    """
        owner = await _machines_owner(account_token, ctx)
        params = _query_params(cursor=cursor, limit=limit, q=query)
        return await machines_api.processes(deps, owner, key, params, client=_client_ip(ctx))

    return mcp


def _query_params(**values: str | int | None) -> dict[str, str]:
    """Tool arguments as the query-string values the shared machine
    operations validate (``None`` = absent)."""
    return {k: str(v) for k, v in values.items() if v is not None}


def build_app() -> Starlette:
    """Build the Starlette app exposing the MCP endpoint and health probes."""
    creation_mode = session_creation_mode(os.environ)
    deps_cfg = load_deployment_defaults(os.environ)
    # sse_starlette logs every SSE chunk at DEBUG, i.e. whole MCP tool answers,
    # which carry one-time secrets (minted account tokens, session tokens).
    # aiosqlite logs every statement with its parameters at DEBUG (submitted
    # ids, session data). Keep both above DEBUG whatever the root level is.
    for noisy in ("sse_starlette", "aiosqlite"):
        logging.getLogger(noisy).setLevel(logging.INFO)
    connections = ConnectionManager()
    metrics = Metrics()
    firehose = FirehoseRegistry()
    registry = SessionRegistry(deps_cfg, connections, metrics=metrics, firehose=firehose)
    handles = HandleRegistry()
    # Per-process startup epoch. Surfaced on /v1/info, session_join,
    # and session_resume so SDK clients can detect relay restart and
    # invalidate their local seq cursor. See #65.
    relay_epoch = int(time.time())
    auth_limiter = AuthFailureLimiter()
    admin_token = os.environ.get("SW_ADMIN_TOKEN", "") or None
    tool_deps = ToolDeps(
        registry,
        relay_epoch=relay_epoch,
        bounds=deps_cfg.bounds,
        metrics=metrics,
        auth_limiter=auth_limiter,
        admin_token=admin_token,
    )
    tool_deps.session_creation = creation_mode
    tool_deps.account_max_sessions = account_max_sessions()
    tool_deps.account_max_tokens = account_max_tokens()

    note_limiter = _note_rate_limiter()
    mcp = _build_fastmcp(
        tool_deps, handles, connections=connections, registry=registry,
        note_limiter=note_limiter, firehose=firehose,
    )
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
        # ``auth_model`` lets SDK clients detect which credentials the relay
        # issues: "scoped-tokens-v1" means a session is joined with a
        # short-lived join token, each participant then authenticates with its
        # own secret participant_token (never its participant_id or the
        # session id), and supervisors / the creator hold their own tokens.
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
                "auth_model": "scoped-tokens-v1",
                "relay_epoch": relay_epoch,
                "session_creation": creation_mode,
                "transports": ["mcp", "http", "ws"],
                "capabilities": {
                    "firehose": True,
                    "supervisor_notes": True,
                    "session_delete": True,
                    "launchers": True,
                    # The broker keeps deployed agent definitions (it has a
                    # persistence DB); in open mode any client may deploy.
                    "agent_definitions": tool_deps.machines is not None,
                    "open_deploy": creation_mode == "open" and tool_deps.machines is not None,
                    "launcher_protocol": PROTOCOL_VERSION,
                },
            }
        )

    metrics_token = os.environ.get("SW_METRICS_TOKEN", "").strip() or None

    async def metrics_endpoint(request: Request) -> Response:
        # When SW_METRICS_TOKEN is set, require a matching bearer token; the
        # relay admin token is accepted too (it already grants far more, and
        # the Cockpit's monitor scrapes with it). Unset → endpoint is open
        # (backwards-compatible default).
        if metrics_token is not None:
            auth = request.headers.get("authorization", "")
            if not auth.lower().startswith("bearer "):
                return Response("unauthorized", status_code=401)
            presented = auth[7:].strip()
            if not (
                admin_matches(presented, metrics_token)
                or admin_matches(presented, admin_token)
            ):
                return Response("forbidden", status_code=403)
        return Response(metrics.render(), media_type=METRICS_CONTENT_TYPE)

    _openapi_path = Path(__file__).parent / "openapi.yaml"
    _openapi_text_cache: dict[str, str] = {}

    async def openapi_yaml(request: Request) -> Response:
        """Canonical OpenAPI spec, shipped alongside the package.

        Served as ``application/yaml``; clients that need JSON can pipe
        through any YAML→JSON converter. CORS is handled by the
        global middleware.
        """
        from sessionweave.http_api import _public_base_url as _pub

        text = _openapi_text_cache.get("t")
        if text is None:
            text = _openapi_path.read_text(encoding="utf-8")
            _openapi_text_cache["t"] = text
        own = _pub(request)
        block = (
            "servers:\n"
            f"  - url: {own}\n"
            "    description: This broker\n"
        )
        # Replace the hosted-production entry with this broker's own; the
        # local-dev entry stays after it. Skip a duplicate of the dev URL.
        out = re.sub(
            r"servers:\n  - url: [^\n]*\n    description: [^\n]*\n", lambda _m: block, text, count=1
        )
        return Response(out, media_type="application/yaml")

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
            events = EventLog(store)
            tool_deps.accounts = AccountStore(store, events=events)
            tool_deps.events = events
            machines = MachineStore(store)
            tool_deps.machines = machines
            tool_deps.deploys.store = machines
            registry.events = events
            retention_days = account_events_retention_days()
            machines_retention_days = open_machines_retention_days()

            async def _prune_events(now_ms: int) -> None:
                await events.prune(now_ms, retention_days)
                online = {
                    lnc.v2.launcher_key
                    for lnc in tool_deps.launchers.for_account(OPEN_OWNER)
                    if lnc.v2.launcher_key is not None
                }
                await machines.prune_open(
                    now_ms, machines_retention_days, online_keys=online
                )

            # Sweep loop
            sweep_task = asyncio.create_task(
                registry.sweep_expired_loop(
                    interval_seconds=deps_cfg.defaults.persistence_sweep_interval_seconds,
                    safety_seconds=deps_cfg.defaults.persistence_until_deleted_safety_seconds,
                    on_tick=_prune_events,
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
            *launcher_routes(),
            *machines_routes(),
            Mount("/", app=McpNewSessionLimitMiddleware(mcp_app)),
        ],
        middleware=[
            cors_cfg.as_middleware(),  # outermost: error responses keep CORS headers
            Middleware(RequestSizeLimitMiddleware),
            Middleware(RequestClientMiddleware),
        ],
        lifespan=lifespan,
    )
    app.state.registry = registry
    app.state.connections = connections
    app.state.handles = handles
    app.state.metrics = metrics
    app.state.tool_deps = tool_deps
    app.state.auth_limiter = auth_limiter
    app.state.state = state
    app.state.firehose = firehose
    # Relay-wide admin token (SW_ADMIN_TOKEN). Unset is a valid, closed
    # configuration: no admin access and no fallback.
    app.state.admin_token = admin_token
    app.state.launcher_token = launcher_token_from_env()
    if creation_mode == "open" and app.state.launcher_token is None:
        logging.getLogger(__name__).warning(
            "launchers can register without a token; set SW_LAUNCHER_TOKEN to restrict"
        )
    if admin_token is None:
        logging.getLogger(__name__).info("admin access disabled")
    app.state.supervisor_rate = note_limiter
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
