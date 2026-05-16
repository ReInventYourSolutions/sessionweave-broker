from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, Response
from starlette.routing import Mount, Route

from sessionweave import __version__
from sessionweave.cockpit_mount import cockpit_mount
from sessionweave.config import load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.cors import CORSConfig
from sessionweave.errors import ToolError
from sessionweave.events_ws import routes as events_ws_routes
from sessionweave.firehose import FirehoseRegistry
from sessionweave.http_api import routes as http_api_routes
from sessionweave.mcp_adapter import HandleRegistry
from sessionweave.metrics import METRICS_CONTENT_TYPE, Metrics
from sessionweave.registry import SessionRegistry
from sessionweave.supervisor import NoteRateLimiter
from sessionweave.tools import (
    ToolDeps,
    session_check_inbox,
    session_create,
    session_describe,
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
    """Build the DNS-rebinding ``allowed_hosts`` list for FastMCP.

    Defaults to loopback only (matches FastMCP's stock policy). Extra
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


def _build_fastmcp(deps: ToolDeps, handles: HandleRegistry) -> FastMCP:
    mcp = FastMCP(
        "sessionweave",
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=_allowed_hosts(),
        ),
    )

    def _handle_for(ctx: Context) -> Any:
        return handles.for_session(ctx.session)

    @mcp.tool(name="session_create")
    async def session_create_tool(
        config: dict[str, Any] | None = None,
        purpose: dict[str, Any] | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        return await session_create(
            deps, {"config": config, "purpose": purpose}
        )

    @mcp.tool(name="session_describe")
    async def session_describe_tool(
        session_id: str,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        return await session_describe(deps, {"session_id": session_id})

    @mcp.tool(name="session_update_purpose")
    async def session_update_purpose_tool(
        purpose: dict[str, Any] | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        assert ctx is not None
        return await session_update_purpose(
            deps, _handle_for(ctx), {"purpose": purpose}
        )

    @mcp.tool(name="session_join")
    async def session_join_tool(
        session_id: str,
        role: str | None = None,
        objective: str | None = None,
        capabilities: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        delivery_mode: str | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
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

    @mcp.tool(name="session_resume")
    async def session_resume_tool(
        session_id: str,
        participant_id: str,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        assert ctx is not None
        return await session_resume(
            deps,
            _handle_for(ctx),
            {"session_id": session_id, "participant_id": participant_id},
        )

    @mcp.tool(name="session_leave")
    async def session_leave_tool(
        feedback: dict[str, Any] | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        assert ctx is not None
        body = {"feedback": feedback} if feedback is not None else None
        return await session_leave(deps, _handle_for(ctx), body)

    @mcp.tool(name="session_update_self")
    async def session_update_self_tool(
        role: str | None = None,
        objective: str | None = None,
        capabilities: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
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
        return await session_update_self(deps, _handle_for(ctx), payload)

    @mcp.tool(name="session_list_participants")
    async def session_list_participants_tool(
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        assert ctx is not None
        return await session_list_participants(deps, _handle_for(ctx))

    @mcp.tool(name="session_check_inbox")
    async def session_check_inbox_tool(
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        assert ctx is not None
        return await session_check_inbox(deps, _handle_for(ctx))

    @mcp.tool(name="session_wait_for_event")
    async def session_wait_for_event_tool(
        session_id: str,
        names: list[str] | None = None,
        timeout_seconds: float = 60.0,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        assert ctx is not None
        body: dict[str, Any] = {
            "session_id": session_id,
            "timeout_seconds": timeout_seconds,
        }
        if names is not None:
            body["names"] = names
        return await session_wait_for_event(deps, _handle_for(ctx), body)

    @mcp.tool(name="session_send")
    async def session_send_tool(
        dispatch: str,
        target: dict[str, Any],
        payload: dict[str, Any],
        timeout_seconds: int | None = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        assert ctx is not None
        body: dict[str, Any] = {
            "dispatch": dispatch,
            "target": target,
            "payload": payload,
        }
        if timeout_seconds is not None:
            body["timeout_seconds"] = timeout_seconds
        return await session_send(deps, _handle_for(ctx), body)

    @mcp.tool(name="session_respond")
    async def session_respond_tool(
        command_id: str,
        payload: dict[str, Any],
        status: str = "ok",
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        assert ctx is not None
        return await session_respond(
            deps,
            _handle_for(ctx),
            {"command_id": command_id, "payload": payload, "status": status},
        )

    return mcp


def build_app() -> Starlette:
    """Build the Starlette app exposing the MCP endpoint and health probes."""
    deps_cfg = load_deployment_defaults(os.environ)
    connections = ConnectionManager()
    metrics = Metrics()
    firehose = FirehoseRegistry()
    registry = SessionRegistry(deps_cfg, connections, metrics=metrics, firehose=firehose)
    handles = HandleRegistry()
    tool_deps = ToolDeps(registry)

    mcp = _build_fastmcp(tool_deps, handles)
    mcp_app = mcp.streamable_http_app()

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
        return JSONResponse(
            {
                "name": "sessionweave",
                "version": __version__,
                "subproject": "#1 (anonymous, transient, in-memory)",
                "transports": ["mcp", "http", "ws"],
                "capabilities": {
                    "firehose": True,
                    "supervisor_notes": True,
                    "session_delete": True,
                },
            }
        )

    async def metrics_endpoint(request: Request) -> Response:
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
        async with AsyncExitStack() as stack:
            await registry.start()
            stack.push_async_callback(_drain_registry, registry, state)
            await stack.enter_async_context(mcp_app.router.lifespan_context(mcp_app))
            yield

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
            cockpit_mount(),
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
    app.state.state = state
    app.state.firehose = firehose
    app.state.supervisor_rate = NoteRateLimiter(per_minute=30, per_hour=200)
    return app


async def _drain_registry(registry: SessionRegistry, state: dict[str, bool]) -> None:
    state["draining"] = True
    await registry.shutdown(reason="server_shutdown")


__all__ = ["ToolError", "build_app"]
