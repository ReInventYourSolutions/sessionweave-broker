from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from sessionweave.config import load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.registry import SessionRegistry


def build_app() -> Starlette:
    """Build the Starlette app with `/healthz`, `/readyz`, and (later) the MCP endpoint.

    Tasks 16/17 will add MCP tool handlers and notifications wiring.
    """
    deps = load_deployment_defaults(os.environ)
    connections = ConnectionManager()
    registry = SessionRegistry(deps, connections)

    state: dict[str, bool] = {"draining": False}

    async def healthz(request: Request) -> JSONResponse:
        if state["draining"]:
            return JSONResponse({"status": "draining"}, status_code=503)
        return JSONResponse({"status": "ok"})

    async def readyz(request: Request) -> JSONResponse:
        return JSONResponse({"status": "ready"})

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        await registry.start()
        try:
            yield
        finally:
            state["draining"] = True
            await registry.shutdown(reason="server_shutdown")

    app = Starlette(
        routes=[
            Route("/healthz", healthz),
            Route("/readyz", readyz),
        ],
        lifespan=lifespan,
    )
    app.state.registry = registry
    app.state.connections = connections
    app.state.state = state
    return app
