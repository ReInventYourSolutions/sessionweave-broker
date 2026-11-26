"""Launcher WebSocket guards: fail-closed account mode, open-mode cap, pinger
errors retrieved, the open-mode startup warning."""
from __future__ import annotations

import gc
import logging
from types import SimpleNamespace
from typing import Any

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from sessionweave import launchers
from sessionweave.access import AuthFailureLimiter
from sessionweave.deploy import DeployManager
from sessionweave.launchers import LauncherRegistry


def _app(mode: str = "open", accounts: Any = None) -> Starlette:
    app = Starlette(routes=launchers.routes())
    app.state.tool_deps = SimpleNamespace(
        session_creation=mode, accounts=accounts, launchers=LauncherRegistry(),
        launches=None, auth_limiter=AuthFailureLimiter(), machines=None,
    )
    deps = app.state.tool_deps
    deps.deploys = DeployManager(deps.launchers, None, deps)
    app.state.launcher_token = None
    return app


_HELLO = {"type": "hello", "machine": "m", "visibility": "personal", "profiles": []}


def test_account_mode_without_accounts_fails_closed() -> None:
    with TestClient(_app("account")) as client, \
            client.websocket_connect("/v1/launchers/connect",
                                     headers={"Authorization": "Bearer x"}) as ws:
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        assert exc.value.code == 1011


def test_open_mode_total_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(launchers, "MAX_LAUNCHERS_OPEN", 2)
    app = _app()
    with TestClient(app) as client, \
            client.websocket_connect("/v1/launchers/connect") as a, \
            client.websocket_connect("/v1/launchers/connect") as b:
        for ws in (a, b):
            ws.send_json(_HELLO)
            assert ws.receive_json()["type"] == "welcome"
        with client.websocket_connect("/v1/launchers/connect") as c:
            c.send_json(_HELLO)
            with pytest.raises(WebSocketDisconnect) as exc:
                c.receive_json()
            assert exc.value.code == 4429


def test_failed_pinger_exception_is_retrieved(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def broken_ping(websocket: Any) -> None:
        raise RuntimeError("socket gone")

    monkeypatch.setattr(launchers, "_ping_loop", broken_ping)
    app = _app()
    with caplog.at_level(logging.ERROR), TestClient(app) as client:
        with client.websocket_connect("/v1/launchers/connect") as ws:
            ws.send_json(_HELLO)
            assert ws.receive_json()["type"] == "welcome"
            with pytest.raises(WebSocketDisconnect):
                ws.receive_json()
        gc.collect()
    assert "never retrieved" not in caplog.text
    assert app.state.tool_deps.launchers._launchers == {}


def test_open_mode_without_launcher_token_warns(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from sessionweave.server import build_app

    monkeypatch.delenv("SW_LAUNCHER_TOKEN", raising=False)
    monkeypatch.delenv("SW_SESSION_CREATION", raising=False)
    with caplog.at_level(logging.WARNING):
        build_app()
    assert "launchers can register without a token; set SW_LAUNCHER_TOKEN to restrict" \
        in caplog.text
    caplog.clear()
    monkeypatch.setenv("SW_LAUNCHER_TOKEN", "secret")
    with caplog.at_level(logging.WARNING):
        build_app()
    assert "launchers can register without a token" not in caplog.text
