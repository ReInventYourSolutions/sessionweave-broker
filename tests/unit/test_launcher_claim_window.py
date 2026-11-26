"""A v2 launcher is not registered (discoverable, launchable, by_key) until
its key claim succeeds; a refused connection leaves no trace."""
from __future__ import annotations

import asyncio
import threading
import time
from types import SimpleNamespace
from typing import Any

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from sessionweave import launchers
from sessionweave.access import AuthFailureLimiter
from sessionweave.deploy import DeployManager
from sessionweave.launchers import OPEN_OWNER, LauncherKeyInUseError, LauncherRegistry

KEY = "lk_" + "ab" * 16
_PROF = {"id": "p1", "domain": "backend", "role": "analyst", "capabilities": [],
         "description": "d", "runtime": "claude", "model": "m",
         "access": {"preset": "reader"}, "limits": {}, "max_instances": 1}
HELLO = {"type": "hello", "machine": "m", "visibility": "personal", "profiles": [_PROF],
         "protocol": 2, "launcher_key": KEY, "mode": "local", "ceiling": "/srv",
         "capabilities": ["definitions"], "sandbox": "bwrap", "platform": "linux"}


class _GatedMachines:
    """claim_machine waits until the test releases it with an outcome."""

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()
        self.outcome: Any = None  # an exception to raise, or the issued secret

    async def claim_machine(self, *args: Any, **kw: Any) -> tuple[Any, str | None]:
        self.entered.set()
        while not self.release.is_set():
            await asyncio.sleep(0.01)
        self.release.clear()
        self.entered.clear()
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return None, self.outcome

    async def set_prereqs(self, *a: Any) -> None:
        return None


def _app(machines: Any) -> Starlette:
    app = Starlette(routes=launchers.routes())
    app.state.tool_deps = SimpleNamespace(
        session_creation="open", accounts=None, launchers=LauncherRegistry(),
        launches=None, auth_limiter=AuthFailureLimiter(), machines=machines,
    )
    deps = app.state.tool_deps
    deps.deploys = DeployManager(deps.launchers, None, deps)
    app.state.launcher_token = None
    return app


def _sess() -> Any:
    return SimpleNamespace(auth=SimpleNamespace(owner_account_id=None))


def _wait(ev: threading.Event) -> None:
    assert ev.wait(5), "claim not reached"


def test_unclaimed_launcher_is_hidden_and_refused_leaves_no_trace() -> None:
    gm = _GatedMachines()
    app = _app(gm)
    reg: LauncherRegistry = app.state.tool_deps.launchers
    with TestClient(app) as client:
        with client.websocket_connect("/v1/launchers/connect") as holder:
            holder.send_json(HELLO)
            _wait(gm.entered)
            # first connection, claim pending: invisible everywhere
            assert reg.for_account(OPEN_OWNER) == [] and reg.by_key(OPEN_OWNER, KEY) is None
            assert reg.profiles_for(_sess(), "backend") == []
            gm.outcome = "lks_first"
            gm.release.set()
            w = holder.receive_json()
            assert w["type"] == "welcome" and w["key_secret"] == "lks_first"
            held = reg.by_key(OPEN_OWNER, KEY)
            assert held is not None and held.launcher_id == w["launcher_id"]

            with client.websocket_connect("/v1/launchers/connect") as intruder:
                intruder.send_json(HELLO)
                _wait(gm.entered)
                for _ in range(10):
                    # the holder stays the one indexed, discovered and launchable
                    assert reg.by_key(OPEN_OWNER, KEY) is held
                    assert [x.launcher_id for x in reg.for_account(OPEN_OWNER)] == [
                        held.launcher_id]
                    assert [p["launcher_id"] for p in reg.profiles_for(_sess(), "backend")] == [
                        held.launcher_id]
                    time.sleep(0.01)
                gm.outcome = LauncherKeyInUseError("launcher_key_in_use")
                gm.release.set()
                with pytest.raises(WebSocketDisconnect) as exc:
                    intruder.receive_json()
                assert (exc.value.code, exc.value.reason) == (4409, "launcher_key_in_use")
            assert reg.by_key(OPEN_OWNER, KEY) is held
            assert list(reg._launchers) == [held.launcher_id]


def test_machine_limit_refusal_leaves_no_trace() -> None:
    from sessionweave.machines import MachineLimitError

    gm = _GatedMachines()
    app = _app(gm)
    reg: LauncherRegistry = app.state.tool_deps.launchers
    with TestClient(app) as client, client.websocket_connect("/v1/launchers/connect") as ws:
        ws.send_json(HELLO)
        _wait(gm.entered)
        assert reg._launchers == {}
        gm.outcome = MachineLimitError("at most 20 machines")
        gm.release.set()
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        assert exc.value.code == 4429
    assert reg._launchers == {}
