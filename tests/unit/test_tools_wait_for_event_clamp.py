"""Unit tests for the transport-specific timeout clamp on session_wait_for_event.

These tests exercise the shared tool function in src/sessionweave/tools.py
with both MCPConnectionHandle and HTTPParticipantHandle instances and verify
the response surface ``effective_timeout_seconds`` correctly.
"""
from __future__ import annotations

from typing import Any

import pytest

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.http_api import HTTPParticipantHandle
from sessionweave.mcp_adapter import MCPConnectionHandle
from sessionweave.session import Session, WaitForEventResult
from sessionweave.tools import ToolDeps, session_wait_for_event


async def _build_session_and_join(
    *,
    config: SessionConfig | None = None,
) -> tuple[Session, str, MCPConnectionHandle, HTTPParticipantHandle, ToolDeps, ConnectionManager]:
    """Build an in-process Session + Registry + connections + two handles
    bound to one participant. Returns (session, pid, mcp_handle, http_handle,
    tool_deps, connections).

    The Participant entry is constructed via Session.join with a fake handle
    so we don't have to chase the Participant dataclass signature.
    """
    cm = ConnectionManager()
    cfg = config or SessionConfig()
    sess = Session("s", cfg, cm)
    deps = ToolDeps(SessionRegistryStub(sess, cm))  # type: ignore[arg-type]

    # Join one participant via the real path (delivery_mode="pull").
    from tests.unit.fakes import FakeHandle
    fake_join_handle = FakeHandle()
    p = await sess.join(fake_join_handle, role="echo", capabilities=["echo"], delivery_mode="pull")
    pid = p.participant_id

    # Release the fake_join_handle binding so we can bind our test handles.
    cm.unbind(fake_join_handle)

    class _FakeMCPHandle(MCPConnectionHandle):
        def __init__(self) -> None:  # bypass real MCP session wiring
            self._alive = True
            self._pending: set[Any] = set()
            self._session = None  # type: ignore[assignment]

    mcp_handle = _FakeMCPHandle()
    http_handle = HTTPParticipantHandle()

    return sess, pid, mcp_handle, http_handle, deps, cm


class SessionRegistryStub:
    """Minimal stub: ToolDeps only needs .registry.connections and
    .registry.get_or_raise(sid) for _session_for_handle to work."""
    def __init__(self, sess: Session, cm: ConnectionManager) -> None:
        self._sess = sess
        self.connections = cm

    def get(self, sid: str) -> Session | None:
        return self._sess if sid == self._sess.session_id else None

    async def get_or_raise(self, sid: str) -> Session:
        s = self.get(sid)
        if s is None:
            from sessionweave.errors import ErrorCode, ToolError
            raise ToolError(ErrorCode.SESSION_NOT_FOUND, sid)
        return s


@pytest.mark.asyncio
async def test_mcp_handle_clamps_to_max_wait_seconds_mcp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sess, pid, mcp_handle, _, deps, cm = await _build_session_and_join()
    cm.bind(mcp_handle, sess.session_id, pid)
    try:
        captured: dict[str, Any] = {}

        async def fake_wait(
            _self: Any,
            _participant_id: str,
            names: list[str] | None = None,
            timeout_seconds: float = 0.0,
        ) -> WaitForEventResult:
            # Called as unbound (class-level patch): first arg is self (Session instance),
            # second is participant_id positional, then keyword args.
            captured["timeout_seconds"] = timeout_seconds
            return WaitForEventResult(
                messages=[],
                timed_out=True,
                effective_timeout_seconds=timeout_seconds,
            )

        monkeypatch.setattr(Session, "wait_for_event", fake_wait)

        out = await session_wait_for_event(
            deps, mcp_handle, {"session_id": sess.session_id, "timeout_seconds": 100.0}
        )
        assert captured["timeout_seconds"] == 20.0
        assert out["effective_timeout_seconds"] == 20.0
        assert out["timed_out"] is True
        assert out["messages"] == []
    finally:
        cm.unbind(mcp_handle)


@pytest.mark.asyncio
async def test_http_handle_uses_max_wait_seconds_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sess, pid, _, http_handle, deps, cm = await _build_session_and_join()
    cm.bind(http_handle, sess.session_id, pid)
    try:
        captured: dict[str, Any] = {}

        async def fake_wait(
            _self: Any,
            _participant_id: str,
            names: list[str] | None = None,
            timeout_seconds: float = 0.0,
        ) -> WaitForEventResult:
            captured["timeout_seconds"] = timeout_seconds
            return WaitForEventResult(
                messages=[],
                timed_out=True,
                effective_timeout_seconds=timeout_seconds,
            )

        monkeypatch.setattr(Session, "wait_for_event", fake_wait)

        out = await session_wait_for_event(
            deps, http_handle, {"session_id": sess.session_id, "timeout_seconds": 100.0}
        )
        assert captured["timeout_seconds"] == 100.0
        assert out["effective_timeout_seconds"] == 100.0
    finally:
        cm.unbind(http_handle)


@pytest.mark.asyncio
async def test_http_handle_clamps_to_global_cap_when_above_max_wait_seconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sess, pid, _, http_handle, deps, cm = await _build_session_and_join()
    cm.bind(http_handle, sess.session_id, pid)
    try:
        captured: dict[str, Any] = {}

        async def fake_wait(
            _self: Any,
            _participant_id: str,
            names: list[str] | None = None,
            timeout_seconds: float = 0.0,
        ) -> WaitForEventResult:
            captured["timeout_seconds"] = timeout_seconds
            return WaitForEventResult(
                messages=[],
                timed_out=True,
                effective_timeout_seconds=timeout_seconds,
            )

        monkeypatch.setattr(Session, "wait_for_event", fake_wait)

        out = await session_wait_for_event(
            deps, http_handle, {"session_id": sess.session_id, "timeout_seconds": 999999.0}
        )
        assert captured["timeout_seconds"] == 600.0
        assert out["effective_timeout_seconds"] == 600.0
    finally:
        cm.unbind(http_handle)


@pytest.mark.asyncio
async def test_below_cap_passes_through_unchanged_on_mcp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sess, pid, mcp_handle, _, deps, cm = await _build_session_and_join()
    cm.bind(mcp_handle, sess.session_id, pid)
    try:
        captured: dict[str, Any] = {}

        async def fake_wait(
            _self: Any,
            _participant_id: str,
            names: list[str] | None = None,
            timeout_seconds: float = 0.0,
        ) -> WaitForEventResult:
            captured["timeout_seconds"] = timeout_seconds
            return WaitForEventResult(
                messages=[],
                timed_out=True,
                effective_timeout_seconds=timeout_seconds,
            )

        monkeypatch.setattr(Session, "wait_for_event", fake_wait)

        out = await session_wait_for_event(
            deps, mcp_handle, {"session_id": sess.session_id, "timeout_seconds": 10.0}
        )
        assert captured["timeout_seconds"] == 10.0
        assert out["effective_timeout_seconds"] == 10.0
    finally:
        cm.unbind(mcp_handle)


@pytest.mark.asyncio
async def test_zero_timeout_not_clamped(monkeypatch: pytest.MonkeyPatch) -> None:
    sess, pid, mcp_handle, _, deps, cm = await _build_session_and_join()
    cm.bind(mcp_handle, sess.session_id, pid)
    try:
        captured: dict[str, Any] = {}

        async def fake_wait(
            _self: Any,
            _participant_id: str,
            names: list[str] | None = None,
            timeout_seconds: float = 0.0,
        ) -> WaitForEventResult:
            captured["timeout_seconds"] = timeout_seconds
            return WaitForEventResult(
                messages=[],
                timed_out=True,
                effective_timeout_seconds=timeout_seconds,
            )

        monkeypatch.setattr(Session, "wait_for_event", fake_wait)

        out = await session_wait_for_event(
            deps, mcp_handle, {"session_id": sess.session_id, "timeout_seconds": 0.0}
        )
        assert captured["timeout_seconds"] == 0.0
        assert out["effective_timeout_seconds"] == 0.0
    finally:
        cm.unbind(mcp_handle)


@pytest.mark.asyncio
async def test_clamped_metric_increments_on_mcp_clamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from sessionweave.metrics import Metrics

    metrics = Metrics()
    sess, pid, mcp_handle, _, deps, cm = await _build_session_and_join()
    sess._metrics = metrics
    cm.bind(mcp_handle, sess.session_id, pid)
    try:
        async def fake_wait(
            _self: Any,
            _participant_id: str,
            names: list[str] | None = None,
            timeout_seconds: float = 0.0,
        ) -> WaitForEventResult:
            return WaitForEventResult(
                messages=[],
                timed_out=True,
                effective_timeout_seconds=timeout_seconds,
            )

        monkeypatch.setattr(Session, "wait_for_event", fake_wait)

        # Above cap → clamp → counter increments.
        await session_wait_for_event(
            deps, mcp_handle, {"session_id": sess.session_id, "timeout_seconds": 60.0}
        )
        rendered = metrics.render().decode("utf-8")
        assert 'sessionweave_wait_for_event_clamped_total{transport="mcp"} 1.0' in rendered

        # Below cap → no clamp → no increment.
        await session_wait_for_event(
            deps, mcp_handle, {"session_id": sess.session_id, "timeout_seconds": 10.0}
        )
        rendered = metrics.render().decode("utf-8")
        # still 1.0 (not 2.0).
        assert 'sessionweave_wait_for_event_clamped_total{transport="mcp"} 1.0' in rendered
    finally:
        cm.unbind(mcp_handle)
