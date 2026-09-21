"""A transport with no cross-request identity cannot carry a participant.

The relay binds a participant to an MCP *connection*, and eight tools take
no ``session_id`` because the connection is the session context. The mcp
2026-07-28 "modern" path breaks that: one POST in, one response out, a
fresh ``Connection`` per request (``session_id`` is ``None``), and the
exit stack unwound before the response is even read.

Left unchecked, a client on that path gets a *successful* ``session_join``
and then ``not_in_session`` on everything after — while the participant it
created sits in the roster, is flipped offline by its own teardown, and is
eventually evicted with ``participant.left(offline_timeout)``. Peers watch
a join/offline/left flap for a client that was never reachable.

Refusing at the door turns that into one clear error. The check is on the
public ``session_id``, not on ``has_standalone_channel``: pull-mode clients
fail on this path too, so a push-only check would not be enough.
"""
from __future__ import annotations

from typing import Any

import pytest
from mcp.server.connection import Connection

from sessionweave.errors import ErrorCode, ToolError
from sessionweave.mcp_adapter import require_bindable_connection


class FakeOutbound:
    def __init__(self) -> None:
        self._closed = False

    async def notify(self, method: str, params: Any, opts: Any = None) -> None:
        pass

    async def send_raw_request(self, *a: Any, **k: Any) -> Any:  # pragma: no cover
        raise NotImplementedError


def test_a_stateful_connection_is_accepted() -> None:
    conn = Connection.for_loop(FakeOutbound(), session_id="sid-1")
    assert require_bindable_connection(conn) is conn


def test_a_single_exchange_connection_is_refused() -> None:
    """``from_envelope`` is the 2026-07-28 path: no Mcp-Session-Id, so the
    next request would arrive on a different Connection."""
    conn = Connection.from_envelope("2026-07-28", None, None)
    assert conn.session_id is None

    with pytest.raises(ToolError) as exc:
        require_bindable_connection(conn)

    assert exc.value.code is ErrorCode.INVALID_CONFIG
    assert "session" in str(exc.value).lower()
