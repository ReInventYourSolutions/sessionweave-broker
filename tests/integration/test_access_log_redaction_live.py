"""A real uvicorn WebSocket handshake never writes the ?token= value to the logs."""
from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import threading

import httpx
import pytest
import uvicorn
import websockets

from sessionweave.access_log_filter import install_uvicorn_log_filters
from sessionweave.server import build_app


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(record.getMessage())


@pytest.mark.asyncio
async def test_ws_handshake_logs_redact_token() -> None:
    with contextlib.closing(socket.socket()) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    names = ("uvicorn.access", "uvicorn.error")
    cap = _Capture()
    saved = {n: list(logging.getLogger(n).filters) for n in names}
    for n in names:
        lg = logging.getLogger(n)
        lg.setLevel(logging.INFO)
        lg.addHandler(cap)
    install_uvicorn_log_filters()
    server = uvicorn.Server(
        uvicorn.Config(build_app(), host="127.0.0.1", port=port, log_config=None,
                       log_level="info", ws="websockets")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{port}"
        for _ in range(100):
            try:
                async with httpx.AsyncClient() as c:
                    if (await c.get(f"{base}/healthz")).status_code == 200:
                        break
            except Exception:
                await asyncio.sleep(0.1)
        async with httpx.AsyncClient() as c:
            made = (await c.post(f"{base}/v1/sessions", json={})).json()
        sid, secret = made["session_id"], made["supervisor_token"]
        async with websockets.connect(
            f"ws://127.0.0.1:{port}/v1/sessions/{sid}/firehose?token={secret}"
        ):
            pass
        async with httpx.AsyncClient() as c:
            await c.get(f"{base}/invite/{sid}", params={"jt": made["join_token"]})
        await asyncio.sleep(0.3)
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        for n in names:
            lg = logging.getLogger(n)
            lg.removeHandler(cap)
            lg.filters[:] = saved[n]
    joined = "\n".join(cap.lines)
    assert "WebSocket" in joined and "firehose" in joined, joined
    assert secret not in joined
    assert made["join_token"] not in joined
    assert "[REDACTED]" in joined
