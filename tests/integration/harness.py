from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import subprocess
import sys
import tempfile
from collections.abc import AsyncIterator

import httpx


def _free_port() -> int:
    with contextlib.closing(socket.socket()) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@contextlib.asynccontextmanager
async def relay_subprocess(
    env_overrides: dict[str, str] | None = None,
    persistence_db_path: str | None = None,
) -> AsyncIterator[str]:
    """Spawn the relay as a subprocess on a free port and yield its base URL."""
    port = _free_port()
    # Keep the test relay's file logging off (tests don't read it).
    env = {**os.environ, "SW_PORT": str(port), "SW_HOST": "127.0.0.1", "SW_LOG_FILE": ""}
    if persistence_db_path is not None:
        env["SW_PERSISTENCE_DB_PATH"] = persistence_db_path
    if env_overrides:
        env.update(env_overrides)
    # Capture the relay's stdout/stderr to temp FILES, not PIPEs. A PIPE has a
    # fixed (~64KB) OS buffer; if it fills (the relay access-logs every request,
    # and stress tests like test_buffer_race emit thousands of lines) and nothing
    # drains it, the relay's logging write() blocks, freezing its event loop and
    # hanging all requests → ReadTimeout. Files have no such limit.
    out_f = tempfile.TemporaryFile()
    err_f = tempfile.TemporaryFile()
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "sessionweave.main",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        env=env,
        stdout=out_f,
        stderr=err_f,
    )
    base = f"http://127.0.0.1:{port}"
    try:
        # Wait for /healthz to come up. Generous cap (~30s): the relay
        # subprocess imports a heavy dependency tree (starlette/uvicorn/mcp/
        # aiosqlite) and binds a port before it's healthy; on a loaded CI box
        # (the full integration suite spawns many relays in parallel) a 5s cap
        # spuriously trips "relay did not become healthy". The loop returns the
        # instant /healthz answers 200, so a high cap costs nothing in the
        # common case — it only adds patience under load.
        for _ in range(150):
            try:
                async with httpx.AsyncClient(timeout=1) as c:
                    r = await c.get(f"{base}/healthz")
                    if r.status_code == 200:
                        break
            except Exception:
                pass
            await asyncio.sleep(0.2)
        else:
            proc.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=2)
            out_f.seek(0)
            err_f.seek(0)
            raise RuntimeError(
                f"relay did not become healthy. "
                f"stdout={out_f.read()!r} stderr={err_f.read()!r}"
            )
        yield base
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        out_f.close()
        err_f.close()
