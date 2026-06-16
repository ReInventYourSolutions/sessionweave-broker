from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import subprocess
import sys
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
    # Disable file logging in the test relay: the CLI defaults it on, but a
    # synchronous RotatingFileHandler write per request throttles the event
    # loop under high-volume stress tests (e.g. test_buffer_race) on slow CI
    # disks, causing ReadTimeouts. Tests don't read the file; stderr is enough.
    env = {**os.environ, "SW_PORT": str(port), "SW_HOST": "127.0.0.1", "SW_LOG_FILE": ""}
    if persistence_db_path is not None:
        env["SW_PERSISTENCE_DB_PATH"] = persistence_db_path
    if env_overrides:
        env.update(env_overrides)
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
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    base = f"http://127.0.0.1:{port}"
    try:
        # Wait for /healthz to come up, max ~5s
        for _ in range(50):
            try:
                async with httpx.AsyncClient(timeout=1) as c:
                    r = await c.get(f"{base}/healthz")
                    if r.status_code == 200:
                        break
            except Exception:
                pass
            await asyncio.sleep(0.1)
        else:
            stdout, stderr = proc.communicate(timeout=2)
            raise RuntimeError(
                f"relay did not become healthy. stdout={stdout!r} stderr={stderr!r}"
            )
        yield base
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
