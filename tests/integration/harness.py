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
) -> AsyncIterator[str]:
    """Spawn the relay as a subprocess on a free port and yield its base URL."""
    port = _free_port()
    env = {**os.environ, "SW_PORT": str(port), "SW_HOST": "127.0.0.1"}
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
