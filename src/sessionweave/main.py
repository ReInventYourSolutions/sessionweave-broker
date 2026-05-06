from __future__ import annotations

import argparse
import os

import uvicorn

from sessionweave.server import build_app


def cli() -> None:
    parser = argparse.ArgumentParser(prog="sessionweave")
    parser.add_argument("--host", default=os.environ.get("SW_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("SW_PORT", "8765")))
    args = parser.parse_args()
    uvicorn.run(build_app(), host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    cli()
