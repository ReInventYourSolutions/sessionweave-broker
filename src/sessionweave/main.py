from __future__ import annotations

import argparse
import os

import uvicorn

from sessionweave.server import build_app


def cli() -> None:
    parser = argparse.ArgumentParser(prog="sessionweave")
    parser.add_argument("--host", default=os.environ.get("SW_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("SW_PORT", "8765")))
    parser.add_argument(
        "--forwarded-allow-ips",
        default=os.environ.get("SW_FORWARDED_ALLOW_IPS", "127.0.0.1"),
        help=(
            "Comma-separated CIDRs/IPs whose X-Forwarded-* and Host headers "
            "uvicorn will trust. Set to '*' or include the docker bridge "
            "(e.g. '172.17.0.0/16,127.0.0.1') when running behind a "
            "reverse proxy / TLS terminator. Default: 127.0.0.1."
        ),
    )
    args = parser.parse_args()
    uvicorn.run(
        build_app(),
        host=args.host,
        port=args.port,
        log_level="info",
        proxy_headers=True,
        forwarded_allow_ips=args.forwarded_allow_ips,
    )


if __name__ == "__main__":
    cli()
