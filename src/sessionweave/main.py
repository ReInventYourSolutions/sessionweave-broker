from __future__ import annotations

import argparse
import os

import uvicorn

from sessionweave.server import build_app
from sessionweave.logging_setup import setup_file_logging


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
    parser.add_argument(
        "--persistence-db-path",
        default=None,
        help=(
            "Override SW_PERSISTENCE_DB_PATH. Use an empty string to disable "
            "persistence. When unset, falls back to the env var (default: "
            "data/sessionweave.db)."
        ),
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help=(
            "Path of the rotating log file. Overrides SW_LOG_FILE. Pass an "
            "empty string to disable file logging. Default: "
            "~/.sessionweave/logs/broker.log"
        ),
    )
    args = parser.parse_args()
    setup_file_logging(
        "broker",
        default_path=os.path.expanduser("~/.sessionweave/logs/broker.log"),
        log_file=args.log_file,
        console=True,
    )
    if args.persistence_db_path is not None:
        os.environ["SW_PERSISTENCE_DB_PATH"] = args.persistence_db_path
    uvicorn.run(
        build_app(),
        host=args.host,
        port=args.port,
        log_level="info",
        log_config=None,
        proxy_headers=True,
        forwarded_allow_ips=args.forwarded_allow_ips,
    )


if __name__ == "__main__":
    cli()
