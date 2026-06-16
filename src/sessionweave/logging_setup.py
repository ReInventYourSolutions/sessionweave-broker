"""Rotating file-log setup.

Self-contained and duplicated verbatim across the broker / cockpit / runner
packages by design: the broker must not depend on the client SDK, so there is
no shared module to host this. Keep the three copies in sync.
"""
from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler

_DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
_DEFAULT_BACKUP_COUNT = 5
_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
_HANDLER_NAME = "sw_file_log"


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def setup_file_logging(
    component: str,
    *,
    default_path: str,
    log_file: str | None = None,
    read_env: bool = True,
    extra_loggers: tuple[str, ...] = (),
) -> None:
    """Attach a rotating file handler. Idempotent; never raises.

    Path: ``log_file`` arg -> ``SW_LOG_FILE`` env (only if ``read_env``) ->
    ``default_path``. An empty resolved path disables file logging.
    """
    root = logging.getLogger()
    if any(getattr(h, "name", None) == _HANDLER_NAME for h in root.handlers):
        return  # already installed

    if log_file is None and read_env:
        log_file = os.environ.get("SW_LOG_FILE")
    path = default_path if log_file is None else log_file
    path = os.path.expanduser(os.path.expandvars(path)).strip()
    if not path:
        return  # explicitly disabled (empty or whitespace-only)

    max_bytes = _int_env("SW_LOG_MAX_BYTES", _DEFAULT_MAX_BYTES) if read_env else _DEFAULT_MAX_BYTES
    backups = _int_env("SW_LOG_BACKUP_COUNT", _DEFAULT_BACKUP_COUNT) if read_env else _DEFAULT_BACKUP_COUNT

    try:
        parent = os.path.dirname(path) or "."
        os.makedirs(parent, mode=0o700, exist_ok=True)
        handler = RotatingFileHandler(
            path, maxBytes=max_bytes, backupCount=backups, encoding="utf-8"
        )
    except OSError as exc:
        print(f"[{component}] file logging disabled: {exc}", file=sys.stderr)
        return

    handler.set_name(_HANDLER_NAME)
    handler.setFormatter(logging.Formatter(_FORMAT))
    for name in ("", *extra_loggers):
        logging.getLogger(name).addHandler(handler)
