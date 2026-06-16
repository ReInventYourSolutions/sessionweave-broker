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
_FILE_HANDLER_NAME = "sw_file_log"
_CONSOLE_HANDLER_NAME = "sw_console"


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _has_handler(logger: logging.Logger, name: str) -> bool:
    return any(getattr(h, "name", None) == name for h in logger.handlers)


def setup_file_logging(
    component: str,
    *,
    default_path: str,
    log_file: str | None = None,
    read_env: bool = True,
    console: bool = False,
    extra_loggers: tuple[str, ...] = (),
) -> None:
    """Configure logging: optional stderr console handler + a rotating file
    handler, attached to the root logger (and any ``extra_loggers`` that set
    ``propagate = False`` and so never reach root). Idempotent; never raises.

    Use ``console=True`` for components that hand uvicorn ``log_config=None`` so
    we own logging entirely — the console handler keeps journald/stderr output
    alive once uvicorn stops installing its own handlers.

    Path: ``log_file`` arg -> ``SW_LOG_FILE`` env (only if ``read_env``) ->
    ``default_path``. An empty/whitespace resolved path disables the file
    handler (the console handler, if any, stays).
    """
    fmt = logging.Formatter(_FORMAT)
    root = logging.getLogger()

    if console and not _has_handler(root, _CONSOLE_HANDLER_NAME):
        console_handler = logging.StreamHandler()  # stderr
        console_handler.set_name(_CONSOLE_HANDLER_NAME)
        console_handler.setFormatter(fmt)
        root.addHandler(console_handler)

    if log_file is None and read_env:
        log_file = os.environ.get("SW_LOG_FILE")
    path = default_path if log_file is None else log_file
    path = os.path.expanduser(os.path.expandvars(path)).strip()
    if not path:
        return  # file logging disabled (empty/whitespace path)

    if _has_handler(root, _FILE_HANDLER_NAME):
        return  # file handler already installed

    if read_env:
        max_bytes = _int_env("SW_LOG_MAX_BYTES", _DEFAULT_MAX_BYTES)
        backups = _int_env("SW_LOG_BACKUP_COUNT", _DEFAULT_BACKUP_COUNT)
    else:
        max_bytes = _DEFAULT_MAX_BYTES
        backups = _DEFAULT_BACKUP_COUNT

    try:
        parent = os.path.dirname(path) or "."
        os.makedirs(parent, mode=0o700, exist_ok=True)
        handler = RotatingFileHandler(
            path, maxBytes=max_bytes, backupCount=backups, encoding="utf-8"
        )
    except OSError as exc:
        print(f"[{component}] file logging disabled: {exc}", file=sys.stderr)
        return

    handler.set_name(_FILE_HANDLER_NAME)
    handler.setFormatter(fmt)
    for name in ("", *extra_loggers):
        logging.getLogger(name).addHandler(handler)
