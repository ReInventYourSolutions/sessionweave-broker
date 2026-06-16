"""Rotating file-log setup (non-blocking).

Self-contained and duplicated verbatim across the broker / cockpit / runner
packages by design: the broker must not depend on the client SDK, so there is
no shared module to host this. Keep the three copies in sync.

Logging is non-blocking: a QueueHandler on the root logger only enqueues
records; a QueueListener background thread owns the real file/stderr handlers
and performs the (potentially slow) I/O off the event-loop thread.
"""
from __future__ import annotations

import atexit
import logging
import os
import queue
import sys
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler

_DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
_DEFAULT_BACKUP_COUNT = 5
_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
_FILE_HANDLER_NAME = "sw_file_log"
_CONSOLE_HANDLER_NAME = "sw_console"
_QUEUE_HANDLER_NAME = "sw_queue"


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
) -> QueueListener | None:
    """Install non-blocking logging and return the started QueueListener.

    A ``QueueHandler`` is attached to the root logger (and each ``extra_loggers``
    entry, for loggers that set ``propagate = False``); it only enqueues records.
    A background ``QueueListener`` owns the real sinks — a rotating file handler
    and, when ``console=True``, a stderr handler — so the slow I/O happens off the
    event-loop thread. Returns ``None`` if nothing was installed (no file + no
    console, or already installed). Idempotent; never raises.

    Path: ``log_file`` arg -> ``SW_LOG_FILE`` env (only if ``read_env``) ->
    ``default_path``. An empty/whitespace resolved path disables the file sink.
    """
    root = logging.getLogger()
    if _has_handler(root, _QUEUE_HANDLER_NAME):
        return None  # already installed

    fmt = logging.Formatter(_FORMAT)
    sinks: list[logging.Handler] = []

    # File sink.
    if log_file is None and read_env:
        log_file = os.environ.get("SW_LOG_FILE")
    path = default_path if log_file is None else log_file
    path = os.path.expanduser(os.path.expandvars(path)).strip()
    if path:
        if read_env:
            max_bytes = _int_env("SW_LOG_MAX_BYTES", _DEFAULT_MAX_BYTES)
            backups = _int_env("SW_LOG_BACKUP_COUNT", _DEFAULT_BACKUP_COUNT)
        else:
            max_bytes = _DEFAULT_MAX_BYTES
            backups = _DEFAULT_BACKUP_COUNT
        try:
            parent = os.path.dirname(path) or "."
            os.makedirs(parent, mode=0o700, exist_ok=True)
            file_handler = RotatingFileHandler(
                path, maxBytes=max_bytes, backupCount=backups, encoding="utf-8"
            )
            file_handler.set_name(_FILE_HANDLER_NAME)
            file_handler.setFormatter(fmt)
            sinks.append(file_handler)
        except OSError as exc:
            print(f"[{component}] file logging disabled: {exc}", file=sys.stderr)

    # Console sink.
    if console:
        console_handler = logging.StreamHandler()  # stderr
        console_handler.set_name(_CONSOLE_HANDLER_NAME)
        console_handler.setFormatter(fmt)
        sinks.append(console_handler)

    if not sinks:
        return None  # nothing to install

    log_queue: queue.Queue[logging.LogRecord] = queue.Queue()  # unbounded
    listener = QueueListener(log_queue, *sinks, respect_handler_level=False)
    listener.start()
    atexit.register(listener.stop)

    queue_handler = QueueHandler(log_queue)
    queue_handler.set_name(_QUEUE_HANDLER_NAME)
    for name in ("", *extra_loggers):
        logging.getLogger(name).addHandler(queue_handler)

    return listener
