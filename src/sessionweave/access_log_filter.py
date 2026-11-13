"""Filter that drops automated-noise records from the uvicorn access log.

Duplicated verbatim in the broker and cockpit packages (the broker must not
depend on the client SDK; the cockpit is a separate package). Keep the two
copies in sync. Attached to the ``uvicorn.access`` logger at startup so the
noise never reaches the file / journald sinks.
"""
from __future__ import annotations

import logging
import re

_REAL_API_PREFIXES = ("/v1", "/mcp", "/invite")
_DROP_PATHS = frozenset({"/healthz", "/metrics"})
# Secrets that travel in the query string (join token, WebSocket token).
_SECRET_QUERY = re.compile(r"([?&](?:jt|token)=)[^&#\s\"]*")


def _redact(value: object) -> object:
    if isinstance(value, str):
        return _SECRET_QUERY.sub(r"\1[REDACTED]", value)
    return value


class AccessLogNoiseFilter(logging.Filter):
    """Drop health-ping, metrics-scrape, and scanner-404 access records.

    Redacts the values of ``jt`` and ``token`` query parameters. Keeps every
    non-access record and all real traffic (``/v1`` / ``/mcp`` /
    ``/invite`` at any status, plus WebSocket access lines). Never raises: on
    any unexpected record shape it keeps the record.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name not in ("uvicorn.access", "uvicorn.error"):
            return True
        # uvicorn writes WebSocket lines ('%s - "WebSocket %s" [accepted]') to
        # uvicorn.error, with the query string in the args: redact there too,
        # but never drop anything outside uvicorn.access.
        if isinstance(record.msg, str):
            record.msg = _redact(record.msg)
        args = record.args
        # Redact join / WebSocket tokens from every string arg (HTTP lines
        # carry the path in args[2]; WebSocket lines in args[1] or inside a
        # "WebSocket <path>" string) before the record is emitted.
        if isinstance(args, tuple):
            try:
                record.args = args = tuple(_redact(a) for a in args)
            except Exception:
                pass
        # uvicorn HTTP access records carry a 5-tuple:
        #   (client_addr, method, full_path, http_version, status_code)
        # WebSocket / other shapes differ in length -> keep them.
        if record.name != "uvicorn.access":
            return True
        if not isinstance(args, tuple) or len(args) != 5:
            return True
        try:
            full_path = str(args[2])
            status = int(str(args[4]))
        except (ValueError, TypeError, IndexError):
            return True
        path = full_path.split("?", 1)[0]
        is_noise = path in _DROP_PATHS or (
            status == 404 and not path.startswith(_REAL_API_PREFIXES)
        )
        return not is_noise


def install_uvicorn_log_filters() -> None:
    """Attach the noise/redaction filter to uvicorn's access and error loggers.

    ``uvicorn.error`` only redacts (WebSocket lines with ``?token=`` land there).
    """
    for name in ("uvicorn.access", "uvicorn.error"):
        logging.getLogger(name).addFilter(AccessLogNoiseFilter())
