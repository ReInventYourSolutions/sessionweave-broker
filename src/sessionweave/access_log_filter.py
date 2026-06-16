"""Filter that drops automated-noise records from the uvicorn access log.

Duplicated verbatim in the broker and cockpit packages (the broker must not
depend on the client SDK; the cockpit is a separate package). Keep the two
copies in sync. Attached to the ``uvicorn.access`` logger at startup so the
noise never reaches the file / journald sinks.
"""
from __future__ import annotations

import logging

_REAL_API_PREFIXES = ("/v1", "/mcp", "/invite")
_DROP_PATHS = frozenset({"/healthz", "/metrics"})


class AccessLogNoiseFilter(logging.Filter):
    """Drop health-ping, metrics-scrape, and scanner-404 access records.

    Keeps every non-access record and all real traffic (``/v1`` / ``/mcp`` /
    ``/invite`` at any status, plus WebSocket access lines). Never raises: on
    any unexpected record shape it keeps the record.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name != "uvicorn.access":
            return True
        args = record.args
        # uvicorn HTTP access records carry a 5-tuple:
        #   (client_addr, method, full_path, http_version, status_code)
        # WebSocket / other shapes differ in length -> keep them.
        if not isinstance(args, tuple) or len(args) != 5:
            return True
        try:
            full_path = str(args[2])
            status = int(str(args[4]))
        except (ValueError, TypeError, IndexError):
            return True
        path = full_path.split("?", 1)[0]
        if path in _DROP_PATHS:
            return False
        if status == 404 and not path.startswith(_REAL_API_PREFIXES):
            return False
        return True
