import contextlib
import logging
from logging.handlers import QueueListener
from typing import Any

import pytest

from sessionweave.logging_setup import setup_file_logging

_LOGGER_NAMES = ("", "uvicorn", "uvicorn.access", "uvicorn.error", "sessionweave_cockpit")
_HANDLER_NAMES = ("sw_queue", "sw_file_log", "sw_console")
_started: list[QueueListener] = []


def _setup(**kwargs: Any) -> QueueListener | None:
    listener = setup_file_logging(**kwargs)
    if listener is not None:
        _started.append(listener)
    return listener


@pytest.fixture(autouse=True)
def _cleanup() -> Any:
    yield
    while _started:
        with contextlib.suppress(Exception):
            _started.pop().stop()
    for name in _LOGGER_NAMES:
        lg = logging.getLogger(name)
        for h in list(lg.handlers):
            if getattr(h, "name", None) in _HANDLER_NAMES:
                lg.removeHandler(h)


def _sink_names(listener: QueueListener) -> set[str | None]:
    return {getattr(h, "name", None) for h in listener.handlers}


def test_queue_handler_attached(tmp_path: Any) -> None:
    listener = _setup(component="broker", default_path=str(tmp_path / "broker.log"))
    assert listener is not None
    root = logging.getLogger()
    assert any(getattr(h, "name", None) == "sw_queue" for h in root.handlers)


def test_writes_to_default_path(tmp_path: Any) -> None:
    target = tmp_path / "broker.log"
    listener = _setup(component="broker", default_path=str(target))
    assert listener is not None
    logging.getLogger().warning("hello-default")
    listener.queue.join()  # type: ignore[attr-defined]
    assert "hello-default" in target.read_text()


def test_env_override_honored(tmp_path: Any, monkeypatch: Any) -> None:
    override = tmp_path / "custom.log"
    monkeypatch.setenv("SW_LOG_FILE", str(override))
    listener = _setup(component="broker", default_path=str(tmp_path / "broker.log"))
    assert listener is not None
    logging.getLogger().warning("hello-override")
    listener.queue.join()  # type: ignore[attr-defined]
    assert override.exists() and "hello-override" in override.read_text()
    assert not (tmp_path / "broker.log").exists()


def test_empty_env_disables_file(tmp_path: Any, monkeypatch: Any) -> None:
    monkeypatch.setenv("SW_LOG_FILE", "")
    listener = _setup(component="broker", default_path=str(tmp_path / "broker.log"))
    assert listener is None
    root = logging.getLogger()
    assert not any(getattr(h, "name", None) == "sw_queue" for h in root.handlers)


def test_console_only_when_file_disabled(tmp_path: Any, monkeypatch: Any) -> None:
    monkeypatch.setenv("SW_LOG_FILE", "")
    listener = _setup(
        component="broker", default_path=str(tmp_path / "broker.log"), console=True
    )
    assert listener is not None
    assert _sink_names(listener) == {"sw_console"}


def test_rotation(tmp_path: Any, monkeypatch: Any) -> None:
    target = tmp_path / "broker.log"
    monkeypatch.setenv("SW_LOG_MAX_BYTES", "200")
    monkeypatch.setenv("SW_LOG_BACKUP_COUNT", "2")
    listener = _setup(component="broker", default_path=str(target))
    assert listener is not None
    log = logging.getLogger()
    for i in range(50):
        log.warning("x" * 50 + f" {i}")
    listener.queue.join()  # type: ignore[attr-defined]
    assert (tmp_path / "broker.log.1").exists()


def test_console_sink_present(tmp_path: Any) -> None:
    listener = _setup(
        component="broker", default_path=str(tmp_path / "broker.log"), console=True
    )
    assert listener is not None
    assert _sink_names(listener) == {"sw_file_log", "sw_console"}


def test_no_console_by_default(tmp_path: Any) -> None:
    listener = _setup(component="broker", default_path=str(tmp_path / "broker.log"))
    assert listener is not None
    assert _sink_names(listener) == {"sw_file_log"}


def test_extra_logger_attached(tmp_path: Any) -> None:
    _setup(
        component="broker",
        default_path=str(tmp_path / "broker.log"),
        extra_loggers=("sessionweave_cockpit",),
    )
    lg = logging.getLogger("sessionweave_cockpit")
    assert any(getattr(h, "name", None) == "sw_queue" for h in lg.handlers)


def test_idempotent(tmp_path: Any) -> None:
    target = str(tmp_path / "broker.log")
    first = _setup(component="broker", default_path=target, console=True)
    second = _setup(component="broker", default_path=target, console=True)
    assert first is not None
    assert second is None
    root = logging.getLogger()
    qh = [h for h in root.handlers if getattr(h, "name", None) == "sw_queue"]
    assert len(qh) == 1


def test_oserror_degrades_gracefully(tmp_path: Any) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("i am a file, not a directory")
    listener = _setup(
        component="broker", default_path=str(blocker / "sub" / "broker.log")
    )
    assert listener is None
