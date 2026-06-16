import logging
import pytest

from sessionweave.logging_setup import setup_file_logging

_NAMES = ("", "uvicorn", "uvicorn.access", "uvicorn.error", "sessionweave_cockpit")
_HANDLER_NAMES = ("sw_file_log", "sw_console")


@pytest.fixture(autouse=True)
def _cleanup_sw_handlers():
    yield
    for name in _NAMES:
        lg = logging.getLogger(name)
        for h in list(lg.handlers):
            if getattr(h, "name", None) in _HANDLER_NAMES:
                lg.removeHandler(h)
                h.close()


def _file_handler(logger_name=""):
    lg = logging.getLogger(logger_name)
    return next((h for h in lg.handlers if getattr(h, "name", None) == "sw_file_log"), None)


def _console_handler(logger_name=""):
    lg = logging.getLogger(logger_name)
    return next((h for h in lg.handlers if getattr(h, "name", None) == "sw_console"), None)


def test_writes_to_default_path(tmp_path):
    target = tmp_path / "broker.log"
    setup_file_logging("broker", default_path=str(target))
    logging.getLogger().warning("hello-default")
    assert "hello-default" in target.read_text()


def test_env_override_honored(tmp_path, monkeypatch):
    override = tmp_path / "custom.log"
    monkeypatch.setenv("SW_LOG_FILE", str(override))
    setup_file_logging("broker", default_path=str(tmp_path / "broker.log"))
    logging.getLogger().warning("hello-override")
    assert override.exists() and "hello-override" in override.read_text()
    assert not (tmp_path / "broker.log").exists()


def test_empty_env_disables(tmp_path, monkeypatch):
    monkeypatch.setenv("SW_LOG_FILE", "")
    setup_file_logging("broker", default_path=str(tmp_path / "broker.log"))
    assert _file_handler() is None


def test_rotation(tmp_path, monkeypatch):
    target = tmp_path / "broker.log"
    monkeypatch.setenv("SW_LOG_MAX_BYTES", "200")
    monkeypatch.setenv("SW_LOG_BACKUP_COUNT", "2")
    setup_file_logging("broker", default_path=str(target))
    log = logging.getLogger()
    for i in range(50):
        log.warning("x" * 50 + f" {i}")
    assert (tmp_path / "broker.log.1").exists()


def test_extra_logger_attached(tmp_path):
    setup_file_logging(
        "broker",
        default_path=str(tmp_path / "broker.log"),
        extra_loggers=("sessionweave_cockpit",),
    )
    assert _file_handler("sessionweave_cockpit") is not None


def test_console_handler_added_when_requested(tmp_path):
    setup_file_logging("broker", default_path=str(tmp_path / "broker.log"), console=True)
    assert _console_handler() is not None


def test_no_console_by_default(tmp_path):
    setup_file_logging("broker", default_path=str(tmp_path / "broker.log"))
    assert _console_handler() is None


def test_console_added_even_when_file_disabled(tmp_path):
    setup_file_logging("broker", default_path="", console=True)
    assert _console_handler() is not None
    assert _file_handler() is None


def test_idempotent(tmp_path):
    target = str(tmp_path / "broker.log")
    setup_file_logging("broker", default_path=target, console=True)
    setup_file_logging("broker", default_path=target, console=True)
    root = logging.getLogger()
    files = [h for h in root.handlers if getattr(h, "name", None) == "sw_file_log"]
    consoles = [h for h in root.handlers if getattr(h, "name", None) == "sw_console"]
    assert len(files) == 1 and len(consoles) == 1


def test_oserror_degrades_gracefully(tmp_path):
    blocker = tmp_path / "blocker"
    blocker.write_text("i am a file, not a directory")
    setup_file_logging("broker", default_path=str(blocker / "sub" / "broker.log"))
    assert _file_handler() is None
