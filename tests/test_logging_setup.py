import logging
import pytest

from sessionweave.logging_setup import setup_file_logging

_NAMES = ("", "uvicorn", "uvicorn.access", "uvicorn.error", "sessionweave_cockpit")


@pytest.fixture(autouse=True)
def _cleanup_sw_handlers():
    yield
    for name in _NAMES:
        lg = logging.getLogger(name)
        for h in list(lg.handlers):
            if getattr(h, "name", None) == "sw_file_log":
                lg.removeHandler(h)
                h.close()


def _file_handler(logger_name=""):
    lg = logging.getLogger(logger_name)
    return next((h for h in lg.handlers if getattr(h, "name", None) == "sw_file_log"), None)


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


def test_uvicorn_loggers_attached(tmp_path):
    setup_file_logging(
        "broker",
        default_path=str(tmp_path / "broker.log"),
        extra_loggers=("uvicorn", "uvicorn.access", "uvicorn.error"),
    )
    assert _file_handler("uvicorn.access") is not None


def test_idempotent(tmp_path):
    target = str(tmp_path / "broker.log")
    setup_file_logging("broker", default_path=target)
    setup_file_logging("broker", default_path=target)
    handlers = [h for h in logging.getLogger().handlers if getattr(h, "name", None) == "sw_file_log"]
    assert len(handlers) == 1


def test_oserror_degrades_gracefully(tmp_path):
    blocker = tmp_path / "blocker"
    blocker.write_text("i am a file, not a directory")
    # parent of the log path is a regular file -> makedirs raises OSError
    setup_file_logging("broker", default_path=str(blocker / "sub" / "broker.log"))
    assert _file_handler() is None
