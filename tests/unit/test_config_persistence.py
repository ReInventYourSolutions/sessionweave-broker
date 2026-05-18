import os

import pytest

from sessionweave.config import (
    ConfigBounds,
    SessionConfig,
    apply_overrides,
    load_deployment_defaults,
)
from sessionweave.errors import ErrorCode, ToolError


def test_session_config_persistence_defaults() -> None:
    cfg = SessionConfig()
    assert cfg.persistence_db_path == ""
    assert cfg.persistence_db_pool_size == 4
    assert cfg.persistence_sweep_interval_seconds == 60
    assert cfg.persistence_until_deleted_safety_seconds == 7_776_000  # 90 days


def test_config_bounds_persistence_defaults() -> None:
    b = ConfigBounds.builtin()
    assert b.persistence_ttl_seconds_min == 3600
    assert b.persistence_ttl_seconds_max == 7_776_000
    assert b.persistence_db_pool_size_min == 1
    assert b.persistence_db_pool_size_max == 32
    assert b.persistence_sweep_interval_seconds_min == 10
    assert b.persistence_sweep_interval_seconds_max == 3600


def test_apply_overrides_validates_persistence_sweep_interval() -> None:
    defaults = SessionConfig()
    bounds = ConfigBounds.builtin()

    out = apply_overrides(defaults, bounds, {"persistence_sweep_interval_seconds": 120})
    assert out.persistence_sweep_interval_seconds == 120

    with pytest.raises(ToolError) as exc:
        apply_overrides(defaults, bounds, {"persistence_sweep_interval_seconds": 5})
    assert exc.value.code is ErrorCode.INVALID_CONFIG

    with pytest.raises(ToolError) as exc:
        apply_overrides(defaults, bounds, {"persistence_sweep_interval_seconds": 4000})
    assert exc.value.code is ErrorCode.INVALID_CONFIG


def test_apply_overrides_validates_persistence_db_pool_size() -> None:
    defaults = SessionConfig()
    bounds = ConfigBounds.builtin()

    out = apply_overrides(defaults, bounds, {"persistence_db_pool_size": 8})
    assert out.persistence_db_pool_size == 8

    with pytest.raises(ToolError) as exc:
        apply_overrides(defaults, bounds, {"persistence_db_pool_size": 0})
    assert exc.value.code is ErrorCode.INVALID_CONFIG

    with pytest.raises(ToolError) as exc:
        apply_overrides(defaults, bounds, {"persistence_db_pool_size": 64})
    assert exc.value.code is ErrorCode.INVALID_CONFIG


def test_defaults_from_env_persistence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SW_PERSISTENCE_DB_PATH", "/var/lib/sessionweave/db.sqlite")
    monkeypatch.setenv("SW_PERSISTENCE_DB_POOL_SIZE", "12")
    monkeypatch.setenv("SW_PERSISTENCE_SWEEP_INTERVAL_SECONDS", "45")
    monkeypatch.setenv("SW_PERSISTENCE_UNTIL_DELETED_SAFETY_SECONDS", "2592000")  # 30 days
    cfg = load_deployment_defaults(os.environ).defaults
    assert cfg.persistence_db_path == "/var/lib/sessionweave/db.sqlite"
    assert cfg.persistence_db_pool_size == 12
    assert cfg.persistence_sweep_interval_seconds == 45
    assert cfg.persistence_until_deleted_safety_seconds == 2_592_000


def test_bounds_from_env_persistence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SW_MIN_PERSISTENCE_TTL_SECONDS", "600")
    monkeypatch.setenv("SW_MAX_PERSISTENCE_TTL_SECONDS", "1296000")  # 15 days
    b = load_deployment_defaults(os.environ).bounds
    assert b.persistence_ttl_seconds_min == 600
    assert b.persistence_ttl_seconds_max == 1_296_000
