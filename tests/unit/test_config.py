import os

import pytest

from sessionweave.config import (
    ConfigBounds,
    DeploymentDefaults,
    SessionConfig,
    apply_overrides,
    load_deployment_defaults,
)
from sessionweave.errors import ErrorCode, ToolError


def test_session_config_default_values() -> None:
    cfg = SessionConfig()
    assert cfg.max_participants == 16
    assert cfg.max_payload_bytes == 262144
    assert cfg.idle_ttl_seconds == 1800
    assert cfg.default_command_timeout_seconds == 60
    assert cfg.max_command_timeout_seconds == 300
    assert cfg.offline_grace_seconds == 300
    assert cfg.max_pending_notifications_per_participant == 1000


def test_apply_overrides_returns_new_config_with_changes() -> None:
    base = SessionConfig()
    bounds = ConfigBounds.builtin()
    out = apply_overrides(base, bounds, {"max_participants": 4})
    assert out.max_participants == 4
    assert out.idle_ttl_seconds == base.idle_ttl_seconds  # unchanged


def test_apply_overrides_rejects_below_min() -> None:
    bounds = ConfigBounds.builtin()
    with pytest.raises(ToolError) as exc:
        apply_overrides(SessionConfig(), bounds, {"max_participants": 1})
    assert exc.value.code is ErrorCode.INVALID_CONFIG


def test_apply_overrides_rejects_above_max() -> None:
    bounds = ConfigBounds.builtin()
    with pytest.raises(ToolError) as exc:
        apply_overrides(SessionConfig(), bounds, {"idle_ttl_seconds": 999_999})
    assert exc.value.code is ErrorCode.INVALID_CONFIG


def test_apply_overrides_rejects_default_command_timeout_above_max_in_same_request() -> None:
    bounds = ConfigBounds.builtin()
    with pytest.raises(ToolError):
        apply_overrides(
            SessionConfig(),
            bounds,
            {"default_command_timeout_seconds": 200, "max_command_timeout_seconds": 100},
        )


def test_apply_overrides_rejects_unknown_key() -> None:
    bounds = ConfigBounds.builtin()
    with pytest.raises(ToolError):
        apply_overrides(SessionConfig(), bounds, {"hax": True})


def test_apply_overrides_cross_field_default_timeout_breaks_when_only_max_lowered() -> None:
    """If a client lowers max_command_timeout_seconds below the default's default,
    the cross-field invariant must fire."""
    bounds = ConfigBounds.builtin()
    base = SessionConfig(default_command_timeout_seconds=60, max_command_timeout_seconds=300)
    with pytest.raises(ToolError) as exc:
        apply_overrides(base, bounds, {"max_command_timeout_seconds": 30})
    assert exc.value.code is ErrorCode.INVALID_CONFIG


def test_load_deployment_defaults_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SW_DEFAULT_MAX_PARTICIPANTS", "4")
    monkeypatch.setenv("SW_MAX_MAX_PARTICIPANTS", "8")
    dep: DeploymentDefaults = load_deployment_defaults(os.environ)
    assert dep.defaults.max_participants == 4
    assert dep.bounds.max_participants_max == 8
