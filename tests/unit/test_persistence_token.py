
import pytest

from sessionweave.persistence_token import (
    DeleteRateLimiter,
    generate_creator_token,
    hash_creator_token,
    verify_creator_token,
)


def test_generate_creator_token_high_entropy() -> None:
    tokens = {generate_creator_token() for _ in range(1000)}
    assert len(tokens) == 1000  # no collisions
    for t in tokens:
        # token_urlsafe(32) ~ 43 chars
        assert 40 <= len(t) <= 50
        assert all(c.isalnum() or c in "-_" for c in t)


def test_hash_creator_token_deterministic() -> None:
    token = "test-token-abc"
    h1 = hash_creator_token(token)
    h2 = hash_creator_token(token)
    assert h1 == h2
    assert len(h1) == 32  # sha256 raw digest


def test_hash_creator_token_different_for_different_inputs() -> None:
    assert hash_creator_token("a") != hash_creator_token("b")


def test_verify_creator_token_match() -> None:
    token = generate_creator_token()
    h = hash_creator_token(token)
    assert verify_creator_token(token, h) is True


def test_verify_creator_token_mismatch() -> None:
    token = generate_creator_token()
    h = hash_creator_token(token)
    assert verify_creator_token("not-the-token", h) is False
    assert verify_creator_token("", h) is False


def test_rate_limiter_no_failures_not_banned() -> None:
    rl = DeleteRateLimiter(max_failures=5, window_seconds=60.0)
    assert rl.is_banned("s_1") is False


def test_rate_limiter_under_threshold_not_banned() -> None:
    rl = DeleteRateLimiter(max_failures=5, window_seconds=60.0)
    for _ in range(4):
        rl.record_failure("s_1")
    assert rl.is_banned("s_1") is False


def test_rate_limiter_at_threshold_banned() -> None:
    rl = DeleteRateLimiter(max_failures=5, window_seconds=60.0)
    for _ in range(5):
        rl.record_failure("s_1")
    assert rl.is_banned("s_1") is True


def test_rate_limiter_other_sid_unaffected() -> None:
    rl = DeleteRateLimiter(max_failures=5, window_seconds=60.0)
    for _ in range(5):
        rl.record_failure("s_1")
    assert rl.is_banned("s_1") is True
    assert rl.is_banned("s_2") is False


def test_rate_limiter_window_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_now = [1000.0]

    def fake_monotonic() -> float:
        return fake_now[0]

    monkeypatch.setattr("sessionweave.persistence_token.time.monotonic", fake_monotonic)
    rl = DeleteRateLimiter(max_failures=5, window_seconds=60.0)
    for _ in range(5):
        rl.record_failure("s_1")
    assert rl.is_banned("s_1") is True

    fake_now[0] += 61.0
    assert rl.is_banned("s_1") is False


def test_rate_limiter_reset_on_success() -> None:
    rl = DeleteRateLimiter(max_failures=5, window_seconds=60.0)
    for _ in range(4):
        rl.record_failure("s_1")
    rl.reset("s_1")
    assert rl.is_banned("s_1") is False
    for _ in range(4):
        rl.record_failure("s_1")
    assert rl.is_banned("s_1") is False  # counter was reset
