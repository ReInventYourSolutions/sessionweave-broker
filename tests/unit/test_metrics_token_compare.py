"""/metrics bearer is compared in constant time."""
from __future__ import annotations

import hmac

import pytest
from starlette.testclient import TestClient

from sessionweave.server import build_app


def test_metrics_token_uses_compare_digest(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SW_METRICS_TOKEN", "s3cret")
    calls: list[tuple[bytes, bytes]] = []
    real = hmac.compare_digest

    def spy(a: bytes, b: bytes) -> bool:
        calls.append((a, b))
        return real(a, b)

    monkeypatch.setattr(hmac, "compare_digest", spy)
    with TestClient(build_app()) as c:
        assert c.get("/metrics").status_code == 401
        assert c.get("/metrics", headers={"Authorization": "Bearer nope"}).status_code == 403
        assert c.get("/metrics", headers={"Authorization": "Bearer s3cret"}).status_code == 200
        assert c.get("/metrics", headers={"Authorization": "bearer  s3cret "}).status_code == 200
    assert len(calls) == 3


def test_metrics_accepts_admin_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """The relay admin token also reads /metrics (the Cockpit monitor sends it)."""
    monkeypatch.setenv("SW_METRICS_TOKEN", "s3cret")
    monkeypatch.setenv("SW_ADMIN_TOKEN", "admin-secret-0123456789")
    with TestClient(build_app()) as c:
        ok = c.get("/metrics", headers={"Authorization": "Bearer admin-secret-0123456789"})
        assert ok.status_code == 200
        assert c.get("/metrics", headers={"Authorization": "Bearer s3cret"}).status_code == 200
        assert c.get("/metrics", headers={"Authorization": "Bearer nope"}).status_code == 403
