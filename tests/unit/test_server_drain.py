from __future__ import annotations

from starlette.testclient import TestClient

from sessionweave.server import build_app


def test_healthz_returns_503_when_draining() -> None:
    app = build_app()
    with TestClient(app) as client:
        resp = client.get("/healthz")
        assert resp.status_code == 200
        app.state.state["draining"] = True
        resp = client.get("/healthz")
        assert resp.status_code == 503
        assert resp.json()["status"] == "draining"


def test_readyz_returns_200_independent_of_drain() -> None:
    app = build_app()
    with TestClient(app) as client:
        app.state.state["draining"] = True
        resp = client.get("/readyz")
        assert resp.status_code == 200


def test_metrics_endpoint_renders_text() -> None:
    app = build_app()
    with TestClient(app) as client:
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/plain")
        assert "sessionweave_sessions_active" in resp.text
