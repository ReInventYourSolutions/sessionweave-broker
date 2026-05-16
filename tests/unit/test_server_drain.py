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


def test_openapi_yaml_route_serves_spec() -> None:
    app = build_app()
    with TestClient(app) as client:
        resp = client.get("/openapi.yaml")
        assert resp.status_code == 200
        assert "yaml" in resp.headers["content-type"]
        text = resp.text
        # Sanity-check it really is the OpenAPI doc with the current
        # surface, not a stale or empty file.
        assert "openapi:" in text
        assert "/v1/sessions" in text
        assert "/v1/sessions/{sid}/participants/me/inbox/wait" in text
