# Releasing the broker

Standard release: bump `version` in `pyproject.toml`, tag `vX.Y.Z`, push — `release.yml`
runs the checks, then publishes `sessionweave-broker` to PyPI (trusted publishing,
environment `pypi`), pushes `ghcr.io/reinventyoursolutions/sessionweave-broker:vX.Y.Z` and
`:latest`, and creates the GitHub Release with the wheel and sdist. When the release
changes the wire protocol, also run the cross-module e2e
(`ReInventYourSolutions/sessionweave-e2e`, `repository_dispatch` event `module-release`,
which the release workflow sends when the `E2E_DISPATCH_TOKEN` secret is set).

## Python SDK dependency

The broker depends on `sessionweave-sdk` (`pyproject.toml`, range `>=0.13.0,<0.14`); `/sdk`
renders the installed SDK's README. Until the SDK is on PyPI, CI and the Dockerfile
install it from `ci/sdk-prerelease.txt` (a direct reference pinned to a full SDK commit
SHA, installed in the same pip command). A new SDK minor needs the range bumped here.

**Release guard:** `release.yml` runs `ci/check_sdk_on_pypi.py` first and fails unless PyPI
lists a `sessionweave-sdk` release that satisfies the pin and whose files carry PEP 740
provenance from the `ReInventYourSolutions/sessionweave-sdk` trusted publisher (`release.yml`):
users install this package from PyPI and pip resolves the SDK by name, so the broker must
never be released before the SDK is published (dependency confusion).

## Runner-facing surface checklist (do this on EVERY release)

The standalone `sessionweave-runner` package (repository `ReInventYourSolutions/sessionweave-runner`)
tests against an **in-process fake relay** pinned to a relay version
(`tests/_fakerelay.py` → `REPRODUCES_RELAY_VERSION`, contract in
`tests/FAKE_RELAY_FIDELITY.md`).

**If this release changes any of the runner-facing surface** — i.e. the endpoints / WS frame
shape / event names / bearer model listed in `FAKE_RELAY_FIDELITY.md`:
- `GET /v1/sessions/{sid}/participants`
- `DELETE /v1/sessions/{sid}/participants/me`
- WS `/v1/sessions/{sid}/participants/me/events` frame shape `{"name","payload"}`
- wake event names: `supervisor.note`, `command.received`, `result.received`,
  `command.completed`, `command.cancelled`, `session.ended`
- supervision: `supervisor.note` kinds `note|halt|continue|redirect` (payload
  `{note_id, kind, target, message, ts_ms}`), the new event `supervisor.note_reply`, the
  `participant_paused` 409, the events-WS close code **4410** (reason `removed`) and the 403
  `participant_removed` error
- the bearer model (join needs a join/creator token; `participant_token` is a secret distinct from `participant_id` and authenticates every participant route)

then, in the Runner repository: update `tests/_fakerelay.py` to match and bump
`REPRODUCES_RELAY_VERSION` to this release's version.

If the release does NOT touch that surface, no runner change is needed.

The GET /invite/{sid} brief endpoint is NOT part of the runner-facing surface in
the Runner's tests/FAKE_RELAY_FIDELITY.md, so adding/changing it needs no fake-relay update.
