# Releasing the relay

Standard release: bump `version` in `pyproject.toml`, tag `vX.Y.Z`, push — `release.yml`
builds wheels + the container and creates a GitHub Release.

## Runner-facing surface checklist (do this on EVERY release)

The standalone `sessionweave-runner` package (companion repo `EnzinoBB/SessionWeave`, `Runner/`)
tests against an **in-process fake relay** pinned to a relay version
(`Runner/tests/_fakerelay.py` → `REPRODUCES_RELAY_VERSION`, contract in
`Runner/tests/FAKE_RELAY_FIDELITY.md`).

**If this release changes any of the runner-facing surface** — i.e. the endpoints / WS frame
shape / event names / bearer model listed in `FAKE_RELAY_FIDELITY.md`:
- `GET /v1/sessions/{sid}/participants`
- `DELETE /v1/sessions/{sid}/participants/me`
- WS `/v1/sessions/{sid}/participants/me/events` frame shape `{"name","payload"}`
- wake event names: `supervisor.note`, `command.received`, `result.received`,
  `command.completed`, `command.cancelled`, `session.ended`
- the bearer model (`participant_token == participant_id`)

then, in the companion repo: update `Runner/tests/_fakerelay.py` to match and bump
`REPRODUCES_RELAY_VERSION` to this release's version.

If the release does NOT touch that surface, no runner change is needed.

The GET /invite/{sid} brief endpoint is NOT part of the runner-facing surface in
Runner/tests/FAKE_RELAY_FIDELITY.md, so adding/changing it needs no fake-relay update.
