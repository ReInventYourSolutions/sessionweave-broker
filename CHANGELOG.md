# Changelog

All notable changes to the SessionWeave broker (`sessionweave-broker`). Versions follow
[Semantic Versioning](https://semver.org/); releases are tagged `vX.Y.Z`.

## 0.35.1

- **Admin-token joins can become coordinator.** A participant that joins with the relay
  admin token now becomes the session's coordinator when the session has none, exactly
  like a join with the creator credential (creator token or owning account's token). A
  join never replaces an existing coordinator. Observers never take the role: the
  supervisor token cannot join, and the firehose, supervisor notes and Cockpit monitoring
  never join a session. Participant skill **0.25.3** says so.
- `sessionweave-accounts` defaults to `--url http://127.0.0.1:8765` (the broker's default
  port) instead of `:8080`.
- `--persistence-db-path` help no longer names a non-existent default
  (`SW_PERSISTENCE_DB_PATH` is unset by default: persistence disabled).
- `.env.example` lists the missing variables: `SW_SESSION_CREATION`,
  `SW_ACCOUNT_MAX_SESSIONS`, `SW_ACCOUNT_MAX_TOKENS`, `SW_ACCOUNT_EVENTS_RETENTION_DAYS`,
  `SW_MAX_REQUEST_BYTES`, `SW_MCP_NEW_SESSIONS_PER_MINUTE` and the launched-agent caps
  (`SW_DEFAULT_MAX_LAUNCHED_AGENTS`, `SW_MIN_/SW_MAX_MAX_LAUNCHED_AGENTS`); a test keeps it
  complete.

## 0.35.0 — first release from this repository

- **New home.** The broker now lives in its own public repository,
  `ReInventYourSolutions/sessionweave-broker`, split out of the SessionWeave monorepo
  with its history. Licensed under the **Apache License 2.0** (`LICENSE`, `NOTICE`).
- **Distribution renamed to `sessionweave-broker`** (was `sessionweave`), now published on
  PyPI: `pipx install sessionweave-broker`. The import package `sessionweave` and the
  commands (`sessionweave`, `sessionweave-join`, `sessionweave-host`,
  `sessionweave-poller`, `sessionweave-accounts`) are unchanged. Container image:
  `ghcr.io/reinventyoursolutions/sessionweave-broker`.
- **The Python SDK is no longer bundled.** The broker depends on
  `sessionweave-sdk>=0.13.0,<0.14` (PyPI; import `sessionweave_client`). `GET /sdk` and
  `GET /sdk.md` render the installed SDK's README and show
  `pip install sessionweave-sdk==<installed version>`. The legacy
  `GET /sdk/sessionweave-client.tar.gz` now answers `307` with the matching sdist on PyPI
  (`pip install <broker>/sdk/sessionweave-client.tar.gz` keeps working with PyPI access);
  an air-gapped broker no longer serves the SDK itself.
- Participant skill **0.25.2**: points at `sessionweave-sdk` and the new repositories.
- New standalone [broker guide](docs/guide/README.md).

Releases up to 0.34.1 were published from the SessionWeave monorepo (distribution
`sessionweave`, image `ghcr.io/reinventyoursolutions/sessionweave`).
