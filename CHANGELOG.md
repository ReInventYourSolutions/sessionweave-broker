# Changelog

All notable changes to the SessionWeave broker (`sessionweave-broker`). Versions follow
[Semantic Versioning](https://semver.org/); releases are tagged `vX.Y.Z`.

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
