# Changelog

All notable changes to the SessionWeave broker (`sessionweave-broker`). Versions follow
[Semantic Versioning](https://semver.org/); releases are tagged `vX.Y.Z`.

## [Unreleased]

## [0.36.0]

Machines and remote deploy: the Cockpit (0.22.0) defines agents and deploys them through
the broker to launchers started with `--local <folder>` or `--root` (Runner 0.9.0). The
broker stores agent definitions and machine descriptors only; remote browse and process
lists are relayed, never stored.

- **Definition checks match the launcher's.** A deploy is now refused up front (400
  `invalid_request`) when a command entry or approval pattern starts with an option
  (`-`), when an observer's `allow` entry is not a read command (it must start with one
  of `ls`, `cat`, `head`, `tail`, `grep`, `journalctl`, `ps`, `top -b`,
  `systemctl status`, ...), or when `observer_as_root` is combined with a `run_as` user.
  Root execution (`run_as`, `privileged` commands, `observer_as_root`) needs a `--root`
  launcher that offers `run_as` (runs as root); otherwise 400 `outside_ceiling`.
  `privileged` entries are stored as written: the launcher runs only the options an
  entry spells, so list each option used (`journalctl -u`, `journalctl -n`).
- **Listed approvals are checked against the allowed commands** (the launcher's rule,
  refused up front with 400 `invalid_request`): with approval `listed`, an `allow` or
  `privileged` entry that shares a pattern's command word must differ from it at a
  subcommand word, with no option word before that point. Allow `git status`, `git log`
  with the pattern `git push` is accepted; allow `git` (it would run `git push` unasked),
  `git push origin` (it would always ask) or `git -C x log` with `git -C x push` are
  refused. An observer's empty allow list counts as the default read commands.
  Listed patterns are also refused with commands mode `shell`, and with any `allow` or
  `privileged` entry that runs other commands (shells, `env`, `xargs`, `sudo`,
  interpreters such as `python*`, `find`, `awk`, `sed`, `ssh`, ...; compared by
  basename), which could run a listed command unchecked.
- **Upper bounds on definition limits** (the launcher's TOML profiles use the same):
  `max_duration_s` and `max_idle_s` at most 2592000 (30 days), `max_cost_usd` at most
  10000.
- **Machines show more per agent:** `observer_as_root` (bool) and `run_as` (user or
  null) of the deployed definition, and the `warnings` the launcher published with the
  profile (at most 20 printable strings of at most 300 characters; the `profiles` frame
  and hello profiles may carry them). Prerequisites keep the launcher's `path` (where it
  found the tool; printable, at most 300 characters, or null).
- **A launcher whose account token is revoked while its machine is being claimed is
  refused** (4401) instead of registering on the revoked token; the token is checked
  again (revoked, expired, account not active, network no longer allowed) right before
  the launcher is registered.
- **SDK range widened** to `sessionweave-sdk>=0.13.0,<0.15` (0.14 adds the launcher
  methods; nothing the broker uses changed).

- **`observer_as_root` in agent definitions** (bool, default `false`). Accepted as `true`
  only for an observer deployed to a `--root` launcher (else 400 `invalid_request`): the
  observer runs as root. Warning: runs as root: can read every file on the machine,
  including passwords and keys (it stays read-only).
- **Remote browse and process list.** `GET /v1/me/launchers/{key}/browse?path=&cursor=&limit=&hidden=`
  and `GET /v1/me/launchers/{key}/processes?q=&cursor=&limit=` (owner's account token only;
  open mode with a persistence DB: `/v1/launchers/{key}/…` without a credential) relay a
  `browse` / `processes` request over the launcher socket and return the launcher's
  `browse_result` / `processes_result` after checking it (entries at most the requested
  limit, names of 1–255 characters, not `.` or `..`, without `/` or NUL, known kinds; process
  command lines at most 400 characters; `limit` digits only). `path` defaults to
  the machine's ceiling; `limit` defaults to 100 and is capped at 300 (browse) and 200
  (processes). The launcher's refusals map to `outside_ceiling` (400), `path_not_found`
  (404), `invalid_request` (400) and `permission_denied` (403). New error codes
  `launcher_offline` (409), `launcher_timeout` (504), `launcher_bad_reply` (502) and
  `path_not_found` (404); at most 8 requests in flight per launcher (429 `rate_limited`).
  A request waiting on a launcher that disconnects (including one closed 1009 for an
  oversized answer) fails with `launcher_offline`. New setting
  `SW_LAUNCHER_RELAY_TIMEOUT_S` (default 10 s, 0.1..60). Browse and process requests are
  logged at INFO without paths and are not account events.
- OpenAPI documents the `/v1/me/launchers…` and `/v1/launchers…` routes (new tag
  `machines`; schemas `Machine`, `MachineDetail`, `AgentDefinition`, `DeployState`,
  `BrowseResult`, `ProcessesResult`, …), the relayed frames, the `definition.*` account
  events and the new `/v1/info` capabilities. The guide gains §10.3 (launcher modes),
  §10.4 (machines and remote definitions, open-mode LAN trust, lost key secrets) and §10.5
  (remote browse), and §12.4 (error codes); the README a "Machines and remote deploy"
  section.
- **Launcher protocol v2.** A launcher's `hello` may carry `"protocol": 2`, a stable
  `launcher_key` (`lk_` + 32 hex), its start `mode` (`config`, `local`, `root`) and
  `ceiling`, the remote `capabilities` it offers (`definitions`, `browse`, `processes`;
  kept in account mode, and in open mode only with a persistence DB), `sandbox`
  (`bwrap` or null), `run_as`, `platform` and `prereqs`. `welcome` carries
  `"protocol": 2`. v1 launchers are unaffected.
- **Launcher keys need proof of possession.** The first registration of a key gets a
  `key_secret` in `welcome` (only its hash is stored); later hellos with that key must
  present it, else the connection is closed 4409 `launcher_key_in_use` and nothing is
  replaced or updated. With it, a newer connection replaces the old one (4409
  `replaced by a newer connection`). A launcher is registered (discoverable, launchable)
  only after its key is claimed.
- **Machines.** With a persistence DB, every v2 launcher's machine descriptor is kept in
  the new `launcher_machines` table (migration 008, which also adds `agent_definitions`),
  at most 20 per account and 100 in open mode (beyond: close 4429 `too many machines;
  forget one in the Cockpit`). Open-mode machines offline for more than
  `SW_OPEN_MACHINES_RETENTION_DAYS` (default 30) are forgotten. Deleting an account
  deletes its machines and definitions. New error code `launcher_not_found` (404).
- Launchers may re-publish their profiles (`{"type":"profiles"}`) and prerequisite
  report (`{"type":"prereqs"}`); prereq states include `not_logged_in`. Profiles may carry
  `source` (`local`/`remote`), `kind` (`observer`/`operator`) and `coordinator_capable`,
  returned by discovery.
- **Agent definitions (schema and store).** `sessionweave.definitions.validate_definition`
  checks an agent definition (unknown keys refused, at most 64 KiB, instructions at most
  16 KiB) against the target machine: paths absolute, normalised and inside its ceiling,
  `run_as` only on a `--root` launcher that allows it, observers never write files nor get
  a shell, command entries are plain command prefixes (no shell syntax). Deployed
  definitions are stored revisioned per machine with the launcher's last status. New
  error codes `definition_not_found` (404) and `outside_ceiling` (400).
- **Deploying agent definitions.** `GET /v1/me/launchers` lists the caller account's
  machines (online or not; keyless v1 launchers with `launcher_key: null`) with their
  mode, ceiling, sandbox, prerequisites and agents (`status` `idle`, `in_session`,
  `offline` or `not_deployed`, plus the `deploy` state). `GET /v1/me/launchers/{key}` adds
  the stored definitions. `PUT /v1/me/launchers/{key}/definitions/{id}` validates and
  stores a definition and sends it to the launcher (`state: sent`) or queues it while the
  machine is offline (`queued`); `DELETE …/definitions/{id}` asks the launcher to remove
  it (refused 409 `definition_in_use` while the agent runs in a session).
  `DELETE /v1/me/launchers/{key}` forgets an offline machine with its definitions and its
  key secret (the reset for a launcher that lost its `key_secret`). The relay admin token
  cannot use these routes. Over the launcher socket the broker sends `definitions_index`
  right after `welcome`, then `definition_delete` for unconfirmed removals and
  `definition_put` for every definition not applied at its revision; the launcher answers
  `definition_status` (`applied`, `pending_in_use`, `rejected`, `removed`). Account events
  `definition.deployed`, `definition.applied`, `definition.rejected`,
  `definition.removed`. New error codes `definitions_unavailable` (503),
  `launcher_capability_missing`, `definition_in_use`, `definition_id_conflict`,
  `definition_limit` and `launcher_online` (409).
- **Open-mode deploy (LAN-only trust).** On an open-mode broker with a persistence DB the
  same routes exist under `/v1/launchers` without any credential: any client that reaches
  the broker may list every machine, deploy to launchers started with `--local` or
  `--root`, and forget an offline machine (freeing its launcher key). In account mode they
  answer 403; without a persistence DB the write routes answer 503 and the list shows live
  launchers only. `GET /v1/info` → `capabilities.agent_definitions`, `open_deploy` and
  `launcher_protocol: 2`.
- **Lost key secrets.** A launcher that never stored its `key_secret` (for example a lost
  `welcome`) is refused 4409 `launcher_key_in_use` until its machine is forgotten
  (`DELETE …/launchers/{key}`, offline only): the messages say "forget the machine in the
  Cockpit". In open mode any LAN client can forget an offline machine and reclaim its key.
- A launcher whose definitions could not be sent after `welcome` is closed 1011
  (`definitions sync failed; reconnect`) so it converges when it reconnects.

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
