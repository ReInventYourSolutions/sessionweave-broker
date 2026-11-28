# Changelog

All notable changes to the SessionWeave broker (`sessionweave-broker`). Versions follow
[Semantic Versioning](https://semver.org/); releases are tagged `vX.Y.Z`.

## [Unreleased]

## [0.37.0]

Account self-service and MCP parity: users mint, list, replace and revoke their own
tokens from the CLI and as MCP tools, the admin does the same for any account, and every
machines/definitions route of 0.36.0 is an MCP tool. Each tool runs the same code as its
REST route (validation, authorisation, throttling of wrong credentials, account events).

- **CLI user mode.** `sessionweave-accounts --token` (env `SW_ACCOUNT_TOKEN`) adds `me`,
  `my-tokens`, `my-token-create`, `my-token-revoke`, `my-token-networks`,
  `my-approvals`, `my-approve`, `my-deny`. Admin mode adds `tokens ACCOUNT_ID`. Both
  create commands take `--revoke TOKEN_ID` to replace a token (checked before minting;
  the new token is printed even if the revoke fails). Giving the wrong kind of
  credential is a clear error that never echoes it.
- **Replace a token over REST.** `POST /v1/me/tokens` and
  `POST /v1/accounts/{id}/tokens` take an optional `revoke_token_id`: the token is
  revoked once the new one exists (an unknown id is 404 `token_not_found` and nothing is
  minted); the answer carries `revoked_token_id`, or `revoke_error` if that step failed.
- **MCP tools: your account** (account token as `account_token` or an `swa_` Bearer
  header): `account_me`, `account_tokens`, `account_token_create`,
  `account_token_revoke`, `account_token_networks`, `account_approvals`,
  `account_approve`, `account_deny`.
- **MCP tools: administration** (admin token as `admin_token` or the Bearer header;
  wrong values count against the REST admin limit): `admin_accounts`, `admin_account`,
  `admin_token_create`, `admin_token_revoke`, `admin_token_networks`,
  `admin_approvals`, `admin_approve`, `admin_deny`.
- **MCP tools: machines and definitions**: `machines_list`, `machine_get`,
  `definition_deploy`, `definition_remove`, `machine_forget`, `machine_browse`,
  `machine_processes`. With an account token: your own machines; without one on an
  open-mode broker: every machine; without one in account mode: `permission_denied`
  (as `/v1/launchers`).
- **Logs.** `sse_starlette`'s DEBUG log of every SSE chunk (whole MCP answers, one-time
  secrets included) is kept above DEBUG.
- **Docs.** Guide: "Broker from the command line" (§16) and an "MCP tools reference"
  (§17) generated from the registered tools (a test keeps it current). Participant skill
  **0.26.0**: account tokens (§3.3), machines and definitions (§4.8), every tool in §6.1.

## [0.36.0]

Machines and remote deploy: the Cockpit (0.22.0) defines agents and deploys them through
the broker to launchers started with `--local <folder>` or `--root` (Runner 0.9.0). The
broker stores agent definitions and machine descriptors only; remote browse and process
lists are relayed, never stored.

- **Definition checks match the launcher's.** A deploy is now refused up front (400
  `invalid_request`) when a command entry or approval pattern starts with an option
  (`-`), or when `observer_as_root` is combined with a `run_as` user.
  Root execution (`run_as`, `privileged` commands, `observer_as_root`) needs a `--root`
  launcher that offers `run_as` (runs as root); otherwise 400 `outside_ceiling`.
  `privileged` entries are stored as written: the launcher runs only the options an
  entry spells, so list each option used (`journalctl -u`, `journalctl -n`).
- **Listed approvals are refused until approvals ship** (the launcher's rule, refused up
  front with 400 `invalid_request`): approval `listed` with patterns is refused for every
  agent, whatever its kind, runtime and commands mode, with "listed approvals aren't
  available yet: use approval 'auto' or 'always'" (`LISTED_NOT_YET`). The command list
  is guidance, so it can't keep a listed operation from running. `listed` without
  patterns asks nothing and is accepted, like `auto`. The bad-pattern checks (a pattern
  starting with an option) come first.
- **Access model v2** (owner decision; the Runner's rules, mirrored exactly): an agent's
  limits are its files (read-only or writable paths) and its network, enforced by the
  launcher's sandbox (`--local`, and observers on `--root` launchers with bubblewrap)
  and the `run_as` user on `--root` launchers. **The command list is guidance, not a
  security limit.**
  - Observers (read-only) default to commands `shell` and may use any mode and any
    command (the "read commands only" rule is gone). An empty `list` (no allow and no
    privileged entry) means no commands everywhere: it needs no sandbox and allows a host
    list.
    Without the sandbox a read-only agent with commands is refused (400, the Runner's
    `READ_ONLY_NEEDS_SANDBOX`: install bubblewrap, or set
    commands to `none`); on a `--root` launcher without bubblewrap only commands
    `none` remains. An agent that cannot write but runs commands gets the deploy warning
    "read-only covers files only: commands can reach the network and call the services
    there (including on this machine)": network `none` turns the web tools off but does
    not isolate commands. **`observer_as_root` needs commands `none`** (or an empty list;
    400, the Runner's `OBSERVER_AS_ROOT_NEEDS_NO_COMMANDS`) until network isolation
    ships: as root its commands could reach every socket and service on the machine; its
    file tools still read everything.
  - Operators (writers) may list any command, command runners and `git` included. A
    deploy in list mode carries the note `LIST_IS_GUIDANCE`; an operator that runs
    commands without the sandbox (a `--root` operator) carries
    `UNSANDBOXED_WRITER_WARNING`: "this agent runs without the OS sandbox: it can do
    anything the OS user {user} can on this machine", naming its `run_as` user, else
    "of its launcher" (the launcher's own status names its actual user).
    The deploy answer has these in `warnings`, and the stored definition's deploy
    warnings show them until the launcher reports its own (which add, for a `--root`
    observer, that denied files are hidden only under its workdir and observed paths:
    the launcher computes that from the real paths).
  - Kept as real boundaries: `privileged` entries (run as root by `swv-root` outside the
    sandbox) never start with a command runner (the launcher's `COMMAND_WRAPPERS`, 114
    entries: shells, `env`, `xargs`, `sudo`, interpreters, shell builtins such as
    `eval`/`source`, `find`, `awk`, `sed`, `ssh`, `git`, ...; compared by basename with a
    version suffix stripped, so `perl5.36` is `perl`), refused with
    `PRIVILEGED_RUNNER_PROBLEM`; protected write globs; the network rules. The broker no longer
    refuses a claude command list outside the sandbox.
- **The launcher's summary texts are exported** by `sessionweave.definitions`, word for
  word, for the Cockpit's preview (the broker does not build the summary):
  `DENY_SUMMARY_SANDBOXED`, `DENY_SUMMARY_ROOT_OBSERVER` and `DENY_ROOT_OBSERVER_WARNING`
  (denied files are hidden by path only: copies under other names, hard links and git
  history stay readable), `DENY_SUMMARY_UNSANDBOXED`, `READ_SUMMARY_LOCAL`,
  `READ_SUMMARY_ROOT_OBSERVER`, `READ_SUMMARY_UNSANDBOXED`, `WRITE_SUMMARY_WHOLE_FOLDER`,
  `WRITE_SUMMARY_UNSANDBOXED`, `NETWORK_SUMMARY_NONE_WITH_COMMANDS` and `LISTED_NOT_YET`.
- **Write globs never name a protected file** (the launcher's
  `protected_write_problem`): every `/` part of a write glob is compared with the
  runtimes' configuration and instruction files and `.git` (`.claude`, `.mcp.json`,
  `opencode.json`, `.opencode`, `.clinerules`, `.cline`, `.agents`, `.cursorrules`,
  `.windsurfrules`, `CLAUDE.md`, `CLAUDE.local.md`, `AGENTS.md`, `.git`): a literal part
  equal to one (`.git`, `src/.git/hooks`, `**/AGENTS.md`) or a wildcard part matching
  one (`.gi?`, `AGENTS.*`, `*.md`, `*.json`) is refused, also with approval `always`.
  `.`, `**`, `src/**`, `*.py`, `.github/**`, `.gitignore` stay allowed: under them the
  launcher's sandbox keeps those files read-only. Name directories instead (`src`,
  `docs/**`) or keep the default `.`.
- **Every launcher rule that does not need the machine is applied at deploy** (400, so
  a definition the launcher would always reject is never stored and never sits
  `queued` until a reconnect): a network host list (also `[]`) needs commands mode
  `none` (or approval `always`); hosts are lowercase names, `*.name` (not on a bare
  TLD) or `name:port`, no IPv6 literals; file globs have no `~` anywhere and no leading
  or trailing spaces; `observe.sources`/`logs` hold no glob characters (`* ? [ ] { }`);
  `model` starts with a letter or digit (never read as a flag) and a cline
  `provider:model` has a lowercase provider; `run_as` is never `root` or the launcher's
  install user `sessionweave`; an observer on a `--root` launcher running as root needs
  `run_as` or `observer_as_root`; `observer_as_root` needs the machine's bubblewrap
  sandbox (`outside_ceiling`) and is refused for cline. What each runtime can enforce
  (the launcher's `RUNTIME_CAPS`): claude refuses a `files.read` list other than `.`,
  `host:port` entries, and paths with whitespace, `,` or parentheses; opencode refuses
  a single `*`
  in read/write globs and `*.name` hosts; cline refuses host lists. `privileged`
  entries under approval `always` (which leaves no commands) no longer need a launcher
  running as root. Randomized comparisons against the Runner's own validator and
  runtime checks find no difference.
- **Ids, domains and roles are never only dots** (`.`, `..`): HTTP clients resolve such
  a definition URL to its parent, so a Remove of `..` became forget-machine. Published
  profiles with such ids are rejected too.
- **Published-profiles budget** (409 `definitions_budget`, new error code): a launcher
  publishes at most 192 KiB of profiles (the Runner's `PUBLISHED_PROFILES_BUDGET`, under
  the 256 KiB frame cap; before, about 9 full-size definitions made its hello too large
  and the machine could never reconnect). A deploy is refused when the machine's
  `profiles` frame would exceed it, counted as the launcher serializes it: its
  published profiles at the size it sent them (TOML profiles, definitions applied at
  their stored revision, warnings included), every other stored definition and the new
  one as the launcher would publish them with their last reported warnings. Offline,
  TOML profiles are unknown and a new definition's warnings are not known before it is
  applied: the launcher still checks the budget itself and rejects what does not fit.
- **A welcome that cannot be sent no longer locks the launcher key**: the fresh key
  secret it carried is released (the machine and its definitions stay), so the next
  hello claims the key again instead of being refused 4409 `launcher_key_in_use`.
- **Removals are not left waiting on a launcher in config mode**: a launcher connected
  without the `definitions` capability holds no definitions, so its pending removals
  are done when it connects, and a Remove while it is connected completes at once.
- **Repeated launcher statuses are recorded once**: the same status for the same
  revision writes nothing and emits no second `definition.applied`/`definition.rejected`
  account event. A machine keeps the revision high-water marks of its 1000 most
  recently purged definition ids (they were unbounded): an older id deployed again
  starts at revision 1.
- **Known limits** (documented in the guide): a Forget that races a launcher's
  reconnect in the same instant can delete the row it just claimed (it answers 404
  until it reconnects); a definition rejected for a machine reason (runtime missing,
  not logged in, workdir missing) is not retried: deploy it again once fixed; every
  account token can deploy to every machine of its account, root-capable definitions
  included on `--root` launchers (owner decision: keep account tokens away from agents
  and machines you do not trust); after a broker database restore to an older state,
  the launcher ignores puts with a revision lower than the one it holds: deploy the
  definition again (or remove and redeploy) to converge.
- OpenAPI: `AgentDefinition` lists every rule above; `observe`/`access` are documented
  as refusing unknown keys; `observer_as_root` refusal codes are exact; hello
  prerequisites include `path`; `Machine.platform` is a string (not nullable).
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
