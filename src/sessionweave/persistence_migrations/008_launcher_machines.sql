-- Machines: the descriptor of each v2 launcher (stable launcher_key), per
-- owner account; account_id '' is the open-mode owner key. key_secret_hash is
-- the sha256 of the secret issued on the key's first registration: a later
-- connection with the key must present that secret. token_id: the account
-- token that first registered the key (account mode).
CREATE TABLE launcher_machines (
    account_id         TEXT NOT NULL,
    launcher_key       TEXT NOT NULL,
    machine            TEXT NOT NULL,
    mode               TEXT NOT NULL,
    ceiling            TEXT,
    capabilities_json  TEXT NOT NULL,
    sandbox            TEXT,
    run_as             INTEGER NOT NULL,
    platform           TEXT NOT NULL,
    prereqs_json       TEXT NOT NULL DEFAULT '[]',
    first_seen         INTEGER NOT NULL,
    last_seen          INTEGER NOT NULL,
    key_secret_hash    BLOB,
    token_id           TEXT,
    PRIMARY KEY (account_id, launcher_key)
) STRICT;

-- Deployed agent definitions (desired state, revisioned) per launcher.
CREATE TABLE agent_definitions (
    account_id        TEXT NOT NULL,
    launcher_key      TEXT NOT NULL,
    def_id            TEXT NOT NULL,
    revision          INTEGER NOT NULL,
    definition_json   TEXT NOT NULL,
    state             TEXT NOT NULL,
    applied_revision  INTEGER,
    problems_json     TEXT NOT NULL DEFAULT '[]',
    warnings_json     TEXT NOT NULL DEFAULT '[]',
    deleted           INTEGER NOT NULL DEFAULT 0,
    updated_at        INTEGER NOT NULL,
    PRIMARY KEY (account_id, launcher_key, def_id)
) STRICT;
