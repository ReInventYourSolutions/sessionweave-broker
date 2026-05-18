CREATE TABLE sessions (
    session_id              TEXT PRIMARY KEY,
    config_json             TEXT NOT NULL,
    purpose_json            TEXT,
    creator_token_hash      BLOB NOT NULL,
    persistence_mode        TEXT NOT NULL,
    persistence_ttl_seconds INTEGER,
    created_at              INTEGER NOT NULL,
    last_activity_at        INTEGER NOT NULL,
    ended_at                INTEGER,
    ended_reason            TEXT,
    relay_epoch_at_create   INTEGER NOT NULL
) STRICT;

CREATE INDEX idx_sessions_sweep ON sessions(persistence_mode, last_activity_at)
    WHERE ended_at IS NULL;

CREATE TABLE events (
    session_id   TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    kind         TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    ts           INTEGER NOT NULL,
    PRIMARY KEY (session_id, seq)
) STRICT, WITHOUT ROWID;

CREATE UNIQUE INDEX idx_events_command_id
    ON events(session_id, json_extract(payload_json, '$.command_id'))
    WHERE kind = 'command.received';

CREATE TABLE participants (
    session_id     TEXT NOT NULL,
    participant_id TEXT NOT NULL,
    snapshot_json  TEXT NOT NULL,
    last_seen_seq  INTEGER NOT NULL,
    joined_at      INTEGER NOT NULL,
    left_at        INTEGER,
    PRIMARY KEY (session_id, participant_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE inflight_commands (
    session_id   TEXT NOT NULL,
    command_id   TEXT NOT NULL,
    tracker_json TEXT NOT NULL,
    created_at   INTEGER NOT NULL,
    PRIMARY KEY (session_id, command_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE buffered_notifications (
    session_id     TEXT NOT NULL,
    participant_id TEXT NOT NULL,
    seq            INTEGER NOT NULL,
    PRIMARY KEY (session_id, participant_id, seq)
) STRICT, WITHOUT ROWID;
