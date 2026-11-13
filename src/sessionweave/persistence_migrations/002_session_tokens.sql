CREATE TABLE session_tokens (
    session_id    TEXT NOT NULL,
    token_id      TEXT NOT NULL,
    kind          TEXT NOT NULL CHECK (kind IN ('join', 'supervisor')),
    token_hash    BLOB NOT NULL,
    created_at    INTEGER NOT NULL,
    expires_at    INTEGER,
    revoked_at    INTEGER,
    PRIMARY KEY (session_id, token_id)
) STRICT, WITHOUT ROWID;
