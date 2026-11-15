CREATE TABLE accounts (
    account_id  TEXT PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE COLLATE NOCASE,
    status      TEXT NOT NULL CHECK (status IN ('pending', 'active', 'disabled')),
    contact     TEXT,
    created_by  TEXT NOT NULL,
    created_at  INTEGER NOT NULL
) STRICT;

CREATE TABLE account_tokens (
    token_id    TEXT PRIMARY KEY,
    account_id  TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    token_hash  BLOB NOT NULL UNIQUE,
    label       TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    expires_at  INTEGER,
    revoked_at  INTEGER
) STRICT;

CREATE INDEX idx_account_tokens_account ON account_tokens(account_id);

ALTER TABLE sessions ADD COLUMN owner_account_id TEXT;
ALTER TABLE sessions ADD COLUMN suspended INTEGER NOT NULL DEFAULT 0;
CREATE INDEX idx_sessions_owner ON sessions(owner_account_id);
