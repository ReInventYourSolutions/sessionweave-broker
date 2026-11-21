ALTER TABLE account_tokens ADD COLUMN network_policy TEXT NOT NULL DEFAULT 'any';
ALTER TABLE account_tokens ADD COLUMN approved_networks TEXT NOT NULL DEFAULT '[]';
ALTER TABLE account_tokens ADD COLUMN last_used_at INTEGER;
ALTER TABLE account_tokens ADD COLUMN last_used_ip TEXT;

CREATE TABLE network_approvals (
    approval_id TEXT PRIMARY KEY,
    token_id    TEXT NOT NULL REFERENCES account_tokens(token_id) ON DELETE CASCADE,
    account_id  TEXT NOT NULL,
    ip          TEXT NOT NULL,
    client      TEXT,
    created_at  INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL
) STRICT;

CREATE INDEX idx_network_approvals_account ON network_approvals(account_id);
CREATE INDEX idx_network_approvals_token ON network_approvals(token_id, ip);
