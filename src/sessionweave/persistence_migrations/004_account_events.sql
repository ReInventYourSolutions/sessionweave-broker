CREATE TABLE account_events (
    event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id  TEXT NOT NULL,
    kind        TEXT NOT NULL,
    ts          INTEGER NOT NULL,
    data_json   TEXT NOT NULL
) STRICT;

CREATE INDEX idx_account_events_account ON account_events(account_id, event_id);
CREATE INDEX idx_account_events_ts ON account_events(ts);

-- Highest event_id pruned per account; lets readers tell "pruned" from "never existed".
CREATE TABLE account_event_prune (
    account_id     TEXT PRIMARY KEY,
    max_pruned_id  INTEGER NOT NULL
) STRICT;
