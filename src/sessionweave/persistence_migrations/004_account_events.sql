CREATE TABLE account_events (
    event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id  TEXT NOT NULL,
    kind        TEXT NOT NULL,
    ts          INTEGER NOT NULL,
    data_json   TEXT NOT NULL
) STRICT;

CREATE INDEX idx_account_events_account ON account_events(account_id, event_id);
