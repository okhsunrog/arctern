-- Baseline schema. Every statement is idempotent so a database created by
-- a release that predates sqlx migrations (which already has these
-- tables) adopts the migration ledger without change.

CREATE TABLE IF NOT EXISTS job_runs (
    run_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name      TEXT NOT NULL,
    started_at    INTEGER NOT NULL,
    finished_at   INTEGER,
    status        TEXT NOT NULL,
    error_message TEXT,
    bytes_sent    INTEGER
);

CREATE TABLE IF NOT EXISTS log_events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    level     TEXT NOT NULL,
    job_name  TEXT,
    message   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_log_recent ON log_events(timestamp DESC);

CREATE TABLE IF NOT EXISTS push_syncs (
    job_name        TEXT NOT NULL,
    peer            TEXT NOT NULL,
    finished_at     INTEGER NOT NULL,
    status          TEXT NOT NULL,
    error           TEXT,
    last_success_at INTEGER,
    PRIMARY KEY (job_name, peer)
);

CREATE TABLE IF NOT EXISTS recv_transfers (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    completed_at  INTEGER NOT NULL,
    job           TEXT NOT NULL,
    identity      TEXT NOT NULL,
    dataset       TEXT NOT NULL,
    to_snapshot   TEXT NOT NULL,
    from_snapshot TEXT,
    bytes         INTEGER NOT NULL,
    duration_ms   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS arcstats_history (
    timestamp INTEGER PRIMARY KEY,
    size      INTEGER NOT NULL,
    c         INTEGER NOT NULL,
    hits      INTEGER NOT NULL,
    misses    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS browser_sessions (
    session_hash BLOB PRIMARY KEY NOT NULL,
    namespace    TEXT NOT NULL,
    expires_at   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_browser_sessions_expiry
    ON browser_sessions(namespace, expires_at);
