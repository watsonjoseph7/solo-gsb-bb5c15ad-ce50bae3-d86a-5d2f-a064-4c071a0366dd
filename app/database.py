"""SQLite persistence: schema, migrations, connection lifecycle, transactions."""
from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone

from fastapi import Request

# v1: timeline merge + post-publish threshold alerting.
SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id                TEXT PRIMARY KEY,
    source_ids                TEXT NOT NULL,          -- JSON array, canonical order
    allowed_lateness_seconds  REAL NOT NULL,
    published_watermark       TEXT,                   -- NULL until first publish
    created_at                TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS session_sources (
    session_id       TEXT NOT NULL REFERENCES sessions(session_id),
    source_id        TEXT NOT NULL,
    max_observed_at  TEXT,                            -- NULL = no report yet
    first_seen_at    TEXT NOT NULL,
    PRIMARY KEY (session_id, source_id)
);

CREATE TABLE IF NOT EXISTS readings (
    session_id             TEXT NOT NULL REFERENCES sessions(session_id),
    source_id              TEXT NOT NULL,
    seq                    INTEGER NOT NULL,
    observed_at            TEXT NOT NULL,             -- normalised UTC, fixed format
    payload                TEXT,                      -- canonical JSON of value
    status                 TEXT NOT NULL CHECK (status IN ('buffered','published','quarantined')),
    first_received_at      TEXT NOT NULL,
    published_watermark    TEXT,                      -- set when published
    quarantined_watermark TEXT,                      -- set when quarantined
    PRIMARY KEY (session_id, source_id, seq)
);

-- Publish scan: buffered readings of a session ordered on the event timeline.
CREATE INDEX IF NOT EXISTS idx_readings_publish
    ON readings (session_id, status, observed_at, source_id, seq);

-- Isolation listing: quarantine queue in arrival order.
CREATE INDEX IF NOT EXISTS idx_readings_quarantine
    ON readings (session_id, status, first_received_at, source_id, seq);

-- ---------------------------------------------------------------------------
-- Post-publish threshold alerting
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS alert_rules (
    rule_id                    TEXT PRIMARY KEY,
    session_id                 TEXT NOT NULL REFERENCES sessions(session_id),
    source_id                  TEXT NOT NULL,
    direction                  TEXT NOT NULL CHECK (direction IN ('above','below')),
    enter_threshold            REAL NOT NULL,
    recover_threshold          REAL NOT NULL,
    enter_consecutive          INTEGER NOT NULL,
    recover_consecutive        INTEGER NOT NULL,
    value_key                  TEXT,                 -- NULL => the reading value itself
    created_at                 TEXT NOT NULL,
    -- Per-source state machine state ('normal' | 'active') plus the current
    -- streak length for the state's relevant predicate.
    state                      TEXT NOT NULL DEFAULT 'normal'
                                   CHECK (state IN ('normal','active')),
    streak                     INTEGER NOT NULL DEFAULT 0,
    active_since_observed_at   TEXT,                 -- set on enter, cleared on recovery
    active_since_seq           INTEGER,
    last_observed_at           TEXT,                 -- last published reading evaluated
    last_seq                   INTEGER
);

-- Look up the rules of a session grouped by source during a publish scan.
CREATE INDEX IF NOT EXISTS idx_alert_rules_lookup
    ON alert_rules (session_id, source_id);

CREATE TABLE IF NOT EXISTS alert_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,  -- stable chronological id
    rule_id         TEXT NOT NULL REFERENCES alert_rules(rule_id),
    session_id      TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    event_type      TEXT NOT NULL CHECK (event_type IN ('entered','recovered')),
    seq             INTEGER NOT NULL,
    observed_at     TEXT NOT NULL,            -- event time of the triggering reading
    value           REAL,                     -- numeric value that crossed the threshold
    threshold       REAL NOT NULL,            -- enter / recover threshold at trigger time
    triggered_at    TEXT NOT NULL,            -- wall-clock time the event was produced
    -- A rule cannot emit the same transition for the same reading twice.
    UNIQUE (rule_id, seq, event_type)
);

-- History listing per session, newest first, keyed on the monotonic event id.
CREATE INDEX IF NOT EXISTS idx_alert_events_session
    ON alert_events (session_id, id);
"""

# v2: cross-source composite (correlated) alerting.
SCHEMA_V2 = """
CREATE TABLE IF NOT EXISTS correlation_rules (
    rule_id          TEXT PRIMARY KEY,
    session_id       TEXT NOT NULL REFERENCES sessions(session_id),
    window_seconds   REAL NOT NULL CHECK (window_seconds > 0),
    conditions       TEXT NOT NULL,           -- JSON array, declaration order
    created_at       TEXT NOT NULL,
    -- 'waiting' = not all conditions aligned inside the window right now;
    -- 'correlated' = a correlated_entered event fired and nothing has fallen
    -- out of the window since.
    state            TEXT NOT NULL DEFAULT 'waiting'
                         CHECK (state IN ('waiting','correlated')),
    hits             TEXT NOT NULL DEFAULT '{}',  -- JSON: source_id -> latest hit
    last_observed_at TEXT,                  -- merged-stream position consumed so far
    last_source_id   TEXT,
    last_seq         INTEGER
);

-- All rules of a session are advanced together on each publish.
CREATE INDEX IF NOT EXISTS idx_correlation_rules_session
    ON correlation_rules (session_id);

CREATE TABLE IF NOT EXISTS correlation_events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,  -- stable chronological id
    rule_id        TEXT NOT NULL REFERENCES correlation_rules(rule_id),
    session_id     TEXT NOT NULL,
    event_type     TEXT NOT NULL CHECK (event_type IN ('correlated_entered')),
    window_seconds REAL NOT NULL,           -- rule window at trigger time
    window_start   TEXT NOT NULL,           -- earliest hit of the aligned set
    window_end     TEXT NOT NULL,           -- latest hit == the completing reading
    hits           TEXT NOT NULL,           -- JSON snapshot of the aligned hits
    triggered_at   TEXT NOT NULL,           -- wall-clock time the event was produced
    -- Leaving 'correlated' requires a strictly newer latest hit, so a rule can
    -- never legitimately enter twice with the same window_end; this makes
    -- "no duplicate events" a database-level guarantee as well.
    UNIQUE (rule_id, window_end)
);

-- History listing per session, newest first, keyed on the monotonic event id.
CREATE INDEX IF NOT EXISTS idx_correlation_events_session
    ON correlation_events (session_id, id);
"""

# Applied in order; ``PRAGMA user_version`` records how far a database got.
# Every migration is idempotent (IF NOT EXISTS) so a pre-migration database
# (user_version = 0, v1 tables already present) upgrades cleanly.
MIGRATIONS: tuple[tuple[int, str], ...] = (
    (1, SCHEMA_V1),
    (2, SCHEMA_V2),
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(db_path: str) -> None:
    """Create the database directory and apply pending migrations.

    Safe to call repeatedly: each migration is applied at most once, tracked
    via ``PRAGMA user_version``.
    """
    parent = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(parent, exist_ok=True)
    conn = connect(db_path)
    try:
        current = conn.execute("PRAGMA user_version").fetchone()[0]
        for version, sql in MIGRATIONS:
            if version <= current:
                continue
            conn.executescript(sql)
            conn.execute(f"PRAGMA user_version = {version}")
            conn.commit()
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Serialised write transaction (required for correct watermark updates).

    BEGIN IMMEDIATE takes a reserved lock up front so concurrent report
    batches are linearised instead of failing with SQLITE_BUSY mid-flight.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


@contextmanager
def read_transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Deferred (read) transaction: consistent snapshot without taking a lock."""
    conn.execute("BEGIN")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def get_db(request: Request) -> Iterator[sqlite3.Connection]:
    """FastAPI dependency: one connection per request."""
    conn = connect(request.app.state.db_path)
    try:
        yield conn
    finally:
        conn.close()
