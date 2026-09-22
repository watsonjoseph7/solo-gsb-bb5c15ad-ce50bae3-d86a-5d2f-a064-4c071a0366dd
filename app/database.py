"""SQLite persistence: schema, connection lifecycle and transactions."""
from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone

from fastapi import Request

SCHEMA = """
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
    """Create the database directory and schema. Safe to call repeatedly."""
    parent = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(parent, exist_ok=True)
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
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
