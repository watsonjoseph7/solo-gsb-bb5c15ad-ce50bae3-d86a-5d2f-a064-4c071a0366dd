"""Cross-source composite (correlated) alert rules.

A correlation rule binds 2-4 conditions, each on a **distinct** source of the
session with its own direction/threshold and optional ``value_key``.  The
rule consumes the session's merged published stream in deterministic order
``(observed_at, source_id, seq)`` and remembers, per condition, the most
recent **hit**: the latest published reading of that source that strictly
breached the condition threshold.

Whenever every condition has a hit and all hits lie within ``window_seconds``
of each other (``max(hit) - min(hit) <= window``, inclusive), the machine
leaves ``waiting`` and emits exactly one ``correlated_entered`` event (state
``correlated``).  While correlated, a new hit that pushes the spread beyond
the window sends the machine back to ``waiting`` silently; the conditions
re-aligning afterwards produces a fresh event.

Only deterministically published readings drive the machine: buffered
readings do not participate and quarantined late readings can never reach it.
Each published reading is applied exactly once (every rule tracks its merged
stream position), so re-published duplicate batches cannot produce duplicate
events.  Rules created after readings already exist replay the published
stream, yielding exactly the state they would have had if created up front.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any, Optional

from .alerts import extract_value, is_breach
from .cursor import decode_cursor, encode_cursor
from .database import read_transaction, transaction, utc_now
from .errors import BadRequestError, ConflictError, NotFoundError
from .time_utils import format_event_time, parse_event_time, store_event_time

STATE_WAITING = "waiting"
STATE_CORRELATED = "correlated"

EVENT_CORRELATED_ENTERED = "correlated_entered"
EVENT_TYPES = (EVENT_CORRELATED_ENTERED,)

MIN_CONDITIONS = 2
MAX_CONDITIONS = 4


# ---------------------------------------------------------------------------
# window evaluation
# ---------------------------------------------------------------------------

def _window_satisfied(
    hits: dict[str, Any], sources: list[str], window_seconds: float
) -> tuple[bool, Optional[str], Optional[str]]:
    """Whether every condition has a hit and all hits fit in one window.

    Returns ``(satisfied, window_start, window_end)`` where the bounds are
    the earliest/latest hit event times in stored format (``None`` while at
    least one condition has never hit).
    """
    if any(s not in hits for s in sources):
        return False, None, None
    times = [parse_event_time(hits[s]["observed_at"]) for s in sources]
    lo, hi = min(times), max(times)
    satisfied = (hi - lo).total_seconds() <= window_seconds
    return satisfied, store_event_time(lo), store_event_time(hi)


# ---------------------------------------------------------------------------
# persistence helpers
# ---------------------------------------------------------------------------

def _load_session(conn: sqlite3.Connection, session_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
    ).fetchone()
    if row is None:
        raise NotFoundError(f"session {session_id!r} not found")
    return row


def _load_rule(conn: sqlite3.Connection, session_id: str, rule_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM correlation_rules WHERE rule_id = ? AND session_id = ?",
        (rule_id, session_id),
    ).fetchone()
    if row is None:
        raise NotFoundError(
            f"correlation rule {rule_id!r} not found in session {session_id!r}"
        )
    return row


def _dump_conditions(conditions: list[dict[str, Any]]) -> str:
    return json.dumps(conditions, separators=(",", ":"), ensure_ascii=False)


def _rule_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "rule_id": row["rule_id"],
        "session_id": row["session_id"],
        "window_seconds": row["window_seconds"],
        "conditions": json.loads(row["conditions"]),
        "state": row["state"],
        "created_at": format_event_time(parse_event_time(row["created_at"])),
    }


def _hit_out(source_id: str, hit: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "seq": hit["seq"],
        "observed_at": format_event_time(parse_event_time(hit["observed_at"])),
        "value": hit["value"],
    }


def _event_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "rule_id": row["rule_id"],
        "session_id": row["session_id"],
        "event_type": row["event_type"],
        "window_seconds": row["window_seconds"],
        "window_start": format_event_time(parse_event_time(row["window_start"])),
        "window_end": format_event_time(parse_event_time(row["window_end"])),
        "hits": [_hit_out(h["source_id"], h) for h in json.loads(row["hits"])],
        "triggered_at": format_event_time(parse_event_time(row["triggered_at"])),
    }


# ---------------------------------------------------------------------------
# rule management
# ---------------------------------------------------------------------------

def create_rule(
    conn: sqlite3.Connection,
    session_id: str,
    conditions: list[dict[str, Any]],
    window_seconds: float,
    rule_id: Optional[str] = None,
) -> dict[str, Any]:
    sources = [c["source_id"] for c in conditions]
    if len(set(sources)) != len(sources):
        raise BadRequestError("conditions must reference distinct source_ids")

    rid = rule_id or uuid.uuid4().hex
    created_at = store_event_time(utc_now())
    with transaction(conn):
        session = _load_session(conn, session_id)
        allowed = set(json.loads(session["source_ids"]))
        unknown = [s for s in sources if s not in allowed]
        if unknown:
            raise BadRequestError(
                f"unknown source_id(s) for this session: {unknown};"
                f" declared sources: {sorted(allowed)}"
            )
        try:
            conn.execute(
                "INSERT INTO correlation_rules (rule_id, session_id, window_seconds,"
                " conditions, created_at, state, hits, last_observed_at,"
                " last_source_id, last_seq)"
                " VALUES (?, ?, ?, ?, ?, 'waiting', '{}', NULL, NULL, NULL)",
                (rid, session_id, window_seconds, _dump_conditions(conditions),
                 created_at),
            )
        except sqlite3.IntegrityError as exc:
            raise ConflictError(f"correlation rule {rid!r} already exists") from exc

        # Replay everything already published for the rule's sources so the
        # machine state (and any events) match the published timeline
        # regardless of when the rule was created.
        rule = _load_rule(conn, session_id, rid)
        _backfill_rule(conn, rule, created_at)
        row = _load_rule(conn, session_id, rid)
    return _rule_out(row)


def _backfill_rule(conn: sqlite3.Connection, rule: sqlite3.Row, triggered_at: str) -> None:
    """Replay existing published readings for a freshly inserted rule."""
    for reading in _fetch_new_readings(conn, rule, watermark=None):
        _apply_reading(conn, rule, reading, triggered_at)
        rule = _load_rule(conn, rule["session_id"], rule["rule_id"])


def list_rules(conn: sqlite3.Connection, session_id: str) -> list[dict[str, Any]]:
    with read_transaction(conn):
        _load_session(conn, session_id)
        rows = conn.execute(
            "SELECT * FROM correlation_rules WHERE session_id = ?"
            " ORDER BY created_at, rule_id",
            (session_id,),
        ).fetchall()
    return [_rule_out(r) for r in rows]


def get_rule(conn: sqlite3.Connection, session_id: str, rule_id: str) -> dict[str, Any]:
    with read_transaction(conn):
        _load_session(conn, session_id)
        row = _load_rule(conn, session_id, rule_id)
    return _rule_out(row)


# ---------------------------------------------------------------------------
# state machine advancement (called from the ingest write transaction)
# ---------------------------------------------------------------------------

def _fetch_new_readings(
    conn: sqlite3.Connection,
    rule: sqlite3.Row,
    watermark: Optional[str],
) -> list[sqlite3.Row]:
    """Published readings of the rule's sources past its consumed position.

    Merged timeline order ``(observed_at, source_id, seq)``; ``watermark``
    bounds the scan to readings published up to now (redundant with
    ``status='published'`` but kept explicit, mirroring the alert machines).
    """
    conditions = json.loads(rule["conditions"])
    sources = [c["source_id"] for c in conditions]
    placeholders = ", ".join("?" for _ in sources)
    sql = (
        f"SELECT observed_at, source_id, seq, payload FROM readings"
        f" WHERE session_id = ? AND status = 'published'"
        f" AND source_id IN ({placeholders})"
    )
    params: list[Any] = [rule["session_id"], *sources]
    if rule["last_observed_at"] is not None:
        sql += (
            " AND (observed_at > ?"
            " OR (observed_at = ? AND source_id > ?)"
            " OR (observed_at = ? AND source_id = ? AND seq > ?))"
        )
        params += [
            rule["last_observed_at"], rule["last_observed_at"],
            rule["last_source_id"], rule["last_observed_at"],
            rule["last_source_id"], rule["last_seq"],
        ]
    if watermark is not None:
        sql += " AND observed_at <= ?"
        params.append(watermark)
    sql += " ORDER BY observed_at, source_id, seq"
    return conn.execute(sql, params).fetchall()


def advance_correlations(
    conn: sqlite3.Connection,
    session_id: str,
    watermark: str,
    triggered_at: str,
) -> None:
    """Feed every correlation rule the newly published merged stream.

    Only deterministically-published readings appear here; quarantined
    readings are status='quarantined' and can never reach the machines.
    """
    rules = conn.execute(
        "SELECT * FROM correlation_rules WHERE session_id = ?"
        " ORDER BY created_at, rule_id",
        (session_id,),
    ).fetchall()
    for rule in rules:
        for reading in _fetch_new_readings(conn, rule, watermark):
            _apply_reading(conn, rule, reading, triggered_at)
            rule = _load_rule(conn, session_id, rule["rule_id"])


def _apply_reading(
    conn: sqlite3.Connection,
    rule: sqlite3.Row,
    reading: sqlite3.Row,
    triggered_at: str,
) -> None:
    """Apply one published reading (merged order) to one correlation rule."""
    conditions = json.loads(rule["conditions"])
    sources = [c["source_id"] for c in conditions]
    cond = next(c for c in conditions if c["source_id"] == reading["source_id"])
    hits = json.loads(rule["hits"])

    value = extract_value(reading["payload"], cond["value_key"])
    if value is not None and is_breach(cond["direction"], value, cond["threshold"]):
        hits[reading["source_id"]] = {
            "seq": reading["seq"],
            "observed_at": reading["observed_at"],
            "value": value,
        }

    satisfied, window_start, window_end = _window_satisfied(
        hits, sources, rule["window_seconds"]
    )
    state = rule["state"]
    entered = False
    if state == STATE_WAITING and satisfied:
        state = STATE_CORRELATED
        entered = True
    elif state == STATE_CORRELATED and not satisfied:
        # A condition fell out of the window: back to waiting, no event.
        state = STATE_WAITING

    conn.execute(
        "UPDATE correlation_rules SET state = ?, hits = ?,"
        " last_observed_at = ?, last_source_id = ?, last_seq = ?"
        " WHERE rule_id = ?",
        (
            state, json.dumps(hits, sort_keys=True, separators=(",", ":")),
            reading["observed_at"], reading["source_id"], reading["seq"],
            rule["rule_id"],
        ),
    )

    if entered:
        snapshot = [
            {"source_id": s, **hits[s]}
            for s in sources
        ]
        conn.execute(
            "INSERT INTO correlation_events (rule_id, session_id, event_type,"
            " window_seconds, window_start, window_end, hits, triggered_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                rule["rule_id"], rule["session_id"], EVENT_CORRELATED_ENTERED,
                rule["window_seconds"], window_start, window_end,
                json.dumps(snapshot, separators=(",", ":")), triggered_at,
            ),
        )


# ---------------------------------------------------------------------------
# queries
# ---------------------------------------------------------------------------

def get_current_hits(
    conn: sqlite3.Connection, session_id: str, rule_id: str
) -> dict[str, Any]:
    """The rule's per-condition latest hits and window alignment right now."""
    with read_transaction(conn):
        _load_session(conn, session_id)
        row = _load_rule(conn, session_id, rule_id)
        conditions = json.loads(row["conditions"])
        hits = json.loads(row["hits"])
        sources = [c["source_id"] for c in conditions]
        satisfied, window_start, window_end = _window_satisfied(
            hits, sources, row["window_seconds"]
        )

    out_conditions = []
    for c in conditions:
        hit = hits.get(c["source_id"])
        out_conditions.append({
            "source_id": c["source_id"],
            "direction": c["direction"],
            "threshold": c["threshold"],
            "value_key": c["value_key"],
            "last_hit": _hit_out(c["source_id"], hit) if hit is not None else None,
        })
    return {
        "rule_id": row["rule_id"],
        "session_id": session_id,
        "state": row["state"],
        "window_seconds": row["window_seconds"],
        "satisfied": satisfied,
        "window_start": (
            format_event_time(parse_event_time(window_start)) if window_start else None
        ),
        "window_end": (
            format_event_time(parse_event_time(window_end)) if window_end else None
        ),
        "conditions": out_conditions,
        "last_observed_at": (
            format_event_time(parse_event_time(row["last_observed_at"]))
            if row["last_observed_at"] else None
        ),
        "last_source_id": row["last_source_id"],
        "last_seq": row["last_seq"],
    }


def list_event_history(
    conn: sqlite3.Connection,
    session_id: str,
    limit: int,
    cursor: Optional[str] = None,
    rule_id: Optional[str] = None,
) -> dict[str, Any]:
    where = "session_id = ?"
    params: list[Any] = [session_id]
    if rule_id is not None:
        where += " AND rule_id = ?"
        params.append(rule_id)
    if cursor:
        (after_id,) = decode_cursor(cursor, 1)
        if not isinstance(after_id, int):
            raise BadRequestError("malformed cursor")
        where += " AND id < ?"
        params.append(after_id)

    with read_transaction(conn):
        _load_session(conn, session_id)
        if rule_id is not None:
            _load_rule(conn, session_id, rule_id)
        rows = conn.execute(
            f"SELECT * FROM correlation_events WHERE {where}"
            " ORDER BY id DESC LIMIT ?",
            (*params, limit + 1),
        ).fetchall()

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = encode_cursor((page[-1]["id"],)) if has_more and page else None
    return {
        "session_id": session_id,
        "next_cursor": next_cursor,
        "events": [_event_out(r) for r in page],
    }
