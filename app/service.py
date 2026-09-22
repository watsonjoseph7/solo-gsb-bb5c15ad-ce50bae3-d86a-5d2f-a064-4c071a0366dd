"""Watermark timeline merge business logic.

Guarantees implemented here:

* A session fixes its source list and ``allowed_lateness_seconds`` at
  creation; reports from unknown sources are rejected.
* The watermark is defined only once *every* source has reported, as
  ``min(source max observed_at) - allowed_lateness``. It is monotonic.
* Buffered readings with ``observed_at`` strictly before the watermark are
  published exactly once, in the deterministic order
  ``(observed_at, source_id, seq)``.
* A reading arriving with ``observed_at`` strictly before the already
  published watermark can never be (re)published: it is diverted to the
  quarantine table and never mutates history.
* Re-posting the same ``(source_id, seq)`` with the same payload is an
  idempotent no-op; a different payload is a 409 conflict.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import timedelta
from typing import Any, Optional

from .cursor import decode_cursor as _decode_cursor
from .cursor import encode_cursor as _encode_cursor
from .database import read_transaction, transaction, utc_now
from .errors import BadRequestError, ConflictError, NotFoundError, ServiceError
from .time_utils import format_event_time, parse_event_time, store_event_time

# Re-exported so existing imports (``app.service.ServiceError`` etc.) keep
# working; the canonical definitions live in :mod:`app.errors`.
__all__ = [
    "ServiceError",
    "NotFoundError",
    "BadRequestError",
    "ConflictError",
    "canonical_payload",
    "create_session",
    "get_session",
    "ingest_batch",
    "list_published",
    "list_quarantined",
]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def canonical_payload(value: Any) -> str:
    """Deterministic JSON serialisation so idempotency is payload-based.

    Raises TypeError for values that cannot be represented as JSON; API
    input already went through Pydantic so this stays strict (no silent
    ``str()`` coercion).
    """
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _load_session(conn: sqlite3.Connection, session_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
    ).fetchone()
    if row is None:
        raise NotFoundError(f"session {session_id!r} not found")
    return row


def _source_maxes(conn: sqlite3.Connection, session_id: str) -> dict[str, Optional[str]]:
    rows = conn.execute(
        "SELECT source_id, max_observed_at FROM session_sources WHERE session_id = ?",
        (session_id,),
    ).fetchall()
    return {r["source_id"]: r["max_observed_at"] for r in rows}


def _compute_watermark(
    source_max: dict[str, Optional[str]], allowed_lateness_seconds: float
) -> Optional[str]:
    """Slowest-source max event time minus allowed lateness.

    Returns None while at least one source has never reported: before that
    the slowest source's event-time clock is unknown, so no reading may be
    released.
    """
    if not source_max or any(v is None for v in source_max.values()):
        return None
    slowest = parse_event_time(min(source_max.values()))  # type: ignore[arg-type]
    return store_event_time(slowest - timedelta(seconds=allowed_lateness_seconds))


def _reading_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "session_id": row["session_id"],
        "source_id": row["source_id"],
        "seq": row["seq"],
        "observed_at": format_event_time(parse_event_time(row["observed_at"])),
        "value": json.loads(row["payload"]) if row["payload"] is not None else None,
        "published_watermark": format_event_time(parse_event_time(row["published_watermark"])),
    }


def _quarantine_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "session_id": row["session_id"],
        "source_id": row["source_id"],
        "seq": row["seq"],
        "observed_at": format_event_time(parse_event_time(row["observed_at"])),
        "value": json.loads(row["payload"]) if row["payload"] is not None else None,
        "quarantined_watermark": format_event_time(
            parse_event_time(row["quarantined_watermark"])
        ),
        "reason": "behind_published_watermark",
    }


# ---------------------------------------------------------------------------
# sessions
# ---------------------------------------------------------------------------

def create_session(
    conn: sqlite3.Connection,
    source_ids: list[str],
    allowed_lateness_seconds: float,
    session_id: Optional[str] = None,
) -> dict[str, Any]:
    sid = session_id or uuid.uuid4().hex
    created_at = store_event_time(utc_now())
    sources = json.dumps(source_ids)
    try:
        with transaction(conn):
            conn.execute(
                "INSERT INTO sessions (session_id, source_ids, allowed_lateness_seconds,"
                " published_watermark, created_at) VALUES (?, ?, ?, NULL, ?)",
                (sid, sources, allowed_lateness_seconds, created_at),
            )
            conn.executemany(
                "INSERT INTO session_sources (session_id, source_id, max_observed_at,"
                " first_seen_at) VALUES (?, ?, NULL, ?)",
                [(sid, s, created_at) for s in source_ids],
            )
    except sqlite3.IntegrityError as exc:
        raise ConflictError(f"session {sid!r} already exists") from exc
    return {
        "session_id": sid,
        "source_ids": source_ids,
        "allowed_lateness_seconds": allowed_lateness_seconds,
        "created_at": format_event_time(parse_event_time(created_at)),
    }


def get_session(conn: sqlite3.Connection, session_id: str) -> dict[str, Any]:
    with read_transaction(conn):
        row = _load_session(conn, session_id)
        source_max = _source_maxes(conn, session_id)
    all_reported = bool(source_max) and all(v is not None for v in source_max.values())
    effective = (
        _compute_watermark(source_max, row["allowed_lateness_seconds"])
        if all_reported
        else None
    )
    return {
        "session_id": row["session_id"],
        "source_ids": json.loads(row["source_ids"]),
        "allowed_lateness_seconds": row["allowed_lateness_seconds"],
        "published_watermark": (
            format_event_time(parse_event_time(row["published_watermark"]))
            if row["published_watermark"]
            else None
        ),
        "all_sources_reported": all_reported,
        "effective_watermark": format_event_time(parse_event_time(effective)) if effective else None,
        "created_at": format_event_time(parse_event_time(row["created_at"])),
    }


# ---------------------------------------------------------------------------
# ingestion
# ---------------------------------------------------------------------------

def ingest_batch(
    conn: sqlite3.Connection, session_id: str, readings: list[dict[str, Any]]
) -> dict[str, Any]:
    # Validate/normalise the whole batch up front: bad timestamps or
    # undumpable values reject the batch atomically (unknown sources are
    # checked against the session inside the transaction).
    parsed_times: dict[int, str] = {}
    payloads: dict[int, str] = {}
    for i, r in enumerate(readings):
        observed = parse_event_time(r["observed_at"])
        parsed_times[i] = store_event_time(observed)
        payloads[i] = canonical_payload(r["value"])

    with transaction(conn):
        session = _load_session(conn, session_id)
        allowed_sources = set(json.loads(session["source_ids"]))
        unknown = {r["source_id"] for r in readings} - allowed_sources
        if unknown:
            raise BadRequestError(
                f"unknown source_id(s) for this session: {sorted(unknown)}"
            )

        received_at = store_event_time(utc_now())
        lateness = session["allowed_lateness_seconds"]
        published_wm: Optional[str] = session["published_watermark"]

        duplicates = 0
        quarantined = 0

        for i, r in enumerate(readings):
            key = (session_id, r["source_id"], r["seq"])
            observed_store = parsed_times[i]
            payload = payloads[i]

            existing = conn.execute(
                "SELECT status, payload FROM readings"
                " WHERE session_id = ? AND source_id = ? AND seq = ?",
                key,
            ).fetchone()
            if existing is not None:
                if existing["payload"] != payload:
                    # Same key, different measurement: never silently overwrite.
                    raise ConflictError(
                        f"reading ({r['source_id']}, seq={r['seq']}) already exists"
                        " with a different payload"
                    )
                duplicates += 1
                continue

            if published_wm is not None and observed_store < published_wm:
                # Too late: the timeline up to the watermark is immutable.
                conn.execute(
                    "INSERT INTO readings (session_id, source_id, seq, observed_at,"
                    " payload, status, first_received_at, published_watermark,"
                    " quarantined_watermark)"
                    " VALUES (?, ?, ?, ?, ?, 'quarantined', ?, NULL, ?)",
                    (*key, observed_store, payload, received_at, published_wm),
                )
                quarantined += 1
                continue

            conn.execute(
                "INSERT INTO readings (session_id, source_id, seq, observed_at,"
                " payload, status, first_received_at, published_watermark,"
                " quarantined_watermark)"
                " VALUES (?, ?, ?, ?, ?, 'buffered', ?, NULL, NULL)",
                (*key, observed_store, payload, received_at),
            )
            # Advance this source's event-time clock.
            conn.execute(
                "UPDATE session_sources"
                " SET max_observed_at ="
                " CASE WHEN max_observed_at IS NULL OR ? > max_observed_at THEN ?"
                " ELSE max_observed_at END"
                " WHERE session_id = ? AND source_id = ?",
                (observed_store, observed_store, session_id, r["source_id"]),
            )

        # Recompute the watermark from the slowest source.
        source_max = _source_maxes(conn, session_id)
        all_reported = bool(source_max) and all(v is not None for v in source_max.values())
        new_wm = _compute_watermark(source_max, lateness) if all_reported else None
        published_now = 0
        if new_wm is not None and (published_wm is None or new_wm > published_wm):
            cur = conn.execute(
                "UPDATE readings SET status = 'published', published_watermark = ?"
                " WHERE session_id = ? AND status = 'buffered' AND observed_at < ?",
                (new_wm, session_id, new_wm),
            )
            published_now = cur.rowcount
            conn.execute(
                "UPDATE sessions SET published_watermark = ? WHERE session_id = ?",
                (new_wm, session_id),
            )
            published_wm = new_wm
            if published_now:
                # Deterministically-published readings are the *only* input to
                # the per-source alert state machines; buffered readings that
                # merely became published in this batch drive it, in timeline
                # order, inside the same serialised write transaction.
                from .alerts import advance_alerts
                from .correlated import advance_correlated

                advance_alerts(conn, session_id, new_wm, received_at)
                # Cross-source rules consume the global merge order (every
                # session source advances event time and can expire a window).
                advance_correlated(conn, session_id, new_wm, received_at)

        published_total = conn.execute(
            "SELECT COUNT(*) AS c FROM readings WHERE session_id = ? AND status = 'published'",
            (session_id,),
        ).fetchone()["c"]
        buffered = conn.execute(
            "SELECT COUNT(*) AS c FROM readings WHERE session_id = ? AND status = 'buffered'",
            (session_id,),
        ).fetchone()["c"]

    return {
        "session_id": session_id,
        "received": len(readings),
        "duplicates": duplicates,
        "quarantined": quarantined,
        "published_now": published_now,
        "published_total": published_total,
        "buffered": buffered,
        "all_sources_reported": all_reported,
        "published_watermark": (
            format_event_time(parse_event_time(published_wm)) if published_wm else None
        ),
    }


# ---------------------------------------------------------------------------
# reads
# ---------------------------------------------------------------------------

def list_published(
    conn: sqlite3.Connection,
    session_id: str,
    limit: int,
    cursor: Optional[str] = None,
) -> dict[str, Any]:
    where = "session_id = ? AND status = 'published'"
    params: list[Any] = [session_id]
    if cursor:
        ts, src, seq = _decode_cursor(cursor, 3)
        # Keyset on the deterministic timeline order
        # (observed_at, source_id, seq).
        where += (
            " AND (observed_at > ?"
            " OR (observed_at = ? AND source_id > ?)"
            " OR (observed_at = ? AND source_id = ? AND seq > ?))"
        )
        params += [ts, ts, src, ts, src, seq]

    with read_transaction(conn):
        session = _load_session(conn, session_id)
        rows = conn.execute(
            f"SELECT * FROM readings WHERE {where}"
            " ORDER BY observed_at, source_id, seq LIMIT ?",
            (*params, limit + 1),
        ).fetchall()

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = None
    if has_more and page:
        last = page[-1]
        next_cursor = _encode_cursor((last["observed_at"], last["source_id"], last["seq"]))

    return {
        "session_id": session_id,
        "next_cursor": next_cursor,
        "published_watermark": (
            format_event_time(parse_event_time(session["published_watermark"]))
            if session["published_watermark"]
            else None
        ),
        "readings": [_reading_out(r) for r in page],
    }


def list_quarantined(
    conn: sqlite3.Connection,
    session_id: str,
    limit: int,
    cursor: Optional[str] = None,
) -> dict[str, Any]:
    where = "session_id = ? AND status = 'quarantined'"
    params: list[Any] = [session_id]
    if cursor:
        received, src, seq = _decode_cursor(cursor, 3)
        where += (
            " AND (first_received_at > ?"
            " OR (first_received_at = ? AND source_id > ?)"
            " OR (first_received_at = ? AND source_id = ? AND seq > ?))"
        )
        params += [received, received, src, received, src, seq]

    with read_transaction(conn):
        _load_session(conn, session_id)
        rows = conn.execute(
            f"SELECT * FROM readings WHERE {where}"
            " ORDER BY first_received_at, source_id, seq LIMIT ?",
            (*params, limit + 1),
        ).fetchall()

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = None
    if has_more and page:
        last = page[-1]
        next_cursor = _encode_cursor(
            (last["first_received_at"], last["source_id"], last["seq"])
        )

    return {
        "session_id": session_id,
        "next_cursor": next_cursor,
        "readings": [_quarantine_out(r) for r in page],
    }
