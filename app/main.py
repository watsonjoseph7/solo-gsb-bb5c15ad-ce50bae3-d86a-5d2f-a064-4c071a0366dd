"""FastAPI application: lab sensor timeline merge API."""
from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import Depends, FastAPI, Query, Request
from fastapi.responses import JSONResponse

from . import alerts, correlations, service
from .config import settings
from .database import get_db, init_db
from .models import (
    AlertHistoryResponse,
    AlertRuleOut,
    AlertRulesResponse,
    CorrelationHitsResponse,
    CorrelationHistoryResponse,
    CorrelationRuleOut,
    CorrelationRulesResponse,
    CreateAlertRuleIn,
    CreateCorrelationRuleIn,
    CreateSessionIn,
    CreatedSessionResponse,
    CurrentAlertsResponse,
    PullResponse,
    QuarantineResponse,
    ReadingIn,
    ReportResponse,
    SessionOut,
)
from .time_utils import TimestampError


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Database directory + schema are created as part of startup.
    init_db(app.state.db_path)
    yield


app = FastAPI(
    title="Lab Sensor Timeline Merge API",
    version="1.2.0",
    description=(
        "Out-of-order sensor reporting with per-session fixed source lists, "
        "a slowest-source watermark (minus allowed lateness), deterministic "
        "publish order and late-reading quarantine. Published readings also "
        "drive per-source threshold alert state machines (enter / active / "
        "recovered) with hysteresis and consecutive-count debouncing, and "
        "cross-source composite rules that fire a correlated_entered event "
        "when 2-4 sources all breach their thresholds inside one event-time "
        "window."
    ),
    lifespan=lifespan,
)
app.state.db_path = settings.db_path


@app.exception_handler(service.ServiceError)
async def service_error_handler(request: Request, exc: service.ServiceError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": str(exc)})


@app.exception_handler(TimestampError)
async def timestamp_error_handler(request: Request, exc: TimestampError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(sqlite3.Error)
async def sqlite_error_handler(request: Request, exc: sqlite3.Error) -> JSONResponse:
    return JSONResponse(status_code=500, content={"detail": f"database error: {exc}"})


def _error(status: int, detail: Any) -> JSONResponse:
    return JSONResponse(status_code=status, content={"detail": detail})


@app.get("/health", tags=["meta"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/sessions", response_model=CreatedSessionResponse, status_code=201, tags=["sessions"])
def create_session(
    body: CreateSessionIn,
    conn: sqlite3.Connection = Depends(get_db),
) -> CreatedSessionResponse:
    result = service.create_session(
        conn,
        source_ids=body.source_ids,
        allowed_lateness_seconds=body.allowed_lateness_seconds,
        session_id=body.session_id,
    )
    return CreatedSessionResponse(**result)


@app.get("/sessions/{session_id}", response_model=SessionOut, tags=["sessions"])
def get_session(
    session_id: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> SessionOut:
    return SessionOut(**service.get_session(conn, session_id))


@app.post(
    "/sessions/{session_id}/readings",
    response_model=ReportResponse,
    status_code=200,
    tags=["readings"],
)
async def report_readings(
    session_id: str,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
) -> ReportResponse:
    """Batch-report readings.

    Accepts either ``{"readings": [ ... ]}`` or a bare JSON array.
    Semantic failures (bad timestamps, unknown sources) return 400;
    structural field failures return 422.
    """
    try:
        payload = await request.json()
    except Exception:
        return _error(400, "request body must be valid JSON")

    if isinstance(payload, dict) and isinstance(payload.get("readings"), list):
        raw = payload["readings"]
    elif isinstance(payload, list):
        raw = payload
    else:
        return _error(400, "body must be {'readings': [...]} or a JSON array")

    if not raw:
        return _error(400, "readings batch must not be empty")

    items: list[dict] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            return _error(400, f"reading at index {idx} must be an object")
        try:
            parsed = ReadingIn(**item)
        except Exception as exc:
            detail = exc.errors() if hasattr(exc, "errors") else str(exc)
            return _error(422, f"invalid reading at index {idx}: {detail}")
        items.append(parsed.model_dump())

    # TimestampError and ServiceError are handled by the registered handlers.
    result = service.ingest_batch(conn, session_id, items)
    return ReportResponse(**result)


@app.get(
    "/sessions/{session_id}/stream",
    response_model=PullResponse,
    tags=["readings"],
)
def pull_stream(
    session_id: str,
    limit: int = Query(100, ge=1, le=1000),
    cursor: Optional[str] = Query(None, description="Opaque next_cursor from a previous page"),
    conn: sqlite3.Connection = Depends(get_db),
) -> PullResponse:
    """Published readings only, in deterministic timeline order."""
    return PullResponse(**service.list_published(conn, session_id, limit, cursor))


@app.get(
    "/sessions/{session_id}/quarantine",
    response_model=QuarantineResponse,
    tags=["readings"],
)
def pull_quarantine(
    session_id: str,
    limit: int = Query(100, ge=1, le=1000),
    cursor: Optional[str] = Query(None, description="Opaque next_cursor from a previous page"),
    conn: sqlite3.Connection = Depends(get_db),
) -> QuarantineResponse:
    """Late readings that arrived behind the already-published watermark."""
    return QuarantineResponse(**service.list_quarantined(conn, session_id, limit, cursor))


# ---------------------------------------------------------------------------
# post-publish threshold alerts
# ---------------------------------------------------------------------------

@app.post(
    "/sessions/{session_id}/alert-rules",
    response_model=AlertRuleOut,
    status_code=201,
    tags=["alerts"],
)
def create_alert_rule(
    session_id: str,
    body: CreateAlertRuleIn,
    conn: sqlite3.Connection = Depends(get_db),
) -> AlertRuleOut:
    """Create a per-source threshold alert rule.

    The rule's state machine immediately replays the session's already
    published readings for that source, so its initial state is independent
    of when the rule was created.  Unknown sources and rules whose
    enter/recover thresholds do not form a valid hysteresis band return 400.
    """
    result = alerts.create_rule(
        conn,
        session_id=session_id,
        source_id=body.source_id,
        direction=body.direction,
        enter_threshold=body.enter_threshold,
        recover_threshold=body.recover_threshold,
        enter_consecutive=body.enter_consecutive,
        recover_consecutive=body.recover_consecutive,
        value_key=body.value_key,
        rule_id=body.rule_id,
    )
    return AlertRuleOut(**result)


@app.get(
    "/sessions/{session_id}/alert-rules",
    response_model=AlertRulesResponse,
    tags=["alerts"],
)
def list_alert_rules(
    session_id: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> AlertRulesResponse:
    return AlertRulesResponse(session_id=session_id, rules=alerts.list_rules(conn, session_id))


@app.get(
    "/sessions/{session_id}/alert-rules/{rule_id}",
    response_model=AlertRuleOut,
    tags=["alerts"],
)
def get_alert_rule(
    session_id: str,
    rule_id: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> AlertRuleOut:
    return AlertRuleOut(**alerts.get_rule(conn, session_id, rule_id))


@app.get(
    "/sessions/{session_id}/alerts/current",
    response_model=CurrentAlertsResponse,
    tags=["alerts"],
)
def current_alerts(
    session_id: str,
    source_id: Optional[str] = Query(
        None, description="Restrict to one declared source of the session"
    ),
    conn: sqlite3.Connection = Depends(get_db),
) -> CurrentAlertsResponse:
    """Rules currently in the ``active`` state (entered, not yet recovered)."""
    return CurrentAlertsResponse(
        session_id=session_id,
        alerts=alerts.list_current_alerts(conn, session_id, source_id),
    )


@app.get(
    "/sessions/{session_id}/alerts/history",
    response_model=AlertHistoryResponse,
    tags=["alerts"],
)
def alert_history(
    session_id: str,
    limit: int = Query(100, ge=1, le=1000),
    cursor: Optional[str] = Query(None, description="Opaque next_cursor from a previous page"),
    source_id: Optional[str] = Query(None, description="Filter by source"),
    rule_id: Optional[str] = Query(None, description="Filter by rule"),
    event_type: Optional[str] = Query(
        None, description="Filter: 'entered' or 'recovered'"
    ),
    conn: sqlite3.Connection = Depends(get_db),
) -> AlertHistoryResponse:
    """Entered/recovered events, newest first, with keyset pagination."""
    if event_type is not None and event_type not in alerts.EVENT_TYPES:
        return _error(
            400,
            f"event_type must be one of {list(alerts.EVENT_TYPES)},"
            f" got {event_type!r}",
        )
    return AlertHistoryResponse(
        **alerts.list_event_history(
            conn, session_id, limit, cursor, source_id, rule_id, event_type
        )
    )


# ---------------------------------------------------------------------------
# cross-source composite (correlated) alerts
# ---------------------------------------------------------------------------

@app.post(
    "/sessions/{session_id}/correlation-rules",
    response_model=CorrelationRuleOut,
    status_code=201,
    tags=["correlations"],
)
def create_correlation_rule(
    session_id: str,
    body: CreateCorrelationRuleIn,
    conn: sqlite3.Connection = Depends(get_db),
) -> CorrelationRuleOut:
    """Create a cross-source composite rule over 2-4 session sources.

    The rule immediately replays the session's already published readings in
    merged timeline order, so its state (and any ``correlated_entered``
    events) are independent of when the rule was created.  Unknown sources
    and duplicate sources within one rule are rejected.
    """
    result = correlations.create_rule(
        conn,
        session_id=session_id,
        conditions=[c.model_dump() for c in body.conditions],
        window_seconds=body.window_seconds,
        rule_id=body.rule_id,
    )
    return CorrelationRuleOut(**result)


@app.get(
    "/sessions/{session_id}/correlation-rules",
    response_model=CorrelationRulesResponse,
    tags=["correlations"],
)
def list_correlation_rules(
    session_id: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> CorrelationRulesResponse:
    return CorrelationRulesResponse(
        session_id=session_id, rules=correlations.list_rules(conn, session_id)
    )


@app.get(
    "/sessions/{session_id}/correlation-rules/{rule_id}",
    response_model=CorrelationRuleOut,
    tags=["correlations"],
)
def get_correlation_rule(
    session_id: str,
    rule_id: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> CorrelationRuleOut:
    return CorrelationRuleOut(**correlations.get_rule(conn, session_id, rule_id))


@app.get(
    "/sessions/{session_id}/correlation-rules/{rule_id}/hits",
    response_model=CorrelationHitsResponse,
    tags=["correlations"],
)
def correlation_rule_hits(
    session_id: str,
    rule_id: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> CorrelationHitsResponse:
    """Current per-condition hits and window alignment for one rule."""
    return CorrelationHitsResponse(
        **correlations.get_current_hits(conn, session_id, rule_id)
    )


@app.get(
    "/sessions/{session_id}/correlation-alerts/history",
    response_model=CorrelationHistoryResponse,
    tags=["correlations"],
)
def correlation_alert_history(
    session_id: str,
    limit: int = Query(100, ge=1, le=1000),
    cursor: Optional[str] = Query(None, description="Opaque next_cursor from a previous page"),
    rule_id: Optional[str] = Query(None, description="Filter by correlation rule"),
    conn: sqlite3.Connection = Depends(get_db),
) -> CorrelationHistoryResponse:
    """``correlated_entered`` events, newest first, with keyset pagination."""
    return CorrelationHistoryResponse(
        **correlations.list_event_history(conn, session_id, limit, cursor, rule_id)
    )
