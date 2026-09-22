"""Pydantic request/response models."""
from __future__ import annotations

import math
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator


class ReadingIn(BaseModel):
    source_id: str = Field(..., min_length=1, description="Fixed sensor source declared on the session")
    seq: int = Field(..., ge=0, description="Per-source strictly increasing sequence number")
    observed_at: str = Field(..., description="Event time, ISO 8601 (UTC or with offset)")
    value: Optional[Any] = Field(None, description="Arbitrary JSON-serializable measurement payload")

    @field_validator("source_id")
    @classmethod
    def _source_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("source_id must not be blank")
        return v

    @field_validator("observed_at")
    @classmethod
    def _observed_not_blank(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("observed_at must be a non-empty ISO 8601 string")
        return v


class CreateSessionIn(BaseModel):
    source_ids: list[str] = Field(..., min_length=1, description="Fixed source list for this session")
    allowed_lateness_seconds: float = Field(
        ..., ge=0, description="How far behind the slowest source the watermark lags"
    )
    session_id: Optional[str] = Field(None, min_length=1, description="Optional caller-chosen id")

    @field_validator("source_ids")
    @classmethod
    def _sources_unique_nonblank(cls, v: list[str]) -> list[str]:
        if any(not isinstance(s, str) or not s.strip() for s in v):
            raise ValueError("source_ids must be non-empty strings")
        if len(set(v)) != len(v):
            raise ValueError("source_ids must be unique")
        return v


class ReadingOut(BaseModel):
    session_id: str
    source_id: str
    seq: int
    observed_at: str
    value: Optional[Any] = None
    published_watermark: str


class QuarantineOut(BaseModel):
    session_id: str
    source_id: str
    seq: int
    observed_at: str
    value: Optional[Any] = None
    quarantined_watermark: str
    reason: str = "behind_published_watermark"


class ReportResponse(BaseModel):
    session_id: str
    received: int
    duplicates: int
    quarantined: int
    published_now: int
    published_total: int
    buffered: int
    all_sources_reported: bool
    published_watermark: Optional[str] = None


class PullResponse(BaseModel):
    session_id: str
    next_cursor: Optional[str] = None
    published_watermark: Optional[str] = None
    readings: list[ReadingOut]


class QuarantineResponse(BaseModel):
    session_id: str
    next_cursor: Optional[str] = None
    readings: list[QuarantineOut]


class SessionOut(BaseModel):
    session_id: str
    source_ids: list[str]
    allowed_lateness_seconds: float
    published_watermark: Optional[str] = None
    all_sources_reported: bool
    effective_watermark: Optional[str] = None
    created_at: str


class CreatedSessionResponse(BaseModel):
    session_id: str
    source_ids: list[str]
    allowed_lateness_seconds: float
    created_at: str


# ---------------------------------------------------------------------------
# alert rules
# ---------------------------------------------------------------------------

class CreateAlertRuleIn(BaseModel):
    source_id: str = Field(
        ..., min_length=1, description="Fixed sensor source declared on the session"
    )
    direction: Literal["above", "below"] = Field(
        ..., description="'above' alerts on high values, 'below' on low values"
    )
    enter_threshold: float = Field(
        ..., description="Breach threshold; crossing it (strictly) counts toward enter"
    )
    recover_threshold: float = Field(
        ...,
        description=(
            "Safe-side threshold forming a hysteresis band with enter_threshold"
        ),
    )
    enter_consecutive: int = Field(
        ..., ge=1, le=10_000,
        description="Consecutive breaching published readings required to enter",
    )
    recover_consecutive: int = Field(
        ..., ge=1, le=10_000,
        description="Consecutive recovered published readings required to clear",
    )
    value_key: Optional[str] = Field(
        default=None,
        min_length=1,
        description=(
            "Top-level key read from an object-valued reading; omit to compare the"
            " reading value itself. Missing/non-numeric values reset the streak."
        ),
    )
    rule_id: Optional[str] = Field(None, min_length=1, description="Optional caller-chosen id")

    @field_validator("source_id")
    @classmethod
    def _source_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("source_id must not be blank")
        return v

    @field_validator("enter_threshold", "recover_threshold")
    @classmethod
    def _finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("threshold must be a finite number")
        return v

    @field_validator("value_key")
    @classmethod
    def _value_key_not_blank(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            raise ValueError("value_key must not be blank")
        return v.strip() if v is not None else None


class AlertRuleOut(BaseModel):
    rule_id: str
    session_id: str
    source_id: str
    direction: Literal["above", "below"]
    enter_threshold: float
    recover_threshold: float
    enter_consecutive: int
    recover_consecutive: int
    value_key: Optional[str] = None
    state: Literal["normal", "active"]
    streak: int
    created_at: str


class AlertRulesResponse(BaseModel):
    session_id: str
    rules: list[AlertRuleOut]


class AlertTriggerOut(BaseModel):
    seq: int
    observed_at: str
    value: Optional[float] = None
    threshold: float
    triggered_at: str


class CurrentAlertOut(BaseModel):
    rule_id: str
    session_id: str
    source_id: str
    direction: Literal["above", "below"]
    enter_threshold: float
    recover_threshold: float
    enter_consecutive: int
    recover_consecutive: int
    value_key: Optional[str] = None
    state: Literal["active"]
    streak: int
    entered: AlertTriggerOut
    last_observed_at: Optional[str] = None
    last_seq: Optional[int] = None


class CurrentAlertsResponse(BaseModel):
    session_id: str
    alerts: list[CurrentAlertOut]


class AlertEventOut(BaseModel):
    id: int
    rule_id: str
    session_id: str
    source_id: str
    event_type: Literal["entered", "recovered"]
    seq: int
    observed_at: str
    value: Optional[float] = None
    threshold: float
    triggered_at: str


class AlertHistoryResponse(BaseModel):
    session_id: str
    next_cursor: Optional[str] = None
    events: list[AlertEventOut]


# ---------------------------------------------------------------------------
# cross-source correlated alert rules
# ---------------------------------------------------------------------------

class CorrelatedConditionIn(BaseModel):
    source_id: str = Field(
        ..., min_length=1, description="Fixed sensor source declared on the session"
    )
    direction: Literal["above", "below"] = Field(
        ..., description="'above' breaches on high values, 'below' on low values"
    )
    threshold: float = Field(
        ..., description="Breach threshold; the comparison is strict"
    )
    window_seconds: float = Field(
        ..., gt=0, le=365 * 24 * 3600,
        description="How long a hit on this condition stays valid (seconds)",
    )
    value_key: Optional[str] = Field(
        default=None,
        min_length=1,
        description=(
            "Top-level key read from an object-valued reading; omit to compare the"
            " reading value itself. Missing/non-numeric values never hit."
        ),
    )

    @field_validator("source_id")
    @classmethod
    def _source_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("source_id must not be blank")
        return v

    @field_validator("threshold")
    @classmethod
    def _finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("threshold must be a finite number")
        return v

    @field_validator("value_key")
    @classmethod
    def _value_key_not_blank(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            raise ValueError("value_key must not be blank")
        return v.strip() if v is not None else None


class CreateCorrelatedRuleIn(BaseModel):
    conditions: list[CorrelatedConditionIn] = Field(
        ..., min_length=2, max_length=4,
        description="2-4 conditions; declaration order is the merge order",
    )
    rule_id: Optional[str] = Field(None, min_length=1, description="Optional caller-chosen id")

    @field_validator("conditions")
    @classmethod
    def _sources_unique(cls, v: list[CorrelatedConditionIn]) -> list[CorrelatedConditionIn]:
        sources = [c.source_id for c in v]
        if len(set(sources)) != len(sources):
            raise ValueError("each condition must reference a distinct source_id")
        return v


class CorrelatedHitOut(BaseModel):
    source_id: str
    seq: int
    observed_at: str
    value: Optional[float] = None


class CorrelatedConditionOut(BaseModel):
    position: int
    source_id: str
    direction: Literal["above", "below"]
    threshold: float
    value_key: Optional[str] = None
    window_seconds: float
    last_hit: Optional[CorrelatedHitOut] = None


class CorrelatedAnchorOut(BaseModel):
    source_id: str
    seq: int
    observed_at: str


class CorrelatedRuleOut(BaseModel):
    rule_id: str
    session_id: str
    created_at: str
    state: Literal["waiting", "correlated"]
    conditions: list[CorrelatedConditionOut]
    correlated_since: Optional[CorrelatedAnchorOut] = None
    last_observed_at: Optional[str] = None
    last_source_id: Optional[str] = None
    last_seq: Optional[int] = None


class CorrelatedRulesResponse(BaseModel):
    session_id: str
    rules: list[CorrelatedRuleOut]


class CorrelatedHitSnapshotOut(CorrelatedHitOut):
    position: int
    direction: Literal["above", "below"]
    threshold: float
    value_key: Optional[str] = None
    window_seconds: float


class CorrelatedEnteredOut(BaseModel):
    id: int
    event_type: Literal["correlated_entered"]
    completed_by: CorrelatedAnchorOut
    conditions: list[CorrelatedHitSnapshotOut]
    triggered_at: str


class CurrentCorrelatedAlertOut(BaseModel):
    rule_id: str
    session_id: str
    state: Literal["correlated"]
    correlated_entered: CorrelatedEnteredOut
    last_observed_at: Optional[str] = None
    last_source_id: Optional[str] = None
    last_seq: Optional[int] = None


class CurrentCorrelatedAlertsResponse(BaseModel):
    session_id: str
    alerts: list[CurrentCorrelatedAlertOut]


class CorrelatedEventOut(BaseModel):
    id: int
    rule_id: str
    session_id: str
    event_type: Literal["correlated_entered"]
    completed_by: CorrelatedAnchorOut
    conditions: list[CorrelatedHitSnapshotOut]
    triggered_at: str


class CorrelatedHistoryResponse(BaseModel):
    session_id: str
    next_cursor: Optional[str] = None
    events: list[CorrelatedEventOut]
