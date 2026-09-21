"""Validated contracts for Phase M1 schedules and collection records."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from tool_contracts import ObservationStatus, ToolObservation
from trace.models import TraceEventType


def utc_now() -> datetime:
    """Return a timezone-aware timestamp for persisted monitoring records."""

    return datetime.now(timezone.utc)


class CollectionSchedule(BaseModel):
    """One persisted, fixed read-only collection plan."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    schedule_id: str = Field(pattern=r"^[A-Za-z0-9._-]+$", min_length=1, max_length=128)
    tool_name: str = Field(pattern=r"^[A-Za-z0-9._-]+$", min_length=1, max_length=128)
    interval_seconds: int = Field(ge=1, le=86_400)
    enabled: bool = True
    timeout_seconds: float = Field(gt=0, le=60)
    arguments: dict[str, Any] = Field(default_factory=dict)
    next_run_at: datetime | None = None
    last_run_at: datetime | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    @field_validator("next_run_at", "last_run_at", "created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("monitoring timestamps must include a timezone")
        return value


class CollectionRun(BaseModel):
    """Persisted outcome of exactly one scheduled tool invocation."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    collection_run_id: str = Field(
        default_factory=lambda: f"col_{uuid4().hex}",
        min_length=8,
        max_length=80,
    )
    schedule_id: str = Field(min_length=1, max_length=128)
    tool: str = Field(min_length=1, max_length=128)
    status: ObservationStatus
    timestamp: datetime = Field(default_factory=utc_now)
    evidence_ref: str = Field(min_length=8, max_length=80)
    duration_ms: int = Field(ge=0)

    @field_validator("timestamp")
    @classmethod
    def require_timestamp_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("collection timestamp must include a timezone")
        return value


class CollectionResult(BaseModel):
    """In-memory result returned by Collector and Scheduler."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    run: CollectionRun
    observation: ToolObservation

    @model_validator(mode="after")
    def validate_links(self) -> "CollectionResult":
        if self.run.evidence_ref != self.observation.evidence_id:
            raise ValueError("collection run evidence_ref must match observation")
        if self.run.status is not self.observation.status:
            raise ValueError("collection run status must match observation")
        return self


class StoredObservation(BaseModel):
    """One persisted collection result reconstructed for deterministic rules."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    collection_run_id: str = Field(min_length=8, max_length=80)
    schedule_id: str = Field(min_length=1, max_length=128)
    timestamp: datetime
    observation: ToolObservation

    @field_validator("timestamp")
    @classmethod
    def require_stored_timestamp_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("stored observation timestamp must include a timezone")
        return value


class MonitoringTraceEvent(BaseModel):
    """Pre-incident trace event stored outside the Incident foreign key."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    event_id: str = Field(
        default_factory=lambda: f"mtrace_{uuid4().hex}",
        min_length=8,
        max_length=80,
    )
    schedule_id: str = Field(min_length=1, max_length=128)
    collection_run_id: str = Field(min_length=8, max_length=80)
    event_type: TraceEventType
    summary: str = Field(min_length=1, max_length=2_000)
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)

    @field_validator("created_at")
    @classmethod
    def require_created_at_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("monitoring trace timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def require_monitoring_event_type(self) -> "MonitoringTraceEvent":
        allowed = {
            TraceEventType.MONITORING_TICK_STARTED,
            TraceEventType.OBSERVATION_COLLECTED,
            TraceEventType.ANOMALY_DETECTED,
        }
        if self.event_type not in allowed:
            raise ValueError("monitoring trace uses a non-monitoring event type")
        return self
