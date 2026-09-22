"""Incident Manager lifecycle and audit models."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from trace.models import TraceEventType


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class IncidentSeverity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class IncidentStatus(str, Enum):
    ACTIVE = "ACTIVE"
    RECOVERED = "RECOVERED"
    ACKNOWLEDGED = "ACKNOWLEDGED"


class DiagnosisStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Incident(BaseModel):
    """Proactive incident created from one or more anomaly signals."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    incident_id: str = Field(
        default_factory=lambda: f"inc_{uuid4().hex}",
        min_length=5,
        max_length=64,
    )
    title: str = Field(min_length=1, max_length=200)
    severity: IncidentSeverity
    status: IncidentStatus = IncidentStatus.ACTIVE
    diagnosis_status: DiagnosisStatus = DiagnosisStatus.PENDING
    trigger_signal_ids: tuple[str, ...] = Field(min_length=1, max_length=100)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    fingerprint: str = Field(min_length=1, max_length=500)
    last_signal_at: datetime
    diagnosis_report_id: str | None = Field(default=None, max_length=80)

    @field_validator("created_at", "updated_at", "last_signal_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("incident timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def validate_signal_ids(self) -> "Incident":
        if len(set(self.trigger_signal_ids)) != len(self.trigger_signal_ids):
            raise ValueError("trigger_signal_ids must be unique")
        return self


class IncidentHandlingResult(BaseModel):
    """Outcome returned to Monitoring after one signal is handled."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    incident: Incident
    created: bool
    diagnosis_triggered: bool
    diagnosis_error: str | None = Field(default=None, max_length=500)
    proposal_id: str | None = Field(default=None, max_length=96)
    permission_decision: str | None = Field(default=None, max_length=32)


class IncidentTraceEvent(BaseModel):
    """Audit record for the pre-runtime Incident Manager lifecycle."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    event_id: str = Field(
        default_factory=lambda: f"itrace_{uuid4().hex}",
        min_length=8,
        max_length=80,
    )
    incident_id: str = Field(min_length=5, max_length=64)
    event_type: TraceEventType
    summary: str = Field(min_length=1, max_length=2_000)
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)

    @field_validator("created_at")
    @classmethod
    def require_trace_timestamp_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("incident trace timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def require_incident_manager_event(self) -> "IncidentTraceEvent":
        allowed = {
            TraceEventType.INCIDENT_CREATED,
            TraceEventType.INCIDENT_RECOVERED,
            TraceEventType.DIAGNOSIS_STARTED,
            TraceEventType.DIAGNOSIS_COMPLETED,
            TraceEventType.ACTION_PROPOSAL_CREATED,
            TraceEventType.PERMISSION_CHECKED,
        }
        if self.event_type not in allowed:
            raise ValueError("incident manager trace uses an unsupported event type")
        return self
