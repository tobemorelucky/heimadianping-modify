"""Pydantic models shared by the future Agent Runtime."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def utc_now() -> datetime:
    """Return a timezone-aware timestamp for persisted models."""

    return datetime.now(timezone.utc)


class IncidentStatus(str, Enum):
    """Lifecycle states supported by the local MVP."""

    QUEUED = "queued"
    ANALYZING = "analyzing"
    PLANNING = "planning"
    INVESTIGATING = "investigating"
    REFLECTING = "reflecting"
    REPORTING = "reporting"
    AWAITING_HUMAN = "awaiting_human"
    CLOSED = "closed"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"


class IncidentSource(str, Enum):
    """Supported incident input sources."""

    HUMAN = "human"
    ALERT = "alert"
    FAULTBENCH = "faultbench"


class IncidentSeverity(str, Enum):
    """Coarse incident severity for local prioritization."""

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ObservationWindow(BaseModel):
    """Time range whose evidence is relevant to an incident."""

    model_config = ConfigDict(frozen=True)

    start: datetime
    end: datetime

    @field_validator("start", "end")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("observation timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def validate_order(self) -> "ObservationWindow":
        if self.end <= self.start:
            raise ValueError("observation window end must be after start")
        return self


class IncidentBudget(BaseModel):
    """Hard execution limits for one diagnosis task."""

    model_config = ConfigDict(frozen=True)

    max_tool_calls: int = Field(default=8, ge=0, le=8)
    max_reflections: int = Field(default=2, ge=0, le=2)
    max_duration_seconds: int = Field(default=180, ge=1, le=180)
    max_total_tokens: int = Field(default=30_000, ge=1, le=30_000)


class IncidentCreate(BaseModel):
    """Validated input accepted when a future incident is created."""

    model_config = ConfigDict(str_strip_whitespace=True)

    title: str = Field(min_length=1, max_length=200)
    description: str = Field(min_length=1, max_length=4_000)
    source: IncidentSource = IncidentSource.HUMAN
    severity: IncidentSeverity = IncidentSeverity.MEDIUM
    affected_components: List[str] = Field(default_factory=list, max_length=20)
    observation_window: Optional[ObservationWindow] = None
    budget: IncidentBudget = Field(default_factory=IncidentBudget)

    @field_validator("affected_components")
    @classmethod
    def normalize_components(cls, values: List[str]) -> List[str]:
        normalized = []
        for value in values:
            component = value.strip().lower()
            if not component:
                raise ValueError("affected component names must not be blank")
            if component not in normalized:
                normalized.append(component)
        return normalized


class IncidentTask(BaseModel):
    """Persistable incident task state owned by the future Task Manager."""

    model_config = ConfigDict(str_strip_whitespace=True)

    incident_id: str = Field(
        default_factory=lambda: f"inc_{uuid4().hex}",
        min_length=5,
        max_length=64,
    )
    title: str = Field(min_length=1, max_length=200)
    description: str = Field(min_length=1, max_length=4_000)
    source: IncidentSource
    severity: IncidentSeverity
    status: IncidentStatus = IncidentStatus.QUEUED
    affected_components: List[str] = Field(default_factory=list, max_length=20)
    observation_window: Optional[ObservationWindow] = None
    budget: IncidentBudget = Field(default_factory=IncidentBudget)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timestamp_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("incident timestamps must include a timezone")
        return value

    @classmethod
    def from_create(cls, request: IncidentCreate) -> "IncidentTask":
        """Create a queued task without implementing lifecycle behavior."""

        return cls(**request.model_dump())

