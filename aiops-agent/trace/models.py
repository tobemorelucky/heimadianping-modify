"""Pydantic models for append-only Agent trace events."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TraceEventType(str, Enum):
    """Stable event names used by trace persistence and replay."""

    MONITORING_TICK_STARTED = "monitoring_tick_started"
    OBSERVATION_COLLECTED = "observation_collected"
    ANOMALY_DETECTED = "anomaly_detected"
    INCIDENT_CREATED = "incident_created"
    SKILL_SELECTED = "skill_selected"
    SKILL_LOADED = "skill_loaded"
    DIAGNOSIS_STARTED = "diagnosis_started"
    DIAGNOSIS_COMPLETED = "diagnosis_completed"
    ACTION_PROPOSAL_CREATED = "action_proposal_created"
    PERMISSION_CHECKED = "permission_checked"
    PLAN_CREATED = "plan_created"
    TOOL_STARTED = "tool_started"
    TOOL_COMPLETED = "tool_completed"
    TOOL_FAILED = "tool_failed"
    EVIDENCE_ADDED = "evidence_added"
    CONTEXT_BUILT = "context_built"
    HYPOTHESIS_UPDATED = "hypothesis_updated"
    REFLECTION_COMPLETED = "reflection_completed"
    REPORT_GENERATED = "report_generated"
    HUMAN_REVIEWED = "human_reviewed"
    INCIDENT_CLOSED = "incident_closed"


class AgentTraceEvent(BaseModel):
    """One auditable event without exposing hidden model reasoning."""

    model_config = ConfigDict(str_strip_whitespace=True)

    event_id: str = Field(
        default_factory=lambda: f"trace_{uuid4().hex}",
        min_length=7,
        max_length=72,
    )
    incident_id: str = Field(min_length=5, max_length=64)
    sequence: int = Field(ge=0)
    event_type: TraceEventType
    stage: str = Field(min_length=1, max_length=64)
    objective: Optional[str] = Field(default=None, max_length=500)
    summary: str = Field(min_length=1, max_length=2_000)
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("created_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("trace timestamps must include a timezone")
        return value
