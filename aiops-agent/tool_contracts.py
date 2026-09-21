"""Shared, tool-agnostic contracts used across MCP and Agent Runtime."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ObservationStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    ERROR = "error"
    TIMEOUT = "timeout"


class EvidenceCompleteness(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


class EvidenceWindow(BaseModel):
    model_config = ConfigDict(frozen=True)

    start: datetime
    end: datetime

    @model_validator(mode="after")
    def validate_order(self) -> "EvidenceWindow":
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("evidence window timestamps must include a timezone")
        if self.end <= self.start:
            raise ValueError("evidence window end must be after start")
        return self


class ToolError(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    error_type: str = Field(min_length=1, max_length=100)
    message: str = Field(min_length=1, max_length=500)
    retryable: bool = False


class ToolObservation(BaseModel):
    """Uniform output of every tool call, including failures and timeouts."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    evidence_id: str = Field(
        default_factory=lambda: f"evi_{uuid4().hex}",
        min_length=8,
        max_length=80,
    )
    status: ObservationStatus
    kind: str = Field(min_length=1, max_length=64)
    source: str = Field(min_length=1, max_length=200)
    source_tool: str = Field(min_length=1, max_length=128)
    collected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    observation_window: EvidenceWindow | None = None
    summary: str = Field(min_length=1, max_length=1_000)
    completeness: EvidenceCompleteness
    raw_ref: str | None = Field(default=None, max_length=500)
    data: dict[str, Any] = Field(default_factory=dict)
    error: ToolError | None = None

    @model_validator(mode="after")
    def validate_status_contract(self) -> "ToolObservation":
        failed = self.status in {ObservationStatus.ERROR, ObservationStatus.TIMEOUT}
        if failed and self.error is None:
            raise ValueError("error details are required for error and timeout observations")
        if not failed and self.error is not None:
            raise ValueError("successful or partial observations cannot contain an error")
        if self.status is ObservationStatus.SUCCESS and self.completeness is not EvidenceCompleteness.COMPLETE:
            raise ValueError("success observations must be complete")
        if self.status is ObservationStatus.PARTIAL and self.completeness is not EvidenceCompleteness.PARTIAL:
            raise ValueError("partial observations must have partial completeness")
        if failed and self.completeness is not EvidenceCompleteness.UNKNOWN:
            raise ValueError("error and timeout observations must have unknown completeness")
        return self


def build_failure_observation(
    *,
    source_tool: str,
    source: str,
    status: ObservationStatus,
    error_type: str,
    message: str,
    retryable: bool,
) -> ToolObservation:
    """Create a schema-valid error or timeout observation."""

    if status not in {ObservationStatus.ERROR, ObservationStatus.TIMEOUT}:
        raise ValueError("failure observation status must be error or timeout")
    return ToolObservation(
        status=status,
        kind="tool_error",
        source=source,
        source_tool=source_tool,
        summary=message,
        completeness=EvidenceCompleteness.UNKNOWN,
        data={"tool_name": source_tool},
        error=ToolError(
            error_type=error_type,
            message=message,
            retryable=retryable,
        ),
    )
