"""Pydantic contracts for deterministic HMDP FaultBench runs."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class FaultCase(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    fault_id: str = Field(pattern=r"^[A-Z][A-Z0-9_]+$", max_length=100)
    name: str = Field(min_length=1, max_length=200)
    description: str = Field(min_length=1, max_length=2_000)
    trigger_condition: str = Field(min_length=1, max_length=1_000)
    expected_root_cause: str = Field(min_length=1, max_length=500)
    expected_tools: tuple[str, ...] = Field(min_length=1)
    expected_evidence: tuple[str, ...] = Field(min_length=1)
    incident_title: str = Field(min_length=1, max_length=200)
    incident_description: str = Field(min_length=1, max_length=2_000)


class KafkaFixture(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    topic: str = Field(min_length=1, max_length=249)
    consumer_group: str = Field(min_length=1, max_length=255)
    lag: int = Field(ge=0)
    member_count: int = Field(ge=0)
    severity: Literal["low", "medium", "high", "critical"]


class LogFixture(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    timestamp: datetime
    message: str = Field(min_length=1, max_length=1_000)

    @field_validator("timestamp")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("fixture timestamp must include a timezone")
        return value


class FaultFixture(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    fault_id: str
    kafka: KafkaFixture
    log: LogFixture


class RootCauseMetric(BaseModel):
    model_config = ConfigDict(frozen=True)

    score: float = Field(ge=0.0, le=1.0)
    matched: bool
    expected: str
    actual: str | None


class EvidenceCoverageMetric(BaseModel):
    model_config = ConfigDict(frozen=True)

    score: float = Field(ge=0.0, le=1.0)
    expected: tuple[str, ...]
    observed: tuple[str, ...]
    missing: tuple[str, ...]


class ToolEfficiencyMetric(BaseModel):
    model_config = ConfigDict(frozen=True)

    score: float = Field(ge=0.0, le=1.0)
    tool_call_count: int = Field(ge=0)
    diagnosis_steps: int = Field(ge=0)
    redundant_tool_calls: int = Field(ge=0)


class TraceCompletenessMetric(BaseModel):
    model_config = ConfigDict(frozen=True)

    score: float = Field(ge=0.0, le=1.0)
    required: tuple[str, ...]
    present: tuple[str, ...]
    missing: tuple[str, ...]


class EvaluationMetrics(BaseModel):
    model_config = ConfigDict(frozen=True)

    passed: bool
    root_cause_accuracy: RootCauseMetric
    evidence_coverage: EvidenceCoverageMetric
    tool_efficiency: ToolEfficiencyMetric
    trace_completeness: TraceCompletenessMetric


class FinalDiagnosis(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: str
    title: str
    conclusion: str
    root_cause: str | None
    confidence: float = Field(ge=0.0, le=1.0)


class TraceSummary(BaseModel):
    model_config = ConfigDict(frozen=True)

    sequence: int = Field(ge=0)
    event_type: str
    stage: str
    summary: str


class EvaluationReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    fault_id: str
    final_diagnosis: FinalDiagnosis
    selected_tools: tuple[str, ...]
    evidence_ids: tuple[str, ...]
    trace_summary: tuple[TraceSummary, ...]
    metrics: EvaluationMetrics
