"""Pydantic contracts for deterministic HMDP FaultBench runs."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from tool_contracts import ToolObservation


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
    observation_source: Literal["fixture", "real_recording"] = "fixture"
    observation_recording: str | None = Field(default=None, max_length=300)

    @model_validator(mode="after")
    def require_real_recording_path(self) -> "FaultCase":
        if self.observation_source == "real_recording" and not self.observation_recording:
            raise ValueError("real-recording FaultCase requires observation_recording")
        if self.observation_source == "fixture" and self.observation_recording is not None:
            raise ValueError("fixture FaultCase must not declare observation_recording")
        return self


class RealObservationRecording(BaseModel):
    """Immutable evidence captured from an isolated real environment run."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    recording_type: Literal["real_environment"]
    fault_id: str = Field(pattern=r"^[A-Z][A-Z0-9_]+$", max_length=100)
    captured_at: datetime
    source_document: str = Field(min_length=1, max_length=300)
    environment: str = Field(min_length=1, max_length=500)
    baseline: tuple[ToolObservation, ...] = Field(min_length=1)
    fault: tuple[ToolObservation, ...] = Field(min_length=1)
    normalized_validation: "RealDetectionValidation | None" = None

    @field_validator("captured_at")
    @classmethod
    def require_recording_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("recording timestamp must include a timezone")
        return value


class RealDetectionValidation(BaseModel):
    """Signal/Incident result backed by persisted real ToolObservations."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    validated_at: datetime
    observations: tuple[ToolObservation, ...] = Field(min_length=4)
    signal_id: str = Field(min_length=8, max_length=80)
    rule_id: str = Field(min_length=1, max_length=128)
    rule_version: int = Field(ge=1)
    observation_refs: tuple[str, ...] = Field(min_length=4)
    signal_facts: dict
    incident_id: str = Field(min_length=8, max_length=80)
    incident_status: Literal["ACTIVE", "RECOVERED", "ACKNOWLEDGED"]
    diagnosis_status: Literal["PENDING", "RUNNING", "COMPLETED", "FAILED"]
    report_id: str = Field(min_length=8, max_length=80)
    root_cause: str = Field(min_length=1, max_length=500)
    confidence: float = Field(ge=0.0, le=1.0)

    @field_validator("validated_at")
    @classmethod
    def require_validation_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("validation timestamp must include a timezone")
        return value

    @model_validator(mode="after")
    def refs_must_be_recorded(self) -> "RealDetectionValidation":
        recorded = {item.evidence_id for item in self.observations}
        if set(self.observation_refs) != recorded:
            raise ValueError("signal observation refs must match recorded observations")
        return self


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


class MysqlPersistenceFixture(BaseModel):
    """Deterministic three-tool diagnostic fixture; no database is mutated."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    scenario: Literal["mysql_persistence_failure"]
    fault_id: str
    timestamp: datetime
    kafka: KafkaFixture
    business_metrics: "BusinessMetricsFixture"
    mysql_health: "MysqlHealthFixture"

    @field_validator("timestamp")
    @classmethod
    def require_timestamp_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("fixture timestamp must include a timezone")
        return value


class BusinessMetricsFixture(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    seckill_request_count: int = Field(ge=0)
    lua_admission_success_count: int = Field(ge=0)
    kafka_message_sent_count: int = Field(ge=0)
    order_created_success_count: int = Field(ge=0)
    order_created_failure_count: int = Field(ge=0)


class MysqlHealthFixture(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source_role: Literal["hmdp-consumer"]
    database_reachable: bool | None
    connection_test_status: Literal[
        "valid", "invalid", "connection_failed", "timeout", "acquisition_error",
        "pool_exhausted",
    ]
    health_state: Literal["HEALTHY", "DEGRADED", "FAILED", "UNKNOWN"]
    failure_class: tuple[Literal[
        "DATABASE_UNAVAILABLE", "CONNECTION_TIMEOUT", "POOL_EXHAUSTED", "UNKNOWN"
    ], ...]
    hikari_active: int | None = Field(ge=0)
    hikari_idle: int | None = Field(ge=0)
    connection_timeout_count: int | None = Field(ge=0)
    error_count: int | None = Field(ge=0)
    unavailable_metrics: tuple[str, ...] = ()


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
