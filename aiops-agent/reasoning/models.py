"""Validated contracts for incident-scoped diagnostic hypotheses."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class HypothesisStatus(str, Enum):
    UNKNOWN = "UNKNOWN"
    SUPPORTED = "SUPPORTED"
    CONTRADICTED = "CONTRADICTED"
    REJECTED = "REJECTED"


class Hypothesis(BaseModel):
    """One explicit hypothesis whose evidence links are bound by the Runtime."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    hypothesis_id: str = Field(
        default_factory=lambda: f"hyp_{uuid4().hex}",
        min_length=8,
        max_length=80,
    )
    description: str = Field(min_length=1, max_length=500)
    status: HypothesisStatus = HypothesisStatus.UNKNOWN
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    supporting_evidence_refs: tuple[str, ...] = Field(default_factory=tuple)
    contradicting_evidence_refs: tuple[str, ...] = Field(default_factory=tuple)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("hypothesis timestamps must include a timezone")
        return value

    @field_validator("supporting_evidence_refs", "contradicting_evidence_refs")
    @classmethod
    def require_unique_evidence_refs(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if len(values) != len(set(values)):
            raise ValueError("hypothesis evidence references must be unique")
        if any(not value.strip() for value in values):
            raise ValueError("hypothesis evidence references must not be blank")
        return values

    @model_validator(mode="after")
    def validate_state(self) -> "Hypothesis":
        overlap = set(self.supporting_evidence_refs).intersection(
            self.contradicting_evidence_refs
        )
        if overlap:
            raise ValueError("one Evidence reference cannot both support and contradict")
        if self.updated_at < self.created_at:
            raise ValueError("hypothesis updated_at cannot precede created_at")
        return self
