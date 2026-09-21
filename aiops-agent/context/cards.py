"""Small, model-safe views derived from persisted Evidence."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator

from tool_contracts import EvidenceCompleteness, ObservationStatus


class EvidenceCard(BaseModel):
    """Bounded evidence representation that never contains raw tool data."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    evidence_id: str = Field(min_length=8, max_length=80)
    source: str = Field(min_length=1, max_length=200)
    kind: str = Field(min_length=1, max_length=64)
    summary: str = Field(min_length=1, max_length=500)
    facts: tuple[str, ...] = Field(default_factory=tuple, max_length=8)
    samples: tuple[str, ...] = Field(default_factory=tuple, max_length=4)
    interpretation: str | None = Field(default=None, max_length=300)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    score: float = Field(ge=0.0, le=1.0)
    status: ObservationStatus
    completeness: EvidenceCompleteness
    collected_at: datetime
    fingerprint: str = Field(min_length=16, max_length=64)

    @field_validator("collected_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("EvidenceCard collected_at must include a timezone")
        return value
