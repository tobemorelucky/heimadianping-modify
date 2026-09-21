"""Diagnosis report contracts for the Phase 1 closed loop."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from reasoning.models import Hypothesis


class DiagnosisStatus(str, Enum):
    """Whether the fake evidence verified a root cause."""

    CONFIRMED = "confirmed"
    INCONCLUSIVE = "inconclusive"


class DiagnosisReport(BaseModel):
    """Final, evidence-linked output of one deterministic diagnosis run."""

    model_config = ConfigDict(frozen=True)

    report_id: str = Field(default_factory=lambda: f"report_{uuid4().hex}")
    incident_id: str = Field(min_length=5, max_length=64)
    status: DiagnosisStatus
    title: str = Field(min_length=1, max_length=200)
    conclusion: str = Field(min_length=1, max_length=2_000)
    root_cause: Optional[str] = Field(default=None, max_length=500)
    confidence: float = Field(ge=0.0, le=1.0)
    evidence_ids: List[str] = Field(default_factory=list)
    final_hypothesis: Hypothesis | None = None
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class DiagnosisRunResult(BaseModel):
    """Return value from the Runtime Orchestrator."""

    incident_id: str
    report: DiagnosisReport
