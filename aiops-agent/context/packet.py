"""Immutable stage-specific context packet contracts."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from context.cards import EvidenceCard
from skills.models import SkillContext


class ContextStage(str, Enum):
    PLANNER = "planner"
    REFLECTION = "reflection"


class ExcludedEvidence(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    evidence_id: str = Field(min_length=8, max_length=80)
    reason: str = Field(min_length=1, max_length=200)


class ContextBudget(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    max_evidence_cards: int = Field(ge=1)
    max_samples_per_card: int = Field(ge=0)
    max_sample_chars: int = Field(ge=1)
    selected_evidence_count: int = Field(ge=0)
    excluded_evidence_count: int = Field(ge=0)
    estimated_chars: int = Field(ge=0)


class ContextPacket(BaseModel):
    """Bounded LLM input assembled from Evidence metadata and summaries."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    context_id: str = Field(
        default_factory=lambda: f"ctx_{uuid4().hex}",
        min_length=8,
        max_length=80,
    )
    stage: ContextStage
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    selected_skill: SkillContext | None = None
    evidence_cards: tuple[EvidenceCard, ...] = Field(default_factory=tuple)
    latest_observation: EvidenceCard | None = None
    selected_evidence_ids: tuple[str, ...] = Field(default_factory=tuple)
    excluded: tuple[ExcludedEvidence, ...] = Field(default_factory=tuple)
    budget: ContextBudget

    @model_validator(mode="after")
    def validate_selected_cards(self) -> "ContextPacket":
        card_ids = [card.evidence_id for card in self.evidence_cards]
        if self.latest_observation is not None:
            card_ids.append(self.latest_observation.evidence_id)
        if len(card_ids) != len(set(card_ids)):
            raise ValueError("ContextPacket cannot contain duplicate EvidenceCards")
        if set(card_ids) != set(self.selected_evidence_ids):
            raise ValueError("selected_evidence_ids must match packet EvidenceCards")
        if len(card_ids) > self.budget.max_evidence_cards:
            raise ValueError("ContextPacket exceeds the evidence card budget")
        if self.stage is ContextStage.REFLECTION and self.latest_observation is None:
            raise ValueError("reflection context requires the latest observation")
        if self.stage is ContextStage.REFLECTION and self.selected_skill is not None:
            raise ValueError("Skills are Planner guidance and cannot enter Reflection context")
        return self
