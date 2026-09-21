"""Build deterministic, bounded ContextPackets from supported Evidence sources."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from context.cards import EvidenceCard
from context.compressor import (
    BusinessMetricsEvidenceCompressor,
    KafkaEvidenceCompressor,
    LogEvidenceCompressor,
)
from context.packet import (
    ContextBudget,
    ContextPacket,
    ContextStage,
    ExcludedEvidence,
)
from context.ranker import EvidenceRanker
from runtime.evidence import Evidence
from reasoning.models import Hypothesis
from skills.models import SkillContext


@dataclass(frozen=True)
class ContextLimits:
    max_evidence_cards: int = 8
    max_samples_per_card: int = 3
    max_sample_chars: int = 300

    def __post_init__(self) -> None:
        if self.max_evidence_cards < 1:
            raise ValueError("max_evidence_cards must be positive")
        if self.max_samples_per_card < 0:
            raise ValueError("max_samples_per_card cannot be negative")
        if self.max_sample_chars < 1:
            raise ValueError("max_sample_chars must be positive")


class ContextManager:
    """Apply compression, ranking, deduplication, and size limits."""

    def __init__(
        self,
        limits: ContextLimits | None = None,
        ranker: EvidenceRanker | None = None,
    ) -> None:
        self.limits = limits or ContextLimits()
        self.ranker = ranker or EvidenceRanker()
        compressor_options = {
            "max_samples": self.limits.max_samples_per_card,
            "max_sample_chars": self.limits.max_sample_chars,
        }
        self.compressors = {
            "search_application_logs": LogEvidenceCompressor(**compressor_options),
            "get_kafka_status": KafkaEvidenceCompressor(**compressor_options),
            "get_business_metrics": BusinessMetricsEvidenceCompressor(
                **compressor_options
            ),
        }

    def build(
        self,
        evidence: Sequence[Evidence],
        *,
        stage: ContextStage | str = ContextStage.PLANNER,
        latest_evidence_id: str | None = None,
        selected_skill: SkillContext | None = None,
        current_hypotheses: Sequence[Hypothesis] = (),
    ) -> ContextPacket:
        stage = ContextStage(stage)
        if stage is ContextStage.REFLECTION and latest_evidence_id is None:
            raise ValueError("reflection context requires latest_evidence_id")

        compressed = [
            self._compress(item)
            for item in evidence
        ]
        ranked = self.ranker.rank(compressed)
        by_id = {card.evidence_id: card for card in ranked}
        if latest_evidence_id is not None and latest_evidence_id not in by_id:
            raise ValueError("latest_evidence_id is not present in Evidence")

        selected, excluded = self._deduplicate(
            ranked,
            preferred_evidence_id=latest_evidence_id,
        )
        selected, limit_excluded = self._apply_limit(
            selected,
            preferred_evidence_id=latest_evidence_id,
        )
        excluded.extend(limit_excluded)

        latest_card: EvidenceCard | None = None
        packet_cards = selected
        if stage is ContextStage.REFLECTION:
            latest_card = next(
                card for card in selected if card.evidence_id == latest_evidence_id
            )
            packet_cards = [
                card for card in selected if card.evidence_id != latest_evidence_id
            ]

        selected_ids = tuple(card.evidence_id for card in selected)
        estimated_chars = self._estimate_chars(selected)
        if selected_skill is not None:
            estimated_chars += len(selected_skill.instructions)
            estimated_chars += sum(
                len(reference.content) for reference in selected_skill.references
            )
        estimated_chars += sum(
            len(item.description)
            + sum(len(ref) for ref in item.supporting_evidence_refs)
            + sum(len(ref) for ref in item.contradicting_evidence_refs)
            for item in current_hypotheses
        )
        budget = ContextBudget(
            max_evidence_cards=self.limits.max_evidence_cards,
            max_samples_per_card=self.limits.max_samples_per_card,
            max_sample_chars=self.limits.max_sample_chars,
            selected_evidence_count=len(selected),
            excluded_evidence_count=len(excluded),
            estimated_chars=estimated_chars,
        )
        return ContextPacket(
            stage=stage,
            selected_skill=selected_skill,
            current_hypotheses=tuple(current_hypotheses),
            evidence_cards=tuple(packet_cards),
            latest_observation=latest_card,
            selected_evidence_ids=selected_ids,
            excluded=tuple(excluded),
            budget=budget,
        )

    def _compress(self, evidence: Evidence) -> EvidenceCard:
        try:
            compressor = self.compressors[evidence.source_tool]
        except KeyError as exc:
            raise ValueError(
                f"unsupported context source_tool: {evidence.source_tool}"
            ) from exc
        return compressor.compress(evidence)

    @staticmethod
    def _deduplicate(
        cards: list[EvidenceCard],
        *,
        preferred_evidence_id: str | None,
    ) -> tuple[list[EvidenceCard], list[ExcludedEvidence]]:
        grouped: dict[str, list[EvidenceCard]] = {}
        for card in cards:
            grouped.setdefault(card.fingerprint, []).append(card)

        kept_ids: set[str] = set()
        excluded: list[ExcludedEvidence] = []
        for duplicates in grouped.values():
            preferred = next(
                (
                    card
                    for card in duplicates
                    if card.evidence_id == preferred_evidence_id
                ),
                duplicates[0],
            )
            kept_ids.add(preferred.evidence_id)
            excluded.extend(
                ExcludedEvidence(
                    evidence_id=card.evidence_id,
                    reason=f"duplicate_fingerprint:{preferred.evidence_id}",
                )
                for card in duplicates
                if card.evidence_id != preferred.evidence_id
            )
        return [card for card in cards if card.evidence_id in kept_ids], excluded

    def _apply_limit(
        self,
        cards: list[EvidenceCard],
        *,
        preferred_evidence_id: str | None,
    ) -> tuple[list[EvidenceCard], list[ExcludedEvidence]]:
        selected = list(cards[: self.limits.max_evidence_cards])
        if preferred_evidence_id is not None and not any(
            card.evidence_id == preferred_evidence_id for card in selected
        ):
            preferred = next(
                card for card in cards if card.evidence_id == preferred_evidence_id
            )
            selected[-1] = preferred

        selected_ids = {card.evidence_id for card in selected}
        excluded = [
            ExcludedEvidence(
                evidence_id=card.evidence_id,
                reason="evidence_card_limit",
            )
            for card in cards
            if card.evidence_id not in selected_ids
        ]
        return selected, excluded

    @staticmethod
    def _estimate_chars(cards: Sequence[EvidenceCard]) -> int:
        return sum(
            len(card.summary)
            + sum(len(fact) for fact in card.facts)
            + sum(len(sample) for sample in card.samples)
            for card in cards
        )
