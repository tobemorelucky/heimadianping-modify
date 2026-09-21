"""Deterministic ranking for the current application-log evidence source."""

from __future__ import annotations

from datetime import datetime

from context.cards import EvidenceCard


class EvidenceRanker:
    """Score cards using recency and an explicit source reliability policy."""

    _SOURCE_RELIABILITY = {
        "hmdp.spring_boot.runtime_logs": 0.9,
        "mcp://hmdp-readonly-tools": 0.7,
        "hmdp.kafka.consumer_group": 0.95,
        "hmdp.seckill.business_metrics": 0.9,
    }

    def score(self, card: EvidenceCard, *, reference_time: datetime) -> float:
        age_seconds = max(0.0, (reference_time - card.collected_at).total_seconds())
        time_relevance = 1.0 / (1.0 + age_seconds / 600.0)
        source_reliability = self._SOURCE_RELIABILITY.get(card.source, 0.5)
        return round(0.6 * time_relevance + 0.4 * source_reliability, 6)

    def rank(self, cards: list[EvidenceCard]) -> list[EvidenceCard]:
        if not cards:
            return []
        reference_time = max(card.collected_at for card in cards)
        scored = [
            card.model_copy(update={"score": self.score(card, reference_time=reference_time)})
            for card in cards
        ]
        return sorted(
            scored,
            key=lambda card: (card.score, card.collected_at, card.evidence_id),
            reverse=True,
        )
