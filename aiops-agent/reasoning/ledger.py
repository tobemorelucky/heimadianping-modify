"""In-memory, incident-scoped Evidence-grounded Hypothesis Ledger."""

from __future__ import annotations

from datetime import datetime
from typing import Callable

from reasoning.models import Hypothesis, HypothesisStatus, utc_now


class HypothesisLedger:
    """Maintain explicit hypothesis state for one diagnosis run only."""

    _STATUS_RANK = {
        HypothesisStatus.SUPPORTED: 3,
        HypothesisStatus.UNKNOWN: 2,
        HypothesisStatus.CONTRADICTED: 1,
        HypothesisStatus.REJECTED: 0,
    }

    def __init__(self, *, clock: Callable[[], datetime] = utc_now) -> None:
        self.clock = clock
        self._items: dict[str, Hypothesis] = {}

    def create(
        self,
        description: str,
        *,
        confidence: float = 0.0,
        hypothesis_id: str | None = None,
        deduplicate: bool = True,
    ) -> Hypothesis:
        """Create UNKNOWN state, reusing an exact active description when requested."""

        normalized = self._normalize(description)
        if deduplicate:
            existing = next(
                (
                    item
                    for item in self._items.values()
                    if self._normalize(item.description) == normalized
                    and item.status is not HypothesisStatus.REJECTED
                ),
                None,
            )
            if existing is not None:
                return existing
        now = self.clock()
        payload = {
            "description": description,
            "confidence": confidence,
            "created_at": now,
            "updated_at": now,
        }
        if hypothesis_id is not None:
            payload["hypothesis_id"] = hypothesis_id
        hypothesis = Hypothesis.model_validate(payload)
        if hypothesis.hypothesis_id in self._items:
            raise ValueError(f"duplicate hypothesis_id: {hypothesis.hypothesis_id}")
        self._items[hypothesis.hypothesis_id] = hypothesis
        return hypothesis

    def update(
        self,
        hypothesis_id: str,
        *,
        description: str | None = None,
        status: HypothesisStatus | str | None = None,
        confidence: float | None = None,
    ) -> Hypothesis:
        current = self.get(hypothesis_id)
        changes = {"updated_at": self.clock()}
        if description is not None:
            changes["description"] = description
        if status is not None:
            changes["status"] = HypothesisStatus(status)
        if confidence is not None:
            changes["confidence"] = confidence
        updated = current.model_copy(update=changes)
        updated = Hypothesis.model_validate(updated.model_dump())
        self._items[hypothesis_id] = updated
        return updated

    def add_supporting_evidence(
        self,
        hypothesis_id: str,
        evidence_ref: str,
        *,
        confidence: float | None = None,
    ) -> Hypothesis:
        current = self.get(hypothesis_id)
        if evidence_ref in current.contradicting_evidence_refs:
            raise ValueError("Evidence already contradicts this hypothesis")
        refs = tuple(dict.fromkeys((*current.supporting_evidence_refs, evidence_ref)))
        updated = current.model_copy(
            update={
                "status": HypothesisStatus.SUPPORTED,
                "confidence": current.confidence if confidence is None else confidence,
                "supporting_evidence_refs": refs,
                "updated_at": self.clock(),
            }
        )
        updated = Hypothesis.model_validate(updated.model_dump())
        self._items[hypothesis_id] = updated
        return updated

    def add_contradicting_evidence(
        self,
        hypothesis_id: str,
        evidence_ref: str,
        *,
        confidence: float | None = None,
    ) -> Hypothesis:
        current = self.get(hypothesis_id)
        if evidence_ref in current.supporting_evidence_refs:
            raise ValueError("Evidence already supports this hypothesis")
        refs = tuple(
            dict.fromkeys((*current.contradicting_evidence_refs, evidence_ref))
        )
        updated = current.model_copy(
            update={
                "status": HypothesisStatus.CONTRADICTED,
                "confidence": current.confidence if confidence is None else confidence,
                "contradicting_evidence_refs": refs,
                "updated_at": self.clock(),
            }
        )
        updated = Hypothesis.model_validate(updated.model_dump())
        self._items[hypothesis_id] = updated
        return updated

    def get(self, hypothesis_id: str) -> Hypothesis:
        try:
            return self._items[hypothesis_id]
        except KeyError as exc:
            raise KeyError(f"unknown hypothesis_id: {hypothesis_id}") from exc

    def rank(self) -> tuple[Hypothesis, ...]:
        """Return deterministic state/confidence ordering for Context and reporting."""

        return tuple(
            sorted(
                self._items.values(),
                key=lambda item: (
                    self._STATUS_RANK[item.status],
                    item.confidence,
                    item.updated_at,
                    item.hypothesis_id,
                ),
                reverse=True,
            )
        )

    @staticmethod
    def _normalize(value: str) -> str:
        return " ".join(value.casefold().split())
