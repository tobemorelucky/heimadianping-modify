"""Fingerprint and cooldown based proactive Incident deduplication."""

from __future__ import annotations

from datetime import timedelta

from incident.models import Incident
from incident.store import IncidentStore
from monitoring.detector.models import AnomalySignal


class IncidentDeduplicator:
    """Find an existing Incident for the same signal fingerprint."""

    def __init__(self, store: IncidentStore, *, cooldown_seconds: int = 600) -> None:
        if cooldown_seconds <= 0:
            raise ValueError("incident cooldown_seconds must be positive")
        self.store = store
        self.cooldown_seconds = cooldown_seconds

    def find_duplicate(self, signal: AnomalySignal) -> Incident | None:
        existing = self.store.latest_for_fingerprint(signal.fingerprint)
        if existing is None:
            return None
        cooldown_end = existing.last_signal_at + timedelta(
            seconds=self.cooldown_seconds
        )
        return existing if signal.last_seen < cooldown_end else None
