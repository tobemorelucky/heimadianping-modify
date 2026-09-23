"""Deterministic Observation-to-AnomalySignal evaluation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from monitoring.detector.models import (
    AnomalySignal, DetectionRule, KafkaConsumerDownCondition,
    OrderPersistenceFailureCondition,
)
from monitoring.detector.order_persistence import (
    DetectionAssessment, assess_order_persistence,
)
from monitoring.detector.signal_store import SignalStore
from monitoring.models import (
    CollectionResult,
    MonitoringTraceEvent,
    StoredObservation,
)
from monitoring.observation_store import ObservationStore
from tool_contracts import EvidenceCompleteness, ObservationStatus, ToolObservation
from trace.models import TraceEventType


@dataclass(frozen=True)
class RecoveryConfirmation:
    fingerprint: str
    observation_refs: tuple[str, ...]
    first_seen: datetime
    last_seen: datetime


class DeterministicAnomalyDetector:
    """Evaluate persisted windows without LLM inference or Incident creation."""

    def __init__(
        self,
        observation_store: ObservationStore,
        signal_store: SignalStore,
    ) -> None:
        self.observation_store = observation_store
        self.signal_store = signal_store

    def evaluate(self, result: CollectionResult) -> list[AnomalySignal]:
        """Evaluate all active rules matching the latest Observation kind."""

        observation = result.observation
        if observation.status in {ObservationStatus.ERROR, ObservationStatus.TIMEOUT}:
            return []
        signals: list[AnomalySignal] = []
        for rule in self.signal_store.list_active_rules(observation.kind):
            if isinstance(rule.condition, OrderPersistenceFailureCondition):
                signal = self._evaluate_order_persistence(rule, result)
            elif observation.status is ObservationStatus.SUCCESS:
                signal = self._evaluate_rule(rule, result)
            else:
                signal = None
            if signal is not None:
                signals.append(signal)
        return signals

    def evaluate_recovery(self, result: CollectionResult) -> list[RecoveryConfirmation]:
        """Confirm recovery only from consecutive complete healthy windows."""

        current = result.observation
        if current.status is not ObservationStatus.SUCCESS:
            return []
        confirmations: list[RecoveryConfirmation] = []
        for rule in self.signal_store.list_active_rules(current.kind):
            if not isinstance(rule.condition, KafkaConsumerDownCondition):
                continue
            values = self._fingerprint_values(current, rule.fingerprint_fields)
            if values is None:
                continue
            history = self.observation_store.list_observations(
                schedule_id=result.run.schedule_id,
                source_kind=None,
                since=result.run.timestamp - timedelta(seconds=rule.lookback_window),
                until=result.run.timestamp,
            )
            required = rule.condition.consecutive_windows
            if len(history) < required:
                continue
            consecutive = history[-required:]
            if consecutive[-1].collection_run_id != result.run.collection_run_id:
                continue
            if not all(
                self._same_fingerprint(item.observation, rule, values)
                and self._matches_recovery(item.observation, rule)
                for item in consecutive
            ):
                continue
            confirmations.append(
                RecoveryConfirmation(
                    fingerprint=self._build_fingerprint(rule, values),
                    observation_refs=tuple(
                        item.observation.evidence_id for item in consecutive
                    ),
                    first_seen=consecutive[0].timestamp,
                    last_seen=consecutive[-1].timestamp,
                )
            )
        return confirmations

    def _evaluate_rule(
        self,
        rule: DetectionRule,
        result: CollectionResult,
    ) -> AnomalySignal | None:
        current = result.observation
        fingerprint_values = self._fingerprint_values(
            current,
            rule.fingerprint_fields,
        )
        if fingerprint_values is None:
            return None
        fingerprint = self._build_fingerprint(rule, fingerprint_values)
        window_end = result.run.timestamp
        window_start = window_end - timedelta(seconds=rule.lookback_window)
        history = self.observation_store.list_observations(
            schedule_id=result.run.schedule_id,
            source_kind=None,
            since=window_start,
            until=window_end,
        )
        required_windows = rule.condition.consecutive_windows
        if len(history) < required_windows:
            return None
        consecutive = history[-required_windows:]
        if consecutive[-1].collection_run_id != result.run.collection_run_id:
            return None
        if not all(
            self._same_fingerprint(item.observation, rule, fingerprint_values)
            and self._matches_condition(item.observation, rule)
            for item in consecutive
        ):
            return None
        previous = self.signal_store.latest_signal(
            rule_id=rule.rule_id,
            version=rule.version,
            fingerprint=fingerprint,
        )
        if previous is not None:
            cooldown_end = previous.last_seen + timedelta(seconds=rule.cooldown)
            if window_end < cooldown_end:
                return None

        lag = self._lag(current.data)
        signal = AnomalySignal(
            rule_id=rule.rule_id,
            version=rule.version,
            fingerprint=fingerprint,
            severity=rule.severity,
            facts={
                **fingerprint_values,
                "member_count": current.data["member_count"],
                "lag": lag,
                "lag_threshold": rule.condition.lag_greater_than,
                "consecutive_windows": required_windows,
                "condition": "member_count == 0 AND lag > threshold",
            },
            observation_refs=tuple(
                item.observation.evidence_id for item in consecutive
            ),
            first_seen=consecutive[0].timestamp,
            last_seen=consecutive[-1].timestamp,
            created_at=window_end,
        )
        self.signal_store.save_signal(signal)
        self.observation_store.save_trace_event(
            MonitoringTraceEvent(
                schedule_id=result.run.schedule_id,
                collection_run_id=result.run.collection_run_id,
                event_type=TraceEventType.ANOMALY_DETECTED,
                summary=f"Rule {rule.rule_id} emitted anomaly signal.",
                payload={
                    "signal_id": signal.signal_id,
                    "rule_id": signal.rule_id,
                    "version": signal.version,
                    "fingerprint": signal.fingerprint,
                    "severity": signal.severity.value,
                    "facts": signal.facts,
                    "observation_refs": list(signal.observation_refs),
                },
                created_at=window_end,
            )
        )
        return signal

    def assess_order_persistence(
        self, rule: DetectionRule, result: CollectionResult
    ) -> DetectionAssessment:
        """Expose insufficient vs non-matching for deterministic tests and audits."""

        if not isinstance(rule.condition, OrderPersistenceFailureCondition):
            raise ValueError("order persistence rule required")
        history = self.observation_store.list_observations_across_schedules(
            since=result.run.timestamp - timedelta(seconds=rule.lookback_window),
            until=result.run.timestamp,
        )
        return assess_order_persistence(history, result.observation, rule.condition)

    def _evaluate_order_persistence(
        self, rule: DetectionRule, result: CollectionResult
    ) -> AnomalySignal | None:
        assessment = self.assess_order_persistence(rule, result)
        if assessment.outcome != "matched":
            return None
        fingerprint = self._build_fingerprint(rule, {
            "topic": assessment.facts["topic"],
            "consumer_group": assessment.facts["consumer_group"],
            "source_role": assessment.facts["source_role"],
        })
        previous = self.signal_store.latest_signal(
            rule_id=rule.rule_id, version=rule.version, fingerprint=fingerprint
        )
        if previous is not None and result.run.timestamp < (
            previous.last_seen + timedelta(seconds=rule.cooldown)
        ):
            return None
        signal = AnomalySignal(
            rule_id=rule.rule_id,
            version=rule.version,
            fingerprint=fingerprint,
            severity=rule.severity,
            facts=assessment.facts,
            observation_refs=assessment.observation_refs,
            first_seen=next(
                (
                    item.timestamp for item in self.observation_store.list_observations_across_schedules(
                        since=result.run.timestamp - timedelta(seconds=rule.lookback_window),
                        until=result.run.timestamp,
                    )
                    if item.observation.evidence_id == assessment.observation_refs[0]
                ),
                result.run.timestamp,
            ),
            last_seen=result.run.timestamp,
            created_at=result.run.timestamp,
        )
        self.signal_store.save_signal(signal)
        self.observation_store.save_trace_event(
            MonitoringTraceEvent(
                schedule_id=result.run.schedule_id,
                collection_run_id=result.run.collection_run_id,
                event_type=TraceEventType.ANOMALY_DETECTED,
                summary=f"Rule {rule.rule_id} emitted correlated anomaly signal.",
                payload={
                    "signal_id": signal.signal_id,
                    "rule_id": signal.rule_id,
                    "fingerprint": signal.fingerprint,
                    "facts": signal.facts,
                    "observation_refs": list(signal.observation_refs),
                },
                created_at=result.run.timestamp,
            )
        )
        return signal

    @staticmethod
    def _matches_condition(
        observation: ToolObservation,
        rule: DetectionRule,
    ) -> bool:
        if observation.status is not ObservationStatus.SUCCESS:
            return False
        if observation.kind != rule.source_kind:
            return False
        member_count = observation.data.get("member_count")
        lag = DeterministicAnomalyDetector._lag(observation.data)
        if isinstance(member_count, bool) or isinstance(lag, bool):
            return False
        if not isinstance(member_count, int) or not isinstance(lag, (int, float)):
            return False
        return (
            member_count == rule.condition.member_count_equals
            and lag > rule.condition.lag_greater_than
        )

    @staticmethod
    def _matches_recovery(
        observation: ToolObservation,
        rule: DetectionRule,
    ) -> bool:
        if (observation.status is not ObservationStatus.SUCCESS
                or observation.completeness is not EvidenceCompleteness.COMPLETE):
            return False
        if observation.kind != rule.source_kind:
            return False
        data = observation.data
        member_count = data.get("member_count")
        lag = DeterministicAnomalyDetector._lag(data)
        return (
            type(member_count) is int
            and member_count > rule.condition.member_count_equals
            and type(lag) in (int, float)
            and 0 <= lag <= rule.condition.lag_greater_than
            and data.get("topic_exists") is True
            and data.get("offsets_complete") is True
            and data.get("partitions_truncated") is False
        )

    @staticmethod
    def _lag(data: dict[str, Any]) -> int | float | None:
        lag = data.get("total_lag")
        return data.get("lag") if lag is None else lag

    @staticmethod
    def _fingerprint_values(
        observation: ToolObservation,
        fields: tuple[str, ...],
    ) -> dict[str, Any] | None:
        values: dict[str, Any] = {}
        for field in fields:
            value: Any = observation.data
            for part in field.split("."):
                if not isinstance(value, dict) or part not in value:
                    return None
                value = value[part]
            if value is None or value == "":
                return None
            values[field] = value
        return values

    @staticmethod
    def _same_fingerprint(
        observation: ToolObservation,
        rule: DetectionRule,
        expected: dict[str, Any],
    ) -> bool:
        actual = DeterministicAnomalyDetector._fingerprint_values(
            observation,
            rule.fingerprint_fields,
        )
        return actual == expected

    @staticmethod
    def _build_fingerprint(
        rule: DetectionRule,
        values: dict[str, Any],
    ) -> str:
        identity = ",".join(
            f"{key}={json.dumps(value, ensure_ascii=False, sort_keys=True)}"
            for key, value in values.items()
        )
        return f"{rule.rule_id}:v{rule.version}:{identity}"
