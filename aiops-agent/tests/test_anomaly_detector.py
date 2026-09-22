"""Phase M2 deterministic Kafka anomaly detection tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from itertools import count

import pytest

from memory.database import SQLiteDatabase
from monitoring.detector.detector import DeterministicAnomalyDetector
from monitoring.detector.rules import (
    KAFKA_CONSUMER_DOWN_RULE_ID,
    kafka_consumer_down_rule,
)
from monitoring.detector.signal_store import SignalStore
from monitoring.models import CollectionResult, CollectionRun, CollectionSchedule
from monitoring.observation_store import ObservationStore
from tool_contracts import (
    EvidenceCompleteness,
    ObservationStatus,
    ToolError,
    ToolObservation,
)


BASE_TIME = datetime(2026, 9, 19, 3, 0, tzinfo=timezone.utc)
_IDS = count(1)


@pytest.fixture
def detector_env(tmp_path):
    database = SQLiteDatabase(tmp_path / "detector.db")
    observation_store = ObservationStore(database)
    observation_store.initialize()
    observation_store.upsert_schedule(
        CollectionSchedule(
            schedule_id="kafka-detector-30s",
            tool_name="get_kafka_status",
            interval_seconds=30,
            enabled=True,
            timeout_seconds=1.0,
            next_run_at=BASE_TIME,
        )
    )
    signal_store = SignalStore(database)
    signal_store.initialize()
    signal_store.ensure_rule(
        kafka_consumer_down_rule(
            lag_threshold=1_000,
            consecutive_windows=2,
            lookback_window="2m",
            cooldown="10m",
        )
    )
    detector = DeterministicAnomalyDetector(observation_store, signal_store)
    return observation_store, signal_store, detector


def persist_kafka_observation(
    observation_store: ObservationStore,
    *,
    timestamp: datetime,
    member_count: int = 1,
    lag: int = 0,
    status: ObservationStatus = ObservationStatus.SUCCESS,
) -> CollectionResult:
    sequence = next(_IDS)
    evidence_id = f"evi_detector_{sequence:04d}"
    run_id = f"col_detector_{sequence:04d}"
    if status in {ObservationStatus.ERROR, ObservationStatus.TIMEOUT}:
        observation = ToolObservation(
            evidence_id=evidence_id,
            status=status,
            kind="kafka_consumer_status",
            source="hmdp.kafka.consumer_group",
            source_tool="get_kafka_status",
            collected_at=timestamp,
            summary=f"Kafka observation returned {status.value}.",
            completeness=EvidenceCompleteness.UNKNOWN,
            data={
                "topic": "hmdp.seckill.order.create.v1",
                "consumer_group": "hmdp-seckill-order-create-v1",
                "member_count": member_count,
                "total_lag": lag,
                "topic_exists": True,
                "offsets_complete": True,
                "partitions_truncated": False,
            },
            error=ToolError(
                error_type="KafkaError",
                message="Kafka status unavailable",
                retryable=True,
            ),
        )
    else:
        completeness = (
            EvidenceCompleteness.COMPLETE
            if status is ObservationStatus.SUCCESS
            else EvidenceCompleteness.PARTIAL
        )
        observation = ToolObservation(
            evidence_id=evidence_id,
            status=status,
            kind="kafka_consumer_status",
            source="hmdp.kafka.consumer_group",
            source_tool="get_kafka_status",
            collected_at=timestamp,
            summary="Kafka consumer group status collected.",
            completeness=completeness,
            data={
                "topic": "hmdp.seckill.order.create.v1",
                "consumer_group": "hmdp-seckill-order-create-v1",
                "member_count": member_count,
                "total_lag": lag,
                "topic_exists": True,
                "offsets_complete": True,
                "partitions_truncated": False,
            },
        )
    run = CollectionRun(
        collection_run_id=run_id,
        schedule_id="kafka-detector-30s",
        tool="get_kafka_status",
        status=status,
        timestamp=timestamp,
        evidence_ref=evidence_id,
        duration_ms=5,
    )
    observation_store.save_collection_run(run, observation)
    return CollectionResult(run=run, observation=observation)


def test_normal_kafka_does_not_emit_signal(detector_env):
    observation_store, signal_store, detector = detector_env
    first = persist_kafka_observation(
        observation_store,
        timestamp=BASE_TIME,
        member_count=1,
        lag=0,
    )
    second = persist_kafka_observation(
        observation_store,
        timestamp=BASE_TIME + timedelta(seconds=30),
        member_count=1,
        lag=25,
    )

    assert detector.evaluate(first) == []
    assert detector.evaluate(second) == []
    assert signal_store.list_signals() == []


def test_kafka_consumer_down_emits_persisted_signal(detector_env):
    observation_store, signal_store, detector = detector_env
    first = persist_kafka_observation(
        observation_store,
        timestamp=BASE_TIME,
        member_count=0,
        lag=50_000,
    )
    second = persist_kafka_observation(
        observation_store,
        timestamp=BASE_TIME + timedelta(seconds=30),
        member_count=0,
        lag=51_000,
    )

    assert detector.evaluate(first) == []
    signals = detector.evaluate(second)

    assert len(signals) == 1
    signal = signals[0]
    assert signal.rule_id == KAFKA_CONSUMER_DOWN_RULE_ID
    assert signal.version == 1
    assert signal.severity.value == "high"
    assert signal.status.value == "open"
    assert signal.facts["member_count"] == 0
    assert signal.facts["lag"] == 51_000
    assert signal.facts["lag_threshold"] == 1_000
    assert signal.observation_refs == (
        first.observation.evidence_id,
        second.observation.evidence_id,
    )
    assert "hmdp.seckill.order.create.v1" in signal.fingerprint
    assert len(signal_store.list_signals()) == 1
    traces = observation_store.list_trace_events()
    assert traces[-1]["event_type"] == "anomaly_detected"


def test_insufficient_consecutive_windows_does_not_emit(detector_env):
    observation_store, signal_store, detector = detector_env
    only_window = persist_kafka_observation(
        observation_store,
        timestamp=BASE_TIME,
        member_count=0,
        lag=50_000,
    )

    assert detector.evaluate(only_window) == []
    assert signal_store.list_signals() == []


def test_normal_window_breaks_consecutive_match(detector_env):
    observation_store, signal_store, detector = detector_env
    observations = [
        persist_kafka_observation(
            observation_store,
            timestamp=BASE_TIME,
            member_count=0,
            lag=50_000,
        ),
        persist_kafka_observation(
            observation_store,
            timestamp=BASE_TIME + timedelta(seconds=30),
            member_count=1,
            lag=50_000,
        ),
        persist_kafka_observation(
            observation_store,
            timestamp=BASE_TIME + timedelta(seconds=60),
            member_count=0,
            lag=50_000,
        ),
    ]

    for result in observations:
        assert detector.evaluate(result) == []
    assert signal_store.list_signals() == []


def test_cooldown_suppresses_duplicate_signal(detector_env):
    observation_store, signal_store, detector = detector_env
    observations = [
        persist_kafka_observation(
            observation_store,
            timestamp=BASE_TIME + timedelta(seconds=offset),
            member_count=0,
            lag=50_000 + offset,
        )
        for offset in (0, 30, 60)
    ]

    assert detector.evaluate(observations[0]) == []
    assert len(detector.evaluate(observations[1])) == 1
    assert detector.evaluate(observations[2]) == []
    assert len(signal_store.list_signals()) == 1
    assert [
        trace["event_type"] for trace in observation_store.list_trace_events()
    ].count("anomaly_detected") == 1


def test_observation_error_breaks_window_and_never_emits(detector_env):
    observation_store, signal_store, detector = detector_env
    first = persist_kafka_observation(
        observation_store,
        timestamp=BASE_TIME,
        member_count=0,
        lag=50_000,
    )
    failed = persist_kafka_observation(
        observation_store,
        timestamp=BASE_TIME + timedelta(seconds=30),
        member_count=0,
        lag=50_000,
        status=ObservationStatus.ERROR,
    )
    third = persist_kafka_observation(
        observation_store,
        timestamp=BASE_TIME + timedelta(seconds=60),
        member_count=0,
        lag=50_000,
    )

    assert detector.evaluate(first) == []
    assert detector.evaluate(failed) == []
    assert detector.evaluate(third) == []
    assert signal_store.list_signals() == []


def test_rule_and_signal_schema_are_persisted(detector_env):
    _, signal_store, _ = detector_env

    rules = signal_store.list_active_rules("kafka_consumer_status")

    assert len(rules) == 1
    assert rules[0].rule_id == KAFKA_CONSUMER_DOWN_RULE_ID
    assert rules[0].lookback_window == 120
    assert rules[0].cooldown == 600


def test_recovery_requires_two_complete_healthy_windows(detector_env):
    observation_store, _, detector = detector_env
    down = persist_kafka_observation(
        observation_store, timestamp=BASE_TIME, member_count=0, lag=50_000
    )
    first_healthy = persist_kafka_observation(
        observation_store,
        timestamp=BASE_TIME + timedelta(seconds=30),
        member_count=1,
        lag=25,
    )
    failed = persist_kafka_observation(
        observation_store,
        timestamp=BASE_TIME + timedelta(seconds=60),
        member_count=1,
        lag=25,
        status=ObservationStatus.ERROR,
    )
    second_healthy = persist_kafka_observation(
        observation_store,
        timestamp=BASE_TIME + timedelta(seconds=90),
        member_count=1,
        lag=25,
    )
    third_healthy = persist_kafka_observation(
        observation_store,
        timestamp=BASE_TIME + timedelta(seconds=120),
        member_count=1,
        lag=0,
    )

    for result in (down, first_healthy, failed, second_healthy):
        assert detector.evaluate_recovery(result) == []
    confirmations = detector.evaluate_recovery(third_healthy)
    assert len(confirmations) == 1
    assert confirmations[0].observation_refs == (
        second_healthy.observation.evidence_id,
        third_healthy.observation.evidence_id,
    )


def test_member_return_with_backlog_is_not_recovery(detector_env):
    observation_store, _, detector = detector_env
    first = persist_kafka_observation(
        observation_store, timestamp=BASE_TIME, member_count=1, lag=50_000
    )
    second = persist_kafka_observation(
        observation_store,
        timestamp=BASE_TIME + timedelta(seconds=30),
        member_count=1,
        lag=49_000,
    )
    assert detector.evaluate_recovery(first) == []
    assert detector.evaluate_recovery(second) == []
