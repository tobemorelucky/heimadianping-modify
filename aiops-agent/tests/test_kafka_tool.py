"""Read-only Kafka MCP tool and context semantics."""

from datetime import datetime, timedelta, timezone

import pytest

from context.manager import ContextManager
from context.packet import ContextStage
from llm.mock import MockLLMProvider
from mcp_tools.kafka_status import (
    KafkaPartitionSnapshot,
    KafkaStatusRequest,
    KafkaStatusSnapshot,
    collect_kafka_status,
)
from runtime.evidence import Evidence
from runtime.reflection import ReflectionDecision, ReflectionEngine
from tool_contracts import EvidenceCompleteness, ObservationStatus


class SnapshotReader:
    def __init__(self, snapshot: KafkaStatusSnapshot) -> None:
        self.snapshot = snapshot

    def read(self, topic: str, consumer_group: str) -> KafkaStatusSnapshot:
        assert topic == self.snapshot.topic
        assert consumer_group == self.snapshot.consumer_group
        return self.snapshot


class FailingReader:
    def read(self, topic: str, consumer_group: str) -> KafkaStatusSnapshot:
        raise ConnectionError("broker unavailable")


def snapshot(*, lag: int, state: str = "stable", members: int = 1):
    return KafkaStatusSnapshot(
        topic="hmdp.seckill.order.create.v1",
        consumer_group="hmdp-seckill-order-create-v1",
        topic_exists=True,
        group_state=state,
        member_count=members,
        offsets_complete=True,
        partitions=(
            KafkaPartitionSnapshot(
                partition=0,
                committed_offset=100,
                end_offset=100 + lag,
                lag=lag,
            ),
        ),
    )


def collect(snapshot_reader, *, threshold: int = 1000):
    return collect_kafka_status(
        KafkaStatusRequest(),
        reader=snapshot_reader,
        default_topic="hmdp.seckill.order.create.v1",
        default_consumer_group="hmdp-seckill-order-create-v1",
        lag_threshold=threshold,
    )


def evidence_from_observation(observation, *, index: int) -> Evidence:
    return Evidence(
        incident_id="inc_kafka_context",
        step_id=f"step_kafka_{index}",
        collected_at=datetime.now(timezone.utc) + timedelta(seconds=index),
        **{
            key: value
            for key, value in observation.model_dump(mode="json").items()
            if key != "collected_at"
        },
    )


def log_evidence() -> Evidence:
    return Evidence(
        evidence_id="evi_log_with_kafka",
        incident_id="inc_kafka_context",
        step_id="step_log",
        status=ObservationStatus.SUCCESS,
        kind="log",
        source="hmdp.spring_boot.runtime_logs",
        source_tool="search_application_logs",
        collected_at=datetime.now(timezone.utc),
        summary="Application log search returned 1 event(s).",
        completeness=EvidenceCompleteness.COMPLETE,
        data={
            "events": [{"message": "INFO request accepted"}],
            "warnings": [],
        },
    )


def test_kafka_normal_status_returns_complete_evidence():
    observation = collect(SnapshotReader(snapshot(lag=4)))

    assert observation.status is ObservationStatus.SUCCESS
    assert observation.kind == "kafka_consumer_status"
    assert observation.source_tool == "get_kafka_status"
    assert observation.data["consumer_status"] == "stable"
    assert observation.data["total_lag"] == 4
    assert observation.data["lag_status"] == "normal"


def test_kafka_abnormal_lag_is_reported_without_mutation():
    observation = collect(SnapshotReader(snapshot(lag=50_000)))

    assert observation.status is ObservationStatus.SUCCESS
    assert observation.data["total_lag"] == 50_000
    assert observation.data["lag_status"] == "abnormal"
    assert observation.data["partitions"][0]["lag"] == 50_000


def test_kafka_reader_failure_returns_error_evidence():
    observation = collect(FailingReader())

    assert observation.status is ObservationStatus.ERROR
    assert observation.kind == "kafka_consumer_status"
    assert observation.completeness is EvidenceCompleteness.UNKNOWN
    assert observation.data["broker_reachable"] is False
    assert observation.error is not None
    assert observation.error.error_type == "ConnectionError"


@pytest.mark.parametrize(
    ("observation", "decision", "hypothesis_status"),
    [
        (
            collect(SnapshotReader(snapshot(lag=50_000))),
            ReflectionDecision.REPORT,
            "supports",
        ),
        (
            collect(SnapshotReader(snapshot(lag=0))),
            ReflectionDecision.REPLAN,
            "contradicts",
        ),
        (
            collect(FailingReader()),
            ReflectionDecision.INCONCLUSIVE,
            "insufficient",
        ),
    ],
)
def test_reflection_classifies_kafka_evidence(
    observation,
    decision,
    hypothesis_status,
):
    evidence = evidence_from_observation(observation, index=1)
    packet = ContextManager().build(
        [evidence],
        stage=ContextStage.REFLECTION,
        latest_evidence_id=evidence.evidence_id,
    )

    result = ReflectionEngine(MockLLMProvider()).evaluate(
        "Kafka 消费停滞导致订单延迟",
        packet,
    )

    assert result.decision is decision
    assert result.hypothesis_status == hypothesis_status


def test_context_contains_log_and_kafka_evidence_together():
    kafka = evidence_from_observation(
        collect(SnapshotReader(snapshot(lag=25))),
        index=2,
    )

    packet = ContextManager().build([log_evidence(), kafka])

    assert {card.kind for card in packet.evidence_cards} == {
        "log",
        "kafka_consumer_status",
    }
    kafka_card = next(
        card for card in packet.evidence_cards
        if card.kind == "kafka_consumer_status"
    )
    assert any("total_lag=25" in fact for fact in kafka_card.facts)
    assert all(not hasattr(card, "data") for card in packet.evidence_cards)
