"""Conservative three-source assessment of order persistence degradation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Sequence

from monitoring.detector.models import OrderPersistenceFailureCondition
from monitoring.models import StoredObservation
from tool_contracts import EvidenceCompleteness, ObservationStatus, ToolObservation


METRICS = (
    "seckill_request_count",
    "lua_admission_success_count",
    "kafka_message_sent_count",
    "order_created_success_count",
    "order_created_failure_count",
)


@dataclass(frozen=True)
class DetectionAssessment:
    outcome: Literal["matched", "not_matched", "insufficient"]
    reason: str
    facts: dict[str, Any] = field(default_factory=dict)
    observation_refs: tuple[str, ...] = ()


def assess_order_persistence(
    observations: Sequence[StoredObservation],
    current: ToolObservation,
    condition: OrderPersistenceFailureCondition,
) -> DetectionAssessment:
    """Never infer MySQL failure from order counters alone.

    Business counts are bounded-log counters, so use two snapshots from the
    same schedule and compare deltas; counter resets and incomplete markers
    make the assessment insufficient.
    """

    mysql = current.data
    if current.status in {ObservationStatus.ERROR, ObservationStatus.TIMEOUT}:
        return DetectionAssessment("insufficient", "MySQL tool failed")
    if (
        current.kind != "mysql_health"
        or current.source_tool != "get_mysql_health"
        or mysql.get("source_role") != "hmdp-consumer"
    ):
        return DetectionAssessment("insufficient", "Consumer MySQL source is unverified")
    raw_failure_classes = mysql.get("failure_class")
    if not isinstance(raw_failure_classes, list) or not all(
        isinstance(item, str) for item in raw_failure_classes
    ):
        return DetectionAssessment("insufficient", "Normalized MySQL failure classes are missing")
    failure_classes = set(raw_failure_classes)
    if "UNKNOWN" in failure_classes:
        return DetectionAssessment("insufficient", "Consumer MySQL failure class is unknown")
    supported = failure_classes.intersection(
        condition.supported_mysql_failure_classes
    )
    if not supported:
        return DetectionAssessment("not_matched", "Consumer MySQL failure was not observed")
    if mysql.get("health_state") not in {"DEGRADED", "FAILED"}:
        return DetectionAssessment("insufficient", "Consumer MySQL health state is inconsistent")

    business = [
        item for item in observations
        if item.observation.kind == "business_metrics"
        and item.observation.source_tool == "get_business_metrics"
    ]
    if len(business) < 2:
        return DetectionAssessment("insufficient", "Two business metric snapshots are required")
    previous, latest = business[-2:]
    if previous.schedule_id != latest.schedule_id:
        return DetectionAssessment("insufficient", "Business snapshots use different schedules")
    snapshots = (previous.observation, latest.observation)
    if any(
        item.status is not ObservationStatus.SUCCESS
        or item.completeness is not EvidenceCompleteness.COMPLETE
        or item.data.get("unavailable_metrics")
        or not set(METRICS).issubset(set(item.data.get("observed_metrics", [])))
        for item in snapshots
    ):
        return DetectionAssessment("insufficient", "Business metric markers are incomplete")
    if any(type(item.data.get(key)) is not int for item in snapshots for key in METRICS):
        return DetectionAssessment("insufficient", "Business metric counters are invalid")
    deltas = {
        key: latest.observation.data[key] - previous.observation.data[key]
        for key in METRICS
    }
    if any(value < 0 for value in deltas.values()):
        return DetectionAssessment("insufficient", "Business metric counters reset")
    requests = deltas["seckill_request_count"]
    lua = deltas["lua_admission_success_count"]
    published = deltas["kafka_message_sent_count"]
    created = deltas["order_created_success_count"]
    failed = deltas["order_created_failure_count"]
    if requests < condition.min_request_delta:
        return DetectionAssessment("insufficient", "No new seckill request activity")
    if lua / requests < condition.min_lua_admission_ratio:
        return DetectionAssessment("not_matched", "Lua admission is degraded")
    if lua == 0 or published / lua < condition.min_kafka_publish_ratio:
        return DetectionAssessment("not_matched", "Kafka publish is degraded")
    if published == 0 or (created / published >= condition.max_order_success_ratio and failed == 0):
        return DetectionAssessment("not_matched", "Order creation is not degraded")

    kafka = [
        item for item in observations
        if item.observation.kind == "kafka_consumer_status"
        and item.observation.source_tool == "get_kafka_status"
        and item.timestamp >= latest.timestamp
    ]
    if not kafka:
        return DetectionAssessment("insufficient", "Kafka observation is missing")
    kafka_item = kafka[-1]
    kafka_observation = kafka_item.observation
    state = kafka_observation.data
    if (
        kafka_observation.status is not ObservationStatus.SUCCESS
        or kafka_observation.completeness is not EvidenceCompleteness.COMPLETE
        or state.get("topic_exists") is not True
        or state.get("offsets_complete") is not True
        or state.get("partitions_truncated") is not False
        or type(state.get("member_count")) is not int
        or type(state.get("total_lag")) is not int
        or not isinstance(state.get("topic"), str)
        or not state["topic"]
        or not isinstance(state.get("consumer_group"), str)
        or not state["consumer_group"]
    ):
        return DetectionAssessment("insufficient", "Kafka status or offsets are incomplete")
    if state["member_count"] <= 0 or state["total_lag"] > condition.max_kafka_lag:
        return DetectionAssessment("not_matched", "Kafka consumer is not healthy")
    if state.get("lag_status") != "normal" or state.get("consumer_status") != "stable":
        return DetectionAssessment("insufficient", "Kafka group state is not decisive")

    return DetectionAssessment(
        "matched",
        "Business downstream conversion degraded while Kafka is healthy and Consumer MySQL probe failed",
        facts={
            "topic": state.get("topic"),
            "consumer_group": state.get("consumer_group"),
            "source_role": "hmdp-consumer",
            "request_delta": requests,
            "lua_admission_delta": lua,
            "kafka_publish_delta": published,
            "order_success_delta": created,
            "order_failure_delta": failed,
            "member_count": state["member_count"],
            "lag": state["total_lag"],
            "mysql_health_state": mysql["health_state"],
            "mysql_failure_class": sorted(supported),
            "database_reachable": mysql.get("database_reachable"),
            "connection_test_status": mysql["connection_test_status"],
            "condition": "business degradation AND Kafka healthy AND Consumer MySQL probe failed",
        },
        observation_refs=(
            previous.observation.evidence_id,
            latest.observation.evidence_id,
            kafka_observation.evidence_id,
            current.evidence_id,
        ),
    )
