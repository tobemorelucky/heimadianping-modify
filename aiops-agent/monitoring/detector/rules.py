"""Built-in Phase M2 deterministic detection rules."""

from __future__ import annotations

from monitoring.detector.models import (
    DetectionRule,
    DetectionSeverity,
    KafkaConsumerDownCondition,
    OrderPersistenceFailureCondition,
)


KAFKA_CONSUMER_DOWN_RULE_ID = "kafka-consumer-down-v1"
ORDER_PERSISTENCE_FAILURE_RULE_ID = "order-persistence-failure-v1"


def kafka_consumer_down_rule(
    *,
    lag_threshold: int = 1_000,
    consecutive_windows: int = 2,
    lookback_window: int | str = "2m",
    cooldown: int | str = "10m",
) -> DetectionRule:
    """Detect a consumer group with no members and accumulated lag."""

    return DetectionRule(
        rule_id=KAFKA_CONSUMER_DOWN_RULE_ID,
        version=1,
        source_kind="kafka_consumer_status",
        condition=KafkaConsumerDownCondition(
            member_count_equals=0,
            lag_greater_than=lag_threshold,
            consecutive_windows=consecutive_windows,
        ),
        severity=DetectionSeverity.HIGH,
        fingerprint_fields=("topic", "consumer_group"),
        lookback_window=lookback_window,
        cooldown=cooldown,
    )


def order_persistence_failure_rule(
    *,
    lookback_window: int | str = "5m",
    cooldown: int | str = "10m",
    max_kafka_lag: int = 1_000,
) -> DetectionRule:
    """Require business, Kafka, and Consumer MySQL observations together."""

    return DetectionRule(
        rule_id=ORDER_PERSISTENCE_FAILURE_RULE_ID,
        version=2,
        source_kind="mysql_health",
        condition=OrderPersistenceFailureCondition(max_kafka_lag=max_kafka_lag),
        severity=DetectionSeverity.HIGH,
        fingerprint_fields=("source_role",),
        lookback_window=lookback_window,
        cooldown=cooldown,
    )
