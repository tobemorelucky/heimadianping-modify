"""Built-in Phase M1 schedule definitions."""

from __future__ import annotations

from monitoring.models import CollectionSchedule, utc_now


DEFAULT_KAFKA_SCHEDULE_ID = "kafka-consumer-status-30s"
DEFAULT_BUSINESS_SCHEDULE_ID = "business-metrics-30s"
DEFAULT_MYSQL_SCHEDULE_ID = "consumer-mysql-health-30s"


def default_kafka_schedule(
    *,
    interval_seconds: int = 30,
    timeout_seconds: float = 10.0,
) -> CollectionSchedule:
    """Return the only built-in Phase M1 schedule."""

    return CollectionSchedule(
        schedule_id=DEFAULT_KAFKA_SCHEDULE_ID,
        tool_name="get_kafka_status",
        interval_seconds=interval_seconds,
        enabled=True,
        timeout_seconds=timeout_seconds,
        arguments={},
        next_run_at=utc_now(),
    )


def default_business_schedule(
    *, interval_seconds: int = 30, timeout_seconds: float = 10.0
) -> CollectionSchedule:
    return CollectionSchedule(
        schedule_id=DEFAULT_BUSINESS_SCHEDULE_ID,
        tool_name="get_business_metrics",
        interval_seconds=interval_seconds,
        timeout_seconds=timeout_seconds,
        arguments={"incident_id": "monitoring"},
        next_run_at=utc_now(),
    )


def default_mysql_schedule(
    *, interval_seconds: int = 30, timeout_seconds: float = 10.0
) -> CollectionSchedule:
    return CollectionSchedule(
        schedule_id=DEFAULT_MYSQL_SCHEDULE_ID,
        tool_name="get_mysql_health",
        interval_seconds=interval_seconds,
        timeout_seconds=timeout_seconds,
        arguments={},
        next_run_at=utc_now(),
    )
