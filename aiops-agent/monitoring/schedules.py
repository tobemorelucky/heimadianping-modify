"""Built-in Phase M1 schedule definitions."""

from __future__ import annotations

from monitoring.models import CollectionSchedule, utc_now


DEFAULT_KAFKA_SCHEDULE_ID = "kafka-consumer-status-30s"


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
