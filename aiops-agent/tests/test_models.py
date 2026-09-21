"""Pydantic schema and enum validation tests."""

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from runtime.models import (
    IncidentCreate,
    IncidentSource,
    IncidentStatus,
    IncidentTask,
    ObservationWindow,
)
from trace.models import AgentTraceEvent, TraceEventType


def test_incident_create_normalizes_components_and_builds_queued_task():
    request = IncidentCreate(
        title="Kafka order delay",
        description="Orders are not being created after accepted seckill requests.",
        source=IncidentSource.HUMAN,
        affected_components=[" Kafka ", "mysql", "kafka"],
    )

    task = IncidentTask.from_create(request)

    assert task.status is IncidentStatus.QUEUED
    assert task.affected_components == ["kafka", "mysql"]
    assert task.incident_id.startswith("inc_")
    assert task.created_at.tzinfo is not None


def test_incident_schema_rejects_blank_title():
    with pytest.raises(ValidationError):
        IncidentCreate(title="   ", description="non-empty")


def test_observation_window_requires_ordered_timezone_aware_values():
    now = datetime.now(timezone.utc)
    valid = ObservationWindow(start=now, end=now + timedelta(minutes=5))
    assert valid.end > valid.start

    with pytest.raises(ValidationError):
        ObservationWindow(start=now, end=now)

    with pytest.raises(ValidationError):
        ObservationWindow(
            start=datetime(2026, 9, 18, 10, 0),
            end=datetime(2026, 9, 18, 10, 5),
        )


def test_trace_event_schema_serializes_stable_enum_value():
    event = AgentTraceEvent(
        incident_id="inc_example",
        sequence=0,
        event_type=TraceEventType.INCIDENT_CREATED,
        stage="task_manager",
        objective="Create an auditable incident",
        summary="Incident accepted and queued.",
        payload={"source": "human"},
    )

    payload = event.model_dump(mode="json")
    assert payload["event_type"] == "incident_created"
    assert payload["payload"] == {"source": "human"}
    assert payload["created_at"].endswith("Z")


def test_trace_event_rejects_unknown_event_type():
    with pytest.raises(ValidationError):
        AgentTraceEvent(
            incident_id="inc_example",
            sequence=0,
            event_type="hidden_reasoning",
            stage="runtime",
            summary="Invalid event type.",
        )

