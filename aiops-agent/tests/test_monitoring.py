"""Phase M1 scheduler, collector, persistence, and permission tests."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Mapping

import pytest

from memory.database import SQLiteDatabase
from governance.models import ToolCategory
from monitoring.collector import MonitoringCollector
from monitoring.models import CollectionSchedule
from monitoring.observation_store import ObservationStore
from monitoring.scheduler import MonitoringScheduler
from runtime.tool_registry import (
    ToolManifest,
    ToolManifestEntry,
    ToolPermissionLevel,
    ToolRegistry,
    ToolRiskLevel,
)
from tool_contracts import (
    EvidenceCompleteness,
    ObservationStatus,
    ToolError,
    ToolObservation,
)


FIXED_TIME = datetime(2026, 9, 19, 2, 0, tzinfo=timezone.utc)


def kafka_observation(
    *,
    status: ObservationStatus = ObservationStatus.SUCCESS,
) -> ToolObservation:
    if status in {ObservationStatus.ERROR, ObservationStatus.TIMEOUT}:
        return ToolObservation(
            status=status,
            kind="kafka_consumer_status",
            source="kafka://127.0.0.1:9092",
            source_tool="get_kafka_status",
            collected_at=FIXED_TIME,
            summary=f"Kafka collection returned {status.value}.",
            completeness=EvidenceCompleteness.UNKNOWN,
            data={"broker_reachable": False},
            error=ToolError(
                error_type="KafkaError",
                message="Kafka unavailable",
                retryable=True,
            ),
        )
    completeness = (
        EvidenceCompleteness.COMPLETE
        if status is ObservationStatus.SUCCESS
        else EvidenceCompleteness.PARTIAL
    )
    return ToolObservation(
        status=status,
        kind="kafka_consumer_status",
        source="kafka://127.0.0.1:9092",
        source_tool="get_kafka_status",
        collected_at=FIXED_TIME,
        summary="Kafka consumer group status collected.",
        completeness=completeness,
        data={
            "topic": "hmdp.seckill.order.create.v1",
            "consumer_group": "hmdp-seckill-order-create-v1",
            "total_lag": 4,
            "member_count": 1,
        },
    )


class FakeMCPClient:
    def __init__(
        self,
        registry: ToolRegistry,
        *,
        observation: ToolObservation | None = None,
        error: Exception | None = None,
        delay_seconds: float = 0.0,
    ) -> None:
        self.registry = registry
        self.observation = observation or kafka_observation()
        self.error = error
        self.delay_seconds = delay_seconds
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get_tool_schemas(self) -> list[dict[str, Any]]:
        return [
            {
                "name": tool.tool_name,
                "description": tool.description,
                "input_schema": tool.input_schema,
                "server": tool.server,
            }
            for tool in self.registry.manifest.tools
        ]

    def call(
        self,
        tool_name: str,
        arguments: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.calls.append((tool_name, dict(arguments or {})))
        if self.delay_seconds:
            time.sleep(self.delay_seconds)
        if self.error is not None:
            raise self.error
        return self.observation.model_dump(mode="json")


@pytest.fixture
def monitoring_store(tmp_path) -> ObservationStore:
    store = ObservationStore(SQLiteDatabase(tmp_path / "monitoring.db"))
    store.initialize()
    return store


@pytest.fixture
def registry() -> ToolRegistry:
    return ToolRegistry.from_file()


def schedule(**overrides: Any) -> CollectionSchedule:
    values: dict[str, Any] = {
        "schedule_id": "kafka-test-30s",
        "tool_name": "get_kafka_status",
        "interval_seconds": 30,
        "enabled": True,
        "timeout_seconds": 1.0,
        "arguments": {},
        "next_run_at": FIXED_TIME,
    }
    values.update(overrides)
    return CollectionSchedule(**values)


def test_scheduler_triggers_due_persisted_schedule(monitoring_store, registry):
    client = FakeMCPClient(registry)
    collector = MonitoringCollector(
        client,
        registry,
        monitoring_store,
        clock=lambda: FIXED_TIME,
    )
    scheduler = MonitoringScheduler(
        monitoring_store,
        collector,
        clock=lambda: FIXED_TIME,
    )
    scheduler.add_schedule(schedule())

    results = scheduler.run_due()

    assert len(results) == 1
    assert client.calls == [("get_kafka_status", {})]
    persisted = monitoring_store.get_schedule("kafka-test-30s")
    assert persisted is not None
    assert persisted.last_run_at == FIXED_TIME
    assert persisted.next_run_at is not None
    assert int((persisted.next_run_at - FIXED_TIME).total_seconds()) == 30
    assert [item["event_type"] for item in monitoring_store.list_trace_events()] == [
        "monitoring_tick_started",
        "observation_collected",
    ]


def test_collector_calls_existing_kafka_mcp_and_saves_evidence_ref(
    monitoring_store,
    registry,
):
    selected_schedule = schedule(schedule_id="kafka-direct")
    monitoring_store.upsert_schedule(selected_schedule)
    client = FakeMCPClient(registry)
    collector = MonitoringCollector(
        client,
        registry,
        monitoring_store,
        clock=lambda: FIXED_TIME,
    )

    result = collector.collect(
        selected_schedule,
        collection_run_id="col_kafka_direct",
    )

    assert result.observation.kind == "kafka_consumer_status"
    assert result.run.status is ObservationStatus.SUCCESS
    rows = monitoring_store.list_collection_runs("kafka-direct")
    assert rows[0]["tool"] == "get_kafka_status"
    assert rows[0]["status"] == "success"
    assert rows[0]["timestamp"] == FIXED_TIME.isoformat()
    assert rows[0]["evidence_ref"] == result.observation.evidence_id


def test_tool_exception_is_saved_as_structured_error(monitoring_store, registry):
    selected_schedule = schedule(schedule_id="kafka-error")
    monitoring_store.upsert_schedule(selected_schedule)
    client = FakeMCPClient(registry, error=ConnectionError("broker unavailable"))
    collector = MonitoringCollector(
        client,
        registry,
        monitoring_store,
        clock=lambda: FIXED_TIME,
    )

    result = collector.collect(
        selected_schedule,
        collection_run_id="col_kafka_error",
    )

    assert result.run.status is ObservationStatus.ERROR
    assert result.observation.error is not None
    assert result.observation.error.error_type == "ConnectionError"
    assert monitoring_store.list_collection_runs()[0]["status"] == "error"
    assert monitoring_store.list_trace_events()[0]["event_type"] == (
        "observation_collected"
    )


@pytest.mark.parametrize(
    "status",
    [
        ObservationStatus.PARTIAL,
        ObservationStatus.ERROR,
        ObservationStatus.TIMEOUT,
    ],
)
def test_collector_preserves_structured_observation_status(
    monitoring_store,
    registry,
    status,
):
    selected_schedule = schedule(schedule_id=f"kafka-{status.value}")
    monitoring_store.upsert_schedule(selected_schedule)
    collector = MonitoringCollector(
        FakeMCPClient(registry, observation=kafka_observation(status=status)),
        registry,
        monitoring_store,
        clock=lambda: FIXED_TIME,
    )

    result = collector.collect(
        selected_schedule,
        collection_run_id=f"col_kafka_{status.value}",
    )

    assert result.run.status is status
    assert result.observation.status is status
    assert monitoring_store.list_collection_runs()[0]["status"] == status.value


def test_collector_records_local_timeout(monitoring_store, registry):
    selected_schedule = schedule(
        schedule_id="kafka-timeout-local",
        timeout_seconds=0.001,
    )
    monitoring_store.upsert_schedule(selected_schedule)
    collector = MonitoringCollector(
        FakeMCPClient(registry, delay_seconds=0.05),
        registry,
        monitoring_store,
        clock=lambda: FIXED_TIME,
    )

    result = collector.collect(
        selected_schedule,
        collection_run_id="col_kafka_timeout_local",
    )

    assert result.run.status is ObservationStatus.TIMEOUT
    assert result.observation.error is not None
    assert result.observation.error.error_type == "CollectionTimeout"
    assert monitoring_store.list_collection_runs()[0]["status"] == "timeout"


def test_collector_denies_non_read_only_tool_without_calling_it(monitoring_store):
    action_entry = ToolManifestEntry(
        tool_name="restart_consumer",
        description="A test-only action that must never be called by monitoring.",
        input_schema={"type": "object", "properties": {}},
        permission="action:consumer_restart",
        server="test-actions",
        category=ToolCategory.ACTION,
        permission_level=ToolPermissionLevel.LOW_RISK_ACTION,
        risk_level=ToolRiskLevel.LOW,
        approval_required=True,
    )
    action_registry = ToolRegistry(ToolManifest(version=1, tools=(action_entry,)))
    client = FakeMCPClient(action_registry)
    selected_schedule = schedule(
        schedule_id="forbidden-action",
        tool_name="restart_consumer",
    )
    monitoring_store.upsert_schedule(selected_schedule)
    collector = MonitoringCollector(
        client,
        action_registry,
        monitoring_store,
        clock=lambda: FIXED_TIME,
    )

    result = collector.collect(
        selected_schedule,
        collection_run_id="col_forbidden_action",
    )

    assert client.calls == []
    assert result.run.status is ObservationStatus.ERROR
    assert result.observation.error is not None
    assert result.observation.error.error_type == "ToolRegistryError"
    assert "not READ_ONLY" in result.observation.error.message
    assert monitoring_store.list_collection_runs()[0]["status"] == "error"
