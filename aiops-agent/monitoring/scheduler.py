"""Persistent scheduler for read-only collection and deterministic detection."""

from __future__ import annotations

import argparse
import json
import threading
from datetime import timedelta
from typing import Callable, Sequence
from uuid import uuid4

from config import get_settings
from governance.permissions import PermissionGateway
from governance.proposal import ProposalGenerator
from incident.manager import IncidentManager
from incident.store import IncidentStore
from llm.factory import build_llm_provider
from memory.database import SQLiteDatabase
from monitoring.collector import MonitoringCollector
from monitoring.detector.detector import DeterministicAnomalyDetector
from monitoring.detector.rules import kafka_consumer_down_rule
from monitoring.detector.signal_store import SignalStore
from monitoring.models import (
    CollectionResult,
    CollectionSchedule,
    MonitoringTraceEvent,
    utc_now,
)
from monitoring.observation_store import ObservationStore
from monitoring.schedules import default_kafka_schedule
from runtime.mcp_client import StdioMCPClient
from runtime.orchestrator import RuntimeOrchestrator
from runtime.tool_registry import ToolRegistry
from trace.models import TraceEventType


class MonitoringScheduler:
    """Run collection, deterministic detection, and optional Incident handling."""

    def __init__(
        self,
        observation_store: ObservationStore,
        collector: MonitoringCollector,
        *,
        detector: DeterministicAnomalyDetector | None = None,
        incident_manager: IncidentManager | None = None,
        clock: Callable = utc_now,
    ) -> None:
        self.observation_store = observation_store
        self.collector = collector
        self.detector = detector
        self.incident_manager = incident_manager
        self.clock = clock

    def add_schedule(self, schedule: CollectionSchedule) -> None:
        self.observation_store.upsert_schedule(schedule)

    def ensure_schedule(self, schedule: CollectionSchedule) -> bool:
        return self.observation_store.ensure_schedule(schedule)

    def run_due(self, *, force: bool = False) -> list[CollectionResult]:
        """Run every due schedule once and persist its next execution time."""

        tick_time = self.clock()
        schedules = (
            self.observation_store.list_enabled_schedules()
            if force
            else self.observation_store.list_due_schedules(tick_time)
        )
        results: list[CollectionResult] = []
        for schedule in schedules:
            collection_run_id = f"col_{uuid4().hex}"
            self.observation_store.save_trace_event(
                MonitoringTraceEvent(
                    schedule_id=schedule.schedule_id,
                    collection_run_id=collection_run_id,
                    event_type=TraceEventType.MONITORING_TICK_STARTED,
                    summary=f"Monitoring tick started for {schedule.tool_name}.",
                    payload={
                        "tool_name": schedule.tool_name,
                        "interval_seconds": schedule.interval_seconds,
                        "timeout_seconds": schedule.timeout_seconds,
                    },
                    created_at=tick_time,
                )
            )
            result = self.collector.collect(
                schedule,
                collection_run_id=collection_run_id,
            )
            results.append(result)
            if self.detector is not None:
                signals = self.detector.evaluate(result)
                if self.incident_manager is not None:
                    for signal in signals:
                        self.incident_manager.handle_signal(signal)
            completed_at = result.run.timestamp
            self.observation_store.mark_schedule_run(
                schedule.schedule_id,
                last_run_at=completed_at,
                next_run_at=completed_at + timedelta(
                    seconds=schedule.interval_seconds
                ),
            )
        return results

    def run_forever(
        self,
        *,
        poll_seconds: float = 1.0,
        stop_event: threading.Event | None = None,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        selected_stop_event = stop_event or threading.Event()
        while not selected_stop_event.is_set():
            self.run_due()
            selected_stop_event.wait(poll_seconds)


def build_local_scheduler() -> MonitoringScheduler:
    """Compose the Windows-local scheduler from existing project services."""

    settings = get_settings()
    database = SQLiteDatabase(settings.database_path)
    store = ObservationStore(database)
    store.initialize()
    client = StdioMCPClient(
        project_root=settings.hmdp_project_root,
        timeout_seconds=settings.mcp_tool_timeout_seconds,
        kafka_bootstrap_servers=settings.kafka_bootstrap_servers,
        kafka_default_topic=settings.kafka_default_topic,
        kafka_default_consumer_group=settings.kafka_default_consumer_group,
        kafka_request_timeout_ms=settings.kafka_request_timeout_ms,
        kafka_lag_threshold=settings.kafka_lag_threshold,
    )
    registry = ToolRegistry.from_file()
    collector = MonitoringCollector(client, registry, store)
    signal_store = SignalStore(database)
    signal_store.initialize()
    signal_store.ensure_rule(
        kafka_consumer_down_rule(lag_threshold=settings.kafka_lag_threshold)
    )
    detector = DeterministicAnomalyDetector(store, signal_store)
    incident_store = IncidentStore(database)
    incident_store.initialize()
    diagnosis_runtime = RuntimeOrchestrator(
        database,
        client,
        llm_provider=build_llm_provider(settings),
        tool_registry=registry,
    )
    incident_manager = IncidentManager(
        incident_store,
        diagnosis_runtime,
        proposal_generator=ProposalGenerator(),
        permission_gateway=PermissionGateway(),
    )
    scheduler = MonitoringScheduler(
        store,
        collector,
        detector=detector,
        incident_manager=incident_manager,
    )
    scheduler.ensure_schedule(
        default_kafka_schedule(
            timeout_seconds=min(settings.mcp_tool_timeout_seconds, 60.0),
        )
    )
    return scheduler


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the read-only monitoring and deterministic detector."
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Collect every enabled schedule once and exit.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=1.0,
        help="Scheduler due-check interval for continuous mode.",
    )
    args = parser.parse_args(argv)
    scheduler = build_local_scheduler()
    if args.once:
        results = scheduler.run_due(force=True)
        print(
            json.dumps(
                [result.run.model_dump(mode="json") for result in results],
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    try:
        scheduler.run_forever(poll_seconds=args.poll_seconds)
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
