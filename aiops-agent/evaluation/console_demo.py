"""Seed one deterministic Kafka FaultBench incident for the read-only Console.

This writes only to the selected local Agent SQLite database. It does not contact
HMDP, Kafka, or any action executor.
"""

from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path

from evaluation.runner import (
    DEFAULT_FAULT_ID,
    EVALUATION_ROOT,
    EvaluationRunner,
    FixtureMCPClient,
    load_fault_case,
    load_fault_fixture,
)
from governance.permissions import PermissionGateway
from governance.proposal import ProposalGenerator
from incident.manager import IncidentManager
from incident.store import IncidentStore
from memory.database import SQLiteDatabase
from monitoring.detector.models import AnomalySignal, DetectionSeverity
from monitoring.detector.rules import kafka_consumer_down_rule
from monitoring.detector.signal_store import SignalStore
from monitoring.models import CollectionRun, CollectionSchedule
from monitoring.observation_store import ObservationStore
from runtime.orchestrator import RuntimeOrchestrator


DEFAULT_DEMO_DATABASE = EVALUATION_ROOT.parent / "data" / "console-demo.db"


def seed_console_demo(database_path: Path | str = DEFAULT_DEMO_DATABASE) -> str:
    """Persist a fixture Observation, Signal, Incident, diagnosis, and governance trace."""

    case = load_fault_case(EVALUATION_ROOT / "faults" / f"{DEFAULT_FAULT_ID}.json")
    fixture = load_fault_fixture(
        EVALUATION_ROOT / "fixtures" / f"{DEFAULT_FAULT_ID}.json"
    )
    database = SQLiteDatabase(database_path)
    observation_store = ObservationStore(database)
    observation_store.initialize()
    signal_store = SignalStore(database)
    signal_store.initialize()
    incident_store = IncidentStore(database)
    incident_store.initialize()

    fingerprint = f"console-demo:{fixture.kafka.topic}:{fixture.kafka.consumer_group}"
    existing = incident_store.latest_for_fingerprint(fingerprint)
    if existing is not None:
        if database.get_report(existing.incident_id) is None:
            raise RuntimeError(
                "Existing Console demo Incident has no report; use a fresh demo database."
            )
        return existing.incident_id

    client = FixtureMCPClient(fixture)
    observation = client._kafka_observation()
    schedule = CollectionSchedule(
        schedule_id="console-demo-kafka",
        tool_name="get_kafka_status",
        interval_seconds=30,
        timeout_seconds=10,
        created_at=observation.collected_at,
        updated_at=observation.collected_at,
    )
    observation_store.ensure_schedule(schedule)
    run = CollectionRun(
        schedule_id=schedule.schedule_id,
        tool="get_kafka_status",
        status=observation.status,
        timestamp=observation.collected_at,
        evidence_ref=observation.evidence_id,
        duration_ms=1,
    )
    observation_store.save_collection_run(run, observation)
    signal_store.ensure_rule(kafka_consumer_down_rule())
    signal = AnomalySignal(
        rule_id="kafka-consumer-down-v1",
        version=1,
        fingerprint=fingerprint,
        severity=DetectionSeverity.HIGH,
        facts={
            "topic": fixture.kafka.topic,
            "consumer_group": fixture.kafka.consumer_group,
            "member_count": fixture.kafka.member_count,
            "lag": fixture.kafka.lag,
            "fault_id": DEFAULT_FAULT_ID,
            "scenario": "Kafka Consumer Failure",
        },
        observation_refs=(observation.evidence_id,),
        first_seen=observation.collected_at - timedelta(seconds=30),
        last_seen=observation.collected_at,
        created_at=observation.collected_at,
    )
    signal_store.save_signal(signal)
    runtime = RuntimeOrchestrator(
        database,
        client,
        llm_provider=EvaluationRunner._provider(case, fixture),
    )
    result = IncidentManager(
        incident_store,
        runtime,
        proposal_generator=ProposalGenerator(),
        permission_gateway=PermissionGateway(),
    ).handle_signal(signal)
    return result.incident.incident_id


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed local Kafka Console demo")
    parser.add_argument("--database", type=Path, default=DEFAULT_DEMO_DATABASE)
    args = parser.parse_args()
    print(seed_console_demo(args.database))


if __name__ == "__main__":
    main()
