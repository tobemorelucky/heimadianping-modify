"""Phase M3 AnomalySignal-to-Incident-to-Diagnosis tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from evaluation.runner import FixtureMCPClient, load_fault_fixture
from governance.permissions import PermissionGateway
from governance.proposal import ProposalGenerator
from incident.manager import IncidentManager
from incident.models import IncidentStatus
from incident.store import IncidentStore
from llm.mock import MockLLMProvider
from memory.database import SQLiteDatabase
from monitoring.detector.models import (
    AnomalySignal,
    DetectionSeverity,
    SignalStatus,
)
from runtime.orchestrator import RuntimeOrchestrator
from runtime.reports import DiagnosisReport, DiagnosisRunResult, DiagnosisStatus


BASE_TIME = datetime(2026, 9, 19, 4, 0, tzinfo=timezone.utc)
AGENT_ROOT = Path(__file__).resolve().parents[2]


def anomaly_signal(
    *,
    signal_id: str = "sig_incident_0001",
    fingerprint: str = (
        'kafka-consumer-down-v1:v1:topic="hmdp.seckill.order.create.v1",'
        'consumer_group="hmdp-seckill-order-create-v1"'
    ),
    offset_seconds: int = 0,
) -> AnomalySignal:
    first_seen = BASE_TIME + timedelta(seconds=offset_seconds)
    return AnomalySignal(
        signal_id=signal_id,
        rule_id="kafka-consumer-down-v1",
        version=1,
        fingerprint=fingerprint,
        severity=DetectionSeverity.HIGH,
        facts={
            "topic": "hmdp.seckill.order.create.v1",
            "consumer_group": "hmdp-seckill-order-create-v1",
            "member_count": 0,
            "lag": 50_000,
            "lag_threshold": 1_000,
            "consecutive_windows": 2,
        },
        observation_refs=(
            f"evi_{signal_id}_first",
            f"evi_{signal_id}_last",
        ),
        first_seen=first_seen,
        last_seen=first_seen + timedelta(seconds=30),
        status=SignalStatus.OPEN,
        created_at=first_seen + timedelta(seconds=30),
    )


class SuccessfulRuntime:
    def __init__(self) -> None:
        self.calls: list[tuple[object, str | None]] = []

    def run(self, request, *, incident_id=None):
        self.calls.append((request, incident_id))
        report = DiagnosisReport(
            report_id="report_incident_success",
            incident_id=incident_id,
            status=DiagnosisStatus.CONFIRMED,
            title="Kafka consumer diagnosis",
            conclusion="Kafka consumer group has no active members and lag accumulated.",
            root_cause="Kafka consumer unavailable",
            confidence=0.95,
            evidence_ids=["evi_kafka"],
        )
        return DiagnosisRunResult(incident_id=incident_id, report=report)


class FailingRuntime:
    def run(self, request, *, incident_id=None):
        raise RuntimeError("diagnosis transport failed")


def incident_store(tmp_path) -> tuple[SQLiteDatabase, IncidentStore]:
    database = SQLiteDatabase(tmp_path / "incidents.db")
    store = IncidentStore(database)
    store.initialize()
    return database, store


def test_anomaly_signal_creates_incident(tmp_path):
    _, store = incident_store(tmp_path)
    runtime = SuccessfulRuntime()
    manager = IncidentManager(
        store,
        runtime,
        clock=lambda: BASE_TIME + timedelta(minutes=1),
    )

    result = manager.handle_signal(anomaly_signal())

    assert result.created is True
    assert result.diagnosis_triggered is True
    assert result.incident.status is IncidentStatus.RESOLVED
    assert result.incident.trigger_signal_ids == ("sig_incident_0001",)
    assert result.incident.diagnosis_report_id == "report_incident_success"
    assert runtime.calls[0][1] == result.incident.incident_id
    assert len(store.list_incidents()) == 1
    assert [row["event_type"] for row in store.list_trace_events(result.incident.incident_id)] == [
        "incident_created",
        "diagnosis_started",
        "diagnosis_completed",
    ]


def test_duplicate_signal_is_merged_without_second_diagnosis(tmp_path):
    _, store = incident_store(tmp_path)
    runtime = SuccessfulRuntime()
    manager = IncidentManager(
        store,
        runtime,
        clock=lambda: BASE_TIME + timedelta(minutes=2),
    )
    first = manager.handle_signal(anomaly_signal())
    duplicate = manager.handle_signal(
        anomaly_signal(
            signal_id="sig_incident_0002",
            offset_seconds=60,
        )
    )

    assert duplicate.created is False
    assert duplicate.diagnosis_triggered is False
    assert duplicate.incident.incident_id == first.incident.incident_id
    assert duplicate.incident.trigger_signal_ids == (
        "sig_incident_0001",
        "sig_incident_0002",
    )
    assert len(store.list_incidents()) == 1
    assert len(runtime.calls) == 1


def test_diagnosis_success_uses_existing_runtime_with_same_incident_id(tmp_path):
    database, store = incident_store(tmp_path)
    fixture = load_fault_fixture(
        AGENT_ROOT / "evaluation" / "fixtures" / "KAFKA_CONSUMER_DOWN_001.json"
    )
    runtime = RuntimeOrchestrator(
        database,
        FixtureMCPClient(fixture),
        llm_provider=MockLLMProvider(),
    )
    manager = IncidentManager(
        store,
        runtime,
        proposal_generator=ProposalGenerator(),
        permission_gateway=PermissionGateway(),
        clock=lambda: BASE_TIME + timedelta(minutes=3),
    )

    result = manager.handle_signal(anomaly_signal())

    assert result.incident.status is IncidentStatus.RESOLVED
    assert result.incident.diagnosis_report_id is not None
    assert result.proposal_id is not None
    assert result.permission_decision == "REQUIRE_APPROVAL"
    runtime_incident = database.get_incident(result.incident.incident_id)
    runtime_report = database.get_report(result.incident.incident_id)
    assert runtime_incident is not None
    assert runtime_incident["source"] == "alert"
    assert runtime_incident["status"] == "awaiting_human"
    assert runtime_report is not None
    assert runtime_report["incident_id"] == result.incident.incident_id
    assert database.list_evidence(result.incident.incident_id)
    assert [
        row["event_type"] for row in store.list_trace_events(result.incident.incident_id)
    ] == [
        "incident_created",
        "diagnosis_started",
        "diagnosis_completed",
        "action_proposal_created",
        "permission_checked",
    ]


def test_diagnosis_failure_sets_failed_status_and_trace(tmp_path):
    _, store = incident_store(tmp_path)
    manager = IncidentManager(
        store,
        FailingRuntime(),
        clock=lambda: BASE_TIME + timedelta(minutes=4),
    )

    result = manager.handle_signal(anomaly_signal())

    assert result.incident.status is IncidentStatus.FAILED
    assert result.diagnosis_error == "diagnosis transport failed"
    traces = store.list_trace_events(result.incident.incident_id)
    assert [row["event_type"] for row in traces] == [
        "incident_created",
        "diagnosis_started",
        "diagnosis_completed",
    ]
    assert '"outcome": "failed"' in traces[-1]["payload_json"]
