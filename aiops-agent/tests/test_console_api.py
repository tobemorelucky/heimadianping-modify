"""Read-only AIOps Console API integration tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from api.main import create_app
from config import Settings
from evaluation.console_demo import seed_console_demo
from evaluation.runner import EvaluationRunner, FixtureMCPClient, load_fault_case, load_fault_fixture
from governance.permissions import PermissionGateway
from governance.proposal import ProposalGenerator
from incident.manager import IncidentManager
from incident.store import IncidentStore
from monitoring.detector.models import AnomalySignal, DetectionSeverity
from monitoring.detector.rules import kafka_consumer_down_rule
from monitoring.detector.signal_store import SignalStore


ROOT = Path(__file__).resolve().parents[1]
FAULT_ID = "KAFKA_CONSUMER_DOWN_001"


def test_console_dashboard_is_healthy_without_incidents(tmp_path):
    app = create_app(Settings(database_path=tmp_path / "empty.db"))
    with TestClient(app) as client:
        incidents = client.get("/api/incidents")
        monitoring = client.get("/api/monitoring/summary")
        missing = client.get("/api/incidents/inc_missing")

    assert incidents.status_code == 200
    assert incidents.json() == []
    assert monitoring.json() == {
        "health": "healthy",
        "active_incident_count": 0,
        "last_inspection_at": None,
        "last_inspection_status": None,
        "last_inspection_tool": None,
        "inspection_mode": None,
    }
    assert missing.status_code == 404


def test_console_demo_seeds_a_readable_incident(tmp_path):
    path = tmp_path / "demo.db"
    incident_id = seed_console_demo(path)
    assert seed_console_demo(path) == incident_id
    app = create_app(Settings(database_path=path))
    with TestClient(app) as client:
        response = client.get(f"/api/incidents/{incident_id}")
        summary = client.get("/api/monitoring/summary")
    assert response.status_code == 200
    assert response.json()["report"]["status"] == "confirmed"
    assert response.json()["demo_scenario"] == "Kafka Consumer Failure"
    assert response.json()["demo_fault_id"] == FAULT_ID
    assert len(response.json()["traces"]) > 10
    assert response.json()["proposals"][0]["permission_result"] == "REQUIRE_APPROVAL"
    assert summary.json()["last_inspection_tool"] == "get_kafka_status"
    assert summary.json()["inspection_mode"] == "fixture"


def test_console_shows_faultbench_incident_timeline_and_governance(tmp_path):
    case = load_fault_case(ROOT / "evaluation" / "faults" / f"{FAULT_ID}.json")
    fixture = load_fault_fixture(ROOT / "evaluation" / "fixtures" / f"{FAULT_ID}.json")
    app = create_app(
        Settings(database_path=tmp_path / "console.db"),
        mcp_client=FixtureMCPClient(fixture),
        llm_provider=EvaluationRunner._provider(case, fixture),
    )
    base_time = datetime(2026, 9, 19, 4, 0, tzinfo=timezone.utc)
    signal = AnomalySignal(
        signal_id="sig_console_kafka_001",
        rule_id="kafka-consumer-down-v1",
        version=1,
        fingerprint="kafka:seckill:consumer",
        severity=DetectionSeverity.HIGH,
        facts={"topic": fixture.kafka.topic, "consumer_group": fixture.kafka.consumer_group},
        observation_refs=("evi_monitoring_1", "evi_monitoring_2"),
        first_seen=base_time,
        last_seen=base_time + timedelta(seconds=30),
        created_at=base_time + timedelta(seconds=30),
    )

    with TestClient(app) as client:
        signal_store = SignalStore(app.state.database)
        signal_store.initialize()
        signal_store.upsert_rule(kafka_consumer_down_rule())
        signal_store.save_signal(signal)
        incident_store = IncidentStore(app.state.database)
        incident_store.initialize()
        manager = IncidentManager(
            incident_store,
            app.state.orchestrator,
            proposal_generator=ProposalGenerator(),
            permission_gateway=PermissionGateway(),
            clock=lambda: base_time + timedelta(minutes=3),
        )
        result = manager.handle_signal(signal)
        detail_response = client.get(f"/api/incidents/{result.incident.incident_id}")
        list_response = client.get("/api/incidents")
        dashboard_response = client.get("/api/monitoring/summary")
        mutation_response = client.post(
            f"/api/incidents/{result.incident.incident_id}/execute"
        )

    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail["incident_id"] == result.incident.incident_id
    assert detail["status"] == "ACTIVE"
    assert detail["diagnosis_status"] == "COMPLETED"
    assert detail["report"]["status"] == "confirmed"
    assert len(detail["evidence"]) == 2
    assert {item["source_tool"] for item in detail["evidence"]} == {
        "get_kafka_status", "search_application_logs"
    }
    assert all("data" not in item and "observation_json" not in item for item in detail["evidence"])
    assert detail["hypotheses"]
    assert any(len(item["history"]) >= 2 for item in detail["hypotheses"])
    event_types = [event["event_type"] for event in detail["traces"]]
    for required in (
        "anomaly_detected", "incident_created", "skill_selected", "skill_loaded",
        "tool_called", "evidence_created", "hypothesis_created",
        "hypothesis_updated", "diagnosis_completed", "action_proposal_created",
        "permission_checked",
    ):
        assert required in event_types
    assert event_types.index("anomaly_detected") < event_types.index("incident_created")
    assert event_types.index("tool_called") < event_types.index("diagnosis_completed")
    assert event_types.index("diagnosis_completed") < event_types.index("action_proposal_created")
    assert [item["payload"]["tool_name"] for item in detail["traces"] if item["event_type"] == "tool_called"] == [
        "get_kafka_status", "search_application_logs"
    ]
    assert len(detail["proposals"]) == 1
    assert detail["proposals"][0]["action_name"] == "restart_consumer"
    assert detail["proposals"][0]["permission_result"] == "REQUIRE_APPROVAL"
    assert detail["proposals"][0]["evidence_refs"]
    assert list_response.json()[0]["incident_id"] == detail["incident_id"]
    assert dashboard_response.json()["health"] == "attention"
    assert dashboard_response.json()["active_incident_count"] == 1
    assert mutation_response.status_code == 404


def test_dashboard_becomes_healthy_only_after_confirmed_recovery(tmp_path):
    app = create_app(Settings(database_path=tmp_path / "recovery.db"))
    base_time = datetime(2026, 9, 19, 4, 0, tzinfo=timezone.utc)
    signal = AnomalySignal(
        signal_id="sig_recovery_001",
        rule_id="kafka-consumer-down-v1",
        version=1,
        fingerprint="kafka:recovery:consumer",
        severity=DetectionSeverity.HIGH,
        facts={"topic": "hmdp.seckill.order.create.v1", "consumer_group": "hmdp-seckill-order-create-v1"},
        observation_refs=("down_1", "down_2"),
        first_seen=base_time,
        last_seen=base_time + timedelta(seconds=30),
        created_at=base_time + timedelta(seconds=30),
    )

    class DiagnosisStub:
        def run(self, request, *, incident_id=None):
            from runtime.reports import DiagnosisReport, DiagnosisRunResult, DiagnosisStatus

            report = DiagnosisReport(
                report_id="report_recovery_001", incident_id=incident_id,
                status=DiagnosisStatus.CONFIRMED, title="Kafka failure",
                conclusion="No consumer members", root_cause="Kafka consumer unavailable",
                confidence=0.9, evidence_ids=["down_1"],
            )
            return DiagnosisRunResult(incident_id=incident_id, report=report)

    with TestClient(app) as client:
        store = IncidentStore(app.state.database)
        manager = IncidentManager(store, DiagnosisStub(), clock=lambda: base_time + timedelta(minutes=3))
        incident = manager.handle_signal(signal).incident
        assert client.get("/api/monitoring/summary").json()["active_incident_count"] == 1
        assert manager.confirm_recovery(
            fingerprint=signal.fingerprint,
            observation_refs=("healthy_1", "healthy_2"),
            first_seen=base_time + timedelta(minutes=1),
            last_seen=base_time + timedelta(minutes=2),
        ) is not None
        summary = client.get("/api/monitoring/summary").json()
        detail = client.get(f"/api/incidents/{incident.incident_id}").json()

    assert summary["health"] == "healthy"
    assert summary["active_incident_count"] == 0
    assert detail["status"] == "RECOVERED"
    assert detail["diagnosis_status"] == "COMPLETED"
    assert "incident_recovered" in [event["event_type"] for event in detail["traces"]]
