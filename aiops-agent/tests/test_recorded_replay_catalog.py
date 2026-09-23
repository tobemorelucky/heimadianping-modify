"""Product-demo replay import, Console projection, and aggregate metrics."""

from fastapi.testclient import TestClient

from api.main import create_app
from config import Settings
from evaluation.benchmark_report import build_report
from evaluation.replay_catalog import (
    DEFAULT_BUNDLED_REPLAYS,
    ReplayCatalog,
    import_replays,
)


def test_bundled_catalog_contains_both_real_incidents():
    records = ReplayCatalog(DEFAULT_BUNDLED_REPLAYS).discover()

    assert {item.fault_id for item in records} == {
        "KAFKA_CONSUMER_DOWN_REAL_001",
        "MYSQL_PERSISTENCE_TIMEOUT_001",
    }
    mysql = next(item for item in records if item.fault_id.startswith("MYSQL"))
    assert [
        item.payload["tool_name"]
        for item in mysql.traces
        if item.event_type == "tool_called"
    ] == ["get_business_metrics", "get_kafka_status", "get_mysql_health"]
    assert {item.kind for item in mysql.evidence} == {
        "business_metrics", "kafka_consumer_status", "mysql_health"
    }


def test_import_is_idempotent_and_does_not_run_agent(tmp_path):
    first = import_replays(DEFAULT_BUNDLED_REPLAYS, tmp_path / "catalog")
    second = import_replays(DEFAULT_BUNDLED_REPLAYS, tmp_path / "catalog")

    assert first == second
    assert len(first) == 2
    assert len(ReplayCatalog(tmp_path / "catalog").discover()) == 2


def test_console_lists_and_replays_recordings_without_affecting_live_health(tmp_path):
    replay_dir = tmp_path / "replays"
    import_replays(DEFAULT_BUNDLED_REPLAYS, replay_dir)
    app = create_app(Settings(
        database_path=tmp_path / "console.db",
        replay_directory=replay_dir,
    ))

    with TestClient(app) as client:
        incidents = client.get("/api/incidents").json()
        summary = client.get("/api/monitoring/summary").json()
        mysql_id = next(
            item["incident_id"] for item in incidents
            if item["demo_fault_id"] == "MYSQL_PERSISTENCE_TIMEOUT_001"
        )
        detail = client.get(f"/api/incidents/{mysql_id}").json()

    assert len(incidents) == 2
    assert all(item["replay_only"] for item in incidents)
    assert summary["health"] == "healthy"
    assert summary["active_incident_count"] == 0
    assert detail["recording_type"] == "recorded_observation"
    assert detail["hypotheses"][0]["status"] == "CONTRADICTED"
    assert detail["hypotheses"][1]["status"] == "SUPPORTED"
    assert detail["report"]["root_cause"] == "MySQL Persistence Failure"
    assert [
        event["payload"]["tool_name"]
        for event in detail["traces"]
        if event["event_type"] == "tool_called"
    ] == ["get_business_metrics", "get_kafka_status", "get_mysql_health"]


def test_product_faultbench_report_unifies_both_real_scenarios():
    report = build_report(DEFAULT_BUNDLED_REPLAYS)

    assert report["scenario_count"] == 2
    assert report["aggregate"] == {
        "accuracy": 1.0,
        "evidence_coverage": 1.0,
        "tool_efficiency": 1.0,
        "trace_completeness": 1.0,
        "false_positive_count": 0,
        "negative_case_count": 0,
        "false_positive_rate": None,
        "false_positive_assessment": "not_applicable_no_negative_cases",
    }
