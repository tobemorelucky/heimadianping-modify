"""Validate the immutable E2.2 real-environment FaultBench recording."""

from pathlib import Path

from evaluation.runner import load_fault_case, load_real_observation_recording
from mcp_tools.mysql_health import (
    MysqlFailureClass,
    MysqlHealthState,
    classify_mysql_health,
)


ROOT = Path(__file__).resolve().parents[1]
FAULT_ID = "MYSQL_PERSISTENCE_TIMEOUT_001"


def test_real_timeout_case_links_to_real_recording():
    case = load_fault_case(ROOT / "evaluation" / "faults" / f"{FAULT_ID}.json")
    recording = load_real_observation_recording(
        ROOT / "evaluation" / "recordings" / f"{FAULT_ID}.json"
    )

    assert case.observation_source == "real_recording"
    assert recording.recording_type == "real_environment"
    assert recording.fault_id == case.fault_id
    assert {item.source_tool for item in recording.fault} == {
        "get_business_metrics", "get_kafka_status", "get_mysql_health"
    }


def test_recorded_raw_mysql_timeout_normalizes_without_mutating_recording():
    recording = load_real_observation_recording(
        ROOT / "evaluation" / "recordings" / f"{FAULT_ID}.json"
    )
    mysql = next(
        item for item in recording.fault if item.source_tool == "get_mysql_health"
    )

    assert mysql.data["database_reachable"] is None
    assert mysql.data["connection_test_status"] == "timeout"
    assert "health_state" not in mysql.data
    classification = classify_mysql_health(
        mysql.data["database_reachable"],
        mysql.data["connection_test_status"],
    )
    assert classification.health_state is MysqlHealthState.DEGRADED
    assert classification.failure_class == (MysqlFailureClass.CONNECTION_TIMEOUT,)


def test_real_normalized_validation_created_signal_and_active_incident():
    recording = load_real_observation_recording(
        ROOT / "evaluation" / "recordings" / f"{FAULT_ID}.json"
    )
    validation = recording.normalized_validation

    assert validation is not None
    assert validation.rule_id == "order-persistence-failure-v1"
    assert validation.rule_version == 2
    assert validation.signal_facts["mysql_failure_class"] == ["CONNECTION_TIMEOUT"]
    assert validation.incident_status == "ACTIVE"
    assert validation.diagnosis_status == "COMPLETED"
    assert validation.root_cause == "MySQL Persistence Failure"
