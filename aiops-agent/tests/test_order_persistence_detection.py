"""Three-source persistence detection, negative evidence, and Incident handoff."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from pathlib import Path

from evaluation.runner import EvaluationRunner, FixtureMCPClient, load_fault_fixture
from incident.manager import IncidentManager
from incident.models import DiagnosisStatus, IncidentStatus
from incident.store import IncidentStore
from llm.mock import MockLLMProvider
from memory.database import SQLiteDatabase
from monitoring.collector import MonitoringCollector
from monitoring.detector.detector import DeterministicAnomalyDetector
from monitoring.detector.rules import order_persistence_failure_rule
from monitoring.detector.signal_store import SignalStore
from monitoring.models import CollectionResult, CollectionRun, CollectionSchedule
from monitoring.observation_store import ObservationStore
from monitoring.scheduler import MonitoringScheduler
from runtime.orchestrator import RuntimeOrchestrator
from runtime.tool_registry import ToolRegistry
from tool_contracts import EvidenceCompleteness, ObservationStatus, ToolError, ToolObservation


BASE = datetime(2026, 9, 22, 2, 0, tzinfo=timezone.utc)
METRICS = (
    "seckill_request_count", "lua_admission_success_count",
    "kafka_message_sent_count", "order_created_success_count",
    "order_created_failure_count",
)


@pytest.fixture
def environment(tmp_path):
    database = SQLiteDatabase(tmp_path / "order-persistence.db")
    observations = ObservationStore(database)
    observations.initialize()
    for schedule_id, tool in (
        ("business", "get_business_metrics"),
        ("kafka", "get_kafka_status"),
        ("mysql", "get_mysql_health"),
    ):
        observations.upsert_schedule(CollectionSchedule(
            schedule_id=schedule_id, tool_name=tool,
            interval_seconds=30, timeout_seconds=1,
        ))
    signals = SignalStore(database)
    signals.initialize()
    rule = order_persistence_failure_rule()
    signals.ensure_rule(rule)
    detector = DeterministicAnomalyDetector(observations, signals)
    return database, observations, signals, detector, rule


def persist(store, schedule, seconds, data, *, status=ObservationStatus.SUCCESS):
    ordinal = len(store.list_collection_runs()) + 1
    timestamp = BASE + timedelta(seconds=seconds)
    error = status in {ObservationStatus.ERROR, ObservationStatus.TIMEOUT}
    observation = ToolObservation(
        evidence_id=f"evi_persistence_{ordinal:04d}",
        status=status,
        kind={"business": "business_metrics", "kafka": "kafka_consumer_status", "mysql": "mysql_health"}[schedule],
        source=f"hmdp.{schedule}",
        source_tool={"business": "get_business_metrics", "kafka": "get_kafka_status", "mysql": "get_mysql_health"}[schedule],
        collected_at=timestamp,
        summary=f"{schedule} observation",
        completeness=(
            EvidenceCompleteness.UNKNOWN if error else
            EvidenceCompleteness.PARTIAL if status is ObservationStatus.PARTIAL else
            EvidenceCompleteness.COMPLETE
        ),
        data=data,
        error=ToolError(error_type="Unavailable", message="tool failed") if error else None,
    )
    run = CollectionRun(
        collection_run_id=f"col_persistence_{ordinal:04d}",
        schedule_id=schedule, tool=observation.source_tool,
        status=status, timestamp=timestamp,
        evidence_ref=observation.evidence_id, duration_ms=1,
    )
    store.save_collection_run(run, observation)
    return CollectionResult(run=run, observation=observation)


def business(requests, lua, published, created, failed, *, missing=False):
    values = dict(zip(METRICS, (requests, lua, published, created, failed)))
    return {
        **values,
        "observed_metrics": list(METRICS[:-1] if missing else METRICS),
        "unavailable_metrics": [METRICS[-1]] if missing else [],
    }


def kafka(*, members=1, lag=0):
    return {
        "topic": "hmdp.seckill.order.create.v1",
        "consumer_group": "hmdp-seckill-order-create-v1",
        "member_count": members,
        "total_lag": lag,
        "lag_status": "normal" if lag <= 1000 else "abnormal",
        "consumer_status": "stable" if members else "empty",
        "topic_exists": True,
        "offsets_complete": True,
        "partitions_truncated": False,
    }


def mysql(*, reachable=False, connection_status=None):
    status = connection_status or ("valid" if reachable else "connection_failed")
    if reachable is True and status == "valid":
        health_state = "HEALTHY"
        failure_class = []
    elif reachable is None and status == "timeout":
        health_state = "DEGRADED"
        failure_class = ["CONNECTION_TIMEOUT"]
    elif reachable is False and status in {"invalid", "connection_failed"}:
        health_state = "FAILED"
        failure_class = ["DATABASE_UNAVAILABLE"]
    else:
        health_state = "UNKNOWN"
        failure_class = ["UNKNOWN"]
    return {
        "source_role": "hmdp-consumer",
        "database_reachable": reachable,
        "connection_test_status": status,
        "health_state": health_state,
        "failure_class": failure_class,
        "hikari_active": None,
        "hikari_idle": None,
        "connection_timeout_count": None,
        "error_count": None,
    }


def populate(environment, *, kafka_members=1, kafka_lag=0, missing_business=False,
             mysql_status=ObservationStatus.PARTIAL):
    _, store, _, detector, rule = environment
    first = persist(store, "business", 0, business(100, 98, 97, 96, 0))
    second = persist(store, "business", 30, business(200, 196, 194, 108, 85, missing=missing_business),
                     status=ObservationStatus.PARTIAL if missing_business else ObservationStatus.SUCCESS)
    kafka_result = persist(store, "kafka", 31, kafka(members=kafka_members, lag=kafka_lag))
    mysql_result = persist(store, "mysql", 32, mysql(), status=mysql_status)
    return first, second, kafka_result, mysql_result, detector.assess_order_persistence(rule, mysql_result)


def test_correlated_signal_has_only_persisted_observation_refs(environment):
    _, store, signal_store, detector, _ = environment
    first, second, kafka_result, mysql_result, assessment = populate(environment)
    assert assessment.outcome == "matched"
    emitted = detector.evaluate(mysql_result)
    assert len(emitted) == 1
    assert emitted[0].observation_refs == tuple(
        item.observation.evidence_id for item in (first, second, kafka_result, mysql_result)
    )
    assert signal_store.list_signals()[0].observation_refs == emitted[0].observation_refs
    assert len(store.list_observations_across_schedules(since=BASE, until=BASE + timedelta(seconds=32))) == 4
    assert detector.evaluate(mysql_result) == []


def test_real_timeout_semantics_emit_correlated_signal(environment):
    _, store, _, detector, rule = environment
    first = persist(store, "business", 0, business(1, 1, 1, 1, 0))
    second = persist(store, "business", 30, business(3, 3, 3, 1, 4))
    kafka_result = persist(store, "kafka", 31, kafka(members=1, lag=0))
    mysql_result = persist(
        store,
        "mysql",
        32,
        mysql(reachable=None, connection_status="timeout"),
        status=ObservationStatus.PARTIAL,
    )

    assessment = detector.assess_order_persistence(rule, mysql_result)
    assert assessment.outcome == "matched"
    assert assessment.facts["mysql_health_state"] == "DEGRADED"
    assert assessment.facts["mysql_failure_class"] == ["CONNECTION_TIMEOUT"]
    signal = detector.evaluate(mysql_result)[0]
    assert signal.observation_refs == tuple(
        item.observation.evidence_id
        for item in (first, second, kafka_result, mysql_result)
    )


@pytest.mark.parametrize("members,lag", [(0, 5_000), (1, 5_000)])
def test_kafka_abnormal_does_not_become_mysql_signal(environment, members, lag):
    _, _, _, detector, _ = environment
    _, _, _, current, assessment = populate(environment, kafka_members=members, kafka_lag=lag)
    assert assessment.outcome == "not_matched"
    assert detector.evaluate(current) == []


def test_missing_business_metrics_is_insufficient(environment):
    _, _, _, detector, _ = environment
    _, _, _, current, assessment = populate(environment, missing_business=True)
    assert assessment.outcome == "insufficient"
    assert detector.evaluate(current) == []


def test_healthy_mysql_does_not_emit_signal(environment):
    _, store, _, detector, rule = environment
    persist(store, "business", 0, business(100, 98, 97, 96, 0))
    persist(store, "business", 30, business(200, 196, 194, 108, 85))
    persist(store, "kafka", 31, kafka())
    current = persist(store, "mysql", 32, mysql(reachable=True), status=ObservationStatus.PARTIAL)
    assert detector.assess_order_persistence(rule, current).outcome == "not_matched"
    assert detector.evaluate(current) == []


def test_unknown_mysql_failure_class_is_insufficient(environment):
    _, store, _, detector, rule = environment
    persist(store, "business", 0, business(100, 98, 97, 96, 0))
    persist(store, "business", 30, business(200, 196, 194, 108, 85))
    persist(store, "kafka", 31, kafka())
    current = persist(
        store,
        "mysql",
        32,
        mysql(reachable=None, connection_status="probe_error"),
        status=ObservationStatus.PARTIAL,
    )
    assessment = detector.assess_order_persistence(rule, current)
    assert assessment.outcome == "insufficient"
    assert detector.evaluate(current) == []


def test_mysql_observation_error_is_insufficient(environment):
    _, _, _, detector, _ = environment
    _, _, _, current, assessment = populate(environment, mysql_status=ObservationStatus.ERROR)
    assert assessment.outcome == "insufficient"
    assert detector.evaluate(current) == []


def test_kafka_observation_error_is_insufficient(environment):
    _, store, _, detector, rule = environment
    persist(store, "business", 0, business(100, 98, 97, 96, 0))
    persist(store, "business", 30, business(200, 196, 194, 108, 85))
    persist(store, "kafka", 31, kafka(), status=ObservationStatus.ERROR)
    current = persist(store, "mysql", 32, mysql(), status=ObservationStatus.PARTIAL)
    assessment = detector.assess_order_persistence(rule, current)
    assert assessment.outcome == "insufficient"
    assert detector.evaluate(current) == []


def test_signal_creates_active_incident_and_existing_runtime_diagnoses(environment):
    database, _, _, detector, _ = environment
    _, _, _, current, _ = populate(environment)
    signal = detector.evaluate(current)[0]
    fixture = load_fault_fixture(
        Path(__file__).resolve().parents[1] / "evaluation" / "fixtures"
        / "MYSQL_PERSISTENCE_FAILURE_001.json"
    )
    incident_store = IncidentStore(database)
    incident_store.initialize()
    runtime = RuntimeOrchestrator(database, FixtureMCPClient(fixture), llm_provider=MockLLMProvider())
    manager = IncidentManager(incident_store, runtime)
    result = manager.handle_signal(signal)
    assert result.created
    assert result.incident.status is IncidentStatus.ACTIVE
    assert result.incident.diagnosis_status is DiagnosisStatus.COMPLETED
    assert result.incident.diagnosis_report_id is not None
    hypothesis_rows = database.list_hypotheses(result.incident.incident_id)
    assert any(row["status"] == "CONTRADICTED" for row in hypothesis_rows)
    assert any(row["status"] == "SUPPORTED" for row in hypothesis_rows)
    trace_rows = database.list_trace_events(result.incident.incident_id)
    tool_names = [
        json.loads(row["payload_json"])["tool_name"]
        for row in trace_rows if row["event_type"] == "tool_started"
    ]
    assert tool_names == ["get_business_metrics", "get_kafka_status", "get_mysql_health"]
    report = database.get_report(result.incident.incident_id)
    assert report is not None
    assert report["root_cause"] == "MySQL Persistence Failure"


def test_mysql_faultbench_case_passes_without_real_fault(tmp_path):
    report = EvaluationRunner(reports_dir=tmp_path).run_fault_id(
        "MYSQL_PERSISTENCE_FAILURE_001", save_report=False
    )
    assert report.metrics.passed
    assert report.selected_tools == (
        "get_business_metrics", "get_kafka_status", "get_mysql_health"
    )


def test_scheduler_automatically_triggers_incident_and_diagnosis(environment):
    database, observations, signals, detector, _ = environment
    fixture = load_fault_fixture(
        Path(__file__).resolve().parents[1] / "evaluation" / "fixtures"
        / "MYSQL_PERSISTENCE_FAILURE_001.json"
    )

    class ScheduledFixtureClient(FixtureMCPClient):
        business_calls = 0

        def call(self, tool_name, arguments=None):
            if tool_name == "get_business_metrics":
                self.business_calls += 1
                baseline = business(100, 98, 97, 96, 0)
                degraded = business(200, 196, 194, 108, 85)
                observation = self._business_observation().model_copy(update={
                    "evidence_id": f"evi_scheduled_business_{self.business_calls}",
                    "data": baseline if self.business_calls == 1 else degraded,
                })
                return observation.model_dump(mode="json")
            return super().call(tool_name, arguments)

    client = ScheduledFixtureClient(fixture)
    registry = ToolRegistry.from_file()
    now = [BASE]
    collector = MonitoringCollector(client, registry, observations, clock=lambda: now[0])
    incident_store = IncidentStore(database)
    incident_store.initialize()
    manager = IncidentManager(
        incident_store,
        RuntimeOrchestrator(database, FixtureMCPClient(fixture), llm_provider=MockLLMProvider()),
        clock=lambda: now[0],
    )
    scheduler = MonitoringScheduler(
        observations, collector, detector=detector,
        incident_manager=manager, clock=lambda: now[0],
    )
    assert len(scheduler.run_due(force=True)) == 3
    assert incident_store.list_incidents() == []
    now[0] += timedelta(seconds=30)
    assert len(scheduler.run_due(force=True)) == 3
    incidents = incident_store.list_incidents()
    assert len(incidents) == 1
    assert incidents[0].status is IncidentStatus.ACTIVE
    assert incidents[0].diagnosis_status is DiagnosisStatus.COMPLETED
    assert len(signals.list_signals()) == 1
