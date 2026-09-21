"""First deterministic HMDP fault diagnosis and evaluation loop."""

from evaluation.evaluator import FaultEvaluator
from evaluation.runner import (
    DEFAULT_FAULT_ID,
    EVALUATION_ROOT,
    EvaluationRunner,
    FixtureMCPClient,
    load_fault_case,
    load_fault_fixture,
)
from context.manager import ContextManager
from context.packet import ContextStage
from llm.mock import MockLLMProvider
from runtime.evidence import Evidence
from runtime.reflection import ReflectionDecision, ReflectionEngine
from runtime.reports import DiagnosisReport, DiagnosisStatus


def load_case_and_fixture():
    case = load_fault_case(
        EVALUATION_ROOT / "faults" / f"{DEFAULT_FAULT_ID}.json"
    )
    fixture = load_fault_fixture(
        EVALUATION_ROOT / "fixtures" / f"{DEFAULT_FAULT_ID}.json"
    )
    return case, fixture


def to_evidence(observation, *, step_id: str) -> Evidence:
    return Evidence(
        incident_id="inc_faultbench_test",
        step_id=step_id,
        **observation,
    )


def test_kafka_fault_case_runs_complete_agent_loop():
    case, fixture = load_case_and_fixture()

    report = EvaluationRunner().run(case, fixture, save_report=False)

    assert report.metrics.passed is True
    assert report.final_diagnosis.status == "confirmed"
    assert report.final_diagnosis.root_cause == case.expected_root_cause
    assert report.selected_tools == case.expected_tools
    assert report.metrics.evidence_coverage.score == 1.0


def test_fault_case_is_repeatable():
    case, fixture = load_case_and_fixture()
    runner = EvaluationRunner()

    first = runner.run(case, fixture, save_report=False)
    second = runner.run(case, fixture, save_report=False)

    assert first == second


def test_context_keeps_log_and_semantic_kafka_evidence():
    _, fixture = load_case_and_fixture()
    client = FixtureMCPClient(fixture)
    kafka = to_evidence(
        client.call("get_kafka_status", {}),
        step_id="step_kafka",
    )
    log = to_evidence(
        client.call("search_application_logs", {}),
        step_id="step_log",
    )

    packet = ContextManager().build([kafka, log])

    assert set(packet.selected_evidence_ids) == {
        kafka.evidence_id,
        log.evidence_id,
    }
    kafka_card = next(
        card for card in packet.evidence_cards
        if card.kind == "kafka_consumer_status"
    )
    assert kafka_card.summary == (
        "Consumer group has no active members and accumulated lag."
    )
    assert kafka_card.interpretation == (
        "supports_kafka_consumer_unavailable_hypothesis"
    )
    assert kafka_card.confidence == 0.95
    assert any("member_count=0" in fact for fact in kafka_card.facts)
    assert any("total_lag=50000" in fact for fact in kafka_card.facts)


def test_reflection_confirms_kafka_hypothesis_from_context():
    case, fixture = load_case_and_fixture()
    kafka = to_evidence(
        FixtureMCPClient(fixture).call("get_kafka_status", {}),
        step_id="step_kafka",
    )
    packet = ContextManager().build(
        [kafka],
        stage=ContextStage.REFLECTION,
        latest_evidence_id=kafka.evidence_id,
    )

    result = ReflectionEngine(MockLLMProvider()).evaluate(
        case.expected_root_cause,
        packet,
    )

    assert result.decision is ReflectionDecision.REPORT
    assert result.hypothesis_status == "supports"


def test_evaluator_distinguishes_success_and_failure():
    case, fixture = load_case_and_fixture()
    successful = EvaluationRunner().run(case, fixture, save_report=False)
    failed_diagnosis = DiagnosisReport(
        incident_id="inc_evaluator_failure",
        status=DiagnosisStatus.CONFIRMED,
        title="错误诊断",
        conclusion="错误地判断为网络问题。",
        root_cause="网络连接异常",
        confidence=0.9,
    )

    failed = FaultEvaluator().evaluate(case, failed_diagnosis, [], [])

    assert successful.metrics.passed is True
    assert failed.passed is False
    assert failed.root_cause_accuracy.score == 0.0
    assert failed.evidence_coverage.score == 0.0
    assert failed.trace_completeness.score == 0.0


def test_fault_case_trace_is_complete():
    case, fixture = load_case_and_fixture()

    report = EvaluationRunner().run(case, fixture, save_report=False)

    trace_metric = report.metrics.trace_completeness
    assert trace_metric.score == 1.0
    assert trace_metric.missing == ()
    assert set(trace_metric.required).issubset(
        {event.event_type for event in report.trace_summary}
    )
