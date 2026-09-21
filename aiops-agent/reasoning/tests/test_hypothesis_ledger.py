"""Evidence-grounded Hypothesis Ledger and Runtime integration tests."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Mapping

from evaluation.runner import (
    DEFAULT_FAULT_ID,
    EVALUATION_ROOT,
    EvaluationRunner,
    FixtureMCPClient,
    load_fault_case,
    load_fault_fixture,
)
from llm.mock import MockLLMProvider
from memory.database import SQLiteDatabase
from reasoning.ledger import HypothesisLedger
from reasoning.models import HypothesisStatus
from runtime.models import IncidentBudget, IncidentCreate, IncidentSource
from runtime.orchestrator import RuntimeOrchestrator
from runtime.tool_registry import ToolRegistry
from tool_contracts import EvidenceCompleteness, ObservationStatus, ToolObservation


class NormalBusinessMetricsMCPClient:
    """Return complete, aligned pipeline counters for contradiction testing."""

    def __init__(self) -> None:
        self.registry = ToolRegistry.from_file()

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
        if tool_name != "get_business_metrics":
            raise AssertionError(f"unexpected tool call: {tool_name}")
        return ToolObservation(
            evidence_id="evi_ledger_business_normal",
            status=ObservationStatus.SUCCESS,
            kind="business_metrics",
            source="hmdp.seckill.business_metrics",
            source_tool=tool_name,
            collected_at=datetime.now(timezone.utc),
            summary="All seckill pipeline counters are aligned.",
            completeness=EvidenceCompleteness.COMPLETE,
            raw_ref="fixture://ledger/business-normal",
            data={
                "seckill_request_count": 100,
                "lua_admission_success_count": 100,
                "kafka_message_sent_count": 100,
                "order_created_success_count": 100,
                "order_created_failure_count": 0,
                "unavailable_metrics": [],
            },
        ).model_dump(mode="json")


def test_ledger_create_update_evidence_and_rank():
    ledger = HypothesisLedger()
    kafka = ledger.create("Kafka consumer is unavailable", hypothesis_id="hyp_kafka")
    database = ledger.create("Database pool is exhausted", hypothesis_id="hyp_database")

    kafka = ledger.add_supporting_evidence(
        kafka.hypothesis_id,
        "evi_kafka_status",
        confidence=0.92,
    )
    database = ledger.add_contradicting_evidence(
        database.hypothesis_id,
        "evi_business_metrics",
        confidence=0.85,
    )
    database = ledger.update(
        database.hypothesis_id,
        status=HypothesisStatus.REJECTED,
    )

    assert kafka.status is HypothesisStatus.SUPPORTED
    assert kafka.supporting_evidence_refs == ("evi_kafka_status",)
    assert database.status is HypothesisStatus.REJECTED
    assert database.contradicting_evidence_refs == ("evi_business_metrics",)
    assert [item.hypothesis_id for item in ledger.rank()] == [
        "hyp_kafka",
        "hyp_database",
    ]


def test_kafka_and_log_evidence_support_same_runtime_hypothesis(tmp_path):
    case = load_fault_case(
        EVALUATION_ROOT / "faults" / f"{DEFAULT_FAULT_ID}.json"
    )
    fixture = load_fault_fixture(
        EVALUATION_ROOT / "fixtures" / f"{DEFAULT_FAULT_ID}.json"
    )
    database = SQLiteDatabase(tmp_path / "kafka-ledger.db")
    database.initialize()
    runtime = RuntimeOrchestrator(
        database,
        FixtureMCPClient(fixture),
        llm_provider=EvaluationRunner._provider(case, fixture),
    )

    result = runtime.run(
        IncidentCreate(
            title=case.incident_title,
            description=case.incident_description,
            source=IncidentSource.FAULTBENCH,
            affected_components=["kafka"],
            budget=IncidentBudget(max_tool_calls=4, max_reflections=2),
        )
    )

    final_hypothesis = result.report.final_hypothesis
    assert final_hypothesis is not None
    assert final_hypothesis.status is HypothesisStatus.SUPPORTED
    assert final_hypothesis.confidence == 0.96
    assert set(final_hypothesis.supporting_evidence_refs) == {
        f"evi_{DEFAULT_FAULT_ID.lower()}_kafka",
        f"evi_{DEFAULT_FAULT_ID.lower()}_log",
    }
    rows = database.list_hypotheses(result.incident_id)
    assert len(rows) == 1
    assert rows[0]["status"] == "SUPPORTED"


def test_business_metrics_contradict_wrong_hypothesis_and_update_confidence(tmp_path):
    database = SQLiteDatabase(tmp_path / "business-ledger.db")
    database.initialize()
    provider = MockLLMProvider()
    runtime = RuntimeOrchestrator(
        database,
        NormalBusinessMetricsMCPClient(),
        llm_provider=provider,
    )

    result = runtime.run(
        IncidentCreate(
            title="秒杀业务链路异常",
            description="业务指标显示订单创建可能下降",
        )
    )

    final_hypothesis = result.report.final_hypothesis
    assert final_hypothesis is not None
    assert final_hypothesis.status is HypothesisStatus.CONTRADICTED
    assert final_hypothesis.confidence == 0.85
    assert final_hypothesis.contradicting_evidence_refs == (
        "evi_ledger_business_normal",
    )
    reflection_call = next(
        call for call in provider.calls if call["operation"] == "reflect"
    )
    context_hypotheses = reflection_call["input"]["context"][
        "current_hypotheses"
    ]
    assert len(context_hypotheses) == 1
    assert context_hypotheses[0]["status"] == "UNKNOWN"


def test_report_and_trace_contain_final_evidence_grounded_hypothesis(tmp_path):
    database = SQLiteDatabase(tmp_path / "report-ledger.db")
    database.initialize()
    runtime = RuntimeOrchestrator(
        database,
        NormalBusinessMetricsMCPClient(),
        llm_provider=MockLLMProvider(),
    )

    result = runtime.run(
        IncidentCreate(
            title="秒杀业务指标检查",
            description="验证业务链路是否异常",
        )
    )

    persisted_report = database.get_report(result.incident_id)
    assert persisted_report is not None
    persisted_hypothesis = json.loads(persisted_report["final_hypothesis_json"])
    assert persisted_hypothesis["hypothesis_id"] == (
        result.report.final_hypothesis.hypothesis_id
    )
    assert persisted_hypothesis["contradicting_evidence_refs"] == [
        "evi_ledger_business_normal"
    ]

    trace = database.list_trace_events(result.incident_id)
    event_types = [item["event_type"] for item in trace]
    assert event_types.index("hypothesis_created") < event_types.index(
        "hypothesis_updated"
    )
    updated_payload = json.loads(
        next(
            item["payload_json"]
            for item in trace
            if item["event_type"] == "hypothesis_updated"
        )
    )
    assert updated_payload["status"] == "CONTRADICTED"
    assert updated_payload["confidence"] == 0.85
