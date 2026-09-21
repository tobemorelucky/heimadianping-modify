"""Read-only business metrics MCP, Context, Reflection, and permission tests."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from context.manager import ContextManager
from context.packet import ContextStage
from governance.models import PermissionLevel, RiskLevel, ToolCategory
from llm.mock import MockLLMProvider
from mcp_tools.business_metrics import (
    BusinessMetricsRequest,
    LogDerivedBusinessMetricsReader,
    collect_business_metrics,
)
from runtime.evidence import Evidence
from runtime.mcp_client import StdioMCPClient
from runtime.models import IncidentCreate, IncidentTask
from runtime.planner import Planner
from runtime.reflection import ReflectionDecision, ReflectionEngine
from runtime.tool_registry import (
    ToolManifest,
    ToolManifestEntry,
    ToolRegistry,
    ToolRegistryError,
)
from tool_contracts import EvidenceCompleteness, ObservationStatus, ToolObservation


def create_metric_project(tmp_path, *, counters: dict[str, int]):
    project_root = tmp_path / "hmdp"
    runtime_dir = project_root / "target" / "runtime"
    runtime_dir.mkdir(parents=True)
    lines = [f"AIOPS_METRIC {name}={value}" for name, value in counters.items()]
    (runtime_dir / "spring-boot.out.log").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )
    (runtime_dir / "spring-boot.error.log").write_text("", encoding="utf-8")
    return project_root


def normal_counters() -> dict[str, int]:
    return {
        "seckill_request_count": 100,
        "lua_admission_success_count": 100,
        "kafka_message_sent_count": 100,
        "order_created_success_count": 100,
        "order_created_failure_count": 0,
    }


def degraded_counters() -> dict[str, int]:
    return {
        "seckill_request_count": 100,
        "lua_admission_success_count": 100,
        "kafka_message_sent_count": 100,
        "order_created_success_count": 20,
        "order_created_failure_count": 80,
    }


def call_metrics(tmp_path, counters: dict[str, int]) -> dict:
    client = StdioMCPClient(project_root=create_metric_project(tmp_path, counters=counters))
    return client.call(
        "get_business_metrics",
        {"incident_id": "inc_business_metrics_test"},
    )


def to_evidence(observation: dict) -> Evidence:
    return Evidence(
        incident_id="inc_business_metrics_test",
        step_id="step_business_metrics",
        **observation,
    )


def kafka_evidence() -> Evidence:
    observation = ToolObservation(
        evidence_id="evi_business_kafka_status",
        status=ObservationStatus.SUCCESS,
        kind="kafka_consumer_status",
        source="hmdp.kafka.consumer_group",
        source_tool="get_kafka_status",
        collected_at=datetime.now(timezone.utc),
        summary="Kafka consumer group is stable with normal lag.",
        completeness=EvidenceCompleteness.COMPLETE,
        raw_ref="fixture://business/kafka",
        data={
            "consumer_status": "stable",
            "member_count": 1,
            "total_lag": 0,
            "lag_threshold": 1000,
            "lag_status": "normal",
            "offsets_complete": True,
            "partitions": [],
        },
    )
    return Evidence(
        incident_id="inc_business_metrics_test",
        step_id="step_kafka",
        **observation.model_dump(mode="json"),
    )


def test_normal_business_metrics_return_complete_observation(tmp_path):
    result = call_metrics(tmp_path, normal_counters())

    assert result["status"] == "success"
    assert result["kind"] == "business_metrics"
    assert result["source_tool"] == "get_business_metrics"
    assert result["data"]["seckill_request_count"] == 100
    assert result["data"]["order_created_success_count"] == 100
    card = ContextManager().build([to_evidence(result)]).evidence_cards[0]
    assert card.interpretation == "contradicts_business_pipeline_degradation"
    assert any("order_creation_rate=1.000" in fact for fact in card.facts)


def test_order_creation_drop_is_preserved_for_reflection(tmp_path):
    evidence = to_evidence(call_metrics(tmp_path, degraded_counters()))
    packet = ContextManager().build(
        [evidence],
        stage=ContextStage.REFLECTION,
        latest_evidence_id=evidence.evidence_id,
    )

    assert packet.latest_observation is not None
    assert packet.latest_observation.interpretation == (
        "supports_downstream_order_creation_degradation"
    )
    reflection = ReflectionEngine(MockLLMProvider()).evaluate(
        "秒杀业务链路订单创建下降",
        packet,
    )
    assert reflection.decision is ReflectionDecision.REPLAN
    assert reflection.hypothesis_status == "supports"
    assert "Consumer" in reflection.reason


def test_business_metrics_tool_failure_is_structured_observation():
    class FailingReader:
        def read(self, request):
            raise OSError("metric source unavailable")

    result = collect_business_metrics(
        BusinessMetricsRequest(incident_id="inc_business_failure"),
        reader=FailingReader(),
    )

    assert result.status is ObservationStatus.ERROR
    assert result.completeness is EvidenceCompleteness.UNKNOWN
    assert result.error is not None
    assert result.error.error_type == "OSError"
    assert result.error.retryable is True


def test_context_keeps_kafka_and_business_metrics_evidence(tmp_path):
    business = to_evidence(call_metrics(tmp_path, degraded_counters()))

    packet = ContextManager().build([business, kafka_evidence()])

    assert {card.kind for card in packet.evidence_cards} == {
        "business_metrics",
        "kafka_consumer_status",
    }
    assert set(packet.selected_evidence_ids) == {
        business.evidence_id,
        "evi_business_kafka_status",
    }


def test_permission_rejects_non_readonly_tool_call():
    action = ToolManifestEntry(
        tool_name="synthetic_action",
        description="Test-only action; no implementation exists.",
        input_schema={"type": "object", "properties": {}},
        permission="action:synthetic",
        server="test-only",
        category=ToolCategory.ACTION,
        permission_level=PermissionLevel.LOW_RISK_ACTION,
        risk_level=RiskLevel.LOW,
        approval_required=True,
    )
    registry = ToolRegistry(ToolManifest(version=1, tools=(action,)))

    with pytest.raises(ToolRegistryError, match="not READ_ONLY"):
        registry.require_read_only("synthetic_action")


def test_business_metrics_manifest_is_read_only():
    tool = ToolRegistry.from_file().require_read_only("get_business_metrics")

    assert tool.category is ToolCategory.OBSERVATION
    assert tool.permission == "read:business_metrics"
    assert tool.approval_required is False


def test_planner_can_select_business_metrics_for_pipeline_incident():
    registry = ToolRegistry.from_file()
    incident_task = IncidentTask.from_create(
        IncidentCreate(
            title="秒杀业务链路异常",
            description="业务指标显示订单创建数量下降",
        )
    )
    context = ContextManager().build([], stage=ContextStage.PLANNER)

    plan = Planner(MockLLMProvider()).create_plan(
        incident_task,
        context,
        [tool.planner_descriptor() for tool in registry.manifest.tools],
        version=1,
    )

    assert plan.action.tool_name == "get_business_metrics"
    assert plan.action.arguments["incident_id"] == incident_task.incident_id
