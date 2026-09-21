"""Phase S1 Skill discovery, progressive loading, and Runtime integration."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from llm.mock import MockLLMProvider
from memory.database import SQLiteDatabase
from runtime.models import IncidentCreate, IncidentTask
from runtime.orchestrator import RuntimeOrchestrator
from runtime.reports import DiagnosisStatus
from runtime.tool_registry import ToolRegistry
from skills.loader import SkillLoader
from skills.registry import SkillRegistry
from tool_contracts import EvidenceCompleteness, ObservationStatus, ToolObservation


def incident(title: str, description: str) -> IncidentTask:
    return IncidentTask.from_create(
        IncidentCreate(title=title, description=description)
    )


class CountingSkillLoader(SkillLoader):
    def __init__(self) -> None:
        super().__init__()
        self.metadata_loads = 0
        self.full_loads = 0

    def load_metadata(self, skill_path):
        self.metadata_loads += 1
        return super().load_metadata(skill_path)

    def load(self, metadata):
        self.full_loads += 1
        return super().load(metadata)


class NormalKafkaMCPClient:
    """Registry-compatible observations showing Kafka and logs are healthy."""

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
        now = datetime.now(timezone.utc)
        if tool_name == "get_kafka_status":
            return ToolObservation(
                evidence_id="evi_skill_kafka_normal",
                status=ObservationStatus.SUCCESS,
                kind="kafka_consumer_status",
                source="hmdp.kafka.consumer_group",
                source_tool=tool_name,
                collected_at=now,
                summary="Kafka consumer group is stable with normal lag.",
                completeness=EvidenceCompleteness.COMPLETE,
                raw_ref="fixture://skills/kafka-normal",
                data={
                    "consumer_status": "stable",
                    "member_count": 1,
                    "total_lag": 0,
                    "lag_threshold": 1000,
                    "lag_status": "normal",
                    "offsets_complete": True,
                    "partitions": [],
                },
            ).model_dump(mode="json")
        if tool_name == "search_application_logs":
            return ToolObservation(
                evidence_id="evi_skill_logs_normal",
                status=ObservationStatus.SUCCESS,
                kind="log",
                source="hmdp.spring_boot.runtime_logs",
                source_tool=tool_name,
                collected_at=now,
                summary="Application log search returned no error events.",
                completeness=EvidenceCompleteness.COMPLETE,
                raw_ref="fixture://skills/logs-normal",
                data={"events": [], "warnings": [], "truncated": False},
            ).model_dump(mode="json")
        raise ValueError(f"unsupported test tool: {tool_name}")


def build_runtime(tmp_path, provider: MockLLMProvider | None = None):
    database = SQLiteDatabase(tmp_path / "skills.db")
    database.initialize()
    selected_provider = provider or MockLLMProvider()
    return (
        database,
        selected_provider,
        RuntimeOrchestrator(
            database,
            NormalKafkaMCPClient(),
            llm_provider=selected_provider,
        ),
    )


def test_skill_metadata_loads_without_full_skill_content():
    loader = CountingSkillLoader()
    registry = SkillRegistry(loader=loader)

    discovered = registry.discover_skills()

    assert {item.skill_id for item in discovered} == {
        "incident-triage",
        "kafka-consumer-diagnosis",
    }
    assert loader.metadata_loads == 2
    assert loader.full_loads == 0
    kafka = registry.get_skill("kafka-consumer-diagnosis")
    assert kafka.name == "Kafka Consumer Diagnosis"
    assert kafka.category == "messaging"

    loaded = registry.load_skill(kafka.skill_id)
    assert loader.full_loads == 1
    assert "get_kafka_status" in loaded.instructions
    assert [reference.name for reference in loaded.references] == ["kafka-runbook"]


def test_kafka_incident_matches_domain_skill_before_general_triage():
    matches = SkillRegistry().match_skills(
        incident(
            "秒杀订单延迟",
            "Kafka consumer异常并且消息积压",
        )
    )

    assert matches[0].skill_id == "kafka-consumer-diagnosis"


def test_ordinary_incident_matches_triage_skill():
    matches = SkillRegistry().match_skills(
        incident("支付服务故障", "用户请求失败，需要只读排查")
    )

    assert matches[0].skill_id == "incident-triage"


def test_unmatched_incident_runs_without_skill(tmp_path):
    database, _, orchestrator = build_runtime(tmp_path)

    result = orchestrator.run(
        IncidentCreate(title="健康基线记录", description="例行采集服务状态")
    )

    event_types = [
        event["event_type"]
        for event in database.list_trace_events(result.incident_id)
    ]
    assert result.report.status is DiagnosisStatus.INCONCLUSIVE
    assert "skill_selected" not in event_types
    assert "skill_loaded" not in event_types


def test_skill_does_not_override_normal_kafka_evidence(tmp_path):
    database, provider, orchestrator = build_runtime(tmp_path)

    result = orchestrator.run(
        IncidentCreate(
            title="Kafka lag 告警",
            description="请诊断秒杀订单延迟",
        )
    )

    assert result.report.status is DiagnosisStatus.INCONCLUSIVE
    assert result.report.root_cause is None
    reflection_calls = [
        call for call in provider.calls if call["operation"] == "reflect"
    ]
    assert reflection_calls[0]["input"]["context"]["selected_skill"] is None
    assert (
        reflection_calls[0]["input"]["context"]["latest_observation"]["interpretation"]
        == "contradicts_kafka_consumer_unavailable_hypothesis"
    )
    assert len(database.list_evidence(result.incident_id)) == 2


def test_planner_receives_selected_skill_context_and_trace(tmp_path):
    database, provider, orchestrator = build_runtime(tmp_path)

    result = orchestrator.run(
        IncidentCreate(
            title="Kafka consumer异常",
            description="秒杀消息积压",
        )
    )

    first_plan = next(call for call in provider.calls if call["operation"] == "plan")
    selected = first_plan["input"]["context"]["selected_skill"]
    assert selected["skill_id"] == "kafka-consumer-diagnosis"
    assert "get_kafka_status" in selected["instructions"]
    assert selected["references"][0]["name"] == "kafka-runbook"

    trace = database.list_trace_events(result.incident_id)
    event_types = [event["event_type"] for event in trace]
    assert event_types.index("skill_selected") < event_types.index("skill_loaded")
    assert event_types.index("skill_loaded") < event_types.index("plan_created")
