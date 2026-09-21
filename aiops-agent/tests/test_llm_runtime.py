"""Structured LLM provider and runtime boundary tests."""

import json

import httpx
import pytest

from config import load_settings
from context.manager import ContextManager
from context.packet import ContextStage
from llm.factory import build_llm_provider
from llm.mock import MockLLMProvider
from llm.provider import LLMResponseValidationError, OpenAICompatibleProvider
from memory.database import SQLiteDatabase
from runtime.mcp_client import StdioMCPClient
from runtime.models import IncidentCreate, IncidentTask
from runtime.orchestrator import RuntimeOrchestrator
from runtime.planner import Planner
from runtime.plans import PlanOutput
from runtime.tool_registry import ToolRegistry


def incident_task() -> IncidentTask:
    return IncidentTask.from_create(
        IncidentCreate(
            title="秒杀订单延迟",
            description="用户反馈订单创建缓慢",
        )
    )


def mcp_client(tmp_path) -> StdioMCPClient:
    project_root = tmp_path / "hmdp"
    runtime_dir = project_root / "target" / "runtime"
    runtime_dir.mkdir(parents=True)
    (runtime_dir / "spring-boot.out.log").write_text(
        "ERROR order creation failed\n",
        encoding="utf-8",
    )
    (runtime_dir / "spring-boot.error.log").write_text("", encoding="utf-8")
    return StdioMCPClient(project_root=project_root)


def test_no_llm_environment_uses_mock_provider(tmp_path):
    settings = load_settings(env_file=tmp_path / "missing.env", environ={})

    provider = build_llm_provider(settings)

    assert isinstance(provider, MockLLMProvider)
    assert settings.ai_model_provider == "mock"


def test_invalid_json_fails_runtime_and_marks_incident_failed(tmp_path):
    provider = MockLLMProvider(scripted_responses={"plan": ["not-json"]})
    database = SQLiteDatabase(tmp_path / "aiops.db")
    database.initialize()
    orchestrator = RuntimeOrchestrator(
        database,
        mcp_client(tmp_path),
        llm_provider=provider,
    )

    with pytest.raises(LLMResponseValidationError, match="not valid JSON"):
        orchestrator.run(
            IncidentCreate(
                title="秒杀订单延迟",
                description="用户反馈订单创建缓慢",
            )
        )

    with database.connect() as connection:
        row = connection.execute("SELECT status FROM incidents").fetchone()
    assert row["status"] == "failed"


def test_planner_rejects_llm_tool_outside_mcp_tool_allowlist():
    provider = MockLLMProvider(
        scripted_responses={
            "plan": [
                {
                    "objective": "读取 Redis",
                    "hypothesis": "Redis 异常",
                    "tool_name": "get_redis_status",
                    "arguments": {},
                }
            ]
        }
    )
    with pytest.raises(LLMResponseValidationError, match="authorized registry"):
        Planner(provider).create_plan(
            incident_task(),
            ContextManager().build([], stage=ContextStage.PLANNER),
            [
                {
                    "name": "search_application_logs",
                    "description": "Search logs",
                    "input_schema": {"type": "object"},
                }
            ],
            version=1,
        )


def test_mock_planner_can_select_registered_kafka_tool():
    provider = MockLLMProvider()
    registry = ToolRegistry.from_file()
    incident = IncidentTask.from_create(
        IncidentCreate(
            title="Kafka lag 告警",
            description="秒杀订单消费者出现消息积压",
        )
    )

    plan = Planner(provider).create_plan(
        incident,
        ContextManager().build([], stage=ContextStage.PLANNER),
        [tool.planner_descriptor() for tool in registry.manifest.tools],
        version=1,
    )

    assert plan.action.tool_name == "get_kafka_status"
    assert plan.action.arguments == {}


def test_runtime_uses_llm_provider_for_plan_reflection_and_report(tmp_path):
    provider = MockLLMProvider()
    database = SQLiteDatabase(tmp_path / "aiops.db")
    database.initialize()
    orchestrator = RuntimeOrchestrator(
        database,
        mcp_client(tmp_path),
        llm_provider=provider,
    )

    orchestrator.run(
        IncidentCreate(
            title="秒杀订单延迟",
            description="用户反馈订单创建缓慢",
        )
    )

    assert [call["operation"] for call in provider.calls] == [
        "plan",
        "reflect",
        "report",
    ]
    plan_input = provider.calls[0]["input"]
    reflection_input = provider.calls[1]["input"]
    assert "context" in plan_input and "evidence" not in plan_input
    assert "context" in reflection_input
    assert "evidence" not in reflection_input
    assert "observation" not in reflection_input


def test_openai_compatible_provider_uses_chat_completions_contract():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == "https://model.example/v1/chat/completions"
        assert request.headers["authorization"] == "Bearer test-key"
        body = json.loads(request.content)
        assert body["model"] == "test-model"
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": (
                                '{"objective":"check logs","hypothesis":"errors",'
                                '"tool_name":"search_application_logs","arguments":{}}'
                            )
                        }
                    }
                ]
            },
        )

    provider = OpenAICompatibleProvider(
        base_url="https://model.example/v1/",
        api_key="test-key",
        model="test-model",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    output = provider.generate_structured(
        operation="plan",
        system_prompt="Return a plan.",
        input_payload={"incident": {}},
        response_model=PlanOutput,
    )

    assert output.tool_name == "search_application_logs"
