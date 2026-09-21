"""Official MCP SDK server, stdio client, and log tool tests."""

import pytest

from llm.mock import MockLLMProvider
from memory.database import SQLiteDatabase
from runtime.mcp_client import StdioMCPClient
from runtime.models import IncidentCreate
from runtime.orchestrator import RuntimeOrchestrator


ALLOWED_LOGS = [
    "target/runtime/spring-boot.out.log",
    "target/runtime/spring-boot.error.log",
]


def create_log_project(tmp_path, *, create_logs: bool = True):
    project_root = tmp_path / "hmdp"
    if create_logs:
        runtime_dir = project_root / "target" / "runtime"
        runtime_dir.mkdir(parents=True)
        (runtime_dir / "spring-boot.out.log").write_text(
            "INFO application started\nERROR order creation failed\n",
            encoding="utf-8",
        )
        (runtime_dir / "spring-boot.error.log").write_text("", encoding="utf-8")
    return project_root


def test_mcp_server_starts_and_lists_all_readonly_observation_tools(tmp_path):
    client = StdioMCPClient(project_root=create_log_project(tmp_path))

    schemas = client.get_tool_schemas()

    by_name = {schema["name"]: schema for schema in schemas}
    assert set(by_name) == {
        "search_application_logs",
        "get_kafka_status",
        "get_business_metrics",
    }
    assert {"incident_id", "log_paths"}.issubset(
        by_name["search_application_logs"]["input_schema"]["required"]
    )
    assert set(
        by_name["get_kafka_status"]["input_schema"]["properties"]
    ) == {"topic", "consumer_group"}
    assert set(
        by_name["get_business_metrics"]["input_schema"]["properties"]
    ) == {"incident_id", "max_lines"}


def test_log_tool_returns_uniform_evidence_envelope(tmp_path):
    client = StdioMCPClient(project_root=create_log_project(tmp_path))

    result = client.call(
        "search_application_logs",
        {
            "incident_id": "inc_mcp_success",
            "log_paths": ALLOWED_LOGS,
            "query": "ERROR",
            "max_lines": 20,
        },
    )

    assert {
        "evidence_id",
        "status",
        "kind",
        "source",
        "source_tool",
        "collected_at",
        "observation_window",
        "summary",
        "completeness",
        "raw_ref",
        "data",
        "error",
    } == set(result)
    assert result["status"] == "success"
    assert result["kind"] == "log"
    assert result["source"] == "hmdp.spring_boot.runtime_logs"
    assert result["source_tool"] == "search_application_logs"
    assert result["completeness"] == "complete"
    assert len(result["data"]["events"]) == 1


def test_missing_log_files_return_partial_evidence(tmp_path):
    client = StdioMCPClient(
        project_root=create_log_project(tmp_path, create_logs=False)
    )

    result = client.call(
        "search_application_logs",
        {
            "incident_id": "inc_missing_logs",
            "log_paths": ALLOWED_LOGS,
        },
    )

    assert result["completeness"] == "partial"
    assert result["status"] == "partial"
    assert result["data"]["events"] == []
    assert len(result["data"]["warnings"]) == 2


def test_non_allowlisted_log_path_is_rejected(tmp_path):
    client = StdioMCPClient(project_root=create_log_project(tmp_path))

    result = client.call(
        "search_application_logs",
        {
            "incident_id": "inc_forbidden_path",
            "log_paths": ["application.yaml"],
        },
    )

    assert result["status"] == "error"
    assert result["completeness"] == "unknown"
    assert result["error"]["error_type"] == "MCPToolError"


def test_tool_error_is_evidence_and_reaches_reflection(tmp_path):
    project_root = create_log_project(tmp_path)
    database = SQLiteDatabase(tmp_path / "aiops.db")
    database.initialize()
    provider = MockLLMProvider(
        scripted_responses={
            "plan": [
                {
                    "objective": "读取非白名单文件",
                    "hypothesis": "配置异常",
                    "tool_name": "search_application_logs",
                    "arguments": {"log_paths": ["application.yaml"]},
                }
            ]
        }
    )
    orchestrator = RuntimeOrchestrator(
        database,
        StdioMCPClient(project_root=project_root),
        llm_provider=provider,
    )

    result = orchestrator.run(
        IncidentCreate(title="日志检查", description="验证工具错误 Trace")
    )

    incident = database.get_incident(result.incident_id)
    traces = database.list_trace_events(result.incident_id)
    evidence = database.list_evidence(result.incident_id)
    assert incident["status"] == "awaiting_human"
    assert "tool_failed" in [item["event_type"] for item in traces]
    assert traces[-1]["event_type"] == "report_generated"
    assert evidence[0]["status"] == "error"
    assert result.report.status.value == "inconclusive"


def test_kafka_mcp_connection_failure_returns_structured_evidence(tmp_path):
    client = StdioMCPClient(
        project_root=create_log_project(tmp_path),
        timeout_seconds=5,
        kafka_bootstrap_servers="127.0.0.1:1",
        kafka_request_timeout_ms=500,
    )

    result = client.call("get_kafka_status", {})

    assert result["status"] == "error"
    assert result["kind"] == "kafka_consumer_status"
    assert result["source_tool"] == "get_kafka_status"
    assert result["completeness"] == "unknown"
    assert result["data"]["broker_reachable"] is False
    assert result["error"]["retryable"] is True
