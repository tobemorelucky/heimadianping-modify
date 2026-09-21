"""End-to-end tests for the Runtime through the real stdio MCP boundary."""

import json

from memory.database import SQLiteDatabase
from runtime.mcp_client import StdioMCPClient
from runtime.models import IncidentCreate
from runtime.orchestrator import RuntimeOrchestrator
from runtime.reports import DiagnosisStatus


def create_log_files(project_root, *, stdout: str = "", stderr: str = ""):
    runtime_dir = project_root / "target" / "runtime"
    runtime_dir.mkdir(parents=True)
    (runtime_dir / "spring-boot.out.log").write_text(stdout, encoding="utf-8")
    (runtime_dir / "spring-boot.error.log").write_text(stderr, encoding="utf-8")


def build_runtime(tmp_path, *, stdout: str = "", stderr: str = ""):
    project_root = tmp_path / "hmdp"
    create_log_files(project_root, stdout=stdout, stderr=stderr)
    database = SQLiteDatabase(tmp_path / "aiops.db")
    database.initialize()
    mcp_client = StdioMCPClient(project_root=project_root)
    return database, RuntimeOrchestrator(database, mcp_client)


def incident_request() -> IncidentCreate:
    return IncidentCreate(
        title="秒杀订单延迟",
        description="用户反馈订单创建缓慢",
    )


def test_runtime_calls_stdio_mcp_and_reports_log_error(tmp_path):
    database, orchestrator = build_runtime(
        tmp_path,
        stderr="2026-09-19 ERROR VoucherOrderService - order creation failed\n",
    )

    result = orchestrator.run(incident_request())

    assert result.report.status is DiagnosisStatus.CONFIRMED
    assert result.report.root_cause == "应用日志中的错误事件与当前故障相关。"
    evidence = database.list_evidence(result.incident_id)
    assert len(evidence) == 1
    assert evidence[0]["source_tool"] == "search_application_logs"
    assert database.get_incident(result.incident_id)["status"] == "awaiting_human"


def test_runtime_reports_inconclusive_when_logs_contain_no_error(tmp_path):
    database, orchestrator = build_runtime(
        tmp_path,
        stdout="2026-09-19 INFO HmDianPingApplication - started\n",
    )

    result = orchestrator.run(incident_request())

    assert result.report.status is DiagnosisStatus.INCONCLUSIVE
    assert result.report.root_cause is None
    assert "没有发现" in result.report.conclusion
    assert len(database.list_plan_versions(result.incident_id)) == 1


def test_trace_contains_complete_mcp_event_sequence(tmp_path):
    database, orchestrator = build_runtime(
        tmp_path,
        stderr="ERROR database connection failed\n",
    )
    result = orchestrator.run(incident_request())

    event_types = [
        row["event_type"]
        for row in database.list_trace_events(result.incident_id)
    ]
    required_order = [
        "incident_created",
        "plan_created",
        "tool_started",
        "tool_completed",
        "evidence_added",
        "hypothesis_updated",
        "report_generated",
    ]

    positions = [event_types.index(event_type) for event_type in required_order]
    assert positions == sorted(positions)


def test_trace_records_bounded_context_selection_and_budget(tmp_path):
    database, orchestrator = build_runtime(
        tmp_path,
        stderr="ERROR database connection failed\n",
    )
    result = orchestrator.run(incident_request())

    contexts = [
        row
        for row in database.list_trace_events(result.incident_id)
        if row["event_type"] == "context_built"
    ]

    assert [item["stage"] for item in contexts] == ["context", "context"]
    assert len(contexts) == 2
    reflection_payload = json.loads(contexts[-1]["payload_json"])
    assert reflection_payload["stage"] == "reflection"
    assert len(reflection_payload["selected_evidence_ids"]) == 1
    assert reflection_payload["budget"]["max_evidence_cards"] == 8
    assert reflection_payload["budget"]["selected_evidence_count"] == 1
