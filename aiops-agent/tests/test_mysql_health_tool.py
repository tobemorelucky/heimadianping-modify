"""Read-only Consumer MySQL health Tool and MCP contract tests."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from context.manager import ContextManager
from context.packet import ContextStage
from mcp_tools.mysql_health import (
    ConsumerMysqlHealthSnapshot,
    MysqlHealthRequest,
    collect_mysql_health,
)
from runtime.mcp_client import StdioMCPClient
from runtime.evidence import Evidence
from runtime.tool_registry import ToolRegistry
from tool_contracts import ObservationStatus


def snapshot(**overrides) -> ConsumerMysqlHealthSnapshot:
    data = {
        "source_role": "hmdp-consumer",
        "collected_at": datetime(2026, 9, 22, tzinfo=timezone.utc),
        "database_reachable": True,
        "connection_test_status": "valid",
        "hikari_active": 2,
        "hikari_idle": 3,
        "connection_timeout_count": None,
        "error_count": None,
        "unavailable_metrics": ["connection_timeout_count", "error_count"],
    }
    data.update(overrides)
    return ConsumerMysqlHealthSnapshot.model_validate(data)


class StaticReader:
    def __init__(self, value=None, error=None):
        self.value = value
        self.error = error

    def read(self):
        if self.error:
            raise self.error
        return self.value


def test_healthy_database_preserves_real_pool_values_and_missing_counters():
    result = collect_mysql_health(
        MysqlHealthRequest(), reader=StaticReader(snapshot())
    )

    assert result.status is ObservationStatus.PARTIAL
    assert result.kind == "mysql_health"
    assert result.source_tool == "get_mysql_health"
    assert result.data["source_role"] == "hmdp-consumer"
    assert result.data["database_reachable"] is True
    assert result.data["hikari_active"] == 2
    assert result.data["hikari_idle"] == 3
    assert result.data["connection_timeout_count"] is None
    assert result.data["error_count"] is None


def test_unreachable_database_is_not_conflated_with_tool_failure():
    result = collect_mysql_health(
        MysqlHealthRequest(),
        reader=StaticReader(snapshot(
            database_reachable=False,
            connection_test_status="connection_failed",
        )),
    )
    assert result.status is ObservationStatus.PARTIAL
    assert result.data["database_reachable"] is False
    assert result.data["connection_test_status"] == "connection_failed"


def test_missing_hikari_metrics_are_null_and_explicit():
    result = collect_mysql_health(
        MysqlHealthRequest(),
        reader=StaticReader(snapshot(hikari_active=None, hikari_idle=None)),
    )
    assert result.status is ObservationStatus.PARTIAL
    assert result.data["hikari_active"] is None
    assert result.data["hikari_idle"] is None
    assert {"hikari_active", "hikari_idle"}.issubset(
        result.data["unavailable_metrics"]
    )


def test_consumer_role_mismatch_is_rejected():
    error = None
    try:
        ConsumerMysqlHealthSnapshot.model_validate({
            **snapshot().model_dump(mode="json"), "source_role": "hmdp-web"
        })
    except ValueError as exc:
        error = exc
    result = collect_mysql_health(MysqlHealthRequest(), reader=StaticReader(error=error))
    assert result.status is ObservationStatus.ERROR
    assert result.error.error_type == "ConsumerRoleMismatch"
    assert result.data["source_role"] is None
    assert result.data["expected_source_role"] == "hmdp-consumer"


def test_endpoint_timeout_is_structured():
    result = collect_mysql_health(
        MysqlHealthRequest(), reader=StaticReader(error=TimeoutError("probe timed out"))
    )
    assert result.status is ObservationStatus.TIMEOUT
    assert result.error.error_type == "HealthEndpointTimeout"


def test_manifest_authorizes_only_readonly_mysql_tool():
    entry = ToolRegistry.from_file().require_read_only("get_mysql_health")
    assert entry.category.value == "observation"
    assert entry.permission_level.value == "READ_ONLY"
    assert entry.risk_level.value == "none"
    assert entry.approval_required is False
    assert set(entry.input_schema["properties"]) == {"incident_id"}


def test_context_packet_keeps_consumer_health_facts_without_raw_data():
    observation = collect_mysql_health(
        MysqlHealthRequest(), reader=StaticReader(snapshot())
    )
    evidence = Evidence.model_validate({
        **observation.model_dump(mode="json"),
        "incident_id": "inc_mysql_context",
        "step_id": "step_mysql",
    })

    packet = ContextManager().build(
        [evidence], stage=ContextStage.REFLECTION,
        latest_evidence_id=evidence.evidence_id,
    )

    assert packet.latest_observation.kind == "mysql_health"
    assert "hikari_active=2" in " ".join(packet.latest_observation.facts)
    assert "connection_timeout_count=unavailable" in " ".join(packet.latest_observation.facts)
    assert "data" not in packet.latest_observation.model_dump()


def test_runtime_calls_mysql_health_through_stdio_mcp(tmp_path):
    payload = snapshot().model_dump(mode="json")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            assert self.path == "/internal/aiops/mysql-health"
            data = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = StdioMCPClient(
            project_root=tmp_path, mysql_health_port=server.server_port
        )
        assert ToolRegistry.from_file().validate_discovery(client.get_tool_schemas())
        result = client.call("get_mysql_health", {"incident_id": "inc_mysql_health"})
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert result["source_tool"] == "get_mysql_health"
    assert result["data"]["source_role"] == "hmdp-consumer"
    assert result["data"]["database_reachable"] is True
