"""Live loopback Web/Consumer business metrics merge contract tests."""

from __future__ import annotations

import json
import socket
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from mcp_tools.business_metrics import (
    BusinessMetricsRequest,
    CONSUMER_METRICS_PATH,
    JvmBusinessMetricsReader,
    WEB_METRICS_PATH,
    collect_business_metrics,
)
from runtime.mcp_client import StdioMCPClient
from tool_contracts import ObservationStatus


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _web_payload() -> dict:
    return {
        "source_role": "hmdp-web",
        "started_at": _now(),
        "collected_at": _now(),
        "seckill_request_count": 7,
        "lua_admission_success_count": 6,
        "kafka_message_sent_count": 6,
    }


def _consumer_payload() -> dict:
    return {
        "source_role": "hmdp-consumer",
        "started_at": _now(),
        "collected_at": _now(),
        "order_created_success_count": 6,
        "order_created_failure_count": 0,
    }


@contextmanager
def _serve(path: str, payload: dict, *, delay: float = 0):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != path:
                self.send_response(404)
                self.end_headers()
                return
            if delay:
                time.sleep(delay)
            body = json.dumps(payload).encode("utf-8")
            try:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, format, *args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_port
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _unused_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _collect(web_port: int, consumer_port: int, *, timeout: float = 1.0):
    return collect_business_metrics(
        BusinessMetricsRequest(incident_id="inc_jvm_metrics"),
        reader=JvmBusinessMetricsReader(
            web_port=web_port,
            consumer_port=consumer_port,
            timeout_seconds=timeout,
        ),
    )


def test_web_and_consumer_metrics_merge_through_mcp(tmp_path):
    with _serve(WEB_METRICS_PATH, _web_payload()) as web_port, _serve(
        CONSUMER_METRICS_PATH, _consumer_payload()
    ) as consumer_port:
        client = StdioMCPClient(
            project_root=tmp_path,
            business_metrics_web_port=web_port,
            business_metrics_consumer_port=consumer_port,
        )
        result = client.call(
            "get_business_metrics",
            {"incident_id": "inc_jvm_metrics"},
        )

    assert result["status"] == "success"
    assert result["completeness"] == "complete"
    assert result["data"]["source_roles"] == ["hmdp-web", "hmdp-consumer"]
    assert result["data"]["metrics"] == {
        "seckill_request_count": 7,
        "lua_admission_success_count": 6,
        "kafka_message_sent_count": 6,
        "order_created_success_count": 6,
        "order_created_failure_count": 0,
    }
    # Flat fields remain available to Context Manager and deterministic Detector.
    assert result["data"]["kafka_message_sent_count"] == 6
    assert result["data"]["order_created_success_count"] == 6


def test_consumer_not_started_returns_partial_web_evidence():
    with _serve(WEB_METRICS_PATH, _web_payload()) as web_port:
        result = _collect(web_port, _unused_port())

    assert result.status is ObservationStatus.PARTIAL
    assert result.data["source_roles"] == ["hmdp-web"]
    assert result.data["source_statuses"]["hmdp-consumer"] in {"error", "timeout"}
    assert "order_created_success_count" in result.data["unavailable_metrics"]


def test_consumer_timeout_is_explicit_and_does_not_hide_web_metrics():
    with _serve(WEB_METRICS_PATH, _web_payload()) as web_port, _serve(
        CONSUMER_METRICS_PATH, _consumer_payload(), delay=0.25
    ) as consumer_port:
        result = _collect(web_port, consumer_port, timeout=0.05)

    assert result.status is ObservationStatus.PARTIAL
    assert result.data["source_statuses"]["hmdp-consumer"] == "timeout"
    assert result.data["seckill_request_count"] == 7


def test_missing_web_metric_rejects_that_source_without_fabricating_value():
    incomplete_web = _web_payload()
    incomplete_web.pop("kafka_message_sent_count")
    with _serve(WEB_METRICS_PATH, incomplete_web) as web_port, _serve(
        CONSUMER_METRICS_PATH, _consumer_payload()
    ) as consumer_port:
        result = _collect(web_port, consumer_port)

    assert result.status is ObservationStatus.PARTIAL
    assert result.data["source_roles"] == ["hmdp-consumer"]
    assert result.data["source_statuses"]["hmdp-web"] == "error"
    assert "kafka_message_sent_count" in result.data["unavailable_metrics"]


def test_both_sources_unavailable_returns_structured_error():
    result = _collect(_unused_port(), _unused_port())

    assert result.status is ObservationStatus.ERROR
    assert result.error is not None
    assert result.error.error_type == "BusinessMetricsSourcesUnavailable"
