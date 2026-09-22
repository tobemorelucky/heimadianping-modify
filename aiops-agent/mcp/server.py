"""Read-only HMDP observability MCP server using the official Python SDK."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any
from uuid import uuid4


AGENT_ROOT = Path(__file__).resolve().parents[1]
if str(AGENT_ROOT) not in sys.path:
    sys.path.insert(0, str(AGENT_ROOT))

from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp_tools.business_metrics import (
    BusinessMetricsRequest,
    LogDerivedBusinessMetricsReader,
    collect_business_metrics,
)
from mcp_tools.kafka_status import (
    KAFKA_NAME_PATTERN,
    KafkaPythonStatusReader,
    KafkaStatusRequest,
    collect_kafka_status,
)
from mcp_tools.mysql_health import (
    LoopbackMysqlHealthReader,
    MysqlHealthRequest,
    collect_mysql_health,
)
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from tool_contracts import (
    EvidenceCompleteness,
    ObservationStatus,
    ToolObservation,
)


DEFAULT_PROJECT_ROOT = AGENT_ROOT.parent
ALLOWED_LOG_PATHS = frozenset(
    {
        "target/runtime/spring-boot.out.log",
        "target/runtime/spring-boot.error.log",
    }
)


class SearchApplicationLogsInput(BaseModel):
    """Validated, bounded input for application log search."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    incident_id: str = Field(min_length=5, max_length=64)
    log_paths: list[str] = Field(min_length=1, max_length=2)
    query: str | None = Field(default=None, max_length=200)
    max_lines: int = Field(default=200, ge=1, le=500)

    @field_validator("log_paths")
    @classmethod
    def validate_log_path_allowlist(cls, values: list[str]) -> list[str]:
        normalized = [value.replace("\\", "/") for value in values]
        invalid = sorted(set(normalized) - ALLOWED_LOG_PATHS)
        if invalid:
            raise ValueError("log path is not in the read-only allowlist")
        return list(dict.fromkeys(normalized))


server = MCPServer(
    name="hmdp-readonly-tools",
    title="HMDP Read-only AIOps Tools",
    description="Read-only local observation tools for the HMDP AIOps Agent.",
    instructions="Never modify HMDP, logs, services, or infrastructure.",
    version="0.1.0",
)


def _project_root() -> Path:
    configured = os.environ.get("HMDP_PROJECT_ROOT")
    return Path(configured).resolve() if configured else DEFAULT_PROJECT_ROOT.resolve()


def _positive_int_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        return default
    return value if value > 0 else default


def _nonnegative_int_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        return default
    return value if value >= 0 else default


@server.tool(
    name="search_application_logs",
    description=(
        "Search the allowlisted Spring Boot stdout and stderr log files. "
        "This tool is read-only and returns a structured evidence envelope."
    ),
    structured_output=True,
)
def search_application_logs(
    incident_id: Annotated[str, Field(min_length=5, max_length=64)],
    log_paths: Annotated[list[str], Field(min_length=1, max_length=2)],
    query: Annotated[str | None, Field(max_length=200)] = None,
    max_lines: Annotated[int, Field(ge=1, le=500)] = 200,
) -> ToolObservation:
    """Search bounded local log files without accepting arbitrary paths."""

    try:
        request = SearchApplicationLogsInput(
            incident_id=incident_id,
            log_paths=log_paths,
            query=query,
            max_lines=max_lines,
        )
    except ValidationError as exc:
        raise ToolError("log search input violates the read-only policy") from exc
    root = _project_root()
    query_folded = request.query.casefold() if request.query else None
    events: list[dict[str, Any]] = []
    files: list[dict[str, Any]] = []
    warnings: list[str] = []
    truncated = False

    for relative_path in request.log_paths:
        path = (root / relative_path).resolve()
        # The model validator checks the logical allowlist; this check protects
        # against a future allowlist change accidentally permitting traversal.
        allowed_resolved_paths = {
            (root / allowed_path).resolve() for allowed_path in ALLOWED_LOG_PATHS
        }
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise ToolError("resolved log path is outside the project root") from exc
        if path not in allowed_resolved_paths or relative_path not in ALLOWED_LOG_PATHS:
            raise ToolError("resolved log path is outside the allowlist")

        if not path.is_file():
            files.append({"path": relative_path, "status": "missing", "matched_lines": 0})
            warnings.append(f"missing log file: {relative_path}")
            continue

        scanned_lines = 0
        matched = 0
        try:
            with path.open("r", encoding="utf-8", errors="replace") as log_file:
                for line_number, raw_message in enumerate(log_file, start=1):
                    scanned_lines = line_number
                    message = raw_message.rstrip("\r\n")
                    if query_folded is not None and query_folded not in message.casefold():
                        continue
                    if len(events) >= request.max_lines:
                        truncated = True
                        break
                    events.append(
                        {
                            "path": relative_path,
                            "line_number": line_number,
                            "message": message,
                        }
                    )
                    matched += 1
        except OSError:
            files.append({"path": relative_path, "status": "unreadable", "matched_lines": 0})
            warnings.append(f"unable to read log file: {relative_path}")
            continue

        files.append(
            {
                "path": relative_path,
                "status": "read",
                "scanned_lines": scanned_lines,
                "matched_lines": matched,
            }
        )
        if truncated:
            break

    completeness = (
        EvidenceCompleteness.PARTIAL
        if warnings or truncated
        else EvidenceCompleteness.COMPLETE
    )
    if truncated:
        warnings.append(f"result truncated at max_lines={request.max_lines}")

    event_count = len(events)
    status = (
        ObservationStatus.PARTIAL
        if completeness is EvidenceCompleteness.PARTIAL
        else ObservationStatus.SUCCESS
    )
    return ToolObservation(
        evidence_id=f"evi_mcp_{uuid4().hex}",
        status=status,
        kind="log",
        source="hmdp.spring_boot.runtime_logs",
        source_tool="search_application_logs",
        collected_at=datetime.now(timezone.utc),
        summary=(
            "Application log search returned "
            f"{event_count} event(s) with completeness={completeness.value}."
        ),
        completeness=completeness,
        raw_ref="logs://hmdp/target/runtime/spring-boot",
        data={
            "incident_id": request.incident_id,
            "query": request.query,
            "events": events,
            "files": files,
            "truncated": truncated,
            "warnings": warnings,
        },
    )


@server.tool(
    name="get_kafka_status",
    description=(
        "Read Kafka topic, consumer-group membership, committed offsets, end offsets, "
        "and lag without consuming messages or changing Kafka state."
    ),
    structured_output=True,
)
def get_kafka_status(
    topic: Annotated[
        str | None,
        Field(min_length=1, max_length=249, pattern=KAFKA_NAME_PATTERN),
    ] = None,
    consumer_group: Annotated[
        str | None,
        Field(min_length=1, max_length=255, pattern=KAFKA_NAME_PATTERN),
    ] = None,
) -> ToolObservation:
    """Return bounded read-only consumer-group evidence for one topic."""

    try:
        request = KafkaStatusRequest(
            topic=topic,
            consumer_group=consumer_group,
        )
    except ValidationError as exc:
        raise ToolError("Kafka status input violates the read-only policy") from exc

    bootstrap_servers = [
        server.strip()
        for server in os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS",
            "127.0.0.1:9092",
        ).split(",")
        if server.strip()
    ]
    timeout_ms = _positive_int_env("AIOPS_KAFKA_REQUEST_TIMEOUT_MS", 3000)
    reader = KafkaPythonStatusReader(
        bootstrap_servers,
        timeout_ms=timeout_ms,
    )
    return collect_kafka_status(
        request,
        reader=reader,
        default_topic=os.environ.get(
            "KAFKA_VOUCHER_ORDER_TOPIC",
            "hmdp.seckill.order.create.v1",
        ),
        default_consumer_group=os.environ.get(
            "KAFKA_CONSUMER_GROUP",
            "hmdp-seckill-order-create-v1",
        ),
        lag_threshold=_nonnegative_int_env("AIOPS_KAFKA_LAG_THRESHOLD", 1000),
    )


@server.tool(
    name="get_business_metrics",
    description=(
        "Derive bounded HMDP seckill request, Lua admission, Kafka send, and order "
        "creation counters from the allowlisted Spring runtime logs without querying "
        "Redis, MySQL, or arbitrary metric expressions."
    ),
    structured_output=True,
)
def get_business_metrics(
    incident_id: Annotated[str, Field(min_length=5, max_length=64)],
    max_lines: Annotated[int, Field(ge=1, le=10_000)] = 10_000,
) -> ToolObservation:
    """Return five read-only business pipeline counters for one Incident."""

    try:
        request = BusinessMetricsRequest(
            incident_id=incident_id,
            max_lines=max_lines,
        )
    except ValidationError as exc:
        raise ToolError("business metrics input violates the read-only policy") from exc
    return collect_business_metrics(
        request,
        reader=LogDerivedBusinessMetricsReader(_project_root()),
    )


@server.tool(
    name="get_mysql_health",
    description=(
        "Read the Consumer JVM loopback MySQL connection-validation and Hikari pool "
        "snapshot without SQL, order data, or configuration access."
    ),
    structured_output=True,
)
def get_mysql_health(
    incident_id: Annotated[str | None, Field(min_length=5, max_length=64)] = None,
) -> ToolObservation:
    """Return only the read-only Consumer-role health snapshot."""

    try:
        request = MysqlHealthRequest(incident_id=incident_id)
    except ValidationError as exc:
        raise ToolError("MySQL health input violates the read-only policy") from exc
    return collect_mysql_health(
        request,
        reader=LoopbackMysqlHealthReader(
            port=_positive_int_env("AIOPS_MYSQL_HEALTH_PORT", 18082),
            timeout_seconds=3.0,
        ),
    )


if __name__ == "__main__":
    server.run("stdio")
