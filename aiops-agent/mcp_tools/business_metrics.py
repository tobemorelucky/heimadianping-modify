"""Read-only HMDP business metrics merged from loopback JVM observation outlets."""

from __future__ import annotations

import json
import re
import socket
from datetime import datetime
from pathlib import Path
from typing import Literal, Protocol, Sequence
from urllib.error import URLError
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from pydantic import BaseModel, ConfigDict, Field, field_validator

from tool_contracts import (
    EvidenceCompleteness,
    ObservationStatus,
    ToolError,
    ToolObservation,
)


BUSINESS_METRIC_LOG_PATHS = (
    "target/runtime/spring-boot.out.log",
    "target/runtime/spring-boot.error.log",
)
WEB_METRICS_PATH = "/internal/aiops/business-metrics/web"
CONSUMER_METRICS_PATH = "/internal/aiops/business-metrics/consumer"
MAX_READ_BYTES_PER_FILE = 2 * 1024 * 1024
MAX_RESPONSE_BYTES = 4096
METRIC_NAMES = (
    "seckill_request_count",
    "lua_admission_success_count",
    "kafka_message_sent_count",
    "order_created_success_count",
    "order_created_failure_count",
)


class BusinessMetricsRequest(BaseModel):
    """Bounded request with no arbitrary endpoint, path, or query capability."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    incident_id: str = Field(min_length=5, max_length=64)
    # Retained for fixture/log-reader compatibility; the live JVM reader ignores it.
    max_lines: int = Field(default=10_000, ge=1, le=10_000)


class _JvmSnapshotBase(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    started_at: datetime
    collected_at: datetime

    @field_validator("started_at", "collected_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("business metric timestamps must be timezone-aware")
        return value


class WebBusinessMetricsSnapshot(_JvmSnapshotBase):
    source_role: Literal["hmdp-web"]
    seckill_request_count: int = Field(ge=0, le=1_000_000_000_000)
    lua_admission_success_count: int = Field(ge=0, le=1_000_000_000_000)
    kafka_message_sent_count: int = Field(ge=0, le=1_000_000_000_000)


class ConsumerBusinessMetricsSnapshot(_JvmSnapshotBase):
    source_role: Literal["hmdp-consumer"]
    order_created_success_count: int = Field(ge=0, le=1_000_000_000_000)
    order_created_failure_count: int = Field(ge=0, le=1_000_000_000_000)


class BusinessMetricsSnapshot(BaseModel):
    """Five counters plus explicit per-source availability and provenance."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    seckill_request_count: int = Field(default=0, ge=0, le=1_000_000_000_000)
    lua_admission_success_count: int = Field(default=0, ge=0, le=1_000_000_000_000)
    kafka_message_sent_count: int = Field(default=0, ge=0, le=1_000_000_000_000)
    order_created_success_count: int = Field(default=0, ge=0, le=1_000_000_000_000)
    order_created_failure_count: int = Field(default=0, ge=0, le=1_000_000_000_000)
    scanned_lines: int = Field(default=0, ge=0)
    files_read: tuple[str, ...] = Field(default_factory=tuple)
    observed_metrics: tuple[str, ...] = Field(default_factory=tuple)
    source_roles: tuple[str, ...] = Field(default_factory=tuple)
    source_statuses: dict[str, str] = Field(default_factory=dict)
    raw_refs: tuple[str, ...] = Field(default_factory=tuple)
    warnings: tuple[str, ...] = Field(default_factory=tuple)


class BusinessMetricsReader(Protocol):
    def read(self, request: BusinessMetricsRequest) -> BusinessMetricsSnapshot: ...


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


class JvmBusinessMetricsReader:
    """Read the two fixed loopback endpoints and merge their validated counters."""

    def __init__(
        self,
        *,
        web_port: int = 18081,
        consumer_port: int = 18083,
        timeout_seconds: float = 3.0,
    ) -> None:
        if not 1 <= web_port <= 65535 or not 1 <= consumer_port <= 65535:
            raise ValueError("invalid business metrics outlet port")
        if not 0 < timeout_seconds <= 10:
            raise ValueError("invalid business metrics outlet timeout")
        self.web_port = web_port
        self.consumer_port = consumer_port
        self.timeout_seconds = timeout_seconds
        self._opener = build_opener(ProxyHandler({}), _NoRedirect())

    def read(self, request: BusinessMetricsRequest) -> BusinessMetricsSnapshot:
        counters = {name: 0 for name in METRIC_NAMES}
        observed: set[str] = set()
        roles: list[str] = []
        raw_refs: list[str] = []
        warnings: list[str] = []
        statuses: dict[str, str] = {}

        sources = (
            (
                "hmdp-web",
                self.web_port,
                WEB_METRICS_PATH,
                WebBusinessMetricsSnapshot,
                METRIC_NAMES[:3],
            ),
            (
                "hmdp-consumer",
                self.consumer_port,
                CONSUMER_METRICS_PATH,
                ConsumerBusinessMetricsSnapshot,
                METRIC_NAMES[3:],
            ),
        )
        for role, port, path, model, metric_names in sources:
            url = f"http://127.0.0.1:{port}{path}"
            try:
                payload = self._read_payload(url)
                snapshot = model.model_validate(payload)
            except Exception as exc:
                timed_out = isinstance(exc, (TimeoutError, socket.timeout)) or (
                    isinstance(exc, URLError)
                    and isinstance(exc.reason, (TimeoutError, socket.timeout))
                )
                statuses[role] = "timeout" if timed_out else "error"
                warnings.append(
                    f"{role} business metrics source "
                    f"{'timed out' if timed_out else 'unavailable or invalid'}"
                )
                continue

            statuses[role] = "success"
            roles.append(role)
            raw_refs.append(url)
            values = snapshot.model_dump(mode="json")
            for metric_name in metric_names:
                counters[metric_name] = values[metric_name]
                observed.add(metric_name)

        return BusinessMetricsSnapshot(
            **counters,
            observed_metrics=tuple(sorted(observed)),
            source_roles=tuple(roles),
            source_statuses=statuses,
            raw_refs=tuple(raw_refs),
            warnings=tuple(warnings),
        )

    def _read_payload(self, url: str) -> dict:
        request = Request(url, method="GET", headers={"Accept": "application/json"})
        with self._opener.open(request, timeout=self.timeout_seconds) as response:
            if response.status != 200:
                raise ValueError("business metrics endpoint returned non-success")
            payload = response.read(MAX_RESPONSE_BYTES + 1)
        if len(payload) > MAX_RESPONSE_BYTES:
            raise ValueError("business metrics response exceeds the size limit")
        decoded = json.loads(payload)
        if not isinstance(decoded, dict):
            raise ValueError("business metrics response must be an object")
        return decoded


class LogDerivedBusinessMetricsReader:
    """Legacy fixture reader retained for deterministic tests, not live collection."""

    _PATTERNS = {
        "seckill_request_count": (
            re.compile(r"\bseckill_request_count\s*=\s*(?P<count>\d+)\b", re.I),
            re.compile(r"\bseckill request received\b|秒杀请求(?:已)?收到", re.I),
        ),
        "lua_admission_success_count": (
            re.compile(r"\blua_admission_success_count\s*=\s*(?P<count>\d+)\b", re.I),
            re.compile(r"\blua admission success\b|lua准入成功|秒杀请求已受理", re.I),
        ),
        "kafka_message_sent_count": (
            re.compile(r"\bkafka_message_sent_count\s*=\s*(?P<count>\d+)\b", re.I),
            re.compile(
                r"\bkafka (?:message )?(?:send|publish)(?:ed)? success\b|"
                r"kafka消息发送成功|voucher order event published",
                re.I,
            ),
        ),
        "order_created_success_count": (
            re.compile(r"\border_created_success_count\s*=\s*(?P<count>\d+)\b", re.I),
            re.compile(r"\border creat(?:ed|ion) success(?:fully)?\b|订单创建成功", re.I),
        ),
        "order_created_failure_count": (
            re.compile(r"\border_created_failure_count\s*=\s*(?P<count>\d+)\b", re.I),
            re.compile(r"\border creat(?:e|ion) fail(?:ed|ure)?\b|订单创建失败", re.I),
        ),
    }

    def __init__(
        self,
        project_root: Path | str,
        *,
        log_paths: Sequence[str] = BUSINESS_METRIC_LOG_PATHS,
    ) -> None:
        self.project_root = Path(project_root).resolve()
        self.log_paths = tuple(log_paths)
        if set(self.log_paths) - set(BUSINESS_METRIC_LOG_PATHS):
            raise ValueError("business metric log path is outside the allowlist")

    def read(self, request: BusinessMetricsRequest) -> BusinessMetricsSnapshot:
        counters = {name: 0 for name in self._PATTERNS}
        files_read: list[str] = []
        warnings: list[str] = []
        observed_metrics: set[str] = set()
        scanned_lines = 0

        for relative_path in self.log_paths:
            path = (self.project_root / relative_path).resolve()
            self._validate_path(path, relative_path)
            if not path.is_file():
                warnings.append(f"missing metric source file: {relative_path}")
                continue
            try:
                lines, truncated = self._bounded_tail(path, request.max_lines)
            except OSError:
                warnings.append(f"unable to read metric source file: {relative_path}")
                continue
            files_read.append(relative_path)
            scanned_lines += len(lines)
            if truncated:
                warnings.append(
                    f"metric source truncated to bounded tail: {relative_path}"
                )
            for line in lines:
                self._count_line(line, counters, observed_metrics)

        missing_metrics = sorted(set(self._PATTERNS) - observed_metrics)
        if missing_metrics:
            warnings.append(
                "unavailable business metric markers: " + ", ".join(missing_metrics)
            )

        return BusinessMetricsSnapshot(
            **counters,
            scanned_lines=scanned_lines,
            files_read=tuple(files_read),
            observed_metrics=tuple(sorted(observed_metrics)),
            source_roles=("hmdp-log-files",) if files_read else (),
            source_statuses={
                "hmdp-log-files": "success" if files_read else "error"
            },
            raw_refs=tuple(f"file://{path}" for path in files_read),
            warnings=tuple(warnings),
        )

    def _validate_path(self, path: Path, relative_path: str) -> None:
        try:
            path.relative_to(self.project_root)
        except ValueError as exc:
            raise ValueError("resolved metric path is outside the project root") from exc
        allowed = {
            (self.project_root / item).resolve()
            for item in BUSINESS_METRIC_LOG_PATHS
        }
        if relative_path not in BUSINESS_METRIC_LOG_PATHS or path not in allowed:
            raise ValueError("resolved metric path is outside the allowlist")

    @staticmethod
    def _bounded_tail(path: Path, max_lines: int) -> tuple[list[str], bool]:
        size = path.stat().st_size
        start = max(0, size - MAX_READ_BYTES_PER_FILE)
        with path.open("rb") as stream:
            stream.seek(start)
            data = stream.read(MAX_READ_BYTES_PER_FILE)
        if start:
            first_newline = data.find(b"\n")
            data = data[first_newline + 1 :] if first_newline >= 0 else b""
        decoded = data.decode("utf-8", errors="replace")
        lines = decoded.splitlines()
        truncated = start > 0 or len(lines) > max_lines
        return lines[-max_lines:], truncated

    @classmethod
    def _count_line(
        cls,
        line: str,
        counters: dict[str, int],
        observed_metrics: set[str],
    ) -> None:
        for metric_name, patterns in cls._PATTERNS.items():
            for pattern in patterns:
                match = pattern.search(line)
                if match is None:
                    continue
                raw_count = match.groupdict().get("count")
                counters[metric_name] += int(raw_count) if raw_count else 1
                observed_metrics.add(metric_name)
                break


def collect_business_metrics(
    request: BusinessMetricsRequest,
    *,
    reader: BusinessMetricsReader,
) -> ToolObservation:
    """Return a uniform Evidence envelope without asserting a root cause."""

    source = "hmdp.seckill.business_metrics"
    try:
        snapshot = reader.read(request)
    except Exception as exc:
        message = (str(exc).strip() or type(exc).__name__)[:500]
        return ToolObservation(
            status=ObservationStatus.ERROR,
            kind="business_metrics",
            source=source,
            source_tool="get_business_metrics",
            summary=f"Business metrics collection failed: {message}",
            completeness=EvidenceCompleteness.UNKNOWN,
            raw_ref=None,
            data={"incident_id": request.incident_id},
            error=ToolError(
                error_type=type(exc).__name__,
                message=message,
                retryable=True,
            ),
        )

    observed = set(snapshot.observed_metrics)
    unavailable = sorted(set(METRIC_NAMES) - observed)
    counters = {name: getattr(snapshot, name) for name in METRIC_NAMES}
    no_source_available = not snapshot.source_roles
    complete = not unavailable and not snapshot.warnings
    if no_source_available:
        status = ObservationStatus.ERROR
        completeness = EvidenceCompleteness.UNKNOWN
    else:
        status = ObservationStatus.SUCCESS if complete else ObservationStatus.PARTIAL
        completeness = (
            EvidenceCompleteness.COMPLETE
            if complete
            else EvidenceCompleteness.PARTIAL
        )

    return ToolObservation(
        status=status,
        kind="business_metrics",
        source=source,
        source_tool="get_business_metrics",
        summary=(
            "Seckill pipeline counters: "
            f"requests={snapshot.seckill_request_count}, "
            f"lua_accepted={snapshot.lua_admission_success_count}, "
            f"kafka_sent={snapshot.kafka_message_sent_count}, "
            f"orders_created={snapshot.order_created_success_count}, "
            f"orders_failed={snapshot.order_created_failure_count}."
        ),
        completeness=completeness,
        raw_ref=(
            "jvm://hmdp-web+hmdp-consumer/business-metrics"
            if snapshot.raw_refs
            else None
        ),
        data={
            "incident_id": request.incident_id,
            **counters,
            "source_roles": list(snapshot.source_roles),
            "source_statuses": snapshot.source_statuses,
            "metrics": counters,
            "scanned_lines": snapshot.scanned_lines,
            "files_read": list(snapshot.files_read),
            "observed_metrics": list(snapshot.observed_metrics),
            "unavailable_metrics": unavailable,
            "source_refs": list(snapshot.raw_refs),
            "warnings": list(snapshot.warnings),
        },
        error=(
            ToolError(
                error_type="BusinessMetricsSourcesUnavailable",
                message="Web and Consumer business metric outlets are unavailable.",
                retryable=True,
            )
            if no_source_available
            else None
        ),
    )
