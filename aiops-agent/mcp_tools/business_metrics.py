"""Read-only HMDP seckill business metrics derived from allowlisted local logs."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Protocol, Sequence

from pydantic import BaseModel, ConfigDict, Field

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
MAX_READ_BYTES_PER_FILE = 2 * 1024 * 1024


class BusinessMetricsRequest(BaseModel):
    """Bounded request with no arbitrary path or query capability."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    incident_id: str = Field(min_length=5, max_length=64)
    max_lines: int = Field(default=10_000, ge=1, le=10_000)


class BusinessMetricsSnapshot(BaseModel):
    """Five diagnostic counters and their local collection provenance."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    seckill_request_count: int = Field(ge=0, le=1_000_000_000_000)
    lua_admission_success_count: int = Field(ge=0, le=1_000_000_000_000)
    kafka_message_sent_count: int = Field(ge=0, le=1_000_000_000_000)
    order_created_success_count: int = Field(ge=0, le=1_000_000_000_000)
    order_created_failure_count: int = Field(ge=0, le=1_000_000_000_000)
    scanned_lines: int = Field(ge=0)
    files_read: tuple[str, ...] = Field(default_factory=tuple)
    observed_metrics: tuple[str, ...] = Field(default_factory=tuple)
    warnings: tuple[str, ...] = Field(default_factory=tuple)


class BusinessMetricsReader(Protocol):
    def read(self, request: BusinessMetricsRequest) -> BusinessMetricsSnapshot: ...


class LogDerivedBusinessMetricsReader:
    """Derive bounded counters from fixed Spring runtime log files."""

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

    expected_files = len(BUSINESS_METRIC_LOG_PATHS)
    complete = len(snapshot.files_read) == expected_files and not snapshot.warnings
    completeness = (
        EvidenceCompleteness.COMPLETE
        if complete
        else EvidenceCompleteness.PARTIAL
    )
    status = (
        ObservationStatus.SUCCESS if complete else ObservationStatus.PARTIAL
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
        raw_ref="logs://hmdp/target/runtime/spring-boot/business-metrics",
        data={
            "incident_id": request.incident_id,
            "seckill_request_count": snapshot.seckill_request_count,
            "lua_admission_success_count": snapshot.lua_admission_success_count,
            "kafka_message_sent_count": snapshot.kafka_message_sent_count,
            "order_created_success_count": snapshot.order_created_success_count,
            "order_created_failure_count": snapshot.order_created_failure_count,
            "scanned_lines": snapshot.scanned_lines,
            "files_read": list(snapshot.files_read),
            "observed_metrics": list(snapshot.observed_metrics),
            "unavailable_metrics": sorted(
                set(LogDerivedBusinessMetricsReader._PATTERNS)
                - set(snapshot.observed_metrics)
            ),
            "warnings": list(snapshot.warnings),
        },
    )
