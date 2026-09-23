"""Bounded, read-only Consumer-JVM MySQL health observation."""

from __future__ import annotations

import json
import socket
from datetime import datetime
from enum import Enum
from typing import Literal, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from tool_contracts import (
    EvidenceCompleteness,
    ObservationStatus,
    ToolError,
    ToolObservation,
)


HEALTH_PATH = "/internal/aiops/mysql-health"
MAX_RESPONSE_BYTES = 4096


class MysqlHealthState(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class MysqlFailureClass(str, Enum):
    DATABASE_UNAVAILABLE = "DATABASE_UNAVAILABLE"
    CONNECTION_TIMEOUT = "CONNECTION_TIMEOUT"
    POOL_EXHAUSTED = "POOL_EXHAUSTED"
    UNKNOWN = "UNKNOWN"


class MysqlHealthClassification(BaseModel):
    """Deterministic normalization derived only from explicit probe facts."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    health_state: MysqlHealthState
    failure_class: tuple[MysqlFailureClass, ...]


def classify_mysql_health(
    database_reachable: bool | None,
    connection_test_status: str | None,
    error_info: str | None = None,
) -> MysqlHealthClassification:
    """Normalize controlled status codes without parsing exception messages."""

    status = (connection_test_status or "").strip().lower()
    error_code = (error_info or "").strip().upper()
    known_error_codes = {item.value for item in MysqlFailureClass}
    if error_code and error_code not in known_error_codes:
        return MysqlHealthClassification(
            health_state=MysqlHealthState.UNKNOWN,
            failure_class=(MysqlFailureClass.UNKNOWN,),
        )

    if database_reachable is True and status == "valid" and not error_code:
        return MysqlHealthClassification(
            health_state=MysqlHealthState.HEALTHY,
            failure_class=(),
        )

    if (
        database_reachable is False
        and status in {"invalid", "connection_failed"}
    ) or error_code == MysqlFailureClass.DATABASE_UNAVAILABLE.value:
        return MysqlHealthClassification(
            health_state=MysqlHealthState.FAILED,
            failure_class=(MysqlFailureClass.DATABASE_UNAVAILABLE,),
        )

    if status == "timeout" or error_code == MysqlFailureClass.CONNECTION_TIMEOUT.value:
        classes = [MysqlFailureClass.CONNECTION_TIMEOUT]
        if database_reachable is False:
            classes.insert(0, MysqlFailureClass.DATABASE_UNAVAILABLE)
        return MysqlHealthClassification(
            health_state=(
                MysqlHealthState.FAILED
                if database_reachable is False
                else MysqlHealthState.DEGRADED
            ),
            failure_class=tuple(classes),
        )

    if status == "pool_exhausted" or error_code == MysqlFailureClass.POOL_EXHAUSTED.value:
        if database_reachable is False:
            return MysqlHealthClassification(
                health_state=MysqlHealthState.UNKNOWN,
                failure_class=(MysqlFailureClass.UNKNOWN,),
            )
        return MysqlHealthClassification(
            health_state=MysqlHealthState.DEGRADED,
            failure_class=(MysqlFailureClass.POOL_EXHAUSTED,),
        )

    return MysqlHealthClassification(
        health_state=MysqlHealthState.UNKNOWN,
        failure_class=(MysqlFailureClass.UNKNOWN,),
    )


class MysqlHealthRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    incident_id: str | None = Field(default=None, min_length=5, max_length=64)


class ConsumerMysqlHealthSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source_role: Literal["hmdp-consumer"]
    collected_at: datetime
    database_reachable: bool | None
    connection_test_status: Literal[
        "valid", "invalid", "connection_failed", "timeout", "busy",
        "interrupted", "acquisition_error", "pool_exhausted", "probe_error",
        "unavailable",
    ]
    hikari_active: int | None = Field(ge=0)
    hikari_idle: int | None = Field(ge=0)
    connection_timeout_count: int | None = Field(ge=0)
    error_count: int | None = Field(ge=0)
    unavailable_metrics: list[Literal[
        "hikari_active", "hikari_idle", "connection_timeout_count", "error_count"
    ]] = Field(default_factory=list, max_length=4)

    @field_validator("collected_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Consumer health timestamp must be timezone-aware")
        return value


class MysqlHealthReader(Protocol):
    def read(self) -> ConsumerMysqlHealthSnapshot: ...


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


class LoopbackMysqlHealthReader:
    """GET the one fixed local path; no model-supplied host or SQL is accepted."""

    def __init__(self, *, port: int = 18082, timeout_seconds: float = 3.0) -> None:
        if not 1 <= port <= 65535 or not 0 < timeout_seconds <= 10:
            raise ValueError("invalid Consumer health endpoint limits")
        self.port = port
        self.timeout_seconds = timeout_seconds
        self._opener = build_opener(ProxyHandler({}), _NoRedirect())

    def read(self) -> ConsumerMysqlHealthSnapshot:
        url = f"http://127.0.0.1:{self.port}{HEALTH_PATH}"
        request = Request(url, method="GET", headers={"Accept": "application/json"})
        with self._opener.open(request, timeout=self.timeout_seconds) as response:
            if response.status != 200:
                raise ValueError("Consumer health endpoint returned a non-success response")
            payload = response.read(MAX_RESPONSE_BYTES + 1)
        if len(payload) > MAX_RESPONSE_BYTES:
            raise ValueError("Consumer health response exceeds the size limit")
        return ConsumerMysqlHealthSnapshot.model_validate(json.loads(payload))


def collect_mysql_health(
    request: MysqlHealthRequest,
    *,
    reader: MysqlHealthReader,
) -> ToolObservation:
    """Preserve Consumer facts and missingness without inferring a root cause."""

    source = "hmdp-consumer.mysql-health"
    try:
        snapshot = reader.read()
    except Exception as exc:
        timeout = isinstance(exc, (TimeoutError, socket.timeout)) or (
            isinstance(exc, URLError)
            and isinstance(exc.reason, (TimeoutError, socket.timeout))
        )
        status = ObservationStatus.TIMEOUT if timeout else ObservationStatus.ERROR
        role_mismatch = isinstance(exc, ValidationError) and any(
            issue["loc"] == ("source_role",) for issue in exc.errors()
        )
        error_type = (
            "HealthEndpointTimeout" if timeout else
            "ConsumerRoleMismatch" if role_mismatch else "HealthEndpointError"
        )
        return ToolObservation(
            status=status,
            kind="mysql_health",
            source=source,
            source_tool="get_mysql_health",
            summary="Consumer MySQL health observation failed or was rejected.",
            completeness=EvidenceCompleteness.UNKNOWN,
            data={
                "source_role": None,
                "expected_source_role": "hmdp-consumer",
                "incident_id": request.incident_id,
                "health_state": MysqlHealthState.UNKNOWN.value,
                "failure_class": [MysqlFailureClass.UNKNOWN.value],
            },
            error=ToolError(
                error_type=error_type,
                message="Consumer health endpoint unavailable or returned invalid data.",
                retryable=timeout or (
                    isinstance(exc, URLError) and not isinstance(exc, HTTPError)
                ),
            ),
        )

    fields = snapshot.model_dump(mode="json")
    classification = classify_mysql_health(
        snapshot.database_reachable,
        snapshot.connection_test_status,
    )
    fields.update(classification.model_dump(mode="json"))
    unavailable = set(snapshot.unavailable_metrics)
    for name in (
        "hikari_active", "hikari_idle", "connection_timeout_count", "error_count"
    ):
        if fields[name] is None:
            unavailable.add(name)
    fields["unavailable_metrics"] = sorted(unavailable)
    fields["incident_id"] = request.incident_id
    complete = (
        snapshot.database_reachable is not None
        and snapshot.connection_test_status in {"valid", "invalid"}
        and not unavailable
    )
    return ToolObservation(
        status=ObservationStatus.SUCCESS if complete else ObservationStatus.PARTIAL,
        kind="mysql_health",
        source=source,
        source_tool="get_mysql_health",
        summary=(
            "Consumer MySQL connection validation succeeded."
            if classification.health_state is MysqlHealthState.HEALTHY
            else (
                "Consumer MySQL health is degraded or failed: "
                + ", ".join(item.value for item in classification.failure_class)
            )
        ),
        completeness=(
            EvidenceCompleteness.COMPLETE if complete else EvidenceCompleteness.PARTIAL
        ),
        raw_ref="http://127.0.0.1/internal/aiops/mysql-health",
        data=fields,
    )
