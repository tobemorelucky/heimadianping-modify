"""Schemas for deterministic rules and anomaly signals."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_duration_seconds(value: Any) -> int:
    """Accept seconds or compact values such as 30s, 2m, and 1h."""

    if isinstance(value, bool):
        raise ValueError("duration must not be boolean")
    if isinstance(value, (int, float)):
        seconds = int(value)
    elif isinstance(value, str):
        match = re.fullmatch(r"\s*(\d+)\s*([smh]?)\s*", value.lower())
        if match is None:
            raise ValueError("duration must use seconds, s, m, or h")
        amount = int(match.group(1))
        multiplier = {"": 1, "s": 1, "m": 60, "h": 3600}[match.group(2)]
        seconds = amount * multiplier
    else:
        raise ValueError("duration must be seconds or a compact duration string")
    if seconds <= 0:
        raise ValueError("duration must be positive")
    return seconds


class DetectionSeverity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class SignalStatus(str, Enum):
    OPEN = "open"
    RECOVERED = "recovered"


class KafkaConsumerDownCondition(BaseModel):
    """Typed condition supported by the first Phase M2 rule."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    condition_type: Literal["kafka_consumer_down"] = "kafka_consumer_down"
    member_count_equals: int = Field(default=0, ge=0)
    lag_greater_than: int = Field(default=1_000, ge=0)
    consecutive_windows: int = Field(default=2, ge=1, le=100)


class OrderPersistenceFailureCondition(BaseModel):
    """Correlate active business traffic with healthy Kafka and failed MySQL."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    condition_type: Literal["order_persistence_failure"] = "order_persistence_failure"
    min_request_delta: int = Field(default=1, ge=1)
    min_lua_admission_ratio: float = Field(default=0.9, ge=0, le=1)
    min_kafka_publish_ratio: float = Field(default=0.9, ge=0, le=1)
    max_order_success_ratio: float = Field(default=0.8, ge=0, le=1)
    max_kafka_lag: int = Field(default=1_000, ge=0)
    supported_mysql_failure_classes: tuple[
        Literal[
            "DATABASE_UNAVAILABLE",
            "CONNECTION_TIMEOUT",
            "POOL_EXHAUSTED",
        ],
        ...,
    ] = (
        "DATABASE_UNAVAILABLE",
        "CONNECTION_TIMEOUT",
        "POOL_EXHAUSTED",
    )

    @field_validator("supported_mysql_failure_classes")
    @classmethod
    def unique_mysql_failure_classes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if not values or len(values) != len(set(values)):
            raise ValueError("supported MySQL failure classes must be non-empty and unique")
        return values


class DetectionRule(BaseModel):
    """Versioned deterministic rule persisted independently of schedules."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    rule_id: str = Field(pattern=r"^[a-z0-9._-]+$", min_length=1, max_length=128)
    version: int = Field(ge=1)
    source_kind: str = Field(min_length=1, max_length=64)
    condition: KafkaConsumerDownCondition | OrderPersistenceFailureCondition = Field(
        discriminator="condition_type"
    )
    severity: DetectionSeverity
    fingerprint_fields: tuple[str, ...] = Field(min_length=1, max_length=20)
    lookback_window: int
    cooldown: int
    enabled: bool = True
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    @field_validator("lookback_window", "cooldown", mode="before")
    @classmethod
    def parse_duration(cls, value: Any) -> int:
        return parse_duration_seconds(value)

    @field_validator("fingerprint_fields")
    @classmethod
    def unique_fingerprint_fields(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value.strip() for value in values):
            raise ValueError("fingerprint fields must not be blank")
        if len(set(values)) != len(values):
            raise ValueError("fingerprint fields must be unique")
        return values

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_rule_timestamp_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("rule timestamps must include a timezone")
        return value


class AnomalySignal(BaseModel):
    """One emitted anomaly after all rule windows and cooldown checks pass."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    signal_id: str = Field(
        default_factory=lambda: f"sig_{uuid4().hex}",
        min_length=8,
        max_length=80,
    )
    rule_id: str = Field(min_length=1, max_length=128)
    version: int = Field(ge=1)
    fingerprint: str = Field(min_length=1, max_length=500)
    severity: DetectionSeverity
    facts: dict[str, Any]
    observation_refs: tuple[str, ...] = Field(min_length=1, max_length=100)
    first_seen: datetime
    last_seen: datetime
    status: SignalStatus = SignalStatus.OPEN
    created_at: datetime = Field(default_factory=utc_now)

    @field_validator("first_seen", "last_seen", "created_at")
    @classmethod
    def require_signal_timestamp_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("signal timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def validate_seen_window(self) -> "AnomalySignal":
        if self.last_seen < self.first_seen:
            raise ValueError("signal last_seen must not precede first_seen")
        if len(set(self.observation_refs)) != len(self.observation_refs):
            raise ValueError("signal observation_refs must be unique")
        return self
