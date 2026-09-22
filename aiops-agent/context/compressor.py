"""Deterministic compression for search_application_logs Evidence."""

from __future__ import annotations

import hashlib
import re
from typing import Any

from context.cards import EvidenceCard
from runtime.evidence import Evidence


_ERROR_PATTERN = re.compile(
    r"\b(error|exception|failed|failure)\b|timed\s+out",
    flags=re.IGNORECASE,
)
_TIMESTAMP_PATTERN = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b"
)
_NUMBER_PATTERN = re.compile(r"\b\d+\b")


class UnsupportedEvidenceError(ValueError):
    """Raised when the MVP compressor receives an unsupported tool source."""


class LogEvidenceCompressor:
    """Convert bounded log tool output into a compact, traceable card."""

    SUPPORTED_TOOL = "search_application_logs"

    def __init__(
        self,
        *,
        max_samples: int,
        max_sample_chars: int,
        max_fact_chars: int = 240,
        max_summary_chars: int = 500,
    ) -> None:
        self.max_samples = max_samples
        self.max_sample_chars = max_sample_chars
        self.max_fact_chars = max_fact_chars
        self.max_summary_chars = max_summary_chars

    def compress(self, evidence: Evidence) -> EvidenceCard:
        if evidence.source_tool != self.SUPPORTED_TOOL:
            raise UnsupportedEvidenceError(
                f"unsupported evidence source_tool: {evidence.source_tool}"
            )

        events = self._events(evidence.data)
        warnings = self._warnings(evidence.data)
        error_messages = [message for message in events if _ERROR_PATTERN.search(message)]

        facts: list[str] = [f"Log search matched {len(events)} event(s)."]
        if error_messages:
            facts.append(f"Detected {len(error_messages)} error-like log event(s).")
        if warnings:
            facts.append(f"Log collection reported {len(warnings)} warning(s).")
        if evidence.error is not None:
            facts.append(
                f"Tool {evidence.status.value}: {evidence.error.message}"
            )

        samples = tuple(
            self._bounded(message, self.max_sample_chars)
            for message in events[: self.max_samples]
        )
        bounded_facts = tuple(
            self._bounded(fact, self.max_fact_chars) for fact in facts[:8]
        )
        summary = self._bounded(evidence.summary, self.max_summary_chars)
        fingerprint = self._fingerprint(
            source=evidence.source,
            kind=evidence.kind,
            status=evidence.status.value,
            summary=summary,
            facts=bounded_facts,
            samples=samples,
        )
        return EvidenceCard(
            evidence_id=evidence.evidence_id,
            source=evidence.source,
            kind=evidence.kind,
            summary=summary,
            facts=bounded_facts,
            samples=samples,
            score=0.0,
            status=evidence.status,
            completeness=evidence.completeness,
            collected_at=evidence.collected_at,
            fingerprint=fingerprint,
        )

    @staticmethod
    def _events(data: dict[str, Any]) -> list[str]:
        raw_events = data.get("events", [])
        if not isinstance(raw_events, list):
            return []
        messages: list[str] = []
        for event in raw_events:
            if isinstance(event, dict):
                message = event.get("message")
                if isinstance(message, str) and message:
                    messages.append(message)
        return messages

    @staticmethod
    def _warnings(data: dict[str, Any]) -> list[str]:
        raw_warnings = data.get("warnings", [])
        if not isinstance(raw_warnings, list):
            return []
        return [warning for warning in raw_warnings if isinstance(warning, str)]

    @staticmethod
    def _bounded(value: str, limit: int) -> str:
        if len(value) <= limit:
            return value
        return value[: max(0, limit - 1)] + "…"

    @staticmethod
    def _fingerprint(
        *,
        source: str,
        kind: str,
        status: str,
        summary: str,
        facts: tuple[str, ...],
        samples: tuple[str, ...],
    ) -> str:
        content = "\n".join((source, kind, status, summary, *facts, *samples))
        normalized = _TIMESTAMP_PATTERN.sub("<timestamp>", content.casefold())
        normalized = _NUMBER_PATTERN.sub("<number>", normalized)
        normalized = " ".join(normalized.split())
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class KafkaEvidenceCompressor:
    """Convert Kafka lag and group state into a bounded evidence card."""

    SUPPORTED_TOOL = "get_kafka_status"

    def __init__(
        self,
        *,
        max_samples: int,
        max_sample_chars: int,
        max_fact_chars: int = 240,
        max_summary_chars: int = 500,
    ) -> None:
        self.max_samples = max_samples
        self.max_sample_chars = max_sample_chars
        self.max_fact_chars = max_fact_chars
        self.max_summary_chars = max_summary_chars

    def compress(self, evidence: Evidence) -> EvidenceCard:
        if evidence.source_tool != self.SUPPORTED_TOOL:
            raise UnsupportedEvidenceError(
                f"unsupported evidence source_tool: {evidence.source_tool}"
            )

        data = evidence.data
        consumer_status = str(data.get("consumer_status", "unknown"))
        member_count = self._integer(data.get("member_count"))
        total_lag = self._integer(data.get("total_lag"))
        lag_threshold = self._integer(data.get("lag_threshold"))
        lag_status = str(data.get("lag_status", "unknown"))
        offsets_complete = bool(data.get("offsets_complete", False))
        facts = [
            (
                f"Kafka consumer_status={consumer_status}; "
                f"member_count={member_count if member_count is not None else 'unknown'}."
            ),
            (
                f"Kafka total_lag={total_lag if total_lag is not None else 'unknown'}; "
                f"lag_status={lag_status}; "
                f"lag_threshold={lag_threshold if lag_threshold is not None else 'unknown'}."
            ),
            f"Kafka offsets_complete={str(offsets_complete).lower()}.",
        ]
        if evidence.error is not None:
            facts.append(
                f"Tool {evidence.status.value}: {evidence.error.message}"
            )

        partitions = data.get("partitions", [])
        samples: list[str] = []
        if isinstance(partitions, list):
            for partition in partitions[: self.max_samples]:
                if not isinstance(partition, dict):
                    continue
                sample = (
                    f"partition={partition.get('partition', 'unknown')}; "
                    f"committed_offset={partition.get('committed_offset', 'unknown')}; "
                    f"end_offset={partition.get('end_offset', 'unknown')}; "
                    f"lag={partition.get('lag', 'unknown')}"
                )
                samples.append(
                    LogEvidenceCompressor._bounded(sample, self.max_sample_chars)
                )

        summary, interpretation, confidence = self._semantic_summary(
            evidence=evidence,
            consumer_status=consumer_status,
            member_count=member_count,
            total_lag=total_lag,
            lag_status=lag_status,
            offsets_complete=offsets_complete,
        )
        bounded_facts = tuple(
            LogEvidenceCompressor._bounded(fact, self.max_fact_chars)
            for fact in facts[:8]
        )
        bounded_samples = tuple(samples)
        summary = LogEvidenceCompressor._bounded(summary, self.max_summary_chars)
        fingerprint = LogEvidenceCompressor._fingerprint(
            source=evidence.source,
            kind=evidence.kind,
            status=evidence.status.value,
            summary=summary,
            facts=bounded_facts,
            samples=bounded_samples,
        )
        return EvidenceCard(
            evidence_id=evidence.evidence_id,
            source=evidence.source,
            kind=evidence.kind,
            summary=summary,
            facts=bounded_facts,
            samples=bounded_samples,
            interpretation=interpretation,
            confidence=confidence,
            score=0.0,
            status=evidence.status,
            completeness=evidence.completeness,
            collected_at=evidence.collected_at,
            fingerprint=fingerprint,
        )

    @staticmethod
    def _integer(value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _semantic_summary(
        *,
        evidence: Evidence,
        consumer_status: str,
        member_count: int | None,
        total_lag: int | None,
        lag_status: str,
        offsets_complete: bool,
    ) -> tuple[str, str, float]:
        if evidence.status.value in {"error", "timeout"}:
            return (
                evidence.summary,
                "insufficient_kafka_observation",
                0.1,
            )
        if not offsets_complete or total_lag is None or member_count is None:
            return (
                "Consumer group status or offset data is incomplete.",
                "insufficient_kafka_observation",
                0.25,
            )
        if member_count == 0 and total_lag > 0:
            return (
                "Consumer group has no active members and accumulated lag.",
                "supports_kafka_consumer_unavailable_hypothesis",
                0.95,
            )
        if (
            consumer_status == "stable"
            and member_count > 0
            and lag_status == "normal"
        ):
            return (
                "Consumer group is stable with active members and normal lag.",
                "contradicts_kafka_consumer_unavailable_hypothesis",
                0.9,
            )
        if lag_status == "abnormal":
            return (
                "Consumer group has accumulated lag.",
                "supports_kafka_consumer_lag_hypothesis",
                0.85,
            )
        return (
            "Consumer group observation is not decisive.",
            "insufficient_kafka_observation",
            0.4,
        )


class BusinessMetricsEvidenceCompressor:
    """Summarize five seckill counters without claiming an infrastructure cause."""

    SUPPORTED_TOOL = "get_business_metrics"

    def __init__(
        self,
        *,
        max_samples: int,
        max_sample_chars: int,
        max_fact_chars: int = 240,
        max_summary_chars: int = 500,
    ) -> None:
        self.max_samples = max_samples
        self.max_sample_chars = max_sample_chars
        self.max_fact_chars = max_fact_chars
        self.max_summary_chars = max_summary_chars

    def compress(self, evidence: Evidence) -> EvidenceCard:
        if evidence.source_tool != self.SUPPORTED_TOOL:
            raise UnsupportedEvidenceError(
                f"unsupported evidence source_tool: {evidence.source_tool}"
            )

        data = evidence.data
        requests = KafkaEvidenceCompressor._integer(
            data.get("seckill_request_count")
        )
        lua_accepted = KafkaEvidenceCompressor._integer(
            data.get("lua_admission_success_count")
        )
        kafka_sent = KafkaEvidenceCompressor._integer(
            data.get("kafka_message_sent_count")
        )
        orders_created = KafkaEvidenceCompressor._integer(
            data.get("order_created_success_count")
        )
        orders_failed = KafkaEvidenceCompressor._integer(
            data.get("order_created_failure_count")
        )
        unavailable_metrics = data.get("unavailable_metrics", [])
        facts = [
            f"Seckill request_count={self._display(requests)}.",
            f"Lua admission_success_count={self._display(lua_accepted)}.",
            f"Kafka message_sent_count={self._display(kafka_sent)}.",
            (
                f"Order created_success_count={self._display(orders_created)}; "
                f"created_failure_count={self._display(orders_failed)}."
            ),
        ]
        if evidence.error is not None:
            facts.append(
                f"Tool {evidence.status.value}: {evidence.error.message}"
            )

        summary, interpretation, confidence, ratios = self._semantic_summary(
            evidence=evidence,
            requests=requests,
            lua_accepted=lua_accepted,
            kafka_sent=kafka_sent,
            orders_created=orders_created,
            orders_failed=orders_failed,
            unavailable_metrics=(
                unavailable_metrics if isinstance(unavailable_metrics, list) else []
            ),
        )
        facts.extend(ratios)
        bounded_facts = tuple(
            LogEvidenceCompressor._bounded(fact, self.max_fact_chars)
            for fact in facts[:10]
        )
        summary = LogEvidenceCompressor._bounded(summary, self.max_summary_chars)
        fingerprint = LogEvidenceCompressor._fingerprint(
            source=evidence.source,
            kind=evidence.kind,
            status=evidence.status.value,
            summary=summary,
            facts=bounded_facts,
            samples=(),
        )
        return EvidenceCard(
            evidence_id=evidence.evidence_id,
            source=evidence.source,
            kind=evidence.kind,
            summary=summary,
            facts=bounded_facts,
            samples=(),
            interpretation=interpretation,
            confidence=confidence,
            score=0.0,
            status=evidence.status,
            completeness=evidence.completeness,
            collected_at=evidence.collected_at,
            fingerprint=fingerprint,
        )

    @staticmethod
    def _display(value: int | None) -> str:
        return str(value) if value is not None else "unknown"

    @staticmethod
    def _rate(numerator: int, denominator: int) -> float:
        return numerator / denominator if denominator > 0 else 0.0

    @classmethod
    def _semantic_summary(
        cls,
        *,
        evidence: Evidence,
        requests: int | None,
        lua_accepted: int | None,
        kafka_sent: int | None,
        orders_created: int | None,
        orders_failed: int | None,
        unavailable_metrics: list[Any],
    ) -> tuple[str, str, float, list[str]]:
        if evidence.status.value in {"error", "timeout"}:
            return (
                evidence.summary,
                "insufficient_business_metrics",
                0.1,
                [],
            )
        if unavailable_metrics:
            return (
                "Business pipeline counters are partially unavailable.",
                "insufficient_business_metrics",
                0.2,
                [
                    "Unavailable business metrics="
                    + ",".join(str(item) for item in unavailable_metrics[:10])
                    + "."
                ],
            )
        values = (
            requests,
            lua_accepted,
            kafka_sent,
            orders_created,
            orders_failed,
        )
        if any(value is None for value in values):
            return (
                "Business pipeline counters are incomplete.",
                "insufficient_business_metrics",
                0.2,
                [],
            )
        assert requests is not None
        assert lua_accepted is not None
        assert kafka_sent is not None
        assert orders_created is not None
        assert orders_failed is not None
        if requests == 0:
            return (
                "No seckill request activity was observed in the bounded log window.",
                "insufficient_business_metrics",
                0.3,
                [],
            )

        lua_rate = cls._rate(lua_accepted, requests)
        kafka_rate = cls._rate(kafka_sent, lua_accepted)
        order_rate = cls._rate(orders_created, kafka_sent)
        ratio_facts = [
            f"Business lua_admission_rate={lua_rate:.3f}.",
            f"Business kafka_send_rate={kafka_rate:.3f}.",
            f"Business order_creation_rate={order_rate:.3f}.",
        ]
        confidence_scale = 0.8 if evidence.status.value == "partial" else 1.0
        if lua_rate >= 0.9 and kafka_rate >= 0.9 and order_rate < 0.8:
            return (
                "Requests, Lua admission, and Kafka sends are aligned, while order creation is degraded.",
                "supports_downstream_order_creation_degradation",
                0.9 * confidence_scale,
                ratio_facts,
            )
        if (
            lua_rate >= 0.9
            and kafka_rate >= 0.9
            and order_rate >= 0.9
            and orders_failed == 0
        ):
            return (
                "Seckill request, admission, publish, and order creation counters are aligned.",
                "contradicts_business_pipeline_degradation",
                0.9 * confidence_scale,
                ratio_facts,
            )
        if lua_rate < 0.8:
            return (
                "Lua admission success is lower than the observed seckill request count.",
                "supports_lua_admission_drop",
                0.75 * confidence_scale,
                ratio_facts,
            )
        if kafka_rate < 0.8:
            return (
                "Kafka send success is lower than Lua admission success.",
                "supports_kafka_publish_drop",
                0.8 * confidence_scale,
                ratio_facts,
            )
        return (
            "Business pipeline counters are not decisive.",
            "insufficient_business_metrics",
            0.4 * confidence_scale,
            ratio_facts,
        )


class MysqlHealthEvidenceCompressor:
    """Keep only bounded Consumer-path health facts, never raw responses."""

    SUPPORTED_TOOL = "get_mysql_health"

    def __init__(self, *, max_samples: int, max_sample_chars: int) -> None:
        self.max_fact_chars = max_sample_chars

    def compress(self, evidence: Evidence) -> EvidenceCard:
        if evidence.source_tool != self.SUPPORTED_TOOL:
            raise UnsupportedEvidenceError(
                f"unsupported evidence source_tool: {evidence.source_tool}"
            )
        data = evidence.data
        def display(value: Any) -> str:
            return "unavailable" if value is None else str(value)

        facts = [
            f"source_role={display(data.get('source_role'))}.",
            f"database_reachable={display(data.get('database_reachable'))}.",
            f"connection_test_status={display(data.get('connection_test_status'))}.",
            f"hikari_active={display(data.get('hikari_active'))}; "
            f"hikari_idle={display(data.get('hikari_idle'))}.",
            f"connection_timeout_count={display(data.get('connection_timeout_count'))}; "
            f"error_count={display(data.get('error_count'))}.",
        ]
        if evidence.error is not None:
            facts.append(f"Tool {evidence.status.value}: {evidence.error.message}")
        bounded = tuple(
            LogEvidenceCompressor._bounded(fact, self.max_fact_chars)
            for fact in facts
        )
        summary = LogEvidenceCompressor._bounded(evidence.summary, 500)
        fingerprint = LogEvidenceCompressor._fingerprint(
            source=evidence.source,
            kind=evidence.kind,
            status=evidence.status.value,
            summary=summary,
            facts=bounded,
            samples=(),
        )
        return EvidenceCard(
            evidence_id=evidence.evidence_id,
            source=evidence.source,
            kind=evidence.kind,
            summary=summary,
            facts=bounded,
            samples=(),
            interpretation="consumer_mysql_health_observation",
            confidence=0.5,
            score=0.0,
            status=evidence.status,
            completeness=evidence.completeness,
            collected_at=evidence.collected_at,
            fingerprint=fingerprint,
        )
