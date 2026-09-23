"""Deterministic metrics for one HMDP FaultBench trajectory."""

from __future__ import annotations

import json
import re
from typing import Any, Mapping, Sequence

from evaluation.schemas import (
    EvaluationMetrics,
    EvidenceCoverageMetric,
    FaultCase,
    RootCauseMetric,
    ToolEfficiencyMetric,
    TraceCompletenessMetric,
)
from runtime.reports import DiagnosisReport


REQUIRED_TRACE_EVENTS = (
    "incident_created",
    "plan_created",
    "tool_started",
    "tool_completed",
    "evidence_added",
    "hypothesis_updated",
    "report_generated",
)


class FaultEvaluator:
    """Score root cause, evidence, tool usage, and trajectory structure."""

    def evaluate(
        self,
        case: FaultCase,
        diagnosis: DiagnosisReport,
        evidence_rows: Sequence[Mapping[str, Any]],
        trace_rows: Sequence[Mapping[str, Any]],
    ) -> EvaluationMetrics:
        root_cause = self._root_cause(case, diagnosis)
        evidence_coverage = self._evidence_coverage(case, evidence_rows)
        selected_tools = self.selected_tools(trace_rows)
        tool_efficiency = self._tool_efficiency(
            case,
            selected_tools,
            trace_rows,
        )
        trace_completeness = self._trace_completeness(trace_rows)
        expected_tools_found = set(case.expected_tools).issubset(selected_tools)
        passed = (
            root_cause.matched
            and evidence_coverage.score == 1.0
            and expected_tools_found
            and trace_completeness.score == 1.0
        )
        return EvaluationMetrics(
            passed=passed,
            root_cause_accuracy=root_cause,
            evidence_coverage=evidence_coverage,
            tool_efficiency=tool_efficiency,
            trace_completeness=trace_completeness,
        )

    @staticmethod
    def selected_tools(trace_rows: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
        tools: list[str] = []
        for row in trace_rows:
            if row.get("event_type") != "tool_started":
                continue
            payload = FaultEvaluator._json_object(row.get("payload_json"))
            tool_name = payload.get("tool_name")
            if isinstance(tool_name, str):
                tools.append(tool_name)
        return tuple(tools)

    @staticmethod
    def _root_cause(
        case: FaultCase,
        diagnosis: DiagnosisReport,
    ) -> RootCauseMetric:
        expected = FaultEvaluator._normalize_text(case.expected_root_cause)
        actual = FaultEvaluator._normalize_text(diagnosis.root_cause or "")
        matched = bool(actual) and (expected in actual or actual in expected)
        return RootCauseMetric(
            score=1.0 if matched else 0.0,
            matched=matched,
            expected=case.expected_root_cause,
            actual=diagnosis.root_cause,
        )

    @staticmethod
    def _evidence_coverage(
        case: FaultCase,
        evidence_rows: Sequence[Mapping[str, Any]],
    ) -> EvidenceCoverageMetric:
        observed: set[str] = set()
        for row in evidence_rows:
            kind = str(row.get("kind", ""))
            source_tool = str(row.get("source_tool", ""))
            if kind:
                observed.add(kind)
            if source_tool:
                observed.add(source_tool)
            data = FaultEvaluator._json_object(row.get("observation_json"))
            if kind == "kafka_consumer_status":
                member_count = data.get("member_count")
                total_lag = data.get("total_lag")
                if member_count == 0 and isinstance(total_lag, int) and total_lag > 0:
                    observed.add("kafka_no_active_members_with_lag")
            if kind == "log":
                events = data.get("events", [])
                if isinstance(events, list) and any(
                    "consumer stopped unexpectedly"
                    in str(event.get("message", "")).casefold()
                    for event in events
                    if isinstance(event, dict)
                ):
                    observed.add("consumer_stopped_log")
            if kind == "business_metrics":
                sent = data.get("kafka_message_sent_count")
                created = data.get("order_created_success_count")
                if type(sent) is int and type(created) is int and sent > 0 and created / sent < 0.8:
                    observed.add("order_creation_degraded_after_publish")
            if kind == "kafka_consumer_status":
                if (
                    type(data.get("member_count")) is int
                    and data["member_count"] > 0
                    and data.get("lag_status") == "normal"
                    and data.get("offsets_complete") is True
                ):
                    observed.add("kafka_consumer_healthy")
            if kind == "mysql_health":
                failure_classes = data.get("failure_class", [])
                if (
                    data.get("source_role") == "hmdp-consumer"
                    and isinstance(failure_classes, list)
                    and set(failure_classes).intersection({
                        "DATABASE_UNAVAILABLE",
                        "CONNECTION_TIMEOUT",
                        "POOL_EXHAUSTED",
                    })
                ):
                    observed.add("consumer_mysql_connection_failed")

        expected = set(case.expected_evidence)
        missing = expected - observed
        score = (len(expected) - len(missing)) / len(expected)
        return EvidenceCoverageMetric(
            score=score,
            expected=tuple(case.expected_evidence),
            observed=tuple(sorted(observed)),
            missing=tuple(sorted(missing)),
        )

    @staticmethod
    def _tool_efficiency(
        case: FaultCase,
        selected_tools: tuple[str, ...],
        trace_rows: Sequence[Mapping[str, Any]],
    ) -> ToolEfficiencyMetric:
        expected = set(case.expected_tools)
        expected_hits = len(expected.intersection(selected_tools))
        denominator = max(len(expected), len(selected_tools), 1)
        redundant = len(selected_tools) - len(set(selected_tools))
        redundant += sum(tool not in expected for tool in selected_tools)
        diagnosis_steps = sum(
            row.get("event_type") == "plan_created" for row in trace_rows
        )
        return ToolEfficiencyMetric(
            score=expected_hits / denominator,
            tool_call_count=len(selected_tools),
            diagnosis_steps=diagnosis_steps,
            redundant_tool_calls=redundant,
        )

    @staticmethod
    def _trace_completeness(
        trace_rows: Sequence[Mapping[str, Any]],
    ) -> TraceCompletenessMetric:
        present_set = {str(row.get("event_type", "")) for row in trace_rows}
        missing = tuple(
            event for event in REQUIRED_TRACE_EVENTS if event not in present_set
        )
        present = tuple(
            event for event in REQUIRED_TRACE_EVENTS if event in present_set
        )
        return TraceCompletenessMetric(
            score=len(present) / len(REQUIRED_TRACE_EVENTS),
            required=REQUIRED_TRACE_EVENTS,
            present=present,
            missing=missing,
        )

    @staticmethod
    def _json_object(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if not isinstance(value, str):
            return {}
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _normalize_text(value: str) -> str:
        return re.sub(r"[^\w\u4e00-\u9fff]+", "", value.casefold())
