"""Aggregate product-demo metrics from immutable real incident replays."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from evaluation.replay_catalog import DEFAULT_BUNDLED_REPLAYS, ReplayCatalog


REQUIRED_TRACE_TYPES = {
    "anomaly_detected", "incident_created", "tool_called", "tool_completed", "evidence_created",
    "hypothesis_updated", "report_generated", "diagnosis_completed",
}
DEFAULT_REPORT = Path(__file__).resolve().parent / "reports" / "FAULTBENCH_PRODUCT_DEMO.json"


def _normalize(value: str) -> str:
    return "".join(value.casefold().split())


def build_report(directory: Path | str = DEFAULT_BUNDLED_REPLAYS) -> dict[str, Any]:
    scenarios = []
    for replay in ReplayCatalog(directory).discover():
        expected = replay.evaluation
        root_cause_accuracy = float(
            _normalize(expected.expected_root_cause) in _normalize(replay.report.root_cause)
            or _normalize(replay.report.root_cause) in _normalize(expected.expected_root_cause)
        )
        observed_tools = [
            item.payload.get("tool_name") for item in replay.traces
            if item.event_type == "tool_called" and item.payload.get("tool_name")
        ]
        expected_tools = set(expected.expected_tools)
        evidence_kinds = {item.kind for item in replay.evidence}
        evidence_coverage = len(
            set(expected.expected_evidence_kinds).intersection(evidence_kinds)
        ) / max(len(expected.expected_evidence_kinds), 1)
        useful_calls = sum(tool in expected_tools for tool in observed_tools)
        tool_efficiency = useful_calls / max(len(observed_tools), len(expected_tools), 1)
        trace_types = {item.event_type for item in replay.traces}
        trace_completeness = len(REQUIRED_TRACE_TYPES.intersection(trace_types)) / len(
            REQUIRED_TRACE_TYPES
        )
        diagnosed_anomaly = replay.report.status == "confirmed"
        false_positive = diagnosed_anomaly and not expected.anomaly_expected
        scenarios.append({
            "fault_id": replay.fault_id,
            "scenario": replay.scenario,
            "accuracy": root_cause_accuracy,
            "evidence_coverage": evidence_coverage,
            "tool_efficiency": tool_efficiency,
            "trace_completeness": trace_completeness,
            "false_positive": false_positive,
            "tool_calls": observed_tools,
        })
    count = len(scenarios)
    if not count:
        raise ValueError("no replay scenarios available for benchmark reporting")
    negative_cases = sum(
        not replay.evaluation.anomaly_expected
        for replay in ReplayCatalog(directory).discover()
    )
    false_positives = sum(item["false_positive"] for item in scenarios)
    return {
        "report_id": "HMDP_FAULTBENCH_PRODUCT_DEMO_V1",
        "source": "recorded_observation_replays",
        "scenario_count": count,
        "scenarios": scenarios,
        "aggregate": {
            "accuracy": sum(item["accuracy"] for item in scenarios) / count,
            "evidence_coverage": sum(item["evidence_coverage"] for item in scenarios) / count,
            "tool_efficiency": sum(item["tool_efficiency"] for item in scenarios) / count,
            "trace_completeness": sum(item["trace_completeness"] for item in scenarios) / count,
            "false_positive_count": false_positives,
            "negative_case_count": negative_cases,
            "false_positive_rate": (
                false_positives / negative_cases if negative_cases else None
            ),
            "false_positive_assessment": (
                "measured" if negative_cases else "not_applicable_no_negative_cases"
            ),
        },
    }


def save_report(report: dict[str, Any], output: Path | str = DEFAULT_REPORT) -> Path:
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the two-scenario recorded FaultBench report")
    parser.add_argument("--replays", type=Path, default=DEFAULT_BUNDLED_REPLAYS)
    parser.add_argument("--output", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = build_report(args.replays)
    save_report(report, args.output)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
