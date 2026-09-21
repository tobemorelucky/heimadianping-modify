"""Execute one deterministic FaultCase through the real Agent Runtime."""

from __future__ import annotations

import argparse
import json
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Mapping

from evaluation.evaluator import FaultEvaluator
from evaluation.schemas import (
    EvaluationReport,
    FaultCase,
    FaultFixture,
    FinalDiagnosis,
    TraceSummary,
)
from llm.mock import MockLLMProvider
from memory.database import SQLiteDatabase
from runtime.models import IncidentBudget, IncidentCreate, IncidentSource
from runtime.orchestrator import RuntimeOrchestrator
from runtime.tool_registry import ToolRegistry
from tool_contracts import EvidenceCompleteness, ObservationStatus, ToolObservation


EVALUATION_ROOT = Path(__file__).resolve().parent
DEFAULT_FAULT_ID = "KAFKA_CONSUMER_DOWN_001"


class FixtureMCPClient:
    """Manifest-compatible deterministic MCP boundary for FaultBench only."""

    def __init__(self, fixture: FaultFixture) -> None:
        self.fixture = fixture
        self.registry = ToolRegistry.from_file()

    def get_tool_schemas(self) -> list[dict[str, Any]]:
        return [
            {
                "name": tool.tool_name,
                "description": tool.description,
                "input_schema": tool.input_schema,
                "server": tool.server,
            }
            for tool in self.registry.manifest.tools
        ]

    def call(
        self,
        tool_name: str,
        arguments: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if tool_name == "get_kafka_status":
            return self._kafka_observation().model_dump(mode="json")
        if tool_name == "search_application_logs":
            return self._log_observation().model_dump(mode="json")
        raise ValueError(f"fixture does not implement tool: {tool_name}")

    def _kafka_observation(self) -> ToolObservation:
        kafka = self.fixture.kafka
        lag_status = "abnormal" if kafka.lag > 0 else "normal"
        consumer_status = "empty" if kafka.member_count == 0 else "stable"
        collected_at = self.fixture.log.timestamp - timedelta(seconds=1)
        return ToolObservation(
            evidence_id=f"evi_{self.fixture.fault_id.lower()}_kafka",
            status=ObservationStatus.SUCCESS,
            kind="kafka_consumer_status",
            source="hmdp.kafka.consumer_group",
            source_tool="get_kafka_status",
            collected_at=collected_at,
            summary=(
                f"Kafka group {kafka.consumer_group} state={consumer_status}, "
                f"members={kafka.member_count}, total_lag={kafka.lag}, "
                f"lag_status={lag_status}."
            ),
            completeness=EvidenceCompleteness.COMPLETE,
            raw_ref=f"fixture://{self.fixture.fault_id}/kafka",
            data={
                "broker_reachable": True,
                "topic": kafka.topic,
                "consumer_group": kafka.consumer_group,
                "topic_exists": True,
                "consumer_status": consumer_status,
                "member_count": kafka.member_count,
                "partition_count": 1,
                "partitions_returned": 1,
                "partitions_truncated": False,
                "offsets_complete": True,
                "total_lag": kafka.lag,
                "lag_threshold": 1000,
                "lag_status": lag_status,
                "severity": kafka.severity,
                "partitions": [
                    {
                        "partition": 0,
                        "committed_offset": 100,
                        "end_offset": 100 + kafka.lag,
                        "lag": kafka.lag,
                    }
                ],
            },
        )

    def _log_observation(self) -> ToolObservation:
        log = self.fixture.log
        return ToolObservation(
            evidence_id=f"evi_{self.fixture.fault_id.lower()}_log",
            status=ObservationStatus.SUCCESS,
            kind="log",
            source="hmdp.spring_boot.runtime_logs",
            source_tool="search_application_logs",
            collected_at=log.timestamp,
            summary="Application log search returned 1 event(s) with completeness=complete.",
            completeness=EvidenceCompleteness.COMPLETE,
            raw_ref=f"fixture://{self.fixture.fault_id}/logs",
            data={
                "query": "consumer",
                "events": [
                    {
                        "path": "target/runtime/spring-boot.error.log",
                        "line_number": 1,
                        "timestamp": log.timestamp.isoformat(),
                        "message": log.message,
                    }
                ],
                "files": [],
                "truncated": False,
                "warnings": [],
            },
        )


class EvaluationRunner:
    """Run an immutable case and optionally persist a deterministic JSON report."""

    def __init__(
        self,
        *,
        evaluator: FaultEvaluator | None = None,
        reports_dir: Path | str = EVALUATION_ROOT / "reports",
    ) -> None:
        self.evaluator = evaluator or FaultEvaluator()
        self.reports_dir = Path(reports_dir)

    def run(
        self,
        case: FaultCase,
        fixture: FaultFixture,
        *,
        save_report: bool = True,
    ) -> EvaluationReport:
        if fixture.fault_id != case.fault_id:
            raise ValueError("FaultCase and fixture fault_id do not match")

        provider = self._provider(case, fixture)
        with TemporaryDirectory(prefix="hmdp-faultbench-") as temp_dir:
            database = SQLiteDatabase(Path(temp_dir) / "faultbench.db")
            database.initialize()
            orchestrator = RuntimeOrchestrator(
                database,
                FixtureMCPClient(fixture),
                llm_provider=provider,
            )
            run_result = orchestrator.run(
                IncidentCreate(
                    title=case.incident_title,
                    description=case.incident_description,
                    source=IncidentSource.FAULTBENCH,
                    affected_components=["kafka"],
                    budget=IncidentBudget(
                        max_tool_calls=4,
                        max_reflections=2,
                    ),
                )
            )
            evidence_rows = database.list_evidence(run_result.incident_id)
            trace_rows = database.list_trace_events(run_result.incident_id)
            metrics = self.evaluator.evaluate(
                case,
                run_result.report,
                evidence_rows,
                trace_rows,
            )
            selected_tools = self.evaluator.selected_tools(trace_rows)
            report = EvaluationReport(
                fault_id=case.fault_id,
                final_diagnosis=FinalDiagnosis(
                    status=run_result.report.status.value,
                    title=run_result.report.title,
                    conclusion=run_result.report.conclusion,
                    root_cause=run_result.report.root_cause,
                    confidence=run_result.report.confidence,
                ),
                selected_tools=selected_tools,
                evidence_ids=tuple(
                    str(row["evidence_id"]) for row in evidence_rows
                ),
                trace_summary=tuple(
                    TraceSummary(
                        sequence=int(row["sequence"]),
                        event_type=str(row["event_type"]),
                        stage=str(row["stage"]),
                        summary=str(row["summary"]),
                    )
                    for row in trace_rows
                ),
                metrics=metrics,
            )

        if save_report:
            self._save(report)
        return report

    def run_fault_id(
        self,
        fault_id: str = DEFAULT_FAULT_ID,
        *,
        save_report: bool = True,
    ) -> EvaluationReport:
        case = load_fault_case(EVALUATION_ROOT / "faults" / f"{fault_id}.json")
        fixture = load_fault_fixture(
            EVALUATION_ROOT / "fixtures" / f"{fault_id}.json"
        )
        return self.run(case, fixture, save_report=save_report)

    def _save(self, report: EvaluationReport) -> Path:
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        output_path = self.reports_dir / f"{report.fault_id}.json"
        output_path.write_text(
            json.dumps(
                report.model_dump(mode="json"),
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        return output_path

    @staticmethod
    def _provider(case: FaultCase, fixture: FaultFixture) -> MockLLMProvider:
        kafka = fixture.kafka
        return MockLLMProvider(
            scripted_responses={
                "plan": [
                    {
                        "objective": "检查 Kafka 消费者成员和 lag。",
                        "hypothesis": case.expected_root_cause,
                        "tool_name": "get_kafka_status",
                        "arguments": {
                            "topic": kafka.topic,
                            "consumer_group": kafka.consumer_group,
                        },
                    },
                    {
                        "objective": "检查应用日志中消费者停止的交叉证据。",
                        "hypothesis": case.expected_root_cause,
                        "tool_name": "search_application_logs",
                        "arguments": {
                            "log_paths": [
                                "target/runtime/spring-boot.error.log",
                                "target/runtime/spring-boot.out.log",
                            ],
                            "query": "consumer",
                            "max_lines": 100,
                        },
                    },
                ],
                "reflect": [
                    {
                        "decision": "replan",
                        "hypothesis": case.expected_root_cause,
                        "hypothesis_status": "supports",
                        "reason": (
                            "Kafka Evidence 显示无活跃消费者且存在积压；"
                            "继续检查应用日志以取得独立证据。"
                        ),
                        "confidence": 0.85,
                    },
                    {
                        "decision": "report",
                        "hypothesis": case.expected_root_cause,
                        "hypothesis_status": "supports",
                        "reason": (
                            "Kafka lag、零活跃成员与 consumer stopped 日志相互印证。"
                        ),
                        "confidence": 0.96,
                    },
                ],
                "report": [
                    {
                        "status": "confirmed",
                        "title": "秒杀订单异步创建延迟诊断报告",
                        "conclusion": (
                            "Kafka 消费者无活跃成员，消息持续积压；"
                            "应用日志同时记录 consumer stopped unexpectedly。"
                        ),
                        "root_cause": case.expected_root_cause,
                        "confidence": 0.96,
                    }
                ],
            }
        )


def load_fault_case(path: Path | str) -> FaultCase:
    return FaultCase.model_validate_json(Path(path).read_text(encoding="utf-8"))


def load_fault_fixture(path: Path | str) -> FaultFixture:
    return FaultFixture.model_validate_json(Path(path).read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one deterministic HMDP FaultCase")
    parser.add_argument("--fault-id", default=DEFAULT_FAULT_ID)
    parser.add_argument("--no-save", action="store_true")
    args = parser.parse_args()

    runner = EvaluationRunner()
    report = runner.run_fault_id(args.fault_id, save_report=not args.no_save)
    print(
        json.dumps(
            {
                "fault_id": report.fault_id,
                "passed": report.metrics.passed,
                "root_cause_accuracy": report.metrics.root_cause_accuracy.score,
                "evidence_coverage": report.metrics.evidence_coverage.score,
                "tool_efficiency": report.metrics.tool_efficiency.score,
                "trace_completeness": report.metrics.trace_completeness.score,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
