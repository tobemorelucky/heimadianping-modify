"""Agent Runtime closed loop with configurable structured LLM decisions."""

from __future__ import annotations

from time import perf_counter
from typing import Any, Callable, Dict, List, Optional

from context.manager import ContextManager
from context.packet import ContextPacket, ContextStage
from llm.mock import MockLLMProvider
from llm.provider import StructuredLLMProvider
from memory.database import SQLiteDatabase
from reasoning.ledger import HypothesisLedger
from reasoning.models import Hypothesis, HypothesisStatus
from runtime.evidence import Evidence
from runtime.mcp_client import MCPClientProtocol
from runtime.models import IncidentCreate, IncidentStatus, IncidentTask
from runtime.planner import Planner
from runtime.plans import InvestigationPlan
from runtime.reflection import (
    ReflectionDecision,
    ReflectionEngine,
    ReflectionResult,
)
from runtime.reporter import ReportGenerator
from runtime.reports import DiagnosisRunResult
from runtime.tool_registry import ToolRegistry
from skills.models import SkillContext
from skills.registry import SkillRegistry
from tool_contracts import ObservationStatus
from trace.models import AgentTraceEvent, TraceEventType


class RuntimeOrchestrator:
    """Run one incident through LLM decisions and a stdio MCP tool."""

    def __init__(
        self,
        database: SQLiteDatabase,
        mcp_client: MCPClientProtocol,
        llm_provider: StructuredLLMProvider | None = None,
        planner: Optional[Planner] = None,
        reflection_engine: Optional[ReflectionEngine] = None,
        report_generator: Optional[ReportGenerator] = None,
        tool_registry: ToolRegistry | None = None,
        context_manager: ContextManager | None = None,
        skill_registry: SkillRegistry | None = None,
    ) -> None:
        self.database = database
        self.mcp_client = mcp_client
        provider = llm_provider or MockLLMProvider()
        self.planner = planner or Planner(provider)
        self.reflection_engine = reflection_engine or ReflectionEngine(provider)
        self.report_generator = report_generator or ReportGenerator(provider)
        self.tool_registry = tool_registry or ToolRegistry.from_file()
        self.context_manager = context_manager or ContextManager()
        self.skill_registry = skill_registry or SkillRegistry()

    def run(
        self,
        request: IncidentCreate,
        *,
        incident_id: str | None = None,
    ) -> DiagnosisRunResult:
        """Execute a complete synchronous read-only diagnosis."""

        incident = IncidentTask.from_create(request)
        if incident_id is not None:
            incident_payload = incident.model_dump()
            incident_payload["incident_id"] = incident_id
            incident = IncidentTask.model_validate(incident_payload)
        self.database.create_incident(incident)

        sequence = 0

        def record(
            event_type: TraceEventType,
            stage: str,
            summary: str,
            *,
            objective: str | None = None,
            payload: Dict[str, Any] | None = None,
        ) -> None:
            nonlocal sequence
            event = AgentTraceEvent(
                incident_id=incident.incident_id,
                sequence=sequence,
                event_type=event_type,
                stage=stage,
                objective=objective,
                summary=summary,
                payload=payload or {},
            )
            self.database.save_trace_event(event)
            sequence += 1

        record(
            TraceEventType.INCIDENT_CREATED,
            "task_manager",
            "Incident created and queued for structured diagnosis.",
            payload={"title": incident.title, "status": incident.status.value},
        )

        evidence_items: List[Evidence] = []
        reflection_count = 0
        tool_call_count = 0
        selected_skill: SkillContext | None = None
        hypothesis_ledger = HypothesisLedger()

        try:
            self.database.update_incident_status(
                incident.incident_id,
                IncidentStatus.PLANNING,
            )
            if "mysql" in incident.affected_components:
                for description in ("Kafka Consumer Failure", "MySQL Persistence Failure"):
                    seeded = hypothesis_ledger.create(description)
                    self.database.save_hypothesis(
                        incident.incident_id,
                        seeded,
                        "Seeded competing hypotheses for Consumer persistence incident.",
                    )
                    record(
                        TraceEventType.HYPOTHESIS_CREATED,
                        "reasoning",
                        description,
                        payload={
                            "hypothesis_id": seeded.hypothesis_id,
                            "status": seeded.status.value,
                            "confidence": seeded.confidence,
                        },
                    )
            matched_skills = self.skill_registry.match_skills(incident)
            if matched_skills:
                selected_metadata = matched_skills[0]
                record(
                    TraceEventType.SKILL_SELECTED,
                    "skill_runtime",
                    f"Selected skill {selected_metadata.name} for this Incident.",
                    payload={
                        "skill_id": selected_metadata.skill_id,
                        "version": selected_metadata.version,
                        "incident_id": incident.incident_id,
                    },
                )
                loaded_skill = self.skill_registry.load_skill(
                    selected_metadata.skill_id
                )
                selected_skill = loaded_skill.planner_context()
                record(
                    TraceEventType.SKILL_LOADED,
                    "skill_runtime",
                    f"Loaded skill {selected_metadata.name} on demand.",
                    payload={
                        "skill_id": selected_metadata.skill_id,
                        "version": selected_metadata.version,
                        "incident_id": incident.incident_id,
                        "reference_count": len(loaded_skill.references),
                    },
                )
            try:
                discovered_tools = self.mcp_client.get_tool_schemas()
                authorized_tools = self.tool_registry.validate_discovery(
                    discovered_tools
                )
                tool_descriptors = [
                    tool.planner_descriptor() for tool in authorized_tools
                ]
                tool_input_schemas = {
                    tool.tool_name: tool.input_schema for tool in authorized_tools
                }
            except Exception as exc:
                record(
                    TraceEventType.TOOL_FAILED,
                    "tool_registry",
                    "MCP server startup or tool discovery failed.",
                    payload={
                        "operation": "list_tools",
                        "error_type": type(exc).__name__,
                        "error": str(exc)[:500],
                    },
                )
                raise
            planner_context = self._build_context(
                evidence_items,
                stage=ContextStage.PLANNER,
                record=record,
                selected_skill=selected_skill,
                current_hypotheses=hypothesis_ledger.rank(),
            )
            plan = self.planner.create_plan(
                incident,
                planner_context,
                tool_descriptors,
                version=1,
            )
            self._save_plan(plan, record)
            current_hypothesis = self._register_plan_hypothesis(
                hypothesis_ledger,
                plan,
                record,
            )

            final_reflection: ReflectionResult | None = None

            while final_reflection is None:
                step = plan.action
                if tool_call_count >= incident.budget.max_tool_calls:
                    final_reflection = self._budget_exhausted_reflection()
                    break

                self.database.update_incident_status(
                    incident.incident_id,
                    IncidentStatus.INVESTIGATING,
                )
                tool_arguments = dict(step.arguments)
                input_properties = tool_input_schemas[step.tool_name].get(
                    "properties",
                    {},
                )
                if "incident_id" in input_properties:
                    tool_arguments["incident_id"] = incident.incident_id
                record(
                    TraceEventType.TOOL_STARTED,
                    "evidence_collector",
                    f"Starting MCP tool {step.tool_name}.",
                    objective=step.objective,
                    payload={
                        "step_id": step.step_id,
                        "tool_name": step.tool_name,
                        "arguments": tool_arguments,
                    },
                )

                started_at = perf_counter()
                try:
                    observation = self.mcp_client.call(
                        step.tool_name,
                        tool_arguments,
                    )
                except Exception as exc:
                    record(
                        TraceEventType.TOOL_FAILED,
                        "evidence_collector",
                        f"MCP tool {step.tool_name} failed.",
                        payload={
                            "step_id": step.step_id,
                            "tool_name": step.tool_name,
                            "error_type": type(exc).__name__,
                            "error": str(exc)[:500],
                            "duration_ms": round((perf_counter() - started_at) * 1000),
                        },
                    )
                    raise
                tool_call_count += 1
                observation_status = ObservationStatus(observation["status"])
                trace_type = (
                    TraceEventType.TOOL_FAILED
                    if observation_status in {
                        ObservationStatus.ERROR,
                        ObservationStatus.TIMEOUT,
                    }
                    else TraceEventType.TOOL_COMPLETED
                )
                record(
                    trace_type,
                    "evidence_collector",
                    (
                        f"MCP tool {step.tool_name} returned "
                        f"{observation_status.value} observation."
                    ),
                    payload={
                        "step_id": step.step_id,
                        "tool_name": step.tool_name,
                        "status": observation_status.value,
                        "evidence_id": observation.get("evidence_id"),
                        "source": observation.get("source"),
                        "completeness": observation.get("completeness"),
                        "duration_ms": round((perf_counter() - started_at) * 1000),
                    },
                )

                evidence = Evidence(
                    incident_id=incident.incident_id,
                    step_id=step.step_id,
                    observation_window=(
                        incident.observation_window.model_dump(mode="json")
                        if incident.observation_window
                        else observation.get("observation_window")
                    ),
                    **{
                        key: value
                        for key, value in observation.items()
                        if key != "observation_window"
                    },
                )
                evidence_items.append(evidence)
                self.database.save_evidence(evidence)
                record(
                    TraceEventType.EVIDENCE_ADDED,
                    "context",
                    evidence.summary,
                    payload={
                        "evidence_id": evidence.evidence_id,
                        "source_tool": evidence.source_tool,
                    },
                )

                reflection_context = self._build_context(
                    evidence_items,
                    stage=ContextStage.REFLECTION,
                    record=record,
                    latest_evidence_id=evidence.evidence_id,
                    current_hypotheses=hypothesis_ledger.rank(),
                )
                reflection = self.reflection_engine.evaluate(
                    plan.hypothesis,
                    reflection_context,
                )
                current_hypothesis = self._update_hypothesis(
                    hypothesis_ledger,
                    current_hypothesis,
                    evidence,
                    reflection,
                    record,
                )
                record(
                    TraceEventType.REFLECTION_COMPLETED,
                    "reflection",
                    f"Reflection selected {reflection.decision.value}.",
                    payload={"decision": reflection.decision.value},
                )

                if reflection.decision in {
                    ReflectionDecision.CONTINUE,
                    ReflectionDecision.REPLAN,
                }:
                    if reflection_count >= incident.budget.max_reflections:
                        final_reflection = self._budget_exhausted_reflection()
                        break
                    reflection_count += 1
                    self.database.update_incident_status(
                        incident.incident_id,
                        IncidentStatus.REFLECTING,
                    )
                    planner_context = self._build_context(
                        evidence_items,
                        stage=ContextStage.PLANNER,
                        record=record,
                        selected_skill=selected_skill,
                        current_hypotheses=hypothesis_ledger.rank(),
                    )
                    plan = self.planner.create_plan(
                        incident,
                        planner_context,
                        tool_descriptors,
                        version=plan.version + 1,
                    )
                    self._save_plan(plan, record)
                    current_hypothesis = self._register_plan_hypothesis(
                        hypothesis_ledger,
                        plan,
                        record,
                    )
                    continue

                final_reflection = reflection

            self.database.update_incident_status(
                incident.incident_id,
                IncidentStatus.REPORTING,
            )
            report = self.report_generator.generate(
                incident.incident_id,
                final_reflection,
                evidence_items,
                self.database.list_trace_events(incident.incident_id),
                hypothesis_ledger.rank(),
                current_hypothesis,
            )
            self.database.save_report(report)
            record(
                TraceEventType.REPORT_GENERATED,
                "reporter",
                report.conclusion,
                payload={
                    "report_id": report.report_id,
                    "status": report.status.value,
                    "root_cause": report.root_cause,
                    "evidence_ids": report.evidence_ids,
                    "final_hypothesis_id": (
                        report.final_hypothesis.hypothesis_id
                        if report.final_hypothesis is not None
                        else None
                    ),
                },
            )
            self.database.update_incident_status(
                incident.incident_id,
                IncidentStatus.AWAITING_HUMAN,
            )
            return DiagnosisRunResult(
                incident_id=incident.incident_id,
                report=report,
            )
        except Exception:
            self.database.update_incident_status(
                incident.incident_id,
                IncidentStatus.FAILED,
            )
            raise

    def _build_context(
        self,
        evidence: List[Evidence],
        *,
        stage: ContextStage,
        record: Callable[..., None],
        latest_evidence_id: str | None = None,
        selected_skill: SkillContext | None = None,
        current_hypotheses: tuple[Hypothesis, ...] = (),
    ) -> ContextPacket:
        packet = self.context_manager.build(
            evidence,
            stage=stage,
            latest_evidence_id=latest_evidence_id,
            selected_skill=selected_skill,
            current_hypotheses=current_hypotheses,
        )
        record(
            TraceEventType.CONTEXT_BUILT,
            "context",
            f"Built bounded {stage.value} context.",
            payload={
                "context_id": packet.context_id,
                "stage": packet.stage.value,
                "selected_skill_id": (
                    packet.selected_skill.skill_id
                    if packet.selected_skill is not None
                    else None
                ),
                "selected_evidence_ids": list(packet.selected_evidence_ids),
                "current_hypothesis_ids": [
                    item.hypothesis_id for item in packet.current_hypotheses
                ],
                "excluded": [
                    item.model_dump(mode="json") for item in packet.excluded
                ],
                "budget": packet.budget.model_dump(mode="json"),
            },
        )
        return packet

    def _save_plan(
        self,
        plan: InvestigationPlan,
        record: Callable[..., None],
    ) -> None:
        self.database.save_plan(plan)
        record(
            TraceEventType.PLAN_CREATED,
            "planner",
            f"Created investigation plan version {plan.version}.",
            payload={
                "plan_id": plan.plan_id,
                "version": plan.version,
                "hypothesis": plan.hypothesis,
                "action": plan.action.model_dump(mode="json"),
            },
        )

    def _register_plan_hypothesis(
        self,
        ledger: HypothesisLedger,
        plan: InvestigationPlan,
        record: Callable[..., None],
    ) -> Hypothesis:
        existing_ids = {item.hypothesis_id for item in ledger.rank()}
        hypothesis = ledger.create(plan.hypothesis)
        if hypothesis.hypothesis_id not in existing_ids:
            self.database.save_hypothesis(
                plan.incident_id,
                hypothesis,
                "Created from an InvestigationPlan before Evidence collection.",
            )
            record(
                TraceEventType.HYPOTHESIS_CREATED,
                "reasoning",
                hypothesis.description,
                payload={
                    "hypothesis_id": hypothesis.hypothesis_id,
                    "status": hypothesis.status.value,
                    "confidence": hypothesis.confidence,
                    "plan_id": plan.plan_id,
                    "plan_version": plan.version,
                },
            )
        return hypothesis

    def _update_hypothesis(
        self,
        ledger: HypothesisLedger,
        hypothesis: Hypothesis,
        evidence: Evidence,
        reflection: ReflectionResult,
        record: Callable[..., None],
    ) -> Hypothesis:
        updated = ledger.update(
            hypothesis.hypothesis_id,
            description=reflection.hypothesis,
            confidence=reflection.confidence,
        )
        reflection_status = reflection.hypothesis_status.casefold()
        if reflection_status in {"supports", "supported", "verified"}:
            updated = ledger.add_supporting_evidence(
                updated.hypothesis_id,
                evidence.evidence_id,
                confidence=reflection.confidence,
            )
        elif reflection_status in {
            "contradicts",
            "contradicted",
            "rejected",
        }:
            updated = ledger.add_contradicting_evidence(
                updated.hypothesis_id,
                evidence.evidence_id,
                confidence=reflection.confidence,
            )
            if reflection_status == "rejected":
                updated = ledger.update(
                    updated.hypothesis_id,
                    status=HypothesisStatus.REJECTED,
                    confidence=reflection.confidence,
                )
        self.database.save_hypothesis(
            evidence.incident_id,
            updated,
            reflection.reason,
        )
        record(
            TraceEventType.HYPOTHESIS_UPDATED,
            "reflection",
            reflection.reason,
            payload={
                "hypothesis_id": updated.hypothesis_id,
                "hypothesis": updated.description,
                "status": updated.status.value,
                "reflection_status": reflection.hypothesis_status,
                "decision": reflection.decision.value,
                "confidence": updated.confidence,
                "supporting_evidence_refs": list(
                    updated.supporting_evidence_refs
                ),
                "contradicting_evidence_refs": list(
                    updated.contradicting_evidence_refs
                ),
            },
        )
        return updated

    @staticmethod
    def _budget_exhausted_reflection() -> ReflectionResult:
        return ReflectionResult(
            decision=ReflectionDecision.REPORT,
            hypothesis="调查预算耗尽，无法确认根因。",
            hypothesis_status="inconclusive",
            reason="Agent reached its reflection or tool-call budget.",
            confidence=0.0,
        )
