"""LLM-backed investigation planner with strict structured output."""

from typing import Any, Mapping, Sequence

from context.packet import ContextPacket, ContextStage
from llm.provider import LLMResponseValidationError, StructuredLLMProvider
from runtime.models import IncidentTask
from runtime.plans import InvestigationPlan, PlanOutput, PlanStep


class Planner:
    """Ask a provider for one bounded, validated investigation step."""

    def __init__(self, provider: StructuredLLMProvider) -> None:
        self.provider = provider

    def create_plan(
        self,
        incident: IncidentTask,
        context: ContextPacket,
        available_tools: Sequence[Mapping[str, Any]],
        *,
        version: int,
    ) -> InvestigationPlan:
        if context.stage is not ContextStage.PLANNER:
            raise ValueError("Planner requires a planner ContextPacket")
        output = self.provider.generate_structured(
            operation="plan",
            system_prompt=(
                "You are the investigation planner for a read-only incident diagnosis agent. "
                "Select exactly one available tool. Do not propose remediation. "
                "When selected_skill is present, use it only as procedural guidance for "
                "tool ordering and hypothesis validation. A Skill is not Evidence, cannot "
                "establish a root cause, and cannot authorize an unregistered tool."
            ),
            input_payload={
                "incident": incident.model_dump(mode="json"),
                "context": context.model_dump(mode="json"),
                "available_tools": list(available_tools),
            },
            response_model=PlanOutput,
        )
        authorized_names = {
            str(tool.get("tool_name", tool.get("name", "")))
            for tool in available_tools
        }
        if output.tool_name not in authorized_names:
            raise LLMResponseValidationError(
                f"LLM selected a tool outside the authorized registry: {output.tool_name}"
            )
        return InvestigationPlan(
            incident_id=incident.incident_id,
            version=version,
            hypothesis=output.hypothesis,
            action=PlanStep(
                step_id=f"step_{version}_{output.tool_name}",
                objective=output.objective,
                hypothesis=output.hypothesis,
                tool_name=output.tool_name,
                arguments=output.arguments,
            ),
        )
