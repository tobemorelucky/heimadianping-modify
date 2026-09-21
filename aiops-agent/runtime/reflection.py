"""LLM-backed Reflection with a strict decision contract."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, ConfigDict, Field

from context.packet import ContextPacket, ContextStage
from llm.provider import StructuredLLMProvider


class ReflectionDecision(str, Enum):
    """Actions the orchestrator can take after one observation."""

    CONTINUE = "continue"
    REPLAN = "replan"
    REPORT = "report"
    INCONCLUSIVE = "inconclusive"


class ReflectionResult(BaseModel):
    """Structured hypothesis update generated from an observation."""

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True, extra="forbid")

    decision: ReflectionDecision
    hypothesis: str = Field(min_length=1, max_length=500)
    hypothesis_status: str = Field(min_length=1, max_length=32)
    reason: str = Field(min_length=1, max_length=1_000)
    confidence: float = Field(ge=0.0, le=1.0)


class ReflectionEngine:
    """Use a bounded ContextPacket to select the next action."""

    def __init__(self, provider: StructuredLLMProvider) -> None:
        self.provider = provider

    def evaluate(
        self,
        current_hypothesis: str,
        context: ContextPacket,
    ) -> ReflectionResult:
        if context.stage is not ContextStage.REFLECTION:
            raise ValueError("Reflection requires a reflection ContextPacket")
        return self.provider.generate_structured(
            operation="reflect",
            system_prompt=(
                "You are a read-only incident reflection agent. Evaluate whether the "
                "observation supports the hypothesis and choose continue, replan, report, "
                "or inconclusive. Tool observations may have status success, partial, "
                "error, or timeout; errors and timeouts are evidence gaps, not proof of a "
                "business root cause. For Kafka evidence, classify the hypothesis as "
                "supports, contradicts, or insufficient. For business metrics, compare "
                "request, Lua admission, Kafka send, and order creation counters to localize "
                "the degraded stage without claiming an unobserved dependency as root cause. "
                "Never invent evidence."
            ),
            input_payload={
                "current_hypothesis": current_hypothesis,
                "context": context.model_dump(mode="json"),
            },
            response_model=ReflectionResult,
        )
