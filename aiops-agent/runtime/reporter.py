"""Generate a validated evidence-backed report through the LLM provider."""

from typing import Any, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field

from llm.provider import StructuredLLMProvider
from runtime.evidence import Evidence
from runtime.reflection import ReflectionResult
from runtime.reports import DiagnosisReport, DiagnosisStatus


class ReportOutput(BaseModel):
    """Exact JSON contract returned by the Reporter LLM call."""

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True, extra="forbid")

    status: DiagnosisStatus
    title: str = Field(min_length=1, max_length=200)
    conclusion: str = Field(min_length=1, max_length=2_000)
    root_cause: str | None = Field(default=None, max_length=500)
    confidence: float = Field(ge=0.0, le=1.0)


class ReportGenerator:
    """Ask a provider for a report while binding evidence IDs locally."""

    def __init__(self, provider: StructuredLLMProvider) -> None:
        self.provider = provider

    def generate(
        self,
        incident_id: str,
        reflection: ReflectionResult,
        evidence: Sequence[Evidence],
        trace: Sequence[Mapping[str, Any]],
    ) -> DiagnosisReport:
        output = self.provider.generate_structured(
            operation="report",
            system_prompt=(
                "You are a read-only incident report generator. Use only supplied evidence "
                "and trace summaries. Clearly return an inconclusive status when proof is weak."
            ),
            input_payload={
                "incident_id": incident_id,
                "hypothesis": reflection.hypothesis,
                "reflection": reflection.model_dump(mode="json"),
                "evidence": [item.model_dump(mode="json") for item in evidence],
                "trace": list(trace),
            },
            response_model=ReportOutput,
        )
        return DiagnosisReport(
            incident_id=incident_id,
            status=output.status,
            title=output.title,
            conclusion=output.conclusion,
            root_cause=output.root_cause,
            confidence=output.confidence,
            evidence_ids=[item.evidence_id for item in evidence],
        )
