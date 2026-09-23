"""Validated, read-only incident replay catalog for the AIOps Console.

Replay import copies an immutable display projection.  It never constructs a
RuntimeOrchestrator and therefore cannot call an LLM, MCP tool, or action.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


EVALUATION_ROOT = Path(__file__).resolve().parent
DEFAULT_BUNDLED_REPLAYS = EVALUATION_ROOT / "replays"
DEFAULT_IMPORTED_REPLAYS = EVALUATION_ROOT.parent / "data" / "console-replays"


class ReplayTrace(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: str
    event_type: str
    raw_event_type: str
    source: str
    stage: str
    summary: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    sequence: int


class ReplayEvidence(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    evidence_id: str
    kind: str
    source: str
    source_tool: str
    status: Literal["success", "partial", "error", "timeout"]
    summary: str
    facts: tuple[str, ...] = ()
    completeness: Literal["complete", "partial", "unknown"]
    collected_at: datetime


class ReplayHypothesisHistory(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    created_at: datetime
    status: str
    confidence: float = Field(ge=0, le=1)
    event_type: str


class ReplayHypothesis(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    hypothesis_id: str
    description: str
    status: str
    confidence: float = Field(ge=0, le=1)
    supporting_evidence_refs: tuple[str, ...] = ()
    contradicting_evidence_refs: tuple[str, ...] = ()
    history: tuple[ReplayHypothesisHistory, ...] = ()


class ReplayReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    report_id: str
    status: str
    title: str
    conclusion: str
    root_cause: str
    confidence: float = Field(ge=0, le=1)
    evidence_ids: tuple[str, ...]
    generated_at: datetime


class ReplayProposal(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    proposal_id: str
    incident_id: str
    action_name: str
    reason: str
    evidence_refs: tuple[str, ...]
    risk_level: str
    approval_required: bool
    permission_result: str
    verification_plan: str | None = None
    rollback_plan: str | None = None


class ReplayEvaluationExpectation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    expected_root_cause: str
    expected_tools: tuple[str, ...]
    expected_evidence_kinds: tuple[str, ...]
    anomaly_expected: bool = True


class RecordedIncidentReplay(BaseModel):
    """Display-safe projection exported from a completed real incident."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal["1.0"]
    recording_type: Literal["recorded_observation"]
    fault_id: str
    scenario: str
    captured_at: datetime
    source: str
    incident_id: str
    title: str
    description: str
    severity: str
    status: str
    diagnosis_status: str
    runtime_status: str
    created_at: datetime
    updated_at: datetime
    trigger_signal_ids: tuple[str, ...]
    traces: tuple[ReplayTrace, ...]
    evidence: tuple[ReplayEvidence, ...]
    hypotheses: tuple[ReplayHypothesis, ...]
    report: ReplayReport
    proposals: tuple[ReplayProposal, ...] = ()
    evaluation: ReplayEvaluationExpectation

    @field_validator("captured_at", "created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("replay timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def validate_references(self) -> "RecordedIncidentReplay":
        evidence_ids = {item.evidence_id for item in self.evidence}
        if not set(self.report.evidence_ids).issubset(evidence_ids):
            raise ValueError("report references unknown replay evidence")
        for proposal in self.proposals:
            if proposal.incident_id != self.incident_id:
                raise ValueError("proposal incident_id does not match replay")
            if not set(proposal.evidence_refs).issubset(evidence_ids):
                raise ValueError("proposal references unknown replay evidence")
        return self

    def summary(self) -> dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "title": self.title,
            "demo_scenario": self.scenario,
            "demo_fault_id": self.fault_id,
            "severity": self.severity,
            "status": self.status,
            "diagnosis_status": self.diagnosis_status,
            "runtime_status": self.runtime_status,
            "source": "recorded_observation",
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "trigger_signal_ids": list(self.trigger_signal_ids),
            "replay_only": True,
            "recording_type": self.recording_type,
        }

    def detail(self) -> dict[str, Any]:
        return {
            **self.summary(),
            "description": self.description,
            "traces": [item.model_dump(mode="json") for item in self.traces],
            "evidence": [item.model_dump(mode="json") for item in self.evidence],
            "hypotheses": [item.model_dump(mode="json") for item in self.hypotheses],
            "report": self.report.model_dump(mode="json"),
            "proposals": [item.model_dump(mode="json") for item in self.proposals],
            "recorded_at": self.captured_at.isoformat(),
            "recording_source": self.source,
        }


class ReplayCatalog:
    def __init__(self, directory: Path | str) -> None:
        self.directory = Path(directory)

    def discover(self) -> tuple[RecordedIncidentReplay, ...]:
        if not self.directory.exists():
            return ()
        records = [
            RecordedIncidentReplay.model_validate_json(path.read_text(encoding="utf-8"))
            for path in sorted(self.directory.glob("*.json"))
        ]
        incident_ids = [item.incident_id for item in records]
        if len(incident_ids) != len(set(incident_ids)):
            raise ValueError("replay catalog contains duplicate incident_id values")
        return tuple(records)

    def get(self, incident_id: str) -> RecordedIncidentReplay | None:
        return next(
            (item for item in self.discover() if item.incident_id == incident_id),
            None,
        )


def import_replays(
    source: Path | str = DEFAULT_BUNDLED_REPLAYS,
    destination: Path | str = DEFAULT_IMPORTED_REPLAYS,
) -> tuple[Path, ...]:
    """Validate and atomically copy replay projections into the local catalog."""

    source_catalog = ReplayCatalog(source)
    records = source_catalog.discover()
    if not records:
        raise ValueError("no recorded incident replays found")
    target = Path(destination)
    target.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for record in records:
        output = target / f"{record.fault_id}.json"
        temporary = output.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(record.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary.replace(output)
        written.append(output)
    return tuple(written)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import immutable real-incident replay projections; no Agent is run"
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_BUNDLED_REPLAYS)
    parser.add_argument("--destination", type=Path, default=DEFAULT_IMPORTED_REPLAYS)
    args = parser.parse_args()
    paths = import_replays(args.source, args.destination)
    print(json.dumps({"imported": [str(path) for path in paths]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
