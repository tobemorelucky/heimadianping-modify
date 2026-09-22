"""AnomalySignal-to-Incident orchestration using the existing Agent Runtime."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Callable, Protocol

from governance.permissions import PermissionGateway
from governance.proposal import ProposalGenerator
from incident.dedup import IncidentDeduplicator
from incident.models import (
    DiagnosisStatus,
    Incident,
    IncidentHandlingResult,
    IncidentSeverity,
    IncidentStatus,
    IncidentTraceEvent,
    utc_now,
)
from incident.store import IncidentStore
from monitoring.detector.models import AnomalySignal
from runtime.models import (
    IncidentCreate,
    IncidentSeverity as RuntimeSeverity,
    IncidentSource,
    ObservationWindow,
)
from runtime.reports import DiagnosisRunResult
from trace.models import TraceEventType


class DiagnosisRuntime(Protocol):
    """Narrow adapter implemented by RuntimeOrchestrator."""

    def run(
        self,
        request: IncidentCreate,
        *,
        incident_id: str | None = None,
    ) -> DiagnosisRunResult: ...


class IncidentManager:
    """Create, deduplicate, and synchronously diagnose proactive Incidents."""

    def __init__(
        self,
        store: IncidentStore,
        diagnosis_runtime: DiagnosisRuntime,
        *,
        deduplicator: IncidentDeduplicator | None = None,
        proposal_generator: ProposalGenerator | None = None,
        permission_gateway: PermissionGateway | None = None,
        clock: Callable = utc_now,
    ) -> None:
        self.store = store
        self.diagnosis_runtime = diagnosis_runtime
        self.deduplicator = deduplicator or IncidentDeduplicator(store)
        self.proposal_generator = proposal_generator
        self.permission_gateway = permission_gateway
        if (proposal_generator is None) != (permission_gateway is None):
            raise ValueError(
                "proposal_generator and permission_gateway must be configured together"
            )
        self.clock = clock

    def handle_signal(self, signal: AnomalySignal) -> IncidentHandlingResult:
        """Merge a duplicate or create and diagnose one new Incident."""

        duplicate = self.deduplicator.find_duplicate(signal)
        if duplicate is not None:
            merged = self.store.merge_signal(
                duplicate.incident_id,
                signal_id=signal.signal_id,
                signal_seen_at=signal.last_seen,
                updated_at=self.clock(),
            )
            return IncidentHandlingResult(
                incident=merged,
                created=False,
                diagnosis_triggered=False,
            )

        now = self.clock()
        incident = Incident(
            title=self._title(signal),
            severity=IncidentSeverity(signal.severity.value),
            status=IncidentStatus.ACTIVE,
            diagnosis_status=DiagnosisStatus.PENDING,
            trigger_signal_ids=(signal.signal_id,),
            fingerprint=signal.fingerprint,
            last_signal_at=signal.last_seen,
            created_at=now,
            updated_at=now,
        )
        self.store.create(incident)
        self._trace(
            incident.incident_id,
            TraceEventType.INCIDENT_CREATED,
            "Created proactive Incident from anomaly signal.",
            {
                "signal_id": signal.signal_id,
                "rule_id": signal.rule_id,
                "fingerprint": signal.fingerprint,
                "severity": signal.severity.value,
            },
        )
        return self._diagnose(incident, signal)

    def _diagnose(
        self,
        incident: Incident,
        signal: AnomalySignal,
    ) -> IncidentHandlingResult:
        diagnosing = self.store.update_diagnosis_status(
            incident.incident_id,
            DiagnosisStatus.RUNNING,
            updated_at=self.clock(),
        )
        self._trace(
            incident.incident_id,
            TraceEventType.DIAGNOSIS_STARTED,
            "Started the existing Agent Runtime for proactive diagnosis.",
            {
                "signal_id": signal.signal_id,
                "runtime_incident_id": incident.incident_id,
            },
        )
        try:
            result = self.diagnosis_runtime.run(
                self._runtime_request(diagnosing, signal),
                incident_id=incident.incident_id,
            )
            completed = self.store.update_diagnosis_status(
                incident.incident_id,
                DiagnosisStatus.COMPLETED,
                updated_at=self.clock(),
                diagnosis_report_id=result.report.report_id,
            )
            self._trace(
                incident.incident_id,
                TraceEventType.DIAGNOSIS_COMPLETED,
                "Agent Runtime completed proactive diagnosis.",
                {
                    "outcome": "success",
                    "report_id": result.report.report_id,
                    "diagnosis_status": DiagnosisStatus.COMPLETED.value,
                    "report_status": result.report.status.value,
                    "root_cause": result.report.root_cause,
                },
            )
            proposal_id, permission_decision = self._govern(result)
            return IncidentHandlingResult(
                incident=completed,
                created=True,
                diagnosis_triggered=True,
                proposal_id=proposal_id,
                permission_decision=permission_decision,
            )
        except Exception as exc:
            failed = self.store.update_diagnosis_status(
                incident.incident_id,
                DiagnosisStatus.FAILED,
                updated_at=self.clock(),
            )
            error_message = str(exc).strip() or type(exc).__name__
            self._trace(
                incident.incident_id,
                TraceEventType.DIAGNOSIS_COMPLETED,
                "Agent Runtime failed proactive diagnosis.",
                {
                    "outcome": "failed",
                    "error_type": type(exc).__name__,
                    "error": error_message[:500],
                },
            )
            return IncidentHandlingResult(
                incident=failed,
                created=True,
                diagnosis_triggered=True,
                diagnosis_error=error_message[:500],
            )

    def confirm_recovery(
        self,
        *,
        fingerprint: str,
        observation_refs: tuple[str, ...],
        first_seen: datetime,
        last_seen: datetime,
    ) -> Incident | None:
        """Close the fault only after later, matching healthy observation windows."""

        incident = self.store.latest_for_fingerprint(fingerprint)
        if (
            incident is None
            or incident.status is not IncidentStatus.ACTIVE
            or first_seen <= incident.last_signal_at
        ):
            return None
        recovered = self.store.update_status(
            incident.incident_id,
            IncidentStatus.RECOVERED,
            expected_status=IncidentStatus.ACTIVE,
            updated_at=self.clock(),
        )
        self._trace(
            incident.incident_id,
            TraceEventType.INCIDENT_RECOVERED,
            "Consecutive read-only observations confirmed fault recovery.",
            {
                "observation_refs": list(observation_refs),
                "first_seen": first_seen.isoformat(),
                "last_seen": last_seen.isoformat(),
            },
        )
        return recovered

    def _govern(self, result: DiagnosisRunResult) -> tuple[str | None, str | None]:
        if self.proposal_generator is None or self.permission_gateway is None:
            return None, None
        proposal = None
        try:
            proposal = self.proposal_generator.generate(result.report)
            if proposal is None:
                return None, None
            self._trace(
                result.incident_id,
                TraceEventType.ACTION_PROPOSAL_CREATED,
                "Created a non-executable Action Proposal from Diagnosis Report.",
                {
                    "proposal_id": proposal.proposal_id,
                    "action_name": proposal.action_name,
                    "risk_level": proposal.risk_level.value,
                    "approval_required": proposal.approval_required,
                    "evidence_refs": list(proposal.evidence_refs),
                },
            )
            permission = self.permission_gateway.check_proposal(proposal)
            self._trace(
                result.incident_id,
                TraceEventType.PERMISSION_CHECKED,
                "Permission Gateway evaluated the Action Proposal.",
                {
                    "proposal_id": proposal.proposal_id,
                    "action_name": proposal.action_name,
                    "decision": permission.decision.value,
                    "permission_level": permission.permission.permission_level.value,
                    "risk_level": permission.permission.risk_level.value,
                    "approval_required": permission.permission.approval_required,
                    "reason": permission.reason,
                },
            )
            return proposal.proposal_id, permission.decision.value
        except Exception as exc:
            self._trace(
                result.incident_id,
                TraceEventType.PERMISSION_CHECKED,
                "Governance failed closed; no action can proceed.",
                {
                    "proposal_id": (
                        proposal.proposal_id if proposal is not None else None
                    ),
                    "decision": "DENY",
                    "error_type": type(exc).__name__,
                    "error": (str(exc).strip() or type(exc).__name__)[:500],
                },
            )
            return (
                proposal.proposal_id if proposal is not None else None,
                "DENY",
            )

    def _runtime_request(
        self,
        incident: Incident,
        signal: AnomalySignal,
    ) -> IncidentCreate:
        end = signal.last_seen
        if end <= signal.first_seen:
            end = signal.first_seen + timedelta(microseconds=1)
        description = (
            "Monitoring detected a deterministic anomaly. "
            f"rule_id={signal.rule_id}; fingerprint={signal.fingerprint}; "
            f"facts={json.dumps(signal.facts, ensure_ascii=False, sort_keys=True)}; "
            f"observation_refs={list(signal.observation_refs)}. "
            "Collect current read-only evidence and independently verify the cause."
        )
        return IncidentCreate(
            title=incident.title,
            description=description,
            source=IncidentSource.ALERT,
            severity=RuntimeSeverity(incident.severity.value),
            affected_components=["kafka"],
            observation_window=ObservationWindow(
                start=signal.first_seen,
                end=end,
            ),
        )

    @staticmethod
    def _title(signal: AnomalySignal) -> str:
        topic = str(signal.facts.get("topic", "unknown-topic"))
        group = str(signal.facts.get("consumer_group", "unknown-group"))
        return f"Kafka consumer anomaly: {topic} / {group}"[:200]

    def _trace(
        self,
        incident_id: str,
        event_type: TraceEventType,
        summary: str,
        payload: dict,
    ) -> None:
        self.store.save_trace_event(
            IncidentTraceEvent(
                incident_id=incident_id,
                event_type=event_type,
                summary=summary,
                payload=payload,
                created_at=self.clock(),
            )
        )
