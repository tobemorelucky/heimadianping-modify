"""Deterministic, non-executing Action Proposal generation."""

from __future__ import annotations

from governance.models import ActionProposal, RiskLevel
from runtime.reports import DiagnosisReport, DiagnosisStatus


class ProposalGenerator:
    """Map supported confirmed diagnoses to evidence-linked proposals."""

    def generate(self, report: DiagnosisReport) -> ActionProposal | None:
        """Return a proposal only for confirmed Kafka consumer-down evidence."""

        if report.status is not DiagnosisStatus.CONFIRMED:
            return None
        if not report.evidence_ids:
            return None
        diagnosis_text = " ".join(
            value for value in (report.root_cause, report.conclusion) if value
        ).casefold()
        kafka_related = "kafka" in diagnosis_text
        consumer_down = any(
            keyword in diagnosis_text
            for keyword in (
                "consumer",
                "消费者",
                "无活跃成员",
                "消费积压",
                "消费停滞",
            )
        )
        if not kafka_related or not consumer_down:
            return None
        reason = (
            "Diagnosis Report confirms a Kafka consumer availability problem. "
            f"{report.conclusion}"
        )[:2_000]
        return ActionProposal(
            incident_id=report.incident_id,
            action_name="restart_consumer",
            reason=reason,
            evidence_refs=tuple(dict.fromkeys(report.evidence_ids)),
            risk_level=RiskLevel.LOW,
            rollback_plan=(
                "No action is executed by this Agent. If an approved operator later "
                "performs the restart, restore the previous consumer process and "
                "configuration if the instance does not rejoin the group."
            ),
            verification_plan=(
                "Use READ_ONLY Kafka status to verify member_count is greater than "
                "zero and lag decreases across consecutive observation windows."
            ),
            approval_required=True,
        )
