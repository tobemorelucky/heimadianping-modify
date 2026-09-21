"""Phase G1 proposal generation and fail-closed permission tests."""

from governance.models import (
    ActionProposal,
    PermissionDecision,
    RiskLevel,
)
from governance.permissions import PermissionGateway
from governance.proposal import ProposalGenerator
from runtime.reports import DiagnosisReport, DiagnosisStatus
from runtime.tool_registry import ToolRegistry


def kafka_report() -> DiagnosisReport:
    return DiagnosisReport(
        report_id="report_governance_kafka",
        incident_id="inc_governance_kafka",
        status=DiagnosisStatus.CONFIRMED,
        title="Kafka consumer diagnosis",
        conclusion=(
            "Kafka consumer group has no active members and accumulated lag."
        ),
        root_cause="Kafka consumer unavailable",
        confidence=0.95,
        evidence_ids=("evi_kafka_status", "evi_consumer_log"),
    )


def test_read_only_tool_is_automatically_allowed():
    registry = ToolRegistry.from_file()
    kafka_tool = registry.require_read_only("get_kafka_status")

    result = PermissionGateway().check_tool(kafka_tool)

    assert result.decision is PermissionDecision.ALLOW
    assert result.permission.permission_level.value == "READ_ONLY"
    assert result.permission.risk_level is RiskLevel.NONE
    assert result.permission.approval_required is False


def test_low_risk_action_requires_approval():
    proposal = ProposalGenerator().generate(kafka_report())
    assert proposal is not None

    result = PermissionGateway().check_proposal(proposal)

    assert proposal.action_name == "restart_consumer"
    assert proposal.risk_level is RiskLevel.LOW
    assert result.decision is PermissionDecision.REQUIRE_APPROVAL
    assert result.permission.approval_required is True


def test_high_risk_action_is_denied_even_when_marked_for_approval():
    proposal = ActionProposal(
        incident_id="inc_governance_high_risk",
        action_name="delete_data",
        reason="Synthetic policy test only; this action is not implemented.",
        evidence_refs=("evi_policy_test",),
        risk_level=RiskLevel.HIGH,
        rollback_plan="No rollback is possible.",
        verification_plan="No execution is permitted.",
        approval_required=True,
    )

    result = PermissionGateway().check_proposal(proposal)

    assert result.decision is PermissionDecision.DENY
    assert result.permission.permission_level.value == "HIGH_RISK_ACTION"
    assert "prohibited" in result.reason


def test_proposal_preserves_diagnosis_evidence_references():
    report = kafka_report()

    proposal = ProposalGenerator().generate(report)

    assert proposal is not None
    assert proposal.incident_id == report.incident_id
    assert proposal.evidence_refs == tuple(report.evidence_ids)
    assert proposal.rollback_plan
    assert proposal.verification_plan
    assert proposal.approval_required is True
