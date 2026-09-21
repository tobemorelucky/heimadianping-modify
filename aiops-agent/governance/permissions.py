"""Fail-closed Permission Gateway without any execution path."""

from __future__ import annotations

from typing import Any, Mapping

from governance.models import (
    ActionProposal,
    Permission,
    PermissionCheck,
    PermissionDecision,
    PermissionLevel,
    RiskLevel,
    ToolCategory,
)
from governance.policies import ACTION_POLICIES, ActionPolicy


class PermissionGateway:
    """Evaluate tools and proposals; never execute either."""

    def __init__(
        self,
        action_policies: Mapping[str, ActionPolicy] | None = None,
    ) -> None:
        self.action_policies = dict(action_policies or ACTION_POLICIES)

    def check_tool(self, manifest_entry: Any) -> PermissionCheck:
        permission = Permission(
            category=ToolCategory(manifest_entry.category),
            permission_level=PermissionLevel(manifest_entry.permission_level),
            risk_level=RiskLevel(manifest_entry.risk_level),
            approval_required=bool(manifest_entry.approval_required),
        )
        if permission.permission_level is PermissionLevel.READ_ONLY:
            return PermissionCheck(
                subject_name=str(manifest_entry.tool_name),
                permission=permission,
                decision=PermissionDecision.ALLOW,
                reason="Manifest-authorized READ_ONLY observation tool.",
            )
        if permission.permission_level is PermissionLevel.HIGH_RISK_ACTION:
            return PermissionCheck(
                subject_name=str(manifest_entry.tool_name),
                permission=permission,
                decision=PermissionDecision.DENY,
                reason="HIGH_RISK_ACTION is prohibited by Phase G1 policy.",
            )
        return PermissionCheck(
            subject_name=str(manifest_entry.tool_name),
            permission=permission,
            decision=PermissionDecision.REQUIRE_APPROVAL,
            reason="LOW_RISK_ACTION requires explicit human approval.",
        )

    def check_proposal(self, proposal: ActionProposal) -> PermissionCheck:
        if proposal.risk_level is RiskLevel.HIGH:
            return self._denied_high_risk(proposal.action_name)
        policy = self.action_policies.get(proposal.action_name)
        if policy is None:
            permission = Permission(
                category=ToolCategory.ACTION,
                permission_level=PermissionLevel.LOW_RISK_ACTION,
                risk_level=RiskLevel.LOW,
                approval_required=True,
            )
            return PermissionCheck(
                subject_name=proposal.action_name,
                permission=permission,
                decision=PermissionDecision.DENY,
                reason="Action is not present in the proposal allowlist.",
            )
        permission = policy.permission
        if permission.permission_level is PermissionLevel.HIGH_RISK_ACTION:
            return self._denied_high_risk(proposal.action_name)
        if (
            proposal.risk_level is not permission.risk_level
            or proposal.approval_required != permission.approval_required
        ):
            return PermissionCheck(
                subject_name=proposal.action_name,
                permission=permission,
                decision=PermissionDecision.DENY,
                reason="Proposal risk metadata does not match the approved policy.",
            )
        return PermissionCheck(
            subject_name=proposal.action_name,
            permission=permission,
            decision=PermissionDecision.REQUIRE_APPROVAL,
            reason=(
                "Action is proposal-only and requires explicit human approval; "
                "Phase G1 has no executor."
            ),
        )

    @staticmethod
    def _denied_high_risk(action_name: str) -> PermissionCheck:
        return PermissionCheck(
            subject_name=action_name,
            permission=Permission(
                category=ToolCategory.ACTION,
                permission_level=PermissionLevel.HIGH_RISK_ACTION,
                risk_level=RiskLevel.HIGH,
                approval_required=True,
            ),
            decision=PermissionDecision.DENY,
            reason="HIGH_RISK_ACTION is prohibited and cannot be approved.",
        )
