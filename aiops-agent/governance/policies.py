"""Static proposal-only action policies for Phase G1."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from governance.models import Permission, PermissionLevel, RiskLevel, ToolCategory


class ActionPolicy(BaseModel):
    """Allowlist metadata; it does not expose or implement an executor."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    action_name: str = Field(pattern=r"^[a-z0-9._-]+$", min_length=1, max_length=128)
    description: str = Field(min_length=1, max_length=1_000)
    permission: Permission


RESTART_CONSUMER_POLICY = ActionPolicy(
    action_name="restart_consumer",
    description=(
        "Proposal-only recommendation to restart one identified Kafka consumer; "
        "Phase G1 contains no executor or service restart implementation."
    ),
    permission=Permission(
        category=ToolCategory.ACTION,
        permission_level=PermissionLevel.LOW_RISK_ACTION,
        risk_level=RiskLevel.LOW,
        approval_required=True,
    ),
)


ACTION_POLICIES: dict[str, ActionPolicy] = {
    RESTART_CONSUMER_POLICY.action_name: RESTART_CONSUMER_POLICY,
}
