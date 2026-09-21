"""Action proposal and permission governance contracts."""

from __future__ import annotations

from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator


class PermissionLevel(str, Enum):
    READ_ONLY = "READ_ONLY"
    LOW_RISK_ACTION = "LOW_RISK_ACTION"
    HIGH_RISK_ACTION = "HIGH_RISK_ACTION"


class RiskLevel(str, Enum):
    NONE = "none"
    LOW = "low"
    HIGH = "high"


class ToolCategory(str, Enum):
    OBSERVATION = "observation"
    ACTION = "action"


class PermissionDecision(str, Enum):
    ALLOW = "ALLOW"
    REQUIRE_APPROVAL = "REQUIRE_APPROVAL"
    DENY = "DENY"


class Permission(BaseModel):
    """Normalized policy attached to a tool or proposal-only action."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    category: ToolCategory
    permission_level: PermissionLevel
    risk_level: RiskLevel
    approval_required: bool

    @model_validator(mode="after")
    def validate_policy(self) -> "Permission":
        if self.permission_level is PermissionLevel.READ_ONLY:
            if self.category is not ToolCategory.OBSERVATION:
                raise ValueError("READ_ONLY permissions require observation category")
            if self.risk_level is not RiskLevel.NONE:
                raise ValueError("READ_ONLY permissions require risk_level=none")
            if self.approval_required:
                raise ValueError("READ_ONLY permissions cannot require approval")
        elif self.permission_level is PermissionLevel.LOW_RISK_ACTION:
            if self.category is not ToolCategory.ACTION:
                raise ValueError("LOW_RISK_ACTION requires action category")
            if self.risk_level is not RiskLevel.LOW:
                raise ValueError("LOW_RISK_ACTION requires risk_level=low")
            if not self.approval_required:
                raise ValueError("LOW_RISK_ACTION must require approval")
        else:
            if self.category is not ToolCategory.ACTION:
                raise ValueError("HIGH_RISK_ACTION requires action category")
            if self.risk_level is not RiskLevel.HIGH:
                raise ValueError("HIGH_RISK_ACTION requires risk_level=high")
            if not self.approval_required:
                raise ValueError("HIGH_RISK_ACTION must require approval")
        return self


class ActionProposal(BaseModel):
    """A non-executable recommendation derived from a Diagnosis Report."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    proposal_id: str = Field(
        default_factory=lambda: f"proposal_{uuid4().hex}",
        min_length=10,
        max_length=96,
    )
    incident_id: str = Field(min_length=5, max_length=64)
    action_name: str = Field(pattern=r"^[a-z0-9._-]+$", min_length=1, max_length=128)
    reason: str = Field(min_length=1, max_length=2_000)
    evidence_refs: tuple[str, ...] = Field(min_length=1, max_length=100)
    risk_level: RiskLevel
    rollback_plan: str = Field(min_length=1, max_length=2_000)
    verification_plan: str = Field(min_length=1, max_length=2_000)
    approval_required: bool

    @model_validator(mode="after")
    def validate_evidence_refs(self) -> "ActionProposal":
        if len(set(self.evidence_refs)) != len(self.evidence_refs):
            raise ValueError("evidence_refs must be unique")
        return self


class PermissionCheck(BaseModel):
    """Auditable, non-executing Permission Gateway decision."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    subject_name: str = Field(min_length=1, max_length=128)
    permission: Permission
    decision: PermissionDecision
    reason: str = Field(min_length=1, max_length=1_000)
