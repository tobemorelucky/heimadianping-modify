"""Validated LLM plan output and versioned investigation plans."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


class PlanStep(BaseModel):
    """The single next action selected by the Planner."""

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    step_id: str = Field(min_length=1, max_length=64)
    objective: str = Field(min_length=1, max_length=500)
    hypothesis: str = Field(min_length=1, max_length=500)
    tool_name: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z][a-z0-9_]*$",
    )
    arguments: Dict[str, Any] = Field(default_factory=dict)


class PlanOutput(BaseModel):
    """Exact JSON contract returned by the Planner LLM call."""

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True, extra="forbid")

    objective: str = Field(min_length=1, max_length=500)
    hypothesis: str = Field(min_length=1, max_length=500)
    tool_name: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z][a-z0-9_]*$",
    )
    arguments: Dict[str, Any] = Field(default_factory=dict)


class InvestigationPlan(BaseModel):
    """Versioned plan containing exactly one next-action tool call."""

    model_config = ConfigDict(frozen=True)

    plan_id: str = Field(default_factory=lambda: f"plan_{uuid4().hex}")
    incident_id: str = Field(min_length=5, max_length=64)
    version: int = Field(ge=1)
    hypothesis: str = Field(min_length=1, max_length=500)
    action: PlanStep
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
