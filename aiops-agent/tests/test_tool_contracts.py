"""P0 tests for dynamic tools, manifest policy, and generic Evidence."""

from datetime import datetime, timedelta, timezone

import pytest

from context.manager import ContextManager
from context.packet import ContextStage
from runtime.evidence import Evidence
from runtime.models import IncidentCreate, IncidentTask
from runtime.planner import Planner
from runtime.plans import PlanOutput
from runtime.tool_registry import ToolRegistry, ToolRegistryError
from llm.mock import MockLLMProvider
from tool_contracts import (
    EvidenceCompleteness,
    EvidenceWindow,
    ObservationStatus,
    build_failure_observation,
)


def test_plan_output_accepts_dynamic_tool_name_without_literal_constraint():
    output = PlanOutput(
        objective="Collect a future read-only signal",
        hypothesis="A generic component may be unhealthy",
        tool_name="future_readonly_tool",
        arguments={},
    )

    assert output.tool_name == "future_readonly_tool"


def test_registry_rejects_discovery_schema_drift():
    registry = ToolRegistry.from_file()
    discovered = [
        {
            "name": authorized.tool_name,
            "description": authorized.description,
            "input_schema": authorized.input_schema,
            "server": authorized.server,
        }
        for authorized in registry.manifest.tools
    ]
    discovered[0]["input_schema"] = {"type": "object", "properties": {}}

    with pytest.raises(ToolRegistryError, match="schema mismatch"):
        registry.validate_discovery(discovered)


def test_manifest_contains_required_authorization_fields():
    entry = ToolRegistry.from_file().manifest.tools[0]

    assert set(entry.model_dump()) == {
        "tool_name",
        "description",
        "input_schema",
        "permission",
        "server",
        "category",
        "permission_level",
        "risk_level",
        "approval_required",
    }
    assert entry.permission.startswith("read:")
    assert entry.permission_level.value == "READ_ONLY"
    assert entry.risk_level.value == "none"
    assert entry.category.value == "observation"
    assert entry.approval_required is False


def test_planner_returns_exactly_one_next_action():
    registry = ToolRegistry.from_file()
    incident = IncidentTask.from_create(
        IncidentCreate(title="log delay", description="inspect application logs")
    )

    plan = Planner(MockLLMProvider()).create_plan(
        incident,
        ContextManager().build([], stage=ContextStage.PLANNER),
        [tool.planner_descriptor() for tool in registry.manifest.tools],
        version=1,
    )

    assert plan.action.tool_name == "search_application_logs"
    assert not hasattr(plan, "steps")


def test_evidence_accepts_tool_agnostic_source_and_window():
    now = datetime.now(timezone.utc)
    evidence = Evidence(
        incident_id="inc_generic_evidence",
        step_id="step_generic",
        status=ObservationStatus.PARTIAL,
        kind="metric",
        source="future.component.metrics",
        source_tool="future_readonly_tool",
        collected_at=now,
        observation_window=EvidenceWindow(
            start=now - timedelta(minutes=10),
            end=now,
        ),
        summary="A generic partial observation.",
        completeness=EvidenceCompleteness.PARTIAL,
        raw_ref="artifact://future/evidence/1",
        data={"value": 1},
    )

    assert evidence.source_tool == "future_readonly_tool"
    assert evidence.observation_window is not None
    assert evidence.data == {"value": 1}


@pytest.mark.parametrize(
    "status",
    [ObservationStatus.ERROR, ObservationStatus.TIMEOUT],
)
def test_tool_failures_are_structured_observations(status):
    observation = build_failure_observation(
        source_tool="search_application_logs",
        source="mcp://hmdp-readonly-tools",
        status=status,
        error_type="TestFailure",
        message="bounded test failure",
        retryable=True,
    )

    assert observation.status is status
    assert observation.completeness is EvidenceCompleteness.UNKNOWN
    assert observation.error is not None
    assert observation.error.retryable is True
