"""Evidence contracts created from MCP observations."""

from __future__ import annotations

from pydantic import ConfigDict, Field

from tool_contracts import ToolObservation


class Evidence(ToolObservation):
    """Persistable observation with a stable identifier and source."""

    model_config = ConfigDict(frozen=True)

    incident_id: str = Field(min_length=5, max_length=64)
    step_id: str = Field(min_length=1, max_length=64)
