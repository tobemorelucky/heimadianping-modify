"""Static read-only Tool Manifest validated against MCP discovery."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field, model_validator

from governance.models import PermissionLevel, RiskLevel, ToolCategory


DEFAULT_MANIFEST_PATH = Path(__file__).resolve().parents[1] / "tool_manifest.json"


class ToolRegistryError(RuntimeError):
    """Raised when MCP discovery does not match the authorized manifest."""


ToolPermissionLevel = PermissionLevel
ToolRiskLevel = RiskLevel


class ToolManifestEntry(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    tool_name: str = Field(min_length=1, max_length=128)
    description: str = Field(min_length=1, max_length=1_000)
    input_schema: dict[str, Any]
    permission: str = Field(pattern=r"^(read|action):[a-z0-9_.-]+$")
    server: str = Field(min_length=1, max_length=128)
    category: ToolCategory
    permission_level: PermissionLevel
    risk_level: RiskLevel
    approval_required: bool

    @model_validator(mode="after")
    def validate_governance_contract(self) -> "ToolManifestEntry":
        if self.permission_level is PermissionLevel.READ_ONLY:
            if self.category is not ToolCategory.OBSERVATION:
                raise ValueError("READ_ONLY tools require observation category")
            if not self.permission.startswith("read:"):
                raise ValueError("READ_ONLY tools require a read permission")
            if self.risk_level is not RiskLevel.NONE:
                raise ValueError("READ_ONLY tools must declare risk_level=none")
            if self.approval_required:
                raise ValueError("READ_ONLY tools must not require approval")
        else:
            if self.category is not ToolCategory.ACTION:
                raise ValueError("action tools require action category")
            if not self.permission.startswith("action:"):
                raise ValueError("action tools require an action permission")
            if not self.approval_required:
                raise ValueError("action tools must require approval")
            expected_risk = (
                RiskLevel.LOW
                if self.permission_level is PermissionLevel.LOW_RISK_ACTION
                else RiskLevel.HIGH
            )
            if self.risk_level is not expected_risk:
                raise ValueError(
                    f"{self.permission_level.value} tools require "
                    f"risk_level={expected_risk.value}"
                )
        return self

    def planner_descriptor(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class ToolManifest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    version: int = Field(ge=1)
    tools: tuple[ToolManifestEntry, ...] = Field(min_length=1)


class ToolRegistry:
    """Authorize only tools whose discovery contract matches the local manifest."""

    def __init__(self, manifest: ToolManifest) -> None:
        names = [tool.tool_name for tool in manifest.tools]
        if len(names) != len(set(names)):
            raise ToolRegistryError("tool manifest contains duplicate tool names")
        self.manifest = manifest
        self._by_name = {tool.tool_name: tool for tool in manifest.tools}

    @classmethod
    def from_file(cls, path: Path | str = DEFAULT_MANIFEST_PATH) -> "ToolRegistry":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(ToolManifest.model_validate(payload))

    def validate_discovery(
        self,
        discovered_tools: Sequence[Mapping[str, Any]],
    ) -> list[ToolManifestEntry]:
        discovered_names = [str(tool.get("name", "")) for tool in discovered_tools]
        if len(discovered_names) != len(set(discovered_names)):
            raise ToolRegistryError("MCP discovery contains duplicate tool names")
        discovered_by_name = {
            str(tool.get("name", "")): tool for tool in discovered_tools
        }
        if set(discovered_by_name) != set(self._by_name):
            raise ToolRegistryError(
                "MCP discovered tool set does not match the authorized manifest"
            )

        for name, authorized in self._by_name.items():
            discovered = discovered_by_name[name]
            if discovered.get("server") != authorized.server:
                raise ToolRegistryError(f"MCP server mismatch for tool: {name}")
            if discovered.get("description") != authorized.description:
                raise ToolRegistryError(f"MCP description mismatch for tool: {name}")
            if discovered.get("input_schema") != authorized.input_schema:
                raise ToolRegistryError(f"MCP input schema mismatch for tool: {name}")
        return list(self.manifest.tools)

    def require_authorized(self, tool_name: str) -> ToolManifestEntry:
        try:
            return self._by_name[tool_name]
        except KeyError as exc:
            raise ToolRegistryError(f"tool is not authorized: {tool_name}") from exc

    def require_read_only(self, tool_name: str) -> ToolManifestEntry:
        """Authorize one tool only when it is explicitly declared read-only."""

        tool = self.require_authorized(tool_name)
        if tool.permission_level is not PermissionLevel.READ_ONLY:
            raise ToolRegistryError(f"tool is not READ_ONLY: {tool_name}")
        return tool
