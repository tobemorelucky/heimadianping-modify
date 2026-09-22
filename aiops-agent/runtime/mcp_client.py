"""Synchronous Runtime adapter for the official MCP stdio client."""

from __future__ import annotations

import os
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Protocol

import anyio
from mcp import Client, StdioServerParameters

from tool_contracts import (
    ObservationStatus,
    ToolObservation,
    build_failure_observation,
)


AGENT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SERVER_SCRIPT = AGENT_ROOT / "mcp" / "server.py"


class MCPClientError(RuntimeError):
    """Base error for MCP startup, protocol, and tool failures."""


class MCPToolTimeoutError(MCPClientError):
    """Raised when MCP control-plane discovery exceeds its configured timeout."""


class MCPToolCallError(MCPClientError):
    """Raised for local policy violations before an MCP tool call."""


class MCPClientProtocol(Protocol):
    """Narrow interface consumed by the synchronous Runtime."""

    def get_tool_schemas(self) -> list[dict[str, Any]]: ...

    def call(self, tool_name: str, arguments: Mapping[str, Any] | None = None) -> dict[str, Any]: ...


class StdioMCPClient:
    """Launch the independent MCP server and communicate only over stdio."""

    def __init__(
        self,
        *,
        project_root: Path | str,
        server_script: Path | str = DEFAULT_SERVER_SCRIPT,
        timeout_seconds: float = 10.0,
        python_executable: Path | str = sys.executable,
        kafka_bootstrap_servers: str | None = None,
        kafka_default_topic: str | None = None,
        kafka_default_consumer_group: str | None = None,
        kafka_request_timeout_ms: int | None = None,
        kafka_lag_threshold: int | None = None,
        mysql_health_port: int | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.project_root = Path(project_root).resolve()
        self.server_script = Path(server_script).resolve()
        self.timeout_seconds = timeout_seconds
        self.python_executable = str(Path(python_executable).resolve())
        self.kafka_bootstrap_servers = kafka_bootstrap_servers or os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS",
            "127.0.0.1:9092",
        )
        self.kafka_default_topic = kafka_default_topic or os.environ.get(
            "KAFKA_VOUCHER_ORDER_TOPIC",
            "hmdp.seckill.order.create.v1",
        )
        self.kafka_default_consumer_group = (
            kafka_default_consumer_group
            or os.environ.get(
                "KAFKA_CONSUMER_GROUP",
                "hmdp-seckill-order-create-v1",
            )
        )
        self.kafka_request_timeout_ms = kafka_request_timeout_ms or int(
            os.environ.get("AIOPS_KAFKA_REQUEST_TIMEOUT_MS", "3000")
        )
        self.kafka_lag_threshold = (
            kafka_lag_threshold
            if kafka_lag_threshold is not None
            else int(os.environ.get("AIOPS_KAFKA_LAG_THRESHOLD", "1000"))
        )
        self.mysql_health_port = (
            mysql_health_port
            if mysql_health_port is not None
            else int(os.environ.get("AIOPS_MYSQL_HEALTH_PORT", "18082"))
        )
        if not 1 <= self.mysql_health_port <= 65535:
            raise ValueError("AIOPS_MYSQL_HEALTH_PORT is invalid")
        self._tool_schemas: list[dict[str, Any]] | None = None

    def get_tool_schemas(self) -> list[dict[str, Any]]:
        if self._tool_schemas is None:
            self._tool_schemas = anyio.run(self._list_tools)
        return deepcopy(self._tool_schemas)

    def call(
        self,
        tool_name: str,
        arguments: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        available = {tool["name"] for tool in self.get_tool_schemas()}
        if tool_name not in available:
            raise MCPToolCallError(f"MCP tool is not registered: {tool_name}")
        return anyio.run(self._call_tool, tool_name, dict(arguments or {}))

    def _server_parameters(self) -> StdioServerParameters:
        if not self.server_script.is_file():
            raise MCPClientError(f"MCP server script not found: {self.server_script}")
        return StdioServerParameters(
            command=self.python_executable,
            args=[str(self.server_script)],
            cwd=AGENT_ROOT,
            env={
                "HMDP_PROJECT_ROOT": str(self.project_root),
                "KAFKA_BOOTSTRAP_SERVERS": self.kafka_bootstrap_servers,
                "KAFKA_VOUCHER_ORDER_TOPIC": self.kafka_default_topic,
                "KAFKA_CONSUMER_GROUP": self.kafka_default_consumer_group,
                "AIOPS_KAFKA_REQUEST_TIMEOUT_MS": str(
                    self.kafka_request_timeout_ms
                ),
                "AIOPS_KAFKA_LAG_THRESHOLD": str(self.kafka_lag_threshold),
                "AIOPS_MYSQL_HEALTH_PORT": str(self.mysql_health_port),
            },
        )

    async def _list_tools(self) -> list[dict[str, Any]]:
        try:
            with anyio.fail_after(self.timeout_seconds):
                async with Client(
                    self._server_parameters(),
                    read_timeout_seconds=self.timeout_seconds,
                ) as client:
                    result = await client.list_tools()
                    server_info = client.server_info
                    server_name = (
                        server_info.name
                        if server_info is not None
                        else "unknown-mcp-server"
                    )
            return [
                {
                    "name": tool.name,
                    "description": tool.description or "",
                    "input_schema": tool.input_schema,
                    "server": server_name,
                }
                for tool in result.tools
            ]
        except TimeoutError as exc:
            raise MCPToolTimeoutError("MCP tool listing timed out") from exc
        except MCPClientError:
            raise
        except Exception as exc:
            raise MCPClientError("MCP server startup or tool listing failed") from exc

    async def _call_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            with anyio.fail_after(self.timeout_seconds):
                async with Client(
                    self._server_parameters(),
                    read_timeout_seconds=self.timeout_seconds,
                ) as client:
                    result = await client.call_tool(
                        tool_name,
                        arguments,
                        read_timeout_seconds=self.timeout_seconds,
                    )
        except TimeoutError as exc:
            return self._failure_observation(
                tool_name,
                status=ObservationStatus.TIMEOUT,
                error_type=type(exc).__name__,
                message=f"MCP tool timed out: {tool_name}",
                retryable=True,
            )
        except MCPClientError:
            raise
        except Exception as exc:
            return self._failure_observation(
                tool_name,
                status=ObservationStatus.ERROR,
                error_type=type(exc).__name__,
                message=f"MCP tool transport failed: {tool_name}",
                retryable=True,
            )

        if result.is_error:
            return self._failure_observation(
                tool_name,
                status=ObservationStatus.ERROR,
                error_type="MCPToolError",
                message=f"MCP tool returned an error: {tool_name}",
                retryable=False,
            )
        if not isinstance(result.structured_content, dict):
            return self._failure_observation(
                tool_name,
                status=ObservationStatus.ERROR,
                error_type="InvalidToolResult",
                message=f"MCP tool returned no structured evidence: {tool_name}",
                retryable=False,
            )

        try:
            observation = ToolObservation.model_validate(result.structured_content)
        except Exception as exc:
            return self._failure_observation(
                tool_name,
                status=ObservationStatus.ERROR,
                error_type=type(exc).__name__,
                message=f"MCP tool returned an invalid observation: {tool_name}",
                retryable=False,
            )
        if observation.source_tool != tool_name:
            return self._failure_observation(
                tool_name,
                status=ObservationStatus.ERROR,
                error_type="ToolIdentityMismatch",
                message="MCP observation source_tool does not match the requested tool",
                retryable=False,
            )
        return observation.model_dump(mode="json")

    def _failure_observation(
        self,
        tool_name: str,
        *,
        status: ObservationStatus,
        error_type: str,
        message: str,
        retryable: bool,
    ) -> dict[str, Any]:
        server = next(
            (
                tool["server"]
                for tool in self._tool_schemas or []
                if tool["name"] == tool_name
            ),
            "unknown-mcp-server",
        )
        observation = build_failure_observation(
            source_tool=tool_name,
            source=f"mcp://{server}",
            status=status,
            error_type=error_type,
            message=message,
            retryable=retryable,
        )
        return observation.model_dump(mode="json")
