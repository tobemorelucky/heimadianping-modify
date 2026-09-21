"""Governed, read-only execution of existing MCP observation tools."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from time import perf_counter
from typing import Callable

from monitoring.models import (
    CollectionResult,
    CollectionRun,
    CollectionSchedule,
    MonitoringTraceEvent,
    utc_now,
)
from monitoring.observation_store import ObservationStore
from runtime.mcp_client import MCPClientProtocol
from runtime.tool_registry import ToolRegistry
from tool_contracts import (
    ObservationStatus,
    ToolObservation,
    build_failure_observation,
)
from trace.models import TraceEventType


class MonitoringCollector:
    """Call one manifest-authorized READ_ONLY tool and persist its observation."""

    def __init__(
        self,
        mcp_client: MCPClientProtocol,
        tool_registry: ToolRegistry,
        observation_store: ObservationStore,
        *,
        clock: Callable = utc_now,
    ) -> None:
        self.mcp_client = mcp_client
        self.tool_registry = tool_registry
        self.observation_store = observation_store
        self.clock = clock

    def collect(
        self,
        schedule: CollectionSchedule,
        *,
        collection_run_id: str,
    ) -> CollectionResult:
        """Collect one observation, including structured policy and tool failures."""

        started_at = perf_counter()
        try:
            authorized = self.tool_registry.require_read_only(schedule.tool_name)
            discovered = self.mcp_client.get_tool_schemas()
            validated = self.tool_registry.validate_discovery(discovered)
            if authorized.tool_name not in {tool.tool_name for tool in validated}:
                raise ValueError(
                    f"authorized tool was not discovered: {authorized.tool_name}"
                )
            raw_observation = self._call_with_timeout(schedule)
            observation = ToolObservation.model_validate(raw_observation)
            if observation.source_tool != schedule.tool_name:
                raise ValueError(
                    "MCP observation source_tool does not match the scheduled tool"
                )
        except FutureTimeoutError:
            observation = build_failure_observation(
                source_tool=schedule.tool_name,
                source="monitoring://collector",
                status=ObservationStatus.TIMEOUT,
                error_type="CollectionTimeout",
                message=f"Monitoring collection timed out: {schedule.tool_name}",
                retryable=True,
            )
        except Exception as exc:
            observation = build_failure_observation(
                source_tool=schedule.tool_name,
                source="monitoring://collector",
                status=ObservationStatus.ERROR,
                error_type=type(exc).__name__,
                message=f"Monitoring collection failed: {str(exc)[:400]}",
                retryable=False,
            )

        completed_at = self.clock()
        duration_ms = max(0, round((perf_counter() - started_at) * 1000))
        run = CollectionRun(
            collection_run_id=collection_run_id,
            schedule_id=schedule.schedule_id,
            tool=schedule.tool_name,
            status=observation.status,
            timestamp=completed_at,
            evidence_ref=observation.evidence_id,
            duration_ms=duration_ms,
        )
        result = CollectionResult(run=run, observation=observation)
        self.observation_store.save_collection_run(run, observation)
        self.observation_store.save_trace_event(
            MonitoringTraceEvent(
                schedule_id=schedule.schedule_id,
                collection_run_id=collection_run_id,
                event_type=TraceEventType.OBSERVATION_COLLECTED,
                summary=(
                    f"Collected {observation.status.value} observation from "
                    f"{schedule.tool_name}."
                ),
                payload={
                    "tool_name": schedule.tool_name,
                    "status": observation.status.value,
                    "evidence_ref": observation.evidence_id,
                    "kind": observation.kind,
                    "completeness": observation.completeness.value,
                    "duration_ms": duration_ms,
                },
                created_at=completed_at,
            )
        )
        return result

    def _call_with_timeout(self, schedule: CollectionSchedule) -> dict:
        executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="monitoring-collector",
        )
        future = executor.submit(
            self.mcp_client.call,
            schedule.tool_name,
            dict(schedule.arguments),
        )
        try:
            return future.result(timeout=schedule.timeout_seconds)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
