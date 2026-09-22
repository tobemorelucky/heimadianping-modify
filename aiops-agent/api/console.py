"""Read-only projections for the independent AIOps Console.

The console never opens SQLite itself. This adapter intentionally returns bounded,
display-safe evidence facts instead of raw tool observations or log lines.
"""

from __future__ import annotations

import json
from typing import Any

from memory.database import SQLiteDatabase


TRACE_PAYLOAD_FIELDS = {
    "skill_id", "version", "tool_name", "status", "evidence_id",
    "source_tool", "hypothesis_id", "hypothesis", "confidence",
    "decision", "reflection_status", "supporting_evidence_refs",
    "contradicting_evidence_refs", "proposal_id", "action_name",
    "risk_level", "approval_required", "evidence_refs", "reason",
    "report_id", "outcome", "error_type", "error", "root_cause",
    "diagnosis_status", "report_status", "observation_refs", "first_seen", "last_seen",
}
EVENT_NAMES = {
    "tool_started": "tool_called",
    "evidence_added": "evidence_created",
}


def _json(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return fallback


class ConsoleReader:
    """Compose persisted Runtime, Monitoring, and Incident Manager records."""

    def __init__(self, database: SQLiteDatabase) -> None:
        self.database = database

    @staticmethod
    def _tables(connection: Any) -> set[str]:
        return set(SQLiteDatabase.list_tables(connection))

    def list_incidents(self, *, limit: int | None = 50) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            tables = self._tables(connection)
            managed = {}
            if "managed_incidents" in tables:
                managed = {
                    row["incident_id"]: dict(row)
                    for row in connection.execute("SELECT * FROM managed_incidents")
                }
            runtime = {
                row["incident_id"]: dict(row)
                for row in connection.execute("SELECT * FROM incidents")
            }
            demo_labels: dict[str, str] = {}
            if "anomaly_signals" in tables:
                for incident_id, lifecycle in managed.items():
                    signal_ids = _json(lifecycle["trigger_signal_ids_json"], [])
                    if not signal_ids:
                        continue
                    signal = connection.execute(
                        "SELECT facts_json FROM anomaly_signals WHERE signal_id = ?",
                        (signal_ids[0],),
                    ).fetchone()
                    if signal:
                        label = _json(signal["facts_json"], {}).get("scenario")
                        if isinstance(label, str) and label:
                            demo_labels[incident_id] = label
        combined = []
        for incident_id in set(runtime) | set(managed):
            task = runtime.get(incident_id)
            lifecycle = managed.get(incident_id)
            primary = lifecycle or task
            combined.append({
                "incident_id": incident_id,
                "title": primary["title"],
                "demo_scenario": demo_labels.get(incident_id),
                "severity": primary["severity"],
                "status": lifecycle["status"] if lifecycle else task["status"],
                "diagnosis_status": lifecycle["diagnosis_status"] if lifecycle else None,
                "runtime_status": task["status"] if task else None,
                "source": task["source"] if task else "alert",
                "created_at": primary["created_at"],
                "updated_at": primary["updated_at"],
                "trigger_signal_ids": _json(
                    lifecycle["trigger_signal_ids_json"], []
                ) if lifecycle else [],
            })
        combined.sort(key=lambda item: (item["created_at"], item["incident_id"]), reverse=True)
        return combined[:limit] if limit is not None else combined

    def monitoring_summary(self) -> dict[str, Any]:
        incidents = self.list_incidents(limit=None)
        active_count = sum(
            item["status"] == "ACTIVE"
            for item in incidents
        )
        latest = None
        with self.database.connect() as connection:
            if "monitoring_collection_runs" in self._tables(connection):
                row = connection.execute(
                    """SELECT timestamp, status, tool, schedule_id FROM monitoring_collection_runs
                    ORDER BY timestamp DESC, rowid DESC LIMIT 1"""
                ).fetchone()
                if row:
                    latest = dict(row)
        return {
            "health": "attention" if active_count else "healthy",
            "active_incident_count": active_count,
            "last_inspection_at": latest["timestamp"] if latest else None,
            "last_inspection_status": latest["status"] if latest else None,
            "last_inspection_tool": latest["tool"] if latest else None,
            "inspection_mode": (
                "fixture" if latest and latest["schedule_id"].startswith("console-demo-")
                else "live" if latest else None
            ),
        }

    def get_incident(self, incident_id: str) -> dict[str, Any] | None:
        summary = next(
            (item for item in self.list_incidents(limit=None) if item["incident_id"] == incident_id),
            None,
        )
        if summary is None:
            return None
        with self.database.connect() as connection:
            tables = self._tables(connection)
            traces: list[dict[str, Any]] = []
            if "managed_incidents" in tables:
                managed_row = connection.execute(
                    "SELECT * FROM managed_incidents WHERE incident_id = ?", (incident_id,)
                ).fetchone()
            else:
                managed_row = None
            if managed_row and "anomaly_signals" in tables:
                demo_scenario = None
                demo_fault_id = None
                for signal_id in _json(managed_row["trigger_signal_ids_json"], []):
                    signal = connection.execute(
                        "SELECT * FROM anomaly_signals WHERE signal_id = ?", (signal_id,)
                    ).fetchone()
                    if signal:
                        facts = _json(signal["facts_json"], {})
                        demo_scenario = facts.get("scenario") or demo_scenario
                        demo_fault_id = facts.get("fault_id") or demo_fault_id
                        traces.append({
                            "event_id": f"signal:{signal_id}",
                            "event_type": "anomaly_detected",
                            "raw_event_type": "anomaly_detected",
                            "source": "anomaly_signal",
                            "stage": "detector",
                            "summary": f"{signal['rule_id']} detected an anomaly.",
                            "payload": {"signal_id": signal_id, "severity": signal["severity"]},
                            "created_at": signal["created_at"],
                            "sequence": -1,
                        })
            else:
                demo_scenario = None
                demo_fault_id = None
            if "incident_manager_trace_events" in tables:
                rows = connection.execute(
                    """SELECT * FROM incident_manager_trace_events
                    WHERE incident_id = ? ORDER BY created_at, rowid""",
                    (incident_id,),
                ).fetchall()
                traces.extend(self._trace(row, "incident_manager", index) for index, row in enumerate(rows))
            rows = connection.execute(
                "SELECT * FROM trace_events WHERE incident_id = ? ORDER BY sequence",
                (incident_id,),
            ).fetchall()
            traces.extend(self._trace(row, "runtime", row["sequence"]) for row in rows)
            # The manager and runtime use independent append-only Trace streams.
            # Compose by lifecycle first, then each stream's persisted sequence;
            # clocks from fixture signals need not match the wall clock of Runtime.
            def trace_order(item: dict[str, Any]) -> tuple[int, int]:
                if item["source"] == "anomaly_signal":
                    return (0, item["sequence"])
                if item["source"] == "runtime":
                    return (2, item["sequence"])
                early = item["event_type"] in {"incident_created", "diagnosis_started"}
                return (1 if early else 3, item["sequence"])

            traces.sort(key=trace_order)
            evidence_rows = connection.execute(
                "SELECT * FROM evidence WHERE incident_id = ? ORDER BY collected_at, rowid",
                (incident_id,),
            ).fetchall()
            hypothesis_rows = connection.execute(
                "SELECT * FROM hypotheses WHERE incident_id = ? ORDER BY created_at, id",
                (incident_id,),
            ).fetchall()
            report_row = connection.execute(
                "SELECT * FROM reports WHERE incident_id = ?", (incident_id,)
            ).fetchone()
        report = self._report(report_row) if report_row else None
        return {
            **summary,
            "description": self._description(incident_id),
            "demo_scenario": demo_scenario,
            "demo_fault_id": demo_fault_id,
            "traces": traces,
            "evidence": [self._evidence(row) for row in evidence_rows],
            "hypotheses": [self._hypothesis(row, traces) for row in hypothesis_rows],
            "report": report,
            "proposals": self._proposals(traces, report, incident_id),
        }

    def _description(self, incident_id: str) -> str | None:
        row = self.database.get_incident(incident_id)
        return row["description"] if row else None

    @staticmethod
    def _trace(row: Any, source: str, sequence: int) -> dict[str, Any]:
        raw_type = row["event_type"]
        payload = _json(row["payload_json"], {})
        return {
            "event_id": row["event_id"],
            "event_type": EVENT_NAMES.get(raw_type, raw_type),
            "raw_event_type": raw_type,
            "source": source,
            "stage": row["stage"] if source == "runtime" else "incident_manager",
            "summary": row["summary"],
            "payload": {key: value for key, value in payload.items() if key in TRACE_PAYLOAD_FIELDS},
            "created_at": row["created_at"],
            "sequence": sequence,
        }

    @staticmethod
    def _evidence(row: Any) -> dict[str, Any]:
        data = _json(row["observation_json"], {})
        facts = []
        if row["kind"] == "kafka_consumer_status":
            for key in ("topic", "consumer_group", "consumer_status", "member_count", "total_lag", "lag_status"):
                if key in data and isinstance(data[key], (str, int, float, bool)):
                    facts.append(f"{key}: {data[key]}")
        elif row["kind"] == "business_metrics":
            for key, value in data.items():
                if isinstance(value, (int, float, bool)) and len(facts) < 6:
                    facts.append(f"{key}: {value}")
        return {
            "evidence_id": row["evidence_id"],
            "kind": row["kind"],
            "source": row["source"],
            "source_tool": row["source_tool"],
            "status": row["status"],
            "summary": row["summary"],
            "facts": facts,
            "completeness": row["completeness"],
            "collected_at": row["collected_at"],
        }

    @staticmethod
    def _hypothesis(row: Any, traces: list[dict[str, Any]]) -> dict[str, Any]:
        history = [
            {
                "created_at": trace["created_at"],
                "status": trace["payload"].get("status"),
                "confidence": trace["payload"].get("confidence"),
                "event_type": trace["event_type"],
            }
            for trace in traces
            if trace["event_type"] in {"hypothesis_created", "hypothesis_updated"}
            and trace["payload"].get("hypothesis_id") == row["hypothesis_id"]
        ]
        return {
            "hypothesis_id": row["hypothesis_id"],
            "description": row["statement"],
            "status": row["status"],
            "confidence": row["confidence"],
            "supporting_evidence_refs": _json(row["supporting_evidence_refs_json"], []),
            "contradicting_evidence_refs": _json(row["contradicting_evidence_refs_json"], []),
            "history": history,
        }

    @staticmethod
    def _report(row: Any) -> dict[str, Any]:
        return {
            "report_id": row["report_id"],
            "status": row["status"],
            "title": row["title"],
            "conclusion": row["conclusion"],
            "root_cause": row["root_cause"],
            "confidence": row["confidence"],
            "evidence_ids": _json(row["evidence_ids_json"], []),
            "generated_at": row["generated_at"],
        }

    @staticmethod
    def _proposals(
        traces: list[dict[str, Any]], report: dict[str, Any] | None, incident_id: str
    ) -> list[dict[str, Any]]:
        checks = {
            trace["payload"].get("proposal_id"): trace["payload"]
            for trace in traces if trace["event_type"] == "permission_checked"
        }
        proposals = []
        for trace in traces:
            if trace["event_type"] != "action_proposal_created":
                continue
            payload = trace["payload"]
            check = checks.get(payload.get("proposal_id"), {})
            proposals.append({
                "proposal_id": payload.get("proposal_id"),
                "incident_id": incident_id,
                "action_name": payload.get("action_name"),
                "reason": report["conclusion"] if report else trace["summary"],
                "evidence_refs": payload.get("evidence_refs", []),
                "risk_level": payload.get("risk_level"),
                "approval_required": payload.get("approval_required", True),
                "permission_result": check.get("decision", "NOT_CHECKED"),
                "verification_plan": None,
                "rollback_plan": None,
            })
        return proposals
