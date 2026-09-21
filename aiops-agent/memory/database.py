"""SQLite connection and idempotent schema initialization."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List

from runtime.evidence import Evidence
from runtime.models import IncidentStatus, IncidentTask
from runtime.plans import InvestigationPlan
from runtime.reports import DiagnosisReport
from reasoning.models import Hypothesis
from trace.models import AgentTraceEvent


SCHEMA_PATH = Path(__file__).with_name("schema.sql")


class SQLiteDatabase:
    """Small connection factory for the local Phase 1 database."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path).resolve()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        """Open one short-lived connection with consistent safety settings."""

        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        """Create the database directory and apply the idempotent base schema."""

        self.path.parent.mkdir(parents=True, exist_ok=True)
        schema = SCHEMA_PATH.read_text(encoding="utf-8")
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(schema)
            self._migrate_evidence_schema(connection)
            self._migrate_hypothesis_schema(connection)
            self._migrate_report_schema(connection)

    def healthcheck(self) -> bool:
        """Verify that SQLite is reachable and its base tables exist."""

        try:
            with self.connect() as connection:
                value = connection.execute("SELECT 1").fetchone()[0]
                tables = set(self.list_tables(connection))
            return value == 1 and {"incidents", "trace_events"}.issubset(tables)
        except sqlite3.Error:
            return False

    def create_incident(self, incident: IncidentTask) -> None:
        """Persist a newly queued incident."""

        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO incidents (
                    incident_id, title, description, source, severity, status,
                    affected_components_json, observation_start, observation_end,
                    budget_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    incident.incident_id,
                    incident.title,
                    incident.description,
                    incident.source.value,
                    incident.severity.value,
                    incident.status.value,
                    json.dumps(incident.affected_components, ensure_ascii=False),
                    (
                        incident.observation_window.start.isoformat()
                        if incident.observation_window
                        else None
                    ),
                    (
                        incident.observation_window.end.isoformat()
                        if incident.observation_window
                        else None
                    ),
                    incident.budget.model_dump_json(),
                    incident.created_at.isoformat(),
                    incident.updated_at.isoformat(),
                ),
            )

    def update_incident_status(
        self,
        incident_id: str,
        status: IncidentStatus,
    ) -> None:
        """Update only Agent-owned lifecycle state."""

        with self.connect() as connection:
            cursor = connection.execute(
                "UPDATE incidents SET status = ?, updated_at = ? WHERE incident_id = ?",
                (status.value, datetime.now(timezone.utc).isoformat(), incident_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"incident not found: {incident_id}")

    def save_plan(self, plan: InvestigationPlan) -> None:
        """Append one immutable plan version."""

        steps_json = json.dumps(
            [plan.action.model_dump(mode="json")],
            ensure_ascii=False,
        )
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO plan_versions (
                    plan_id, incident_id, version, hypothesis, steps_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    plan.plan_id,
                    plan.incident_id,
                    plan.version,
                    plan.hypothesis,
                    steps_json,
                    plan.created_at.isoformat(),
                ),
            )

    def save_evidence(self, evidence: Evidence) -> None:
        """Persist one normalized tool observation."""

        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO evidence (
                    evidence_id, incident_id, step_id, kind, source, source_tool,
                    status, observation_start, observation_end, completeness,
                    raw_ref, observation_json, error_json, summary, collected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    evidence.evidence_id,
                    evidence.incident_id,
                    evidence.step_id,
                    evidence.kind,
                    evidence.source,
                    evidence.source_tool,
                    evidence.status.value,
                    (
                        evidence.observation_window.start.isoformat()
                        if evidence.observation_window
                        else None
                    ),
                    (
                        evidence.observation_window.end.isoformat()
                        if evidence.observation_window
                        else None
                    ),
                    evidence.completeness.value,
                    evidence.raw_ref,
                    json.dumps(evidence.data, ensure_ascii=False),
                    (
                        evidence.error.model_dump_json()
                        if evidence.error is not None
                        else None
                    ),
                    evidence.summary,
                    evidence.collected_at.isoformat(),
                ),
            )

    def save_hypothesis(
        self,
        incident_id: str,
        hypothesis: Hypothesis,
        reason: str,
    ) -> None:
        """Insert or update the latest state of one hypothesis."""

        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO hypotheses (
                    incident_id, hypothesis_id, statement, status,
                    reason, confidence, supporting_evidence_refs_json,
                    contradicting_evidence_refs_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (incident_id, hypothesis_id) DO UPDATE SET
                    statement = excluded.statement,
                    status = excluded.status,
                    reason = excluded.reason,
                    confidence = excluded.confidence,
                    supporting_evidence_refs_json = excluded.supporting_evidence_refs_json,
                    contradicting_evidence_refs_json = excluded.contradicting_evidence_refs_json,
                    updated_at = excluded.updated_at
                """,
                (
                    incident_id,
                    hypothesis.hypothesis_id,
                    hypothesis.description,
                    hypothesis.status.value,
                    reason,
                    hypothesis.confidence,
                    json.dumps(hypothesis.supporting_evidence_refs, ensure_ascii=False),
                    json.dumps(hypothesis.contradicting_evidence_refs, ensure_ascii=False),
                    hypothesis.created_at.isoformat(),
                    hypothesis.updated_at.isoformat(),
                ),
            )

    def save_trace_event(self, event: AgentTraceEvent) -> None:
        """Append an auditable runtime event."""

        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO trace_events (
                    event_id, incident_id, sequence, event_type, stage,
                    objective, summary, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.incident_id,
                    event.sequence,
                    event.event_type.value,
                    event.stage,
                    event.objective,
                    event.summary,
                    json.dumps(event.payload, ensure_ascii=False),
                    event.created_at.isoformat(),
                ),
            )

    def save_report(self, report: DiagnosisReport) -> None:
        """Persist the final report for human review."""

        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO reports (
                    report_id, incident_id, status, title, conclusion,
                    root_cause, confidence, evidence_ids_json,
                    final_hypothesis_json, generated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report.report_id,
                    report.incident_id,
                    report.status.value,
                    report.title,
                    report.conclusion,
                    report.root_cause,
                    report.confidence,
                    json.dumps(report.evidence_ids, ensure_ascii=False),
                    (
                        report.final_hypothesis.model_dump_json()
                        if report.final_hypothesis is not None
                        else None
                    ),
                    report.generated_at.isoformat(),
                ),
            )

    def get_incident(self, incident_id: str) -> Dict[str, Any] | None:
        """Return one incident row for API and tests."""

        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM incidents WHERE incident_id = ?",
                (incident_id,),
            ).fetchone()
        return dict(row) if row else None

    def get_report(self, incident_id: str) -> Dict[str, Any] | None:
        """Return the persisted report for one incident."""

        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM reports WHERE incident_id = ?",
                (incident_id,),
            ).fetchone()
        return dict(row) if row else None

    def list_trace_events(self, incident_id: str) -> List[Dict[str, Any]]:
        """Return trace events in deterministic sequence order."""

        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM trace_events WHERE incident_id = ? ORDER BY sequence",
                (incident_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_evidence(self, incident_id: str) -> List[Dict[str, Any]]:
        """Return evidence in collection order."""

        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM evidence WHERE incident_id = ? ORDER BY collected_at",
                (incident_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_plan_versions(self, incident_id: str) -> List[Dict[str, Any]]:
        """Return all plan versions for reflection tests."""

        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM plan_versions WHERE incident_id = ? ORDER BY version",
                (incident_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_hypotheses(self, incident_id: str) -> List[Dict[str, Any]]:
        """Return the persisted current state of each incident hypothesis."""

        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM hypotheses WHERE incident_id = ? ORDER BY created_at",
                (incident_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def list_tables(connection: sqlite3.Connection) -> List[str]:
        """Return application table names for health checks and tests."""

        rows = connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        return [str(row[0]) for row in rows]

    @staticmethod
    def _migrate_evidence_schema(connection: sqlite3.Connection) -> None:
        """Add normalized Evidence columns to databases created before P0."""

        existing = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(evidence)").fetchall()
        }
        additions = {
            "kind": "TEXT NOT NULL DEFAULT 'unknown'",
            "source": "TEXT NOT NULL DEFAULT 'unknown'",
            "status": "TEXT NOT NULL DEFAULT 'partial'",
            "observation_start": "TEXT",
            "observation_end": "TEXT",
            "completeness": "TEXT NOT NULL DEFAULT 'partial'",
            "raw_ref": "TEXT",
            "error_json": "TEXT",
        }
        for column, definition in additions.items():
            if column not in existing:
                connection.execute(
                    f"ALTER TABLE evidence ADD COLUMN {column} {definition}"
                )
        connection.execute(
            """
            UPDATE evidence
            SET status = 'partial', completeness = 'partial'
            WHERE kind = 'unknown'
              AND status = 'success'
              AND completeness = 'unknown'
            """
        )

    @staticmethod
    def _migrate_hypothesis_schema(connection: sqlite3.Connection) -> None:
        """Add Evidence-grounded Ledger fields to pre-D2 databases."""

        existing = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(hypotheses)").fetchall()
        }
        additions = {
            "supporting_evidence_refs_json": "TEXT NOT NULL DEFAULT '[]'",
            "contradicting_evidence_refs_json": "TEXT NOT NULL DEFAULT '[]'",
            "created_at": "TEXT",
        }
        for column, definition in additions.items():
            if column not in existing:
                connection.execute(
                    f"ALTER TABLE hypotheses ADD COLUMN {column} {definition}"
                )
        connection.execute(
            "UPDATE hypotheses SET created_at = updated_at WHERE created_at IS NULL"
        )

    @staticmethod
    def _migrate_report_schema(connection: sqlite3.Connection) -> None:
        """Persist the final incident-scoped hypothesis in Diagnosis Reports."""

        existing = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(reports)").fetchall()
        }
        if "final_hypothesis_json" not in existing:
            connection.execute(
                "ALTER TABLE reports ADD COLUMN final_hypothesis_json TEXT"
            )
