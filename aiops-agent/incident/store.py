"""SQLite persistence for proactive incidents and manager trace events."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from incident.models import (
    Incident,
    IncidentSeverity,
    IncidentStatus,
    IncidentTraceEvent,
)
from memory.database import SQLiteDatabase


INCIDENT_MANAGER_SCHEMA = """
CREATE TABLE IF NOT EXISTS managed_incidents (
    incident_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('critical', 'high', 'medium', 'low')),
    status TEXT NOT NULL CHECK (status IN ('OPEN', 'DIAGNOSING', 'RESOLVED', 'FAILED')),
    trigger_signal_ids_json TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    last_signal_at TEXT NOT NULL,
    diagnosis_report_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_managed_incidents_fingerprint_signal
    ON managed_incidents (fingerprint, last_signal_at);

CREATE TABLE IF NOT EXISTS incident_manager_trace_events (
    event_id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (
        event_type IN (
            'incident_created', 'diagnosis_started', 'diagnosis_completed',
            'action_proposal_created', 'permission_checked'
        )
    ),
    summary TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    FOREIGN KEY (incident_id) REFERENCES managed_incidents (incident_id)
);

CREATE INDEX IF NOT EXISTS idx_incident_manager_trace_time
    ON incident_manager_trace_events (incident_id, created_at);
"""


class IncidentStore:
    """Persist the proactive lifecycle separately from Runtime task state."""

    def __init__(self, database: SQLiteDatabase) -> None:
        self.database = database

    def initialize(self) -> None:
        self.database.initialize()
        with self.database.connect() as connection:
            connection.executescript(INCIDENT_MANAGER_SCHEMA)
            self._migrate_incident_trace_event_types(connection)

    def create(self, incident: Incident) -> None:
        with self.database.connect() as connection:
            connection.execute(
                """
                INSERT INTO managed_incidents (
                    incident_id, title, severity, status,
                    trigger_signal_ids_json, fingerprint, last_signal_at,
                    diagnosis_report_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    incident.incident_id,
                    incident.title,
                    incident.severity.value,
                    incident.status.value,
                    json.dumps(incident.trigger_signal_ids, ensure_ascii=False),
                    incident.fingerprint,
                    incident.last_signal_at.isoformat(),
                    incident.diagnosis_report_id,
                    incident.created_at.isoformat(),
                    incident.updated_at.isoformat(),
                ),
            )

    def get(self, incident_id: str) -> Incident | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM managed_incidents WHERE incident_id = ?",
                (incident_id,),
            ).fetchone()
        return self._incident_from_row(dict(row)) if row else None

    def latest_for_fingerprint(self, fingerprint: str) -> Incident | None:
        with self.database.connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM managed_incidents
                WHERE fingerprint = ?
                ORDER BY last_signal_at DESC, created_at DESC
                LIMIT 1
                """,
                (fingerprint,),
            ).fetchone()
        return self._incident_from_row(dict(row)) if row else None

    def merge_signal(
        self,
        incident_id: str,
        *,
        signal_id: str,
        signal_seen_at: datetime,
        updated_at: datetime,
    ) -> Incident:
        """Append one signal idempotently without starting another diagnosis."""

        with self.database.connect() as connection:
            row = connection.execute(
                """
                SELECT trigger_signal_ids_json, last_signal_at
                FROM managed_incidents WHERE incident_id = ?
                """,
                (incident_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"managed incident not found: {incident_id}")
            signal_ids = list(json.loads(row["trigger_signal_ids_json"]))
            if signal_id not in signal_ids:
                signal_ids.append(signal_id)
            existing_last_signal_at = datetime.fromisoformat(row["last_signal_at"])
            effective_last_signal_at = max(existing_last_signal_at, signal_seen_at)
            connection.execute(
                """
                UPDATE managed_incidents
                SET trigger_signal_ids_json = ?, last_signal_at = ?, updated_at = ?
                WHERE incident_id = ?
                """,
                (
                    json.dumps(signal_ids, ensure_ascii=False),
                    effective_last_signal_at.isoformat(),
                    updated_at.isoformat(),
                    incident_id,
                ),
            )
        merged = self.get(incident_id)
        if merged is None:
            raise KeyError(f"managed incident not found after merge: {incident_id}")
        return merged

    def update_status(
        self,
        incident_id: str,
        status: IncidentStatus,
        *,
        updated_at: datetime,
        diagnosis_report_id: str | None = None,
    ) -> Incident:
        with self.database.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE managed_incidents
                SET status = ?, diagnosis_report_id = COALESCE(?, diagnosis_report_id),
                    updated_at = ?
                WHERE incident_id = ?
                """,
                (
                    status.value,
                    diagnosis_report_id,
                    updated_at.isoformat(),
                    incident_id,
                ),
            )
            changed = cursor.rowcount
        if changed != 1:
            raise KeyError(f"managed incident not found: {incident_id}")
        incident = self.get(incident_id)
        if incident is None:
            raise KeyError(f"managed incident not found after update: {incident_id}")
        return incident

    def save_trace_event(self, event: IncidentTraceEvent) -> None:
        with self.database.connect() as connection:
            connection.execute(
                """
                INSERT INTO incident_manager_trace_events (
                    event_id, incident_id, event_type, summary,
                    payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.incident_id,
                    event.event_type.value,
                    event.summary,
                    json.dumps(event.payload, ensure_ascii=False),
                    event.created_at.isoformat(),
                ),
            )

    def list_incidents(self) -> list[Incident]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM managed_incidents ORDER BY created_at, incident_id"
            ).fetchall()
        return [self._incident_from_row(dict(row)) for row in rows]

    def list_trace_events(self, incident_id: str) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM incident_manager_trace_events
                WHERE incident_id = ?
                ORDER BY rowid
                """,
                (incident_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _incident_from_row(row: dict[str, Any]) -> Incident:
        return Incident(
            incident_id=row["incident_id"],
            title=row["title"],
            severity=IncidentSeverity(row["severity"]),
            status=IncidentStatus(row["status"]),
            trigger_signal_ids=tuple(json.loads(row["trigger_signal_ids_json"])),
            fingerprint=row["fingerprint"],
            last_signal_at=datetime.fromisoformat(row["last_signal_at"]),
            diagnosis_report_id=row["diagnosis_report_id"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _migrate_incident_trace_event_types(connection: Any) -> None:
        """Allow Phase G1 trace events in databases created by Phase M3."""

        row = connection.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type = 'table' AND name = 'incident_manager_trace_events'
            """
        ).fetchone()
        if row is None or "action_proposal_created" in str(row[0]):
            return
        connection.executescript(
            """
            DROP INDEX IF EXISTS idx_incident_manager_trace_time;
            ALTER TABLE incident_manager_trace_events
                RENAME TO incident_manager_trace_events_phase_m3;
            CREATE TABLE incident_manager_trace_events (
                event_id TEXT PRIMARY KEY,
                incident_id TEXT NOT NULL,
                event_type TEXT NOT NULL CHECK (
                    event_type IN (
                        'incident_created', 'diagnosis_started',
                        'diagnosis_completed', 'action_proposal_created',
                        'permission_checked'
                    )
                ),
                summary TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                FOREIGN KEY (incident_id) REFERENCES managed_incidents (incident_id)
            );
            INSERT INTO incident_manager_trace_events (
                event_id, incident_id, event_type, summary, payload_json, created_at
            )
            SELECT event_id, incident_id, event_type, summary, payload_json, created_at
            FROM incident_manager_trace_events_phase_m3;
            DROP TABLE incident_manager_trace_events_phase_m3;
            CREATE INDEX idx_incident_manager_trace_time
                ON incident_manager_trace_events (incident_id, created_at);
            """
        )
