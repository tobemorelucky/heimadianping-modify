"""SQLite persistence for schedules, collection runs, and monitoring trace."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from memory.database import SQLiteDatabase
from monitoring.models import (
    CollectionRun,
    CollectionSchedule,
    MonitoringTraceEvent,
    StoredObservation,
)
from tool_contracts import ToolObservation


MONITORING_SCHEMA = """
CREATE TABLE IF NOT EXISTS monitoring_schedules (
    schedule_id TEXT PRIMARY KEY,
    tool_name TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL CHECK (interval_seconds > 0),
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    timeout_seconds REAL NOT NULL CHECK (timeout_seconds > 0),
    arguments_json TEXT NOT NULL DEFAULT '{}',
    next_run_at TEXT,
    last_run_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_monitoring_schedules_due
    ON monitoring_schedules (enabled, next_run_at);

CREATE TABLE IF NOT EXISTS monitoring_collection_runs (
    collection_run_id TEXT PRIMARY KEY,
    schedule_id TEXT NOT NULL,
    tool TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('success', 'partial', 'error', 'timeout')),
    timestamp TEXT NOT NULL,
    evidence_ref TEXT NOT NULL,
    duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
    observation_json TEXT NOT NULL,
    FOREIGN KEY (schedule_id) REFERENCES monitoring_schedules (schedule_id)
);

CREATE INDEX IF NOT EXISTS idx_monitoring_runs_schedule_time
    ON monitoring_collection_runs (schedule_id, timestamp);

CREATE TABLE IF NOT EXISTS monitoring_trace_events (
    event_id TEXT PRIMARY KEY,
    schedule_id TEXT NOT NULL,
    collection_run_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (
        event_type IN (
            'monitoring_tick_started', 'observation_collected', 'anomaly_detected'
        )
    ),
    summary TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_monitoring_trace_schedule_time
    ON monitoring_trace_events (schedule_id, created_at);
"""


class ObservationStore:
    """Persist the Phase M1 control plane without creating Incidents."""

    def __init__(self, database: SQLiteDatabase) -> None:
        self.database = database

    def initialize(self) -> None:
        """Create the monitoring tables idempotently."""

        self.database.initialize()
        with self.database.connect() as connection:
            connection.executescript(MONITORING_SCHEMA)
            self._migrate_monitoring_trace_event_types(connection)

    def upsert_schedule(self, schedule: CollectionSchedule) -> None:
        """Create or replace a schedule definition and its due state."""

        with self.database.connect() as connection:
            connection.execute(
                """
                INSERT INTO monitoring_schedules (
                    schedule_id, tool_name, interval_seconds, enabled,
                    timeout_seconds, arguments_json, next_run_at, last_run_at,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (schedule_id) DO UPDATE SET
                    tool_name = excluded.tool_name,
                    interval_seconds = excluded.interval_seconds,
                    enabled = excluded.enabled,
                    timeout_seconds = excluded.timeout_seconds,
                    arguments_json = excluded.arguments_json,
                    next_run_at = excluded.next_run_at,
                    last_run_at = excluded.last_run_at,
                    updated_at = excluded.updated_at
                """,
                self._schedule_values(schedule),
            )

    def ensure_schedule(self, schedule: CollectionSchedule) -> bool:
        """Insert a default schedule once without resetting persisted due state."""

        with self.database.connect() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO monitoring_schedules (
                    schedule_id, tool_name, interval_seconds, enabled,
                    timeout_seconds, arguments_json, next_run_at, last_run_at,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                self._schedule_values(schedule),
            )
        return cursor.rowcount == 1

    def get_schedule(self, schedule_id: str) -> CollectionSchedule | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM monitoring_schedules WHERE schedule_id = ?",
                (schedule_id,),
            ).fetchone()
        return self._schedule_from_row(dict(row)) if row else None

    def list_enabled_schedules(self) -> list[CollectionSchedule]:
        with self.database.connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM monitoring_schedules
                WHERE enabled = 1
                ORDER BY schedule_id
                """
            ).fetchall()
        return [self._schedule_from_row(dict(row)) for row in rows]

    def list_due_schedules(self, at: datetime) -> list[CollectionSchedule]:
        if at.tzinfo is None or at.utcoffset() is None:
            raise ValueError("due timestamp must include a timezone")
        with self.database.connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM monitoring_schedules
                WHERE enabled = 1
                  AND (next_run_at IS NULL OR next_run_at <= ?)
                ORDER BY COALESCE(next_run_at, ''), schedule_id
                """,
                (at.isoformat(),),
            ).fetchall()
        return [self._schedule_from_row(dict(row)) for row in rows]

    def mark_schedule_run(
        self,
        schedule_id: str,
        *,
        last_run_at: datetime,
        next_run_at: datetime,
    ) -> None:
        with self.database.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE monitoring_schedules
                SET last_run_at = ?, next_run_at = ?, updated_at = ?
                WHERE schedule_id = ?
                """,
                (
                    last_run_at.isoformat(),
                    next_run_at.isoformat(),
                    last_run_at.isoformat(),
                    schedule_id,
                ),
            )
        if cursor.rowcount != 1:
            raise KeyError(f"monitoring schedule not found: {schedule_id}")

    def save_collection_run(
        self,
        run: CollectionRun,
        observation: ToolObservation,
    ) -> None:
        with self.database.connect() as connection:
            connection.execute(
                """
                INSERT INTO monitoring_collection_runs (
                    collection_run_id, schedule_id, tool, status, timestamp,
                    evidence_ref, duration_ms, observation_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.collection_run_id,
                    run.schedule_id,
                    run.tool,
                    run.status.value,
                    run.timestamp.isoformat(),
                    run.evidence_ref,
                    run.duration_ms,
                    observation.model_dump_json(),
                ),
            )

    def save_trace_event(self, event: MonitoringTraceEvent) -> None:
        with self.database.connect() as connection:
            connection.execute(
                """
                INSERT INTO monitoring_trace_events (
                    event_id, schedule_id, collection_run_id, event_type,
                    summary, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.schedule_id,
                    event.collection_run_id,
                    event.event_type.value,
                    event.summary,
                    json.dumps(event.payload, ensure_ascii=False),
                    event.created_at.isoformat(),
                ),
            )

    def list_collection_runs(self, schedule_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM monitoring_collection_runs"
        parameters: tuple[str, ...] = ()
        if schedule_id is not None:
            query += " WHERE schedule_id = ?"
            parameters = (schedule_id,)
        query += " ORDER BY timestamp, collection_run_id"
        with self.database.connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [dict(row) for row in rows]

    def list_observations(
        self,
        *,
        schedule_id: str,
        source_kind: str | None,
        since: datetime,
        until: datetime,
    ) -> list[StoredObservation]:
        """Load one schedule's ordered Observation window for detection."""

        for value in (since, until):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("observation query timestamps must include a timezone")
        if until < since:
            raise ValueError("observation query until must not precede since")
        with self.database.connect() as connection:
            rows = connection.execute(
                """
                SELECT collection_run_id, schedule_id, timestamp, observation_json
                FROM monitoring_collection_runs
                WHERE schedule_id = ?
                  AND timestamp >= ?
                  AND timestamp <= ?
                ORDER BY timestamp, rowid
                """,
                (schedule_id, since.isoformat(), until.isoformat()),
            ).fetchall()
        observations: list[StoredObservation] = []
        for row in rows:
            observation = ToolObservation.model_validate_json(row["observation_json"])
            if source_kind is not None and observation.kind != source_kind:
                continue
            observations.append(
                StoredObservation(
                    collection_run_id=row["collection_run_id"],
                    schedule_id=row["schedule_id"],
                    timestamp=datetime.fromisoformat(row["timestamp"]),
                    observation=observation,
                )
            )
        return observations

    def list_trace_events(self, schedule_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM monitoring_trace_events"
        parameters: tuple[str, ...] = ()
        if schedule_id is not None:
            query += " WHERE schedule_id = ?"
            parameters = (schedule_id,)
        query += " ORDER BY rowid"
        with self.database.connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _schedule_values(schedule: CollectionSchedule) -> tuple[Any, ...]:
        return (
            schedule.schedule_id,
            schedule.tool_name,
            schedule.interval_seconds,
            int(schedule.enabled),
            schedule.timeout_seconds,
            json.dumps(schedule.arguments, ensure_ascii=False),
            schedule.next_run_at.isoformat() if schedule.next_run_at else None,
            schedule.last_run_at.isoformat() if schedule.last_run_at else None,
            schedule.created_at.isoformat(),
            schedule.updated_at.isoformat(),
        )

    @staticmethod
    def _schedule_from_row(row: dict[str, Any]) -> CollectionSchedule:
        return CollectionSchedule(
            schedule_id=row["schedule_id"],
            tool_name=row["tool_name"],
            interval_seconds=row["interval_seconds"],
            enabled=bool(row["enabled"]),
            timeout_seconds=row["timeout_seconds"],
            arguments=json.loads(row["arguments_json"]),
            next_run_at=(
                datetime.fromisoformat(row["next_run_at"])
                if row["next_run_at"]
                else None
            ),
            last_run_at=(
                datetime.fromisoformat(row["last_run_at"])
                if row["last_run_at"]
                else None
            ),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _migrate_monitoring_trace_event_types(connection: Any) -> None:
        """Allow Phase M2 trace events in databases created by Phase M1."""

        row = connection.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type = 'table' AND name = 'monitoring_trace_events'
            """
        ).fetchone()
        if row is None or "anomaly_detected" in str(row[0]):
            return
        connection.executescript(
            """
            DROP INDEX IF EXISTS idx_monitoring_trace_schedule_time;
            ALTER TABLE monitoring_trace_events
                RENAME TO monitoring_trace_events_phase_m1;
            CREATE TABLE monitoring_trace_events (
                event_id TEXT PRIMARY KEY,
                schedule_id TEXT NOT NULL,
                collection_run_id TEXT NOT NULL,
                event_type TEXT NOT NULL CHECK (
                    event_type IN (
                        'monitoring_tick_started',
                        'observation_collected',
                        'anomaly_detected'
                    )
                ),
                summary TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            );
            INSERT INTO monitoring_trace_events (
                event_id, schedule_id, collection_run_id, event_type,
                summary, payload_json, created_at
            )
            SELECT event_id, schedule_id, collection_run_id, event_type,
                   summary, payload_json, created_at
            FROM monitoring_trace_events_phase_m1;
            DROP TABLE monitoring_trace_events_phase_m1;
            CREATE INDEX idx_monitoring_trace_schedule_time
                ON monitoring_trace_events (schedule_id, created_at);
            """
        )
