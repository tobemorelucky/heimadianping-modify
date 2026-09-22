"""Existing proactive incidents must not be mistaken for recovered faults."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from incident.models import DiagnosisStatus, IncidentStatus
from incident.store import IncidentStore
from memory.database import SQLiteDatabase


@pytest.mark.parametrize(
    ("legacy_status", "expected_diagnosis"),
    [
        ("OPEN", DiagnosisStatus.PENDING),
        ("DIAGNOSING", DiagnosisStatus.RUNNING),
        ("RESOLVED", DiagnosisStatus.COMPLETED),
        ("FAILED", DiagnosisStatus.FAILED),
    ],
)
def test_legacy_status_migrates_without_claiming_recovery(
    tmp_path, legacy_status, expected_diagnosis
):
    database = SQLiteDatabase(tmp_path / "legacy.db")
    database.initialize()
    now = datetime(2026, 9, 19, tzinfo=timezone.utc).isoformat()
    with database.connect() as connection:
        connection.executescript(
            """
            CREATE TABLE managed_incidents (
                incident_id TEXT PRIMARY KEY, title TEXT NOT NULL, severity TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('OPEN', 'DIAGNOSING', 'RESOLVED', 'FAILED')),
                trigger_signal_ids_json TEXT NOT NULL, fingerprint TEXT NOT NULL,
                last_signal_at TEXT NOT NULL, diagnosis_report_id TEXT,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE incident_manager_trace_events (
                event_id TEXT PRIMARY KEY, incident_id TEXT NOT NULL,
                event_type TEXT NOT NULL CHECK (
                    event_type IN ('incident_created', 'diagnosis_started', 'diagnosis_completed',
                                   'action_proposal_created', 'permission_checked')
                ),
                summary TEXT NOT NULL, payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                FOREIGN KEY (incident_id) REFERENCES managed_incidents (incident_id)
            );
            """
        )
        connection.execute(
            """INSERT INTO managed_incidents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("inc_legacy", "Old fault", "high", legacy_status, '["sig_old"]',
             "kafka:old", now, "report_old", now, now),
        )
        connection.execute(
            """INSERT INTO incident_manager_trace_events VALUES (?, ?, ?, ?, ?, ?)""",
            ("trace_old", "inc_legacy", "incident_created", "Old trace", "{}", now),
        )

    store = IncidentStore(database)
    store.initialize()
    store.initialize()  # idempotent on the same database
    incident = store.get("inc_legacy")

    assert incident is not None
    assert incident.status is IncidentStatus.ACTIVE
    assert incident.diagnosis_status is expected_diagnosis
    assert incident.trigger_signal_ids == ("sig_old",)
    assert incident.diagnosis_report_id == "report_old"
    assert store.list_trace_events("inc_legacy")[0]["event_id"] == "trace_old"
    with database.connect() as connection:
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
