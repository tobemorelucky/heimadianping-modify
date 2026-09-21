"""SQLite initialization tests."""

import sqlite3

from memory.database import SQLiteDatabase


def test_initialize_creates_expected_schema_and_is_idempotent(tmp_path):
    database = SQLiteDatabase(tmp_path / "nested" / "aiops.db")

    database.initialize()
    database.initialize()

    with database.connect() as connection:
        tables = set(database.list_tables(connection))
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]

    assert {"incidents", "trace_events"}.issubset(tables)
    assert journal_mode.lower() == "wal"
    assert database.healthcheck() is True


def test_initialize_migrates_legacy_evidence_table(tmp_path):
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE evidence (
                evidence_id TEXT PRIMARY KEY,
                incident_id TEXT NOT NULL,
                step_id TEXT NOT NULL,
                source_tool TEXT NOT NULL,
                observation_json TEXT NOT NULL,
                summary TEXT NOT NULL,
                collected_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO evidence VALUES (
                'evi_legacy', 'inc_legacy', 'step_legacy', 'legacy_tool',
                '{}', 'legacy evidence', '2026-09-19T00:00:00+00:00'
            )
            """
        )

    database = SQLiteDatabase(path)
    database.initialize()

    with database.connect() as connection:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(evidence)")
        }
        row = connection.execute(
            "SELECT status, completeness FROM evidence WHERE evidence_id = 'evi_legacy'"
        ).fetchone()
    assert {
        "kind",
        "source",
        "status",
        "observation_start",
        "observation_end",
        "completeness",
        "raw_ref",
        "error_json",
    }.issubset(columns)
    assert dict(row) == {"status": "partial", "completeness": "partial"}
