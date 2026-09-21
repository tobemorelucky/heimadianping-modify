"""SQLite persistence for DetectionRule and AnomalySignal."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from memory.database import SQLiteDatabase
from monitoring.detector.models import (
    AnomalySignal,
    DetectionRule,
    DetectionSeverity,
    SignalStatus,
)


DETECTION_SCHEMA = """
CREATE TABLE IF NOT EXISTS detection_rules (
    rule_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    source_kind TEXT NOT NULL,
    condition_json TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('critical', 'high', 'medium', 'low')),
    fingerprint_fields_json TEXT NOT NULL,
    lookback_window INTEGER NOT NULL CHECK (lookback_window > 0),
    cooldown INTEGER NOT NULL CHECK (cooldown > 0),
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (rule_id, version)
);

CREATE INDEX IF NOT EXISTS idx_detection_rules_kind_enabled
    ON detection_rules (source_kind, enabled, rule_id, version);

CREATE TABLE IF NOT EXISTS anomaly_signals (
    signal_id TEXT PRIMARY KEY,
    rule_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    fingerprint TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('critical', 'high', 'medium', 'low')),
    facts_json TEXT NOT NULL,
    observation_refs_json TEXT NOT NULL,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('open', 'recovered')),
    created_at TEXT NOT NULL,
    FOREIGN KEY (rule_id, version) REFERENCES detection_rules (rule_id, version)
);

CREATE INDEX IF NOT EXISTS idx_anomaly_signals_fingerprint_last_seen
    ON anomaly_signals (rule_id, version, fingerprint, last_seen);
"""


class SignalStore:
    """Store versioned rules and emitted signals without creating Incidents."""

    def __init__(self, database: SQLiteDatabase) -> None:
        self.database = database

    def initialize(self) -> None:
        self.database.initialize()
        with self.database.connect() as connection:
            connection.executescript(DETECTION_SCHEMA)

    def upsert_rule(self, rule: DetectionRule) -> None:
        with self.database.connect() as connection:
            connection.execute(
                """
                INSERT INTO detection_rules (
                    rule_id, version, source_kind, condition_json, severity,
                    fingerprint_fields_json, lookback_window, cooldown, enabled,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (rule_id, version) DO UPDATE SET
                    source_kind = excluded.source_kind,
                    condition_json = excluded.condition_json,
                    severity = excluded.severity,
                    fingerprint_fields_json = excluded.fingerprint_fields_json,
                    lookback_window = excluded.lookback_window,
                    cooldown = excluded.cooldown,
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at
                """,
                self._rule_values(rule),
            )

    def ensure_rule(self, rule: DetectionRule) -> bool:
        """Insert a built-in rule once without rewriting persisted policy."""

        with self.database.connect() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO detection_rules (
                    rule_id, version, source_kind, condition_json, severity,
                    fingerprint_fields_json, lookback_window, cooldown, enabled,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                self._rule_values(rule),
            )
        return cursor.rowcount == 1

    def list_active_rules(self, source_kind: str) -> list[DetectionRule]:
        """Return only the latest enabled version of each matching rule."""

        with self.database.connect() as connection:
            rows = connection.execute(
                """
                SELECT rules.*
                FROM detection_rules AS rules
                JOIN (
                    SELECT rule_id, MAX(version) AS version
                    FROM detection_rules
                    GROUP BY rule_id
                ) AS latest
                  ON latest.rule_id = rules.rule_id
                 AND latest.version = rules.version
                WHERE rules.enabled = 1 AND rules.source_kind = ?
                ORDER BY rules.rule_id
                """,
                (source_kind,),
            ).fetchall()
        return [self._rule_from_row(dict(row)) for row in rows]

    def save_signal(self, signal: AnomalySignal) -> None:
        with self.database.connect() as connection:
            connection.execute(
                """
                INSERT INTO anomaly_signals (
                    signal_id, rule_id, version, fingerprint, severity,
                    facts_json, observation_refs_json, first_seen, last_seen,
                    status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.signal_id,
                    signal.rule_id,
                    signal.version,
                    signal.fingerprint,
                    signal.severity.value,
                    json.dumps(signal.facts, ensure_ascii=False),
                    json.dumps(signal.observation_refs, ensure_ascii=False),
                    signal.first_seen.isoformat(),
                    signal.last_seen.isoformat(),
                    signal.status.value,
                    signal.created_at.isoformat(),
                ),
            )

    def latest_signal(
        self,
        *,
        rule_id: str,
        version: int,
        fingerprint: str,
    ) -> AnomalySignal | None:
        with self.database.connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM anomaly_signals
                WHERE rule_id = ? AND version = ? AND fingerprint = ?
                ORDER BY last_seen DESC, created_at DESC
                LIMIT 1
                """,
                (rule_id, version, fingerprint),
            ).fetchone()
        return self._signal_from_row(dict(row)) if row else None

    def list_signals(self) -> list[AnomalySignal]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM anomaly_signals ORDER BY created_at, signal_id"
            ).fetchall()
        return [self._signal_from_row(dict(row)) for row in rows]

    @staticmethod
    def _rule_values(rule: DetectionRule) -> tuple[Any, ...]:
        return (
            rule.rule_id,
            rule.version,
            rule.source_kind,
            rule.condition.model_dump_json(),
            rule.severity.value,
            json.dumps(rule.fingerprint_fields, ensure_ascii=False),
            rule.lookback_window,
            rule.cooldown,
            int(rule.enabled),
            rule.created_at.isoformat(),
            rule.updated_at.isoformat(),
        )

    @staticmethod
    def _rule_from_row(row: dict[str, Any]) -> DetectionRule:
        return DetectionRule(
            rule_id=row["rule_id"],
            version=row["version"],
            source_kind=row["source_kind"],
            condition=json.loads(row["condition_json"]),
            severity=DetectionSeverity(row["severity"]),
            fingerprint_fields=tuple(json.loads(row["fingerprint_fields_json"])),
            lookback_window=row["lookback_window"],
            cooldown=row["cooldown"],
            enabled=bool(row["enabled"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _signal_from_row(row: dict[str, Any]) -> AnomalySignal:
        return AnomalySignal(
            signal_id=row["signal_id"],
            rule_id=row["rule_id"],
            version=row["version"],
            fingerprint=row["fingerprint"],
            severity=DetectionSeverity(row["severity"]),
            facts=json.loads(row["facts_json"]),
            observation_refs=tuple(json.loads(row["observation_refs_json"])),
            first_seen=datetime.fromisoformat(row["first_seen"]),
            last_seen=datetime.fromisoformat(row["last_seen"]),
            status=SignalStatus(row["status"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )
