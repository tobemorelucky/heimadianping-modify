CREATE TABLE IF NOT EXISTS incidents (
    incident_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('human', 'alert', 'faultbench')),
    severity TEXT NOT NULL CHECK (severity IN ('critical', 'high', 'medium', 'low')),
    status TEXT NOT NULL CHECK (
        status IN (
            'queued', 'analyzing', 'planning', 'investigating', 'reflecting',
            'reporting', 'awaiting_human', 'closed', 'failed', 'timed_out', 'cancelled'
        )
    ),
    affected_components_json TEXT NOT NULL DEFAULT '[]',
    observation_start TEXT,
    observation_end TEXT,
    budget_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trace_events (
    event_id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    event_type TEXT NOT NULL,
    stage TEXT NOT NULL,
    objective TEXT,
    summary TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    FOREIGN KEY (incident_id) REFERENCES incidents (incident_id),
    UNIQUE (incident_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_trace_events_incident_created
    ON trace_events (incident_id, created_at);

CREATE TABLE IF NOT EXISTS plan_versions (
    plan_id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    hypothesis TEXT NOT NULL,
    steps_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (incident_id) REFERENCES incidents (incident_id),
    UNIQUE (incident_id, version)
);

CREATE TABLE IF NOT EXISTS evidence (
    evidence_id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    source TEXT NOT NULL,
    source_tool TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('success', 'partial', 'error', 'timeout')),
    observation_start TEXT,
    observation_end TEXT,
    completeness TEXT NOT NULL CHECK (completeness IN ('complete', 'partial', 'unknown')),
    raw_ref TEXT,
    observation_json TEXT NOT NULL,
    error_json TEXT,
    summary TEXT NOT NULL,
    collected_at TEXT NOT NULL,
    FOREIGN KEY (incident_id) REFERENCES incidents (incident_id)
);

CREATE TABLE IF NOT EXISTS hypotheses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id TEXT NOT NULL,
    hypothesis_id TEXT NOT NULL,
    statement TEXT NOT NULL,
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    confidence REAL NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    updated_at TEXT NOT NULL,
    FOREIGN KEY (incident_id) REFERENCES incidents (incident_id),
    UNIQUE (incident_id, hypothesis_id)
);

CREATE TABLE IF NOT EXISTS reports (
    report_id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('confirmed', 'inconclusive')),
    title TEXT NOT NULL,
    conclusion TEXT NOT NULL,
    root_cause TEXT,
    confidence REAL NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    evidence_ids_json TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    FOREIGN KEY (incident_id) REFERENCES incidents (incident_id)
);

CREATE INDEX IF NOT EXISTS idx_evidence_incident_collected
    ON evidence (incident_id, collected_at);
