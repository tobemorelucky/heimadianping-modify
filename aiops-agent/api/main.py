"""FastAPI entry point for the Phase 1 AIOps Agent skeleton."""

from __future__ import annotations

from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request

from api.console import ConsoleReader
from api.schemas import HealthResponse, IncidentCreatedResponse, IncidentRequest
from config import Settings, get_settings
from incident.store import IncidentStore
from llm.factory import build_llm_provider
from llm.provider import LLMTimeoutError, StructuredLLMProvider
from memory.database import SQLiteDatabase
from runtime.mcp_client import MCPClientProtocol, StdioMCPClient
from runtime.orchestrator import RuntimeOrchestrator
from runtime.reports import DiagnosisReport, DiagnosisStatus


def create_app(
    settings: Settings | None = None,
    mcp_client: MCPClientProtocol | None = None,
    llm_provider: StructuredLLMProvider | None = None,
) -> FastAPI:
    """Create an application with an injectable configuration for tests."""

    selected_settings = settings or get_settings()
    database = SQLiteDatabase(selected_settings.database_path)
    client = mcp_client or StdioMCPClient(
        project_root=selected_settings.hmdp_project_root,
        timeout_seconds=selected_settings.mcp_tool_timeout_seconds,
        kafka_bootstrap_servers=selected_settings.kafka_bootstrap_servers,
        kafka_default_topic=selected_settings.kafka_default_topic,
        kafka_default_consumer_group=selected_settings.kafka_default_consumer_group,
        kafka_request_timeout_ms=selected_settings.kafka_request_timeout_ms,
        kafka_lag_threshold=selected_settings.kafka_lag_threshold,
        mysql_health_port=selected_settings.mysql_health_port,
        business_metrics_web_port=selected_settings.business_metrics_web_port,
        business_metrics_consumer_port=(
            selected_settings.business_metrics_consumer_port
        ),
    )
    provider = llm_provider or build_llm_provider(selected_settings)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        database.initialize()
        IncidentStore(database).initialize()
        application.state.settings = selected_settings
        application.state.database = database
        application.state.console_reader = ConsoleReader(
            database,
            replay_directory=selected_settings.replay_directory,
        )
        application.state.llm_provider = provider
        application.state.orchestrator = RuntimeOrchestrator(
            database,
            client,
            llm_provider=provider,
        )
        yield

    application = FastAPI(
        title=selected_settings.app_name,
        version="0.1.0",
        description="Local HMDP AIOps Agent with a configurable LLM Runtime.",
        lifespan=lifespan,
    )

    @application.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        database_ok = database.healthcheck()
        return HealthResponse(
            service=selected_settings.app_name,
            status="ok" if database_ok else "degraded",
            database="ok" if database_ok else "error",
            environment=selected_settings.environment,
        )

    @application.post("/incidents", response_model=IncidentCreatedResponse)
    def create_incident(
        incident_request: IncidentRequest,
        request: Request,
    ) -> IncidentCreatedResponse:
        orchestrator: RuntimeOrchestrator = request.app.state.orchestrator
        runtime_request = incident_request.to_runtime_request()
        incident_id = f"inc_{uuid4().hex}"
        try:
            result = orchestrator.run(runtime_request, incident_id=incident_id)
        except LLMTimeoutError:
            evidence = database.list_evidence(incident_id)
            if database.get_report(incident_id) is None:
                database.save_report(
                    DiagnosisReport(
                        incident_id=incident_id,
                        status=DiagnosisStatus.INCONCLUSIVE,
                        title="模型调用超时，诊断结果不完整",
                        conclusion=(
                            "诊断过程中模型调用超过阶段时限。已保留当前采集的"
                            "只读证据，未推断未经验证的根因，请人工复核或稍后重试。"
                        ),
                        root_cause=None,
                        confidence=0.0,
                        evidence_ids=[row["evidence_id"] for row in evidence],
                    )
                )
            return IncidentCreatedResponse(
                incident_id=incident_id,
                diagnosis_status="partial",
                reason="llm_timeout",
                message="模型调用超时；已有证据已保存，当前诊断为部分结果。",
                evidence_count=len(evidence),
            )
        return IncidentCreatedResponse(
            incident_id=result.incident_id,
            diagnosis_status="completed",
            evidence_count=len(database.list_evidence(result.incident_id)),
        )

    @application.get("/api/incidents")
    def list_console_incidents(request: Request) -> list[dict]:
        return request.app.state.console_reader.list_incidents()

    @application.get("/api/incidents/{incident_id}")
    def get_console_incident(incident_id: str, request: Request) -> dict:
        incident = request.app.state.console_reader.get_incident(incident_id)
        if incident is None:
            raise HTTPException(status_code=404, detail="Incident not found")
        return incident

    @application.get("/api/monitoring/summary")
    def get_console_monitoring_summary(request: Request) -> dict:
        return request.app.state.console_reader.monitoring_summary()

    return application


app = create_app()
