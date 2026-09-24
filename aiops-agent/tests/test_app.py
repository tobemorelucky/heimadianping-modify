"""Application startup tests."""

from fastapi.testclient import TestClient

from api.main import create_app
from config import Settings
from llm.mock import MockLLMProvider
from llm.provider import LLMTimeoutError


class ReflectionTimeoutProvider(MockLLMProvider):
    """Let Planner complete, then simulate one bounded model timeout."""

    def _complete(self, **kwargs):
        if kwargs["operation"] == "reflect":
            raise LLMTimeoutError("LLM operation timed out: reflect")
        return super()._complete(**kwargs)


def test_application_starts_and_initializes_database(tmp_path):
    database_path = tmp_path / "startup" / "aiops.db"
    app = create_app(Settings(database_path=database_path))

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "service": "HMDP AIOps Agent",
        "status": "ok",
        "database": "ok",
        "environment": "local",
    }
    assert database_path.exists()


def test_post_incidents_runs_diagnosis_and_returns_incident_id(tmp_path):
    database_path = tmp_path / "api" / "aiops.db"
    project_root = tmp_path / "hmdp"
    runtime_dir = project_root / "target" / "runtime"
    runtime_dir.mkdir(parents=True)
    (runtime_dir / "spring-boot.out.log").write_text("", encoding="utf-8")
    (runtime_dir / "spring-boot.error.log").write_text(
        "ERROR order creation failed\n",
        encoding="utf-8",
    )
    app = create_app(
        Settings(database_path=database_path, hmdp_project_root=project_root)
    )

    with TestClient(app) as client:
        response = client.post(
            "/incidents",
            json={
                "title": "秒杀订单延迟",
                "description": "用户反馈订单创建缓慢",
                "time_window": "10m",
            },
        )
        database = app.state.database

    assert response.status_code == 200
    incident_id = response.json()["incident_id"]
    assert incident_id.startswith("inc_")
    assert database.get_report(incident_id)["status"] == "confirmed"


def test_post_incidents_rejects_invalid_time_window(tmp_path):
    app = create_app(Settings(database_path=tmp_path / "invalid.db"))

    with TestClient(app) as client:
        response = client.post(
            "/incidents",
            json={
                "title": "秒杀订单延迟",
                "description": "用户反馈订单创建缓慢",
                "time_window": "ten-minutes",
            },
        )

    assert response.status_code == 422


def test_llm_timeout_returns_partial_diagnosis_and_preserves_evidence(tmp_path):
    database_path = tmp_path / "timeout" / "aiops.db"
    project_root = tmp_path / "hmdp"
    runtime_dir = project_root / "target" / "runtime"
    runtime_dir.mkdir(parents=True)
    (runtime_dir / "spring-boot.out.log").write_text(
        "ERROR order creation delayed\n",
        encoding="utf-8",
    )
    (runtime_dir / "spring-boot.error.log").write_text("", encoding="utf-8")
    app = create_app(
        Settings(database_path=database_path, hmdp_project_root=project_root),
        llm_provider=ReflectionTimeoutProvider(),
    )

    with TestClient(app) as client:
        response = client.post(
            "/incidents",
            json={
                "title": "秒杀订单延迟",
                "description": "验证模型超时降级",
                "time_window": "10m",
            },
        )
        database = app.state.database

    assert response.status_code == 200
    payload = response.json()
    assert payload["diagnosis_status"] == "partial"
    assert payload["reason"] == "llm_timeout"
    assert payload["evidence_count"] == 1
    assert len(database.list_evidence(payload["incident_id"])) == 1
    report = database.get_report(payload["incident_id"])
    assert report["status"] == "inconclusive"
    assert report["root_cause"] is None
