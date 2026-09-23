"""Environment-based configuration for the local AIOps Agent skeleton."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Literal, Mapping

from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"


class Settings(BaseModel):
    """Validated local settings used by the Phase 1 application."""

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    app_name: str = Field(default="HMDP AIOps Agent", min_length=1)
    environment: str = Field(default="local", min_length=1)
    host: str = "127.0.0.1"
    port: int = Field(default=8010, ge=1, le=65535)
    database_path: Path = PROJECT_ROOT / "data" / "aiops.db"
    replay_directory: Path | None = None
    log_level: str = "INFO"
    ai_model_provider: Literal["mock", "openai_compatible"] = "mock"
    ai_model_base_url: str | None = None
    ai_model_api_key: SecretStr | None = None
    ai_model_name: str | None = None
    ai_model_timeout_seconds: float = Field(default=30.0, gt=0, le=120)
    hmdp_project_root: Path = PROJECT_ROOT.parent
    mcp_tool_timeout_seconds: float = Field(default=10.0, gt=0, le=60)
    kafka_bootstrap_servers: str = Field(
        default="127.0.0.1:9092",
        min_length=3,
        max_length=500,
    )
    kafka_default_topic: str = Field(
        default="hmdp.seckill.order.create.v1",
        min_length=1,
        max_length=249,
    )
    kafka_default_consumer_group: str = Field(
        default="hmdp-seckill-order-create-v1",
        min_length=1,
        max_length=255,
    )
    kafka_request_timeout_ms: int = Field(default=3000, ge=250, le=30000)
    kafka_lag_threshold: int = Field(default=1000, ge=0, le=1_000_000_000)
    mysql_health_port: int = Field(default=18082, ge=1, le=65535)
    business_metrics_web_port: int = Field(default=18081, ge=1, le=65535)
    business_metrics_consumer_port: int = Field(default=18083, ge=1, le=65535)

    @field_validator("host")
    @classmethod
    def validate_loopback_host(cls, value: str) -> str:
        """Keep the MVP API local-only."""

        if value not in {"127.0.0.1", "localhost"}:
            raise ValueError("AIOPS_HOST must be a loopback address")
        return value

    @field_validator("database_path", "replay_directory", mode="before")
    @classmethod
    def resolve_database_path(cls, value: object) -> Path:
        """Resolve relative database paths against the project directory."""

        path = Path(str(value))
        return path if path.is_absolute() else (PROJECT_ROOT / path).resolve()

    @field_validator("hmdp_project_root", mode="before")
    @classmethod
    def resolve_hmdp_project_root(cls, value: object) -> Path:
        path = Path(str(value))
        return path if path.is_absolute() else (PROJECT_ROOT / path).resolve()

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        """Accept only conventional Python log levels."""

        normalized = value.upper()
        if normalized not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("AIOPS_LOG_LEVEL is invalid")
        return normalized


def load_settings(
    env_file: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> Settings:
    """Load .env values, then let process environment values override them."""

    selected_env_file = env_file or DEFAULT_ENV_FILE
    file_values = {
        key: value
        for key, value in dotenv_values(selected_env_file).items()
        if value is not None
    }
    process_values = dict(os.environ if environ is None else environ)
    merged = {**file_values, **process_values}

    field_mapping = {
        "app_name": ("AIOPS_APP_NAME",),
        "environment": ("AIOPS_ENVIRONMENT",),
        "host": ("AIOPS_HOST",),
        "port": ("AIOPS_PORT",),
        "database_path": ("AIOPS_DATABASE_PATH",),
        "replay_directory": ("AIOPS_REPLAY_DIRECTORY",),
        "log_level": ("AIOPS_LOG_LEVEL",),
        "ai_model_provider": ("AI_MODEL_PROVIDER",),
        "ai_model_base_url": ("AI_MODEL_BASE_URL", "BASE_URL", "DOUBAO_BASE_URL"),
        "ai_model_api_key": ("AI_MODEL_API_KEY", "ARK_API_KEY", "DOUBAO_API_KEY"),
        "ai_model_name": ("AI_MODEL_NAME", "MODEL", "DOUBAO_MODEL"),
        "ai_model_timeout_seconds": ("AI_MODEL_TIMEOUT_SECONDS",),
        "hmdp_project_root": ("HMDP_PROJECT_ROOT",),
        "mcp_tool_timeout_seconds": ("AIOPS_MCP_TOOL_TIMEOUT_SECONDS",),
        "kafka_bootstrap_servers": ("KAFKA_BOOTSTRAP_SERVERS",),
        "kafka_default_topic": ("KAFKA_VOUCHER_ORDER_TOPIC",),
        "kafka_default_consumer_group": ("KAFKA_CONSUMER_GROUP",),
        "kafka_request_timeout_ms": ("AIOPS_KAFKA_REQUEST_TIMEOUT_MS",),
        "kafka_lag_threshold": ("AIOPS_KAFKA_LAG_THRESHOLD",),
        "mysql_health_port": ("AIOPS_MYSQL_HEALTH_PORT",),
        "business_metrics_web_port": ("AIOPS_BUSINESS_METRICS_WEB_PORT",),
        "business_metrics_consumer_port": (
            "AIOPS_BUSINESS_METRICS_CONSUMER_PORT",
        ),
    }
    values: dict[str, str] = {}
    for field_name, environment_names in field_mapping.items():
        for environment_name in environment_names:
            if merged.get(environment_name) not in (None, ""):
                values[field_name] = merged[environment_name]
                break
    return Settings.model_validate(values)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return one immutable Settings instance for the process."""

    return load_settings()
