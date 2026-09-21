"""Environment-based configuration for the standalone AI service."""

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

# Load only the AI service's local file. Real secrets remain outside version control.
AI_ASSISTANT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(AI_ASSISTANT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    """Runtime settings required by the OpenAI-compatible LLM client."""

    llm_base_url: str
    llm_api_key: str
    llm_model: str
    spring_boot_base_url: str

    @classmethod
    def from_environment(cls) -> "Settings":
        """Build settings without embedding any provider credential in code."""

        return cls(
            llm_base_url=os.getenv("LLM_BASE_URL", "").strip(),
            llm_api_key=os.getenv("LLM_API_KEY", "").strip(),
            llm_model=os.getenv("LLM_MODEL", "").strip(),
            spring_boot_base_url=os.getenv("SPRING_BOOT_BASE_URL", "").strip(),
        )

    def validate_llm(self) -> None:
        """Validate only the variables required for an LLM request."""

        missing = []
        if not self.llm_base_url:
            missing.append("LLM_BASE_URL")
        if not self.llm_api_key:
            missing.append("LLM_API_KEY")
        if not self.llm_model:
            missing.append("LLM_MODEL")
        if missing:
            raise ValueError(
                "Missing required environment variables: " + ", ".join(missing)
            )

    def validate_spring_boot(self) -> None:
        """Validate the Spring Boot endpoint only when a Tool needs it."""

        if not self.spring_boot_base_url:
            raise ValueError(
                "Missing required environment variable: SPRING_BOOT_BASE_URL"
            )

    def validate(self) -> None:
        """Keep the Phase 1 validation entry point for LLM callers."""

        self.validate_llm()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return one immutable settings object for the process lifetime."""

    return Settings.from_environment()
