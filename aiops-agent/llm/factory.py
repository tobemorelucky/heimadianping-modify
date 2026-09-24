"""Build an LLM provider from application settings."""

from config import Settings
from llm.mock import MockLLMProvider
from llm.provider import LLMConfigurationError, OpenAICompatibleProvider, StructuredLLMProvider


def build_llm_provider(settings: Settings) -> StructuredLLMProvider:
    if settings.ai_model_provider == "mock":
        return MockLLMProvider()

    api_key = (
        settings.ai_model_api_key.get_secret_value()
        if settings.ai_model_api_key is not None
        else ""
    )
    if not settings.ai_model_base_url or not api_key or not settings.ai_model_name:
        raise LLMConfigurationError(
            "AI_MODEL_BASE_URL, AI_MODEL_API_KEY, and AI_MODEL_NAME are required "
            "when AI_MODEL_PROVIDER=openai_compatible"
        )
    return OpenAICompatibleProvider(
        base_url=settings.ai_model_base_url,
        api_key=api_key,
        model=settings.ai_model_name,
        thinking=settings.ai_model_thinking,
        timeout_seconds=settings.ai_model_timeout_seconds,
        operation_timeouts={
            "plan": (
                settings.ai_model_planner_timeout_seconds
                or settings.ai_model_timeout_seconds
            ),
            "reflect": (
                settings.ai_model_reflection_timeout_seconds
                or settings.ai_model_timeout_seconds
            ),
            "report": (
                settings.ai_model_reporter_timeout_seconds
                or settings.ai_model_timeout_seconds
            ),
        },
    )
