"""LLM provider abstraction with strict JSON and Pydantic validation."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any, TypeVar

import httpx
from pydantic import BaseModel, ValidationError


StructuredOutputT = TypeVar("StructuredOutputT", bound=BaseModel)


class LLMProviderError(RuntimeError):
    """Base exception for an LLM provider failure."""


class LLMConfigurationError(LLMProviderError):
    """Raised when a provider is selected without required configuration."""


class LLMResponseValidationError(LLMProviderError):
    """Raised when a model response is not valid structured output."""


class StructuredLLMProvider(ABC):
    """Generate and validate one JSON object against a Pydantic model."""

    def generate_structured(
        self,
        *,
        operation: str,
        system_prompt: str,
        input_payload: dict[str, Any],
        response_model: type[StructuredOutputT],
    ) -> StructuredOutputT:
        raw_response = self._complete(
            operation=operation,
            system_prompt=system_prompt,
            input_payload=input_payload,
            response_schema=response_model.model_json_schema(),
        )
        if not isinstance(raw_response, str):
            raise LLMResponseValidationError("LLM response must be a JSON string")

        try:
            payload = json.loads(raw_response)
        except json.JSONDecodeError as exc:
            raise LLMResponseValidationError("LLM response is not valid JSON") from exc

        if not isinstance(payload, dict):
            raise LLMResponseValidationError("LLM response must be one JSON object")

        try:
            return response_model.model_validate(payload)
        except ValidationError as exc:
            raise LLMResponseValidationError(
                f"LLM response does not match {response_model.__name__}"
            ) from exc

    @abstractmethod
    def _complete(
        self,
        *,
        operation: str,
        system_prompt: str,
        input_payload: dict[str, Any],
        response_schema: dict[str, Any],
    ) -> str:
        """Return the raw assistant content."""


class OpenAICompatibleProvider(StructuredLLMProvider):
    """Minimal client for OpenAI-compatible chat completion endpoints."""

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        timeout_seconds: float = 30.0,
        client: httpx.Client | None = None,
    ) -> None:
        if not base_url.strip() or not api_key.strip() or not model.strip():
            raise LLMConfigurationError(
                "OpenAI-compatible provider requires base_url, api_key, and model"
            )
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._model = model
        self._client = client or httpx.Client(timeout=timeout_seconds)

    def _complete(
        self,
        *,
        operation: str,
        system_prompt: str,
        input_payload: dict[str, Any],
        response_schema: dict[str, Any],
    ) -> str:
        user_message = json.dumps(
            {
                "operation": operation,
                "input": input_payload,
                "output_schema": response_schema,
                "instruction": "Return exactly one JSON object. Do not use markdown fences.",
            },
            ensure_ascii=False,
            default=str,
        )
        try:
            response = self._client.post(
                f"{self._base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self._model,
                    "temperature": 0,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                f"{system_prompt}\n"
                                "Your entire response must be a single valid JSON object."
                            ),
                        },
                        {"role": "user", "content": user_message},
                    ],
                },
            )
            response.raise_for_status()
            response_payload = response.json()
            content = response_payload["choices"][0]["message"]["content"]
        except (httpx.HTTPError, KeyError, IndexError, TypeError, ValueError) as exc:
            raise LLMProviderError("OpenAI-compatible API request failed") from exc

        if not isinstance(content, str):
            raise LLMProviderError("OpenAI-compatible API returned no text content")
        return content
