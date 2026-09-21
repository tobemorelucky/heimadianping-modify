"""Read-only shop tools backed exclusively by Spring Boot HTTP APIs."""

from typing import Any, Dict

import httpx
from langchain_core.tools import ToolException, tool

from app.config import get_settings


@tool("get_shop_info")
def get_shop_info(shop_id: int) -> Dict[str, Any]:
    """Get a shop's id, name, type and address by its positive numeric ID."""

    if shop_id <= 0:
        raise ToolException("shop_id must be a positive integer")

    settings = get_settings()
    settings.validate_spring_boot()
    endpoint = (
        f"{settings.spring_boot_base_url.rstrip('/')}/api/ai/shop/{shop_id}"
    )

    try:
        # The Tool owns only an HTTP client and never receives database credentials.
        with httpx.Client(timeout=5.0) as client:
            response = client.get(endpoint)
            response.raise_for_status()
            payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise ToolException("Spring Boot shop service is unavailable") from exc

    if not isinstance(payload, dict):
        raise ToolException("Spring Boot returned an invalid response")
    if payload.get("success") is not True:
        error_message = payload.get("errorMsg") or "shop query failed"
        raise ToolException(str(error_message))

    data = payload.get("data")
    if not isinstance(data, dict):
        raise ToolException("Spring Boot returned no shop data")

    # Enforce the AI Tool response whitelist even if Java adds fields later.
    return {
        "id": data.get("id"),
        "name": data.get("name"),
        "type": data.get("type"),
        "address": data.get("address"),
    }
