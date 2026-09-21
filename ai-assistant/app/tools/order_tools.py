"""Read-only order statistics Tool backed by the Spring Boot AI API."""

from typing import Any, Dict

import httpx
from langchain_core.tools import ToolException, tool

from app.config import get_settings


@tool("get_order_statistics")
def get_order_statistics(shop_id: int) -> Dict[str, Any]:
    """Get a shop's order count and transaction amount for the most recent 7 days."""

    if shop_id <= 0:
        raise ToolException("shop_id must be a positive integer")

    settings = get_settings()
    settings.validate_spring_boot()
    endpoint = (
        f"{settings.spring_boot_base_url.rstrip('/')}/api/ai/shop/{shop_id}/orders"
    )

    try:
        # Business data is obtained only from the allowlisted Spring Boot endpoint.
        with httpx.Client(timeout=5.0) as client:
            response = client.get(endpoint)
            response.raise_for_status()
            payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise ToolException("Spring Boot order statistics service is unavailable") from exc

    if not isinstance(payload, dict):
        raise ToolException("Spring Boot returned an invalid response")
    if payload.get("success") is not True:
        error_message = payload.get("errorMsg") or "order statistics query failed"
        raise ToolException(str(error_message))

    data = payload.get("data")
    if not isinstance(data, dict):
        raise ToolException("Spring Boot returned no order statistics")

    # Keep the Tool contract stable when the Java DTO grows additional fields.
    return {
        "orderCount": data.get("orderCount"),
        "transactionAmount": data.get("transactionAmount"),
    }
