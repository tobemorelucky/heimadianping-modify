"""Read-only coupon statistics Tool backed by the Spring Boot AI API."""

from typing import Any, Dict

import httpx
from langchain_core.tools import ToolException, tool

from app.config import get_settings


@tool("get_coupon_statistics")
def get_coupon_statistics(shop_id: int) -> Dict[str, Any]:
    """Get a shop's cumulative issued/used coupon counts and usage-rate percentage."""

    if shop_id <= 0:
        raise ToolException("shop_id must be a positive integer")

    settings = get_settings()
    settings.validate_spring_boot()
    endpoint = (
        f"{settings.spring_boot_base_url.rstrip('/')}/api/ai/shop/{shop_id}/coupons"
    )

    try:
        # The Python service never bypasses Spring Boot to access MySQL directly.
        with httpx.Client(timeout=5.0) as client:
            response = client.get(endpoint)
            response.raise_for_status()
            payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise ToolException("Spring Boot coupon statistics service is unavailable") from exc

    if not isinstance(payload, dict):
        raise ToolException("Spring Boot returned an invalid response")
    if payload.get("success") is not True:
        error_message = payload.get("errorMsg") or "coupon statistics query failed"
        raise ToolException(str(error_message))

    data = payload.get("data")
    if not isinstance(data, dict):
        raise ToolException("Spring Boot returned no coupon statistics")

    # Expose only the metrics approved for the Agent.
    return {
        "issuedCount": data.get("issuedCount"),
        "usedCount": data.get("usedCount"),
        "usageRate": data.get("usageRate"),
    }
