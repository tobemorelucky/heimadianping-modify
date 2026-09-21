"""Tests for the Spring Boot-backed order statistics Tool."""

import os
import unittest
from unittest.mock import MagicMock, patch

from app.config import get_settings
from app.tools.order_tools import get_order_statistics


class OrderStatisticsToolTest(unittest.TestCase):
    """Verify endpoint delegation and the stable order metric contract."""

    def setUp(self) -> None:
        self.previous_base_url = os.environ.get("SPRING_BOOT_BASE_URL")
        os.environ["SPRING_BOOT_BASE_URL"] = "http://spring-boot.test"
        get_settings.cache_clear()

    def tearDown(self) -> None:
        if self.previous_base_url is None:
            os.environ.pop("SPRING_BOOT_BASE_URL", None)
        else:
            os.environ["SPRING_BOOT_BASE_URL"] = self.previous_base_url
        get_settings.cache_clear()

    @patch("app.tools.order_tools.httpx.Client")
    def test_should_call_order_statistics_endpoint(self, client_class) -> None:
        response = MagicMock()
        response.json.return_value = {
            "success": True,
            "data": {
                "orderCount": 8,
                "transactionAmount": 128.50,
                "internalMetric": "must-not-be-exposed",
            },
        }
        client = client_class.return_value.__enter__.return_value
        client.get.return_value = response

        result = get_order_statistics.invoke({"shop_id": 1})

        self.assertEqual(
            result,
            {"orderCount": 8, "transactionAmount": 128.50},
        )
        client.get.assert_called_once_with(
            "http://spring-boot.test/api/ai/shop/1/orders"
        )
        response.raise_for_status.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
