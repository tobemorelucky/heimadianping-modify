"""Tests for the Spring Boot-backed coupon statistics Tool."""

import os
import unittest
from unittest.mock import MagicMock, patch

from app.config import get_settings
from app.tools.coupon_tools import get_coupon_statistics


class CouponStatisticsToolTest(unittest.TestCase):
    """Verify endpoint delegation and the stable coupon metric contract."""

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

    @patch("app.tools.coupon_tools.httpx.Client")
    def test_should_call_coupon_statistics_endpoint(self, client_class) -> None:
        response = MagicMock()
        response.json.return_value = {
            "success": True,
            "data": {
                "issuedCount": 20,
                "usedCount": 5,
                "usageRate": 25.00,
                "internalMetric": "must-not-be-exposed",
            },
        }
        client = client_class.return_value.__enter__.return_value
        client.get.return_value = response

        result = get_coupon_statistics.invoke({"shop_id": 1})

        self.assertEqual(
            result,
            {"issuedCount": 20, "usedCount": 5, "usageRate": 25.00},
        )
        client.get.assert_called_once_with(
            "http://spring-boot.test/api/ai/shop/1/coupons"
        )
        response.raise_for_status.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
