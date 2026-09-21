"""Tests for the Spring Boot-backed shop Tool."""

import os
import unittest
from unittest.mock import MagicMock, patch

from app.config import get_settings
from app.tools.shop_tools import get_shop_info


class ShopToolTest(unittest.TestCase):
    """Verify HTTP delegation and response-field filtering without a real Java service."""

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

    @patch("app.tools.shop_tools.httpx.Client")
    def test_should_call_spring_boot_and_filter_fields(self, client_class) -> None:
        response = MagicMock()
        response.json.return_value = {
            "success": True,
            "data": {
                "id": 1,
                "name": "测试店铺",
                "type": "美食",
                "address": "测试路 1 号",
                "images": "must-not-be-exposed.jpg",
            },
        }
        client = client_class.return_value.__enter__.return_value
        client.get.return_value = response

        result = get_shop_info.invoke({"shop_id": 1})

        self.assertEqual(
            result,
            {
                "id": 1,
                "name": "测试店铺",
                "type": "美食",
                "address": "测试路 1 号",
            },
        )
        client.get.assert_called_once_with(
            "http://spring-boot.test/api/ai/shop/1"
        )
        response.raise_for_status.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
