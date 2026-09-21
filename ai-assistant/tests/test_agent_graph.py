"""Tests for one complete Agent -> Tool -> Agent cycle."""

import os
import unittest
from unittest.mock import MagicMock, patch

from langchain_core.messages import AIMessage, ToolMessage

from app.agent import graph as graph_module
from app.config import get_settings


class SequencedAgentModel:
    """Return two business Tool Calls followed by one analytical answer."""

    def __init__(self) -> None:
        self.call_count = 0

    def invoke(self, messages):
        self.call_count += 1
        if self.call_count == 1:
            return AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "get_order_statistics",
                        "args": {"shop_id": 1},
                        "id": "order-call-1",
                        "type": "tool_call",
                    },
                    {
                        "name": "get_coupon_statistics",
                        "args": {"shop_id": 1},
                        "id": "coupon-call-1",
                        "type": "tool_call",
                    }
                ],
            )
        return AIMessage(content="店铺1近7天有8笔订单，优惠券使用率为25%。")


class AgentGraphTest(unittest.TestCase):
    """Verify the graph executes the real Tool node before answering."""

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
    @patch("app.tools.order_tools.httpx.Client")
    def test_should_execute_multiple_tool_calls(
        self, order_client_class, coupon_client_class
    ) -> None:
        order_response = MagicMock()
        order_response.json.return_value = {
            "success": True,
            "data": {
                "orderCount": 8,
                "transactionAmount": 128.50,
            },
        }
        coupon_response = MagicMock()
        coupon_response.json.return_value = {
            "success": True,
            "data": {
                "issuedCount": 20,
                "usedCount": 5,
                "usageRate": 25.00,
            },
        }
        order_client_class.return_value.__enter__.return_value.get.return_value = (
            order_response
        )
        coupon_client_class.return_value.__enter__.return_value.get.return_value = (
            coupon_response
        )
        fake_model = SequencedAgentModel()

        with patch.object(graph_module, "_get_agent_llm", return_value=fake_model):
            result = graph_module.agent_graph.invoke(
                {
                    "message": "分析一下店铺1最近经营情况",
                    "answer": "",
                    "messages": [],
                }
            )

        self.assertEqual(fake_model.call_count, 2)
        self.assertEqual(
            result["answer"],
            "店铺1近7天有8笔订单，优惠券使用率为25%。",
        )
        tool_messages = [
            message for message in result["messages"]
            if isinstance(message, ToolMessage)
        ]
        self.assertEqual(2, len(tool_messages))


if __name__ == "__main__":
    unittest.main()
