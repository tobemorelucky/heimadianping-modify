"""Stateless Agent -> Tool -> Agent LangGraph workflow."""

from functools import lru_cache
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition

from app.agent.state import AgentState
from app.config import get_settings
from app.tools import (
    get_coupon_statistics,
    get_order_statistics,
    get_shop_info,
)

TOOLS = [get_shop_info, get_order_statistics, get_coupon_statistics]

AGENT_INSTRUCTIONS = """你是 HM-DianPing 商家运营分析助手。
只能通过已提供的只读工具获取业务事实；不得臆造数据。
当用户要求分析店铺经营情况时，必须同时调用订单统计和优惠券统计工具，
再基于两个工具的结果给出简洁分析。订单指标是最近7天口径，优惠券指标是累计口径，
回答时必须准确说明各自时间范围。"""


@lru_cache(maxsize=1)
def _get_llm() -> ChatOpenAI:
    """Create the OpenAI-compatible DeepSeek client from environment settings."""

    settings = get_settings()
    settings.validate_llm()
    return ChatOpenAI(
        base_url=settings.llm_base_url,
        api_key=settings.llm_api_key,
        model=settings.llm_model,
        temperature=0,
    )


def _content_as_text(content: Any) -> str:
    """Normalize provider response content into the API's string contract."""

    if isinstance(content, str):
        return content
    return str(content)


@lru_cache(maxsize=1)
def _get_agent_llm():
    """Bind the allowlisted tools to the configured chat model once."""

    return _get_llm().bind_tools(TOOLS)


def agent_node(state: AgentState):
    """Let the model request a Tool or produce the final natural-language answer."""

    current_messages = state.get("messages", [])
    first_turn = not current_messages
    model_messages = current_messages or [
        SystemMessage(content=AGENT_INSTRUCTIONS),
        HumanMessage(content=state["message"]),
    ]
    response = _get_agent_llm().invoke(model_messages)

    # Preserve the initial user message so the second Agent pass has full context.
    messages_to_add = [
        SystemMessage(content=AGENT_INSTRUCTIONS),
        HumanMessage(content=state["message"]),
        response,
    ] \
        if first_turn else [response]
    update = {"messages": messages_to_add}
    if not response.tool_calls:
        update["answer"] = _content_as_text(response.content)
    return update

def build_graph():
    """Compile the Agent loop with the allowlisted Spring Boot HTTP Tools."""

    builder = StateGraph(AgentState)
    builder.add_node("agent", agent_node)
    builder.add_node("tools", ToolNode(TOOLS))
    builder.add_edge(START, "agent")
    builder.add_conditional_edges(
        "agent",
        tools_condition,
        {
            "tools": "tools",
            "__end__": END,
        },
    )
    builder.add_edge("tools", "agent")
    return builder.compile()


agent_graph = build_graph()
