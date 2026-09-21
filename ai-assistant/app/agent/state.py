"""State definition shared by nodes in the AI Assistant graph."""

from typing import Annotated, List, TypedDict

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages


class AgentState(TypedDict):
    """Per-request state for the stateless Agent and Tool loop."""

    message: str
    answer: str
    # This list exists only during one request and is not persistent Agent Memory.
    messages: Annotated[List[AnyMessage], add_messages]
