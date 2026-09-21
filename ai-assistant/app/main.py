"""FastAPI entry point for the standalone AI Assistant service."""

import logging

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.agent.graph import agent_graph

logger = logging.getLogger(__name__)


class ChatRequest(BaseModel):
    """Request payload accepted by the chat endpoint."""

    message: str = Field(min_length=1, max_length=4000)


class ChatResponse(BaseModel):
    """Successful response returned by the chat endpoint."""

    answer: str


app = FastAPI(
    title="HM-DianPing Plus AI Assistant",
    version="0.1.0",
    description="Standalone FastAPI and LangGraph service for the AI Assistant.",
)


@app.post("/chat", response_model=ChatResponse)
def chat(request: ChatRequest) -> ChatResponse:
    """Run one stateless user message through the LangGraph workflow."""

    if not request.message.strip():
        raise HTTPException(status_code=422, detail="message must not be blank")

    try:
        result = agent_graph.invoke(
            {
                "message": request.message.strip(),
                "answer": "",
                "messages": [],
            }
        )
        return ChatResponse(answer=result["answer"])
    except ValueError as exc:
        # Configuration errors are safe to expose because they never include values.
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        # Provider details stay in server logs instead of leaking through the API.
        logger.exception("LLM request failed")
        raise HTTPException(
            status_code=502,
            detail="LLM service is temporarily unavailable",
        ) from exc
