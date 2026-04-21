"""
AI 챗봇 라우터 (종목 질의).

POST /api/chat
  body: { "messages": [ { "role": "user"|"assistant", "content": str }, ... ],
          "tickers"?: ["NVDA", ...] }
  response: text/event-stream (SSE)

GET /api/chat/health
  챗봇 관련 설정·OpenAI 키 존재 여부를 간단히 확인한다.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from config import (
    CHAT_MAX_HISTORY_MESSAGES,
    CHAT_MAX_TICKERS_PER_QUERY,
    CHAT_OPENAI_MODEL,
    OPENAI_API_KEY,
)
from services.chat import extract_tickers, stream_chat

router = APIRouter(prefix="/api", tags=["Chat"])


class ChatMessage(BaseModel):
    role: str = Field(..., description="user | assistant")
    content: str = Field(..., min_length=1)


class ChatRequest(BaseModel):
    messages: list[ChatMessage] = Field(..., min_length=1)
    tickers: list[str] | None = Field(
        default=None,
        description=f"명시적 티커 목록(최대 {CHAT_MAX_TICKERS_PER_QUERY}개). 미지정 시 메시지에서 자동 추출.",
    )


@router.post("/chat")
async def api_chat(req: ChatRequest):
    """
    종목 질의 AI 챗봇 (SSE 스트리밍).

    이벤트 타입:
      - start   : 파이프라인 시작
      - context : 수집된 컨텍스트 요약(종목 시세, 전략 존재 여부 등)
      - token   : LLM 응답 토큰 (delta)
      - done    : 최종 전체 답변 + 경과 시간
      - error   : 오류
    """
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=503, detail="OPENAI_API_KEY가 설정되지 않았습니다.")

    # 마지막 메시지가 user 인지 검증
    last = req.messages[-1]
    if last.role.lower() != "user":
        raise HTTPException(status_code=400, detail="마지막 메시지는 role=user 이어야 합니다.")

    # tickers override 정규화
    tickers_override: list[str] | None = None
    if req.tickers:
        cleaned = [t.strip().upper() for t in req.tickers if t and t.strip()]
        tickers_override = cleaned[:CHAT_MAX_TICKERS_PER_QUERY] if cleaned else None

    messages_payload: list[dict[str, Any]] = [
        {"role": m.role, "content": m.content} for m in req.messages
    ]

    async def _generate():
        async for event in stream_chat(messages_payload, tickers_override=tickers_override):
            yield event

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream; charset=utf-8",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/chat/health")
async def api_chat_health():
    """챗봇 설정/의존성 상태 확인 (디버그용)."""
    return {
        "openai_configured": bool(OPENAI_API_KEY),
        "model": CHAT_OPENAI_MODEL,
        "max_tickers_per_query": CHAT_MAX_TICKERS_PER_QUERY,
        "max_history_messages": CHAT_MAX_HISTORY_MESSAGES,
    }


@router.get("/chat/extract-tickers")
async def api_chat_extract_tickers(q: str):
    """(디버그/프론트 힌트용) 질의에서 티커만 추출."""
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="q 파라미터가 필요합니다.")
    return {"query": q, "tickers": extract_tickers(q)}
