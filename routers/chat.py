"""
AI 챗봇 라우터 (종목 질의 + 세션/파일 영구 저장).

스트리밍:
  POST /api/chat
    body: { messages:[{role,content}], tickers?:[], session_id?:str, file_ids?:[] }
    response: text/event-stream (SSE)

세션 CRUD:
  GET    /api/chat/sessions?limit&offset   — 최신순 세션 목록
  POST   /api/chat/sessions                 — { title? } 세션 생성
  GET    /api/chat/sessions/{id}            — 세션 상세 + 메시지 히스토리
  DELETE /api/chat/sessions/{id}            — cascade 삭제

파일 첨부:
  POST /api/chat/files                     — multipart: file, form: session_id?
    response: { id, filename, size, content_type, char_count, truncated, session_id? }

기타:
  GET /api/chat/health
  GET /api/chat/extract-tickers?q=
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from config import (
    CHAT_FILE_MAX_BYTES,
    CHAT_FILES_PER_REQUEST_MAX,
    CHAT_MAX_HISTORY_MESSAGES,
    CHAT_MAX_TICKERS_PER_QUERY,
    CHAT_OPENAI_MODEL,
    CHAT_SESSIONS_LIST_LIMIT,
    CHAT_SESSION_TITLE_MAX_CHARS,
    OPENAI_API_KEY,
)
from services.chat import extract_tickers, stream_chat
from services.chat_files import FileExtractionError, extract_text
from services.chat_store import (
    create_session,
    delete_session,
    file_summary,
    get_files,
    get_session,
    get_session_messages,
    list_sessions,
    save_file,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["Chat"])


# ---------------------------------------------------------------------------
# 스트리밍 엔드포인트
# ---------------------------------------------------------------------------

class ChatMessage(BaseModel):
    role: str = Field(..., description="user | assistant")
    content: str = Field(..., min_length=1)


class ChatRequest(BaseModel):
    messages: list[ChatMessage] = Field(..., min_length=1)
    tickers: list[str] | None = Field(
        default=None,
        description=f"명시적 티커 목록(최대 {CHAT_MAX_TICKERS_PER_QUERY}개). 미지정 시 메시지에서 자동 추출.",
    )
    session_id: str | None = Field(
        default=None,
        description="연결할 세션 ID. 지정 시 user/assistant 메시지를 영구 저장.",
    )
    file_ids: list[str] | None = Field(
        default=None,
        description=f"첨부 파일 ID 목록 (최대 {CHAT_FILES_PER_REQUEST_MAX}개). POST /api/chat/files 로 선업로드.",
    )


@router.post("/chat")
async def api_chat(req: ChatRequest):
    """종목 질의 AI 챗봇 (SSE 스트리밍)."""
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=503, detail="OPENAI_API_KEY가 설정되지 않았습니다.")

    last = req.messages[-1]
    if last.role.lower() != "user":
        raise HTTPException(status_code=400, detail="마지막 메시지는 role=user 이어야 합니다.")

    # tickers override 정규화
    tickers_override: list[str] | None = None
    if req.tickers:
        cleaned = [t.strip().upper() for t in req.tickers if t and t.strip()]
        tickers_override = cleaned[:CHAT_MAX_TICKERS_PER_QUERY] if cleaned else None

    # session_id 검증
    session_id = (req.session_id or "").strip() or None
    if session_id:
        session = await asyncio.to_thread(get_session, session_id)
        if session is None:
            raise HTTPException(status_code=404, detail=f"세션을 찾을 수 없습니다: {session_id}")

    # file_ids → 파일 레코드 로드
    attachments: list[dict[str, Any]] | None = None
    if req.file_ids:
        ids = [fid.strip() for fid in req.file_ids if fid and fid.strip()]
        ids = ids[:CHAT_FILES_PER_REQUEST_MAX]
        if ids:
            try:
                attachments = await asyncio.to_thread(get_files, ids)
            except Exception as e:
                logger.warning("file_ids 로드 실패: %s", e, exc_info=True)
                raise HTTPException(status_code=500, detail="첨부 파일 조회 실패")
            found_ids = {a["id"] for a in attachments or []}
            missing = [fid for fid in ids if fid not in found_ids]
            if missing:
                raise HTTPException(
                    status_code=404,
                    detail=f"존재하지 않는 파일 ID: {', '.join(missing)}",
                )

    messages_payload: list[dict[str, Any]] = [
        {"role": m.role, "content": m.content} for m in req.messages
    ]

    async def _generate():
        async for event in stream_chat(
            messages_payload,
            tickers_override=tickers_override,
            session_id=session_id,
            attachments=attachments,
        ):
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


# ---------------------------------------------------------------------------
# 세션 CRUD
# ---------------------------------------------------------------------------

class CreateSessionRequest(BaseModel):
    title: str | None = Field(default=None, max_length=CHAT_SESSION_TITLE_MAX_CHARS)


@router.get("/chat/sessions")
async def api_chat_list_sessions(limit: int = CHAT_SESSIONS_LIST_LIMIT, offset: int = 0):
    """세션 목록 (updated_at DESC)."""
    try:
        items = await asyncio.to_thread(list_sessions, limit, offset)
    except Exception as e:
        logger.exception("세션 목록 조회 실패: %s", e)
        raise HTTPException(status_code=500, detail="세션 목록 조회 실패")
    return {"items": items, "count": len(items), "limit": limit, "offset": offset}


@router.post("/chat/sessions")
async def api_chat_create_session(body: CreateSessionRequest | None = None):
    """새 세션 생성."""
    title = (body.title if body else None) or None
    try:
        session = await asyncio.to_thread(create_session, title)
    except Exception as e:
        logger.exception("세션 생성 실패: %s", e)
        raise HTTPException(status_code=500, detail="세션 생성 실패")
    return session


@router.get("/chat/sessions/{session_id}")
async def api_chat_get_session(session_id: str, message_limit: int | None = None):
    """세션 메타 + 메시지 히스토리."""
    session_id = (session_id or "").strip()
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id가 비어 있습니다.")
    session = await asyncio.to_thread(get_session, session_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"세션을 찾을 수 없습니다: {session_id}")
    messages = await asyncio.to_thread(get_session_messages, session_id, message_limit)
    return {"session": session, "messages": messages, "count": len(messages)}


@router.delete("/chat/sessions/{session_id}")
async def api_chat_delete_session(session_id: str):
    """세션 삭제 (메시지 CASCADE)."""
    session_id = (session_id or "").strip()
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id가 비어 있습니다.")
    deleted = await asyncio.to_thread(delete_session, session_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"세션을 찾을 수 없습니다: {session_id}")
    return {"deleted": True, "session_id": session_id}


# ---------------------------------------------------------------------------
# 파일 업로드
# ---------------------------------------------------------------------------

@router.post("/chat/files")
async def api_chat_upload_file(
    file: UploadFile = File(...),
    session_id: str | None = Form(default=None),
):
    """
    파일 업로드 → 텍스트 추출 → Supabase(chat_files) 저장.
    session_id 는 선택 (미연결 파일도 허용).

    지원 확장자: config.CHAT_FILE_ALLOWED_EXT (txt/md/csv/json/log/코드 등).
    """
    raw = await file.read()
    if len(raw) == 0:
        raise HTTPException(status_code=400, detail="빈 파일입니다.")
    if len(raw) > CHAT_FILE_MAX_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"파일이 너무 큽니다 (최대 {CHAT_FILE_MAX_BYTES} bytes)",
        )

    try:
        extracted = extract_text(
            filename=file.filename,
            content_type=file.content_type,
            data=raw,
        )
    except FileExtractionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("파일 추출 실패 (%s): %s", file.filename, e)
        raise HTTPException(status_code=500, detail="파일 추출 실패")

    sid = (session_id or "").strip() or None
    if sid:
        session = await asyncio.to_thread(get_session, sid)
        if session is None:
            raise HTTPException(status_code=404, detail=f"세션을 찾을 수 없습니다: {sid}")

    try:
        row = await asyncio.to_thread(
            save_file,
            filename=file.filename or "unnamed",
            content_type=file.content_type,
            size_bytes=len(raw),
            extracted_text=extracted["text"],
            session_id=sid,
        )
    except Exception as e:
        logger.exception("파일 저장 실패: %s", e)
        raise HTTPException(status_code=500, detail="파일 저장 실패")

    return {
        **file_summary(row),
        "ext": extracted["ext"],
        "truncated": extracted["truncated"],
    }


# ---------------------------------------------------------------------------
# 기타
# ---------------------------------------------------------------------------

@router.get("/chat/health")
async def api_chat_health():
    """챗봇 설정/의존성 상태 확인 (디버그용)."""
    return {
        "openai_configured": bool(OPENAI_API_KEY),
        "model": CHAT_OPENAI_MODEL,
        "max_tickers_per_query": CHAT_MAX_TICKERS_PER_QUERY,
        "max_history_messages": CHAT_MAX_HISTORY_MESSAGES,
        "max_files_per_request": CHAT_FILES_PER_REQUEST_MAX,
        "file_max_bytes": CHAT_FILE_MAX_BYTES,
    }


@router.get("/chat/extract-tickers")
async def api_chat_extract_tickers(q: str):
    """(디버그/프론트 힌트용) 질의에서 티커만 추출."""
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="q 파라미터가 필요합니다.")
    return {"query": q, "tickers": extract_tickers(q)}
