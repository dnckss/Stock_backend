"""
챗봇 세션·메시지·파일 영구 저장 (Supabase).

기존 `services/crud.py` 패턴을 따라 sync supabase-py client를 사용한다.
테이블은 `supabase_schema.sql`에 정의되어 있으며 Supabase SQL Editor에서 수동으로 생성한다.

데이터 모델:
  chat_sessions(id, title, last_message_preview, message_count, created_at, updated_at)
  chat_messages(id, session_id FK CASCADE, role, content, attachments_json, created_at)
  chat_files(id, session_id FK nullable, filename, content_type, size_bytes,
             extracted_text, char_count, created_at)
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from services.crud import _get_client, sanitize_for_json
from config import (
    CHAT_SESSION_MESSAGE_LIMIT,
    CHAT_SESSION_PREVIEW_MAX_CHARS,
    CHAT_SESSION_TITLE_MAX_CHARS,
    CHAT_SESSIONS_LIST_LIMIT,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 내부 유틸
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return uuid.uuid4().hex


def _truncate(text: str | None, limit: int) -> str | None:
    if text is None:
        return None
    t = text.strip()
    if not t:
        return None
    if len(t) <= limit:
        return t
    return t[:limit].rstrip() + "…"


def _first_line(text: str | None) -> str:
    if not text:
        return ""
    for line in text.splitlines():
        s = line.strip()
        if s:
            return s
    return text.strip()


# ---------------------------------------------------------------------------
# 세션
# ---------------------------------------------------------------------------

def create_session(title: str | None = None) -> dict[str, Any]:
    """새 세션 생성. title 미지정 시 '새 채팅'."""
    client = _get_client()
    now = _now_iso()
    row = {
        "id": _new_id(),
        "title": _truncate(title, CHAT_SESSION_TITLE_MAX_CHARS) or "새 채팅",
        "last_message_preview": None,
        "message_count": 0,
        "created_at": now,
        "updated_at": now,
    }
    client.table("chat_sessions").insert(row).execute()
    return sanitize_for_json(row)


def list_sessions(limit: int | None = None, offset: int = 0) -> list[dict[str, Any]]:
    """세션 목록 (최신 updated_at 기준 DESC)."""
    client = _get_client()
    safe_limit = max(1, min(limit or CHAT_SESSIONS_LIST_LIMIT, 200))
    safe_offset = max(0, offset)
    resp = (
        client.table("chat_sessions")
        .select("id, title, last_message_preview, message_count, created_at, updated_at")
        .order("updated_at", desc=True)
        .range(safe_offset, safe_offset + safe_limit - 1)
        .execute()
    )
    return sanitize_for_json(resp.data or [])


def get_session(session_id: str) -> dict[str, Any] | None:
    """세션 메타 조회 (메시지는 별도 함수)."""
    if not session_id:
        return None
    client = _get_client()
    resp = (
        client.table("chat_sessions")
        .select("*")
        .eq("id", session_id)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    return sanitize_for_json(rows[0]) if rows else None


def get_session_messages(session_id: str, limit: int | None = None) -> list[dict[str, Any]]:
    """세션의 메시지를 생성 순(오래된 → 최신)으로 반환."""
    if not session_id:
        return []
    client = _get_client()
    safe_limit = max(1, min(limit or CHAT_SESSION_MESSAGE_LIMIT, 1000))
    resp = (
        client.table("chat_messages")
        .select("id, session_id, role, content, attachments_json, created_at")
        .eq("session_id", session_id)
        .order("created_at", desc=False)
        .limit(safe_limit)
        .execute()
    )
    return sanitize_for_json(resp.data or [])


def delete_session(session_id: str) -> bool:
    """세션 삭제 (CASCADE로 메시지 함께 제거, 파일은 session_id만 null로)."""
    if not session_id:
        return False
    client = _get_client()
    resp = client.table("chat_sessions").delete().eq("id", session_id).execute()
    deleted = bool(resp.data)
    if not deleted:
        logger.debug("세션 삭제 시도 — 존재하지 않음: %s", session_id)
    return deleted


def update_session_touch(
    session_id: str,
    *,
    last_message_preview: str | None = None,
    increment_count: int = 0,
    title: str | None = None,
) -> None:
    """세션의 updated_at + 요약 필드를 갱신한다."""
    if not session_id:
        return
    client = _get_client()
    patch: dict[str, Any] = {"updated_at": _now_iso()}
    if last_message_preview is not None:
        patch["last_message_preview"] = _truncate(
            last_message_preview, CHAT_SESSION_PREVIEW_MAX_CHARS
        )
    if title is not None:
        patch["title"] = _truncate(title, CHAT_SESSION_TITLE_MAX_CHARS) or "새 채팅"

    if increment_count:
        current = get_session(session_id)
        base = int((current or {}).get("message_count") or 0)
        patch["message_count"] = base + increment_count

    try:
        client.table("chat_sessions").update(patch).eq("id", session_id).execute()
    except Exception as e:
        logger.warning("세션 갱신 실패 (%s): %s", session_id, e)


# ---------------------------------------------------------------------------
# 메시지
# ---------------------------------------------------------------------------

def append_message(
    session_id: str,
    role: str,
    content: str,
    attachments: list[dict[str, Any]] | None = None,
    *,
    auto_title: bool = True,
) -> dict[str, Any] | None:
    """
    메시지 저장 + 세션 메타 갱신.

    - role: 'user' | 'assistant'
    - 세션이 존재하지 않으면 None 반환 (호출측 로깅만)
    - auto_title=True: 세션 제목이 '새 채팅'이고 첫 user 메시지면 본문 앞줄로 대체
    """
    if not session_id or role not in ("user", "assistant") or not content:
        return None

    session = get_session(session_id)
    if session is None:
        logger.debug("append_message: 세션 없음 %s", session_id)
        return None

    client = _get_client()
    row = {
        "id": _new_id(),
        "session_id": session_id,
        "role": role,
        "content": content,
        "attachments_json": attachments or None,
        "created_at": _now_iso(),
    }
    try:
        client.table("chat_messages").insert(row).execute()
    except Exception as e:
        logger.warning("메시지 저장 실패 (%s): %s", session_id, e)
        return None

    # 세션 메타 업데이트
    new_title: str | None = None
    if (
        auto_title
        and role == "user"
        and session.get("title") in (None, "", "새 채팅")
        and int(session.get("message_count") or 0) == 0
    ):
        new_title = _first_line(content)

    update_session_touch(
        session_id,
        last_message_preview=content,
        increment_count=1,
        title=new_title,
    )
    return sanitize_for_json(row)


# ---------------------------------------------------------------------------
# 파일
# ---------------------------------------------------------------------------

def save_file(
    *,
    filename: str,
    content_type: str | None,
    size_bytes: int,
    extracted_text: str,
    session_id: str | None = None,
) -> dict[str, Any]:
    """업로드된 파일 메타와 추출 텍스트를 저장하고 메타를 반환."""
    client = _get_client()
    row = {
        "id": _new_id(),
        "session_id": session_id,
        "filename": filename or "unnamed",
        "content_type": content_type,
        "size_bytes": int(size_bytes or 0),
        "extracted_text": extracted_text or "",
        "char_count": len(extracted_text or ""),
        "created_at": _now_iso(),
    }
    client.table("chat_files").insert(row).execute()
    return sanitize_for_json(row)


def get_files(file_ids: list[str]) -> list[dict[str, Any]]:
    """파일 ID 목록 → 메타 + extracted_text 조회. 존재하지 않는 ID는 조용히 스킵."""
    if not file_ids:
        return []
    client = _get_client()
    resp = (
        client.table("chat_files")
        .select("id, session_id, filename, content_type, size_bytes, extracted_text, char_count, created_at")
        .in_("id", list(file_ids))
        .execute()
    )
    return sanitize_for_json(resp.data or [])


def file_summary(row: dict[str, Any]) -> dict[str, Any]:
    """외부 응답용 — 추출 본문을 제외한 메타만."""
    return {
        "id": row.get("id"),
        "session_id": row.get("session_id"),
        "filename": row.get("filename"),
        "content_type": row.get("content_type"),
        "size_bytes": row.get("size_bytes"),
        "char_count": row.get("char_count"),
        "created_at": row.get("created_at"),
    }
