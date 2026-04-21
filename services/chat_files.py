"""
챗봇 파일 첨부 — 업로드된 파일에서 텍스트를 추출한다.

MVP는 텍스트 기반 파일만 지원(txt/md/csv/json/코드 등).
PDF/docx/xlsx는 파서 의존성(pypdf, python-docx 등)이 미설치 → 추후 확장.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from config import (
    CHAT_FILE_ALLOWED_EXT,
    CHAT_FILE_MAX_BYTES,
    CHAT_FILE_TEXT_MAX_CHARS,
)

logger = logging.getLogger(__name__)


class FileExtractionError(ValueError):
    """파일 처리 실패. 라우터에서 400/413 로 변환."""


def _detect_extension(filename: str | None, content_type: str | None) -> str:
    """파일 확장자 추출 (소문자). 확장자 없으면 MIME에서 추론."""
    ext = ""
    if filename:
        _, dot_ext = os.path.splitext(filename)
        ext = dot_ext.lstrip(".").lower()
    if ext:
        return ext
    # MIME → 확장자 보정 (매우 제한적, 대표 케이스만)
    mime = (content_type or "").lower()
    if mime == "text/plain":
        return "txt"
    if mime == "text/markdown":
        return "md"
    if mime in ("text/csv", "application/csv"):
        return "csv"
    if mime in ("application/json", "text/json"):
        return "json"
    if mime in ("text/html", "application/xhtml+xml"):
        return "html"
    if mime == "text/xml" or mime == "application/xml":
        return "xml"
    return ext


def _decode_bytes(data: bytes) -> str:
    """UTF-8 우선, 실패 시 cp949/latin-1 fallback."""
    for enc in ("utf-8", "utf-8-sig", "cp949", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    # 마지막 safety net
    return data.decode("utf-8", errors="replace")


def extract_text(
    *,
    filename: str | None,
    content_type: str | None,
    data: bytes,
) -> dict[str, Any]:
    """
    업로드된 파일에서 텍스트를 추출한다.

    성공 시: { "ext": str, "text": str, "char_count": int, "truncated": bool }
    실패 시: FileExtractionError 발생.
    """
    if not data:
        raise FileExtractionError("빈 파일입니다.")

    size = len(data)
    if size > CHAT_FILE_MAX_BYTES:
        raise FileExtractionError(
            f"파일이 너무 큽니다 ({size} bytes, 최대 {CHAT_FILE_MAX_BYTES} bytes).",
        )

    ext = _detect_extension(filename, content_type)
    if not ext:
        raise FileExtractionError("확장자를 판단할 수 없습니다. 파일명을 확인해 주세요.")
    if ext not in CHAT_FILE_ALLOWED_EXT:
        raise FileExtractionError(
            f"지원하지 않는 파일 형식입니다(.{ext}). "
            f"지원: {', '.join(sorted(CHAT_FILE_ALLOWED_EXT))}",
        )

    text = _decode_bytes(data).replace("\r\n", "\n").replace("\r", "\n")
    text = text.strip()
    if not text:
        raise FileExtractionError("파일에서 텍스트를 추출하지 못했습니다.")

    truncated = False
    if len(text) > CHAT_FILE_TEXT_MAX_CHARS:
        text = text[:CHAT_FILE_TEXT_MAX_CHARS].rstrip() + "\n\n[...이하 생략...]"
        truncated = True

    return {
        "ext": ext,
        "text": text,
        "char_count": len(text),
        "truncated": truncated,
    }
