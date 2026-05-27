"""공용 헬퍼 — 여러 모듈에서 중복되던 로직의 단일 출처(SSOT).

- ``normalize_ticker``    : 티커 정규화(공백 제거 + 대문자)
- ``make_openai_client``  : OpenAI 클라이언트 생성(키 없으면 None) — 5개+ 모듈의 중복 제거
- ``spawn_logged``        : fire-and-forget asyncio 태스크의 예외를 삼키지 않고 로깅
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

from openai import OpenAI

from config import OPENAI_API_KEY

logger = logging.getLogger(__name__)


def normalize_ticker(ticker: str | None) -> str:
    """티커 정규화 — 앞뒤 공백 제거 + 대문자. 빈/None 입력은 빈 문자열."""
    return (ticker or "").strip().upper()


def make_openai_client(
    *,
    timeout: float | int | None = None,
    max_retries: int = 2,
) -> OpenAI | None:
    """OpenAI 클라이언트 생성 단일 출처.

    API 키가 없으면 ``None`` 을 반환한다(호출부는 None 일 때 폴백 경로를 타야 한다).
    ``timeout`` 미지정 시 SDK 기본값을 사용한다.
    """
    if not OPENAI_API_KEY:
        return None
    kwargs: dict[str, Any] = {"api_key": OPENAI_API_KEY, "max_retries": max_retries}
    if timeout is not None:
        kwargs["timeout"] = timeout
    return OpenAI(**kwargs)


def spawn_logged(coro: Coroutine[Any, Any, Any], *, name: str) -> asyncio.Task:
    """``asyncio.create_task`` 래퍼 — 완료 콜백으로 예외를 로깅한다.

    fire-and-forget 태스크가 예외로 조용히 죽는 것을 방지한다. 취소(CancelledError)는
    정상 종료로 간주해 무시한다.
    """
    task = asyncio.create_task(coro, name=name)

    def _on_done(t: asyncio.Task) -> None:
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            logger.error("백그라운드 태스크 '%s' 실패: %s", name, exc, exc_info=exc)

    task.add_done_callback(_on_done)
    return task
