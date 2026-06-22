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

from openai import OpenAI, RateLimitError

from config import (
    LLM_FREE_FALLBACK_ENABLED,
    OPENAI_API_KEY,
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    OPENROUTER_MODEL,
    OPENROUTER_SITE_NAME,
    OPENROUTER_SITE_URL,
)

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


def make_openrouter_client(
    *,
    timeout: float | int | None = None,
    max_retries: int = 2,
) -> OpenAI | None:
    """OpenRouter(OpenAI 호환) 클라이언트 — 무료 모델 폴백용. 키 없으면 None.

    base_url 만 OpenRouter 로 바꾼 동일한 OpenAI SDK. HTTP-Referer/X-Title 는 OpenRouter
    랭킹용 선택 헤더. 호출 모델은 OPENROUTER_MODEL(기본 nvidia nemotron :free).
    """
    if not OPENROUTER_API_KEY:
        return None
    kwargs: dict[str, Any] = {
        "api_key": OPENROUTER_API_KEY,
        "base_url": OPENROUTER_BASE_URL,
        "max_retries": max_retries,
    }
    headers: dict[str, str] = {}
    if OPENROUTER_SITE_URL:
        headers["HTTP-Referer"] = OPENROUTER_SITE_URL
    if OPENROUTER_SITE_NAME:
        headers["X-Title"] = OPENROUTER_SITE_NAME
    if headers:
        kwargs["default_headers"] = headers
    if timeout is not None:
        kwargs["timeout"] = timeout
    return OpenAI(**kwargs)


def _is_quota_or_rate_error(exc: BaseException) -> bool:
    """OpenAI 쿼터 소진/레이트리밋 류 오류인지 — 무료 폴백 트리거 판정."""
    if isinstance(exc, RateLimitError):
        return True
    m = str(exc).lower()
    return any(k in m for k in ("insufficient_quota", "quota", "rate limit", "429", "too many requests"))


def llm_chat_create(
    primary_client: OpenAI | None,
    *,
    model: str,
    allow_free: bool = True,
    openrouter_model: str | None = None,
    timeout: float | int | None = None,
    **params: Any,
):
    """LLM chat.completions 호출 + OpenAI→OpenRouter(무료) 자동 폴백.

    동작:
      1) primary_client(OpenAI)로 호출 시도.
      2) 쿼터/레이트리밋 실패면(그리고 allow_free=True) OpenRouter 무료 모델로 재시도.
      3) primary 가 None(키 없음)이고 allow_free=True 면 바로 OpenRouter.

    ⚠️ allow_free=False: 사용자 개인데이터(챗 등)는 무료 NVIDIA 엔드포인트(입력 로깅)로 보내지
       않는다. 이 경우 OpenAI 만 사용하고, 실패 시 예외를 그대로 올린다(호출부가 graceful 처리).
    비-쿼터 오류는 폴백하지 않고 그대로 raise(호출부의 기존 강등/재시도 로직 보존).
    """
    free_ok = allow_free and LLM_FREE_FALLBACK_ENABLED
    last_exc: BaseException | None = None

    if primary_client is not None:
        try:
            return primary_client.chat.completions.create(model=model, **params)
        except Exception as e:  # noqa: BLE001 — 폴백 판정 후 재-raise
            last_exc = e
            if not (free_ok and _is_quota_or_rate_error(e)):
                raise
            logger.warning("LLM primary 실패(쿼터/레이트) → OpenRouter 무료 폴백 시도: %s", e)

    if not free_ok:
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("LLM 사용 불가 — OpenAI 키 없음 + 무료 폴백 비허용(allow_free=False)")

    orc = make_openrouter_client(timeout=timeout)
    if orc is None:
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("LLM 사용 불가 — OpenAI·OpenRouter 모두 미설정")
    return orc.chat.completions.create(model=(openrouter_model or OPENROUTER_MODEL), **params)


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
