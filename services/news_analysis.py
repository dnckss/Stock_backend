from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

from openai import BadRequestError, OpenAI

from config import (
    OPENAI_API_KEY,
    NEWS_ANALYSIS_FALLBACK_OPENAI_MODEL,
    NEWS_ANALYSIS_INPUT_MAX_CHARS,
    NEWS_ANALYSIS_OPENAI_MODEL,
    NEWS_ANALYSIS_TEMPERATURE,
    NEWS_ANALYSIS_TIMEOUT_SEC,
    NEWS_ANALYSIS_THREAD_BUFFER_SEC,
)

logger = logging.getLogger(__name__)

_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# JSON mode 사용 시 OpenAI는 대화에 "JSON으로 출력" 지시가 있어야 하는 경우가 많다.
_SYSTEM = (
    "너는 월스트리트 최고 등급의 퀀트 애널리스트 겸 시장 해설가야. "
    "투자 조언 불가 같은 면책 조항은 절대 쓰지 마. "
    "주어진 뉴스의 핵심만 정확히 요약하고, 어느 시장/섹터/테마에 어떤 방향으로 영향을 주는지 정리해. "
    "응답은 반드시 단일 JSON 객체로만 출력한다(설명 문구·마크다운 코드펜스 금지). "
    "필수 키: ko_summary(string), impact(object). "
    "impact는 sectors(string[]), themes(string[]), "
    "direction(문자열: positive|negative|mixed|unclear 중 하나), "
    "confidence(0~1 숫자), reason_ko(string)를 포함한다. "
    "선택 키: tickers_mentioned(string[])."
)

_REQUIRED_TOP_KEYS = {"ko_summary", "impact"}
_REQUIRED_IMPACT_KEYS = {"sectors", "themes", "direction", "confidence", "reason_ko"}
_VALID_DIRECTIONS = {"positive", "negative", "mixed", "unclear"}


def _model_omits_temperature(model: str) -> bool:
    m = (model or "").lower().strip()
    if m.startswith("o1") or m.startswith("o3"):
        return True
    if "gpt-5" in m:
        return True
    return False


def _log_bad_request(model: str, attempt: str, exc: BadRequestError) -> None:
    body = getattr(exc, "body", None)
    msg = getattr(exc, "message", None) or str(exc)
    logger.warning(
        "news_analysis OpenAI 400 model=%s attempt=%s message=%s body=%s",
        model,
        attempt,
        msg,
        body,
    )


def _truncate_article(text: str | None) -> str:
    if not text:
        return ""
    t = text.strip()
    if len(t) <= NEWS_ANALYSIS_INPUT_MAX_CHARS:
        return t
    return t[:NEWS_ANALYSIS_INPUT_MAX_CHARS].rstrip() + "\n\n[…본문 일부 생략…]"


def _parse_llm_json(content: str) -> dict[str, Any]:
    content = (content or "").strip()
    if not content:
        raise ValueError("빈 응답")
    # ```json ... ``` 제거
    fence = re.match(r"^```(?:json)?\s*([\s\S]*?)\s*```$", content, re.IGNORECASE)
    if fence:
        content = fence.group(1).strip()
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        start = content.find("{")
        end = content.rfind("}")
        if start >= 0 and end > start:
            parsed = json.loads(content[start : end + 1])
        else:
            raise
    if not isinstance(parsed, dict):
        raise ValueError("JSON 최상위가 객체가 아닙니다")
    return parsed


def _validate(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("news_analysis JSON이 객체가 아닙니다")
    missing = _REQUIRED_TOP_KEYS - set(data.keys())
    if missing:
        raise ValueError(f"news_analysis 필수 키 누락: {sorted(missing)}")

    if not isinstance(data.get("ko_summary"), str) or not data["ko_summary"].strip():
        raise ValueError("ko_summary가 비어있습니다")

    impact = data.get("impact")
    if not isinstance(impact, dict):
        raise ValueError("impact가 객체가 아닙니다")
    missing2 = _REQUIRED_IMPACT_KEYS - set(impact.keys())
    if missing2:
        raise ValueError(f"impact 필수 키 누락: {sorted(missing2)}")

    sectors = impact.get("sectors")
    themes = impact.get("themes")
    if not isinstance(sectors, list) or not all(isinstance(x, str) for x in sectors):
        raise ValueError("impact.sectors 형식 오류")
    if not isinstance(themes, list) or not all(isinstance(x, str) for x in themes):
        raise ValueError("impact.themes 형식 오류")

    direction = impact.get("direction")
    if direction not in _VALID_DIRECTIONS:
        raise ValueError("impact.direction 값 오류")

    conf = impact.get("confidence")
    if not isinstance(conf, (int, float)):
        raise ValueError("impact.confidence 형식 오류")

    if not isinstance(impact.get("reason_ko"), str) or not impact["reason_ko"].strip():
        raise ValueError("impact.reason_ko가 비어있습니다")

    if "tickers_mentioned" in data:
        tm = data["tickers_mentioned"]
        if not isinstance(tm, list) or not all(isinstance(x, str) for x in tm):
            raise ValueError("tickers_mentioned 형식 오류")

    return data


def _chat_create(
    *,
    model: str,
    messages: list[dict[str, str]],
    temperature: float | None,
    response_format: dict[str, str] | None,
) -> Any:
    kwargs: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "timeout": NEWS_ANALYSIS_TIMEOUT_SEC,
    }
    if temperature is not None:
        kwargs["temperature"] = temperature
    if response_format is not None:
        kwargs["response_format"] = response_format
    return _client.chat.completions.create(**kwargs)


def _run_completion_strategies(model: str, messages: list[dict[str, str]]) -> Any:
    """400(파라미터 불일치 등) 시 temperature/json_mode 조합을 바꿔 재시도."""
    omit_temp = _model_omits_temperature(model)
    strategies: list[tuple[str, dict[str, Any]]] = []

    if not omit_temp:
        strategies.append(
            (
                "temperature+json_object",
                {"temperature": NEWS_ANALYSIS_TEMPERATURE, "response_format": {"type": "json_object"}},
            )
        )
    strategies.append(("json_object_only", {"temperature": None, "response_format": {"type": "json_object"}}))
    strategies.append(("plain_no_json_mode", {"temperature": None, "response_format": None}))

    last_exc: Exception | None = None
    for name, params in strategies:
        try:
            return _chat_create(
                model=model,
                messages=messages,
                temperature=params["temperature"],
                response_format=params["response_format"],
            )
        except BadRequestError as e:
            _log_bad_request(model, name, e)
            last_exc = e
            continue
    assert last_exc is not None
    raise last_exc


async def analyze_news_korean(
    *,
    title: str | None,
    publisher: str | None,
    article_markdown: str | None,
    url: str | None,
) -> dict[str, Any]:
    if _client is None:
        raise RuntimeError("OPENAI_API_KEY가 설정되지 않았습니다")

    payload = {
        "title": title,
        "publisher": publisher,
        "url": url,
        "article_markdown": _truncate_article(article_markdown),
        "instructions": {
            "output_language": "ko",
            "sectors": "가능하면 GICS 스타일(Technology, Financials 등)로",
            "themes": "AI/반도체/크립토/원자재/금리/규제/실적 등 키워드",
            "direction": ["positive", "negative", "mixed", "unclear"],
        },
    }

    messages = [
        {"role": "system", "content": _SYSTEM},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]

    models_to_try = [NEWS_ANALYSIS_OPENAI_MODEL]
    fb = (NEWS_ANALYSIS_FALLBACK_OPENAI_MODEL or "").strip()
    if fb and fb != NEWS_ANALYSIS_OPENAI_MODEL:
        models_to_try.append(fb)

    last_error: Exception | None = None
    for model in models_to_try:
        def _run() -> Any:
            return _run_completion_strategies(model, messages)

        try:
            resp = await asyncio.wait_for(
                asyncio.to_thread(_run),
                timeout=NEWS_ANALYSIS_TIMEOUT_SEC + NEWS_ANALYSIS_THREAD_BUFFER_SEC,
            )
            content = resp.choices[0].message.content or ""
            parsed = _parse_llm_json(content)
            return _validate(parsed)
        except BadRequestError as e:
            # 전략별 400은 _run_completion_strategies에서 이미 로깅됨
            last_error = e
            continue
        except Exception as e:
            logger.warning("news_analysis 실패 model=%s: %s", model, e, exc_info=True)
            last_error = e
            continue

    assert last_error is not None
    raise last_error
