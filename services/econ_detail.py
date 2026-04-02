"""
경제 일정 상세 분석 서비스.
경제 지표의 역할, 의미, 시장 영향 등을 AI로 생성하고 DB에 캐싱한다.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any

from openai import OpenAI

from config import (
    OPENAI_API_KEY,
    STRATEGIST_OPENAI_MODEL,
    STRATEGIST_OPENAI_TIMEOUT_SEC,
    STRATEGIST_OPENAI_THREAD_BUFFER_SEC,
)
from services.crud import sanitize_for_json, _get_client, _sanitize

logger = logging.getLogger(__name__)

_ai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# 프로세스 내 캐시 (이벤트명 기준 — 같은 지표는 매번 AI 호출 불필요)
_detail_cache: dict[str, dict[str, Any]] = {}

_SYSTEM_PROMPT = """\
너는 글로벌 매크로 경제 전문 애널리스트야.
면책 조항이나 사전적 정의 나열은 금지. 실무적이고 핵심적인 정보만 제공해.

경제 지표/이벤트에 대해 아래 JSON 구조로 분석해:

```json
{
  "name_ko": "한국어 지표명",
  "category": "고용|물가|성장|소비|제조업|주택|무역|금리|기타",
  "description": "이 지표가 무엇을 측정하는지 2~3문장",
  "why_important": "왜 중요한지, 시장이 왜 주목하는지 2~3문장",
  "market_impact": {
    "stocks": "주식 시장에 미치는 영향 (예상 상회/하회 시 각각)",
    "bonds": "채권/금리에 미치는 영향",
    "currency": "달러/환율에 미치는 영향",
    "sectors": ["특히 영향받는 섹터 목록"]
  },
  "reading_guide": {
    "above_expected": "예상치 상회 시 의미와 시장 반응",
    "below_expected": "예상치 하회 시 의미와 시장 반응",
    "key_threshold": "주요 기준점이 있다면 (예: PMI 50, 실업률 4% 등)"
  },
  "release_info": {
    "frequency": "발표 주기 (월간/분기/주간 등)",
    "source": "발표 기관",
    "typical_impact_duration": "시장 영향 지속 시간 (즉시/수시간/수일)"
  },
  "related_indicators": ["관련 지표 이름 목록"],
  "summary": "투자자가 알아야 할 핵심 한줄 요약"
}
```

모든 내용은 한국어로 작성해."""


def _get_cached_detail(event_name: str) -> dict[str, Any] | None:
    """DB에서 캐시된 상세 정보를 조회한다."""
    if event_name in _detail_cache:
        return _detail_cache[event_name]

    client = _get_client()
    resp = (
        client.table("econ_event_details")
        .select("*")
        .eq("event_name", event_name)
        .limit(1)
        .execute()
    )
    if resp.data:
        row = _sanitize(resp.data)[0]
        detail_json = row.get("detail_json")
        if isinstance(detail_json, str):
            try:
                detail = json.loads(detail_json)
            except Exception:
                return None
        elif isinstance(detail_json, dict):
            detail = detail_json
        else:
            return None
        _detail_cache[event_name] = detail
        return detail
    return None


def _save_detail(event_name: str, detail: dict[str, Any]) -> None:
    """상세 정보를 DB에 저장한다."""
    client = _get_client()
    client.table("econ_event_details").upsert({
        "event_name": event_name,
        "detail_json": json.dumps(detail, ensure_ascii=False),
    }, on_conflict="event_name").execute()
    _detail_cache[event_name] = detail


async def get_econ_event_detail(event_name: str, event_data: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    경제 이벤트 상세 정보를 반환한다.
    DB 캐시 → 없으면 AI 생성 → DB 저장.
    """
    if not event_name or not event_name.strip():
        return {"error": "event_name이 비어있습니다"}

    # 1) 캐시 확인
    cached = _get_cached_detail(event_name)
    if cached:
        return sanitize_for_json({
            "event_name": event_name,
            "detail": cached,
            "cache_hit": True,
            **({"event_data": event_data} if event_data else {}),
        })

    # 2) AI 생성
    if _ai_client is None:
        return {"error": "OPENAI_API_KEY가 설정되지 않았습니다"}

    user_content = {"event_name": event_name}
    if event_data:
        user_content["context"] = {
            "country": event_data.get("country_name"),
            "currency": event_data.get("currency"),
            "importance": event_data.get("importance"),
            "actual": event_data.get("actual"),
            "forecast": event_data.get("forecast"),
            "previous": event_data.get("previous"),
        }

    def _create() -> Any:
        model = STRATEGIST_OPENAI_MODEL or "gpt-5"
        kwargs: dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(user_content, ensure_ascii=False)},
            ],
            "response_format": {"type": "json_object"},
            "timeout": STRATEGIST_OPENAI_TIMEOUT_SEC,
        }
        model_lower = model.lower()
        if "gpt-5" not in model_lower and "o1" not in model_lower and "o3" not in model_lower:
            kwargs["temperature"] = 0.3
        return _ai_client.chat.completions.create(**kwargs)

    try:
        resp = await asyncio.wait_for(
            asyncio.to_thread(_create),
            timeout=STRATEGIST_OPENAI_TIMEOUT_SEC + STRATEGIST_OPENAI_THREAD_BUFFER_SEC,
        )
    except asyncio.TimeoutError:
        return {"error": "AI 분석 타임아웃", "event_name": event_name}
    except Exception as e:
        logger.warning("경제 이벤트 상세 AI 생성 실패 (%s): %s", event_name, e)
        return {"error": f"AI 분석 실패: {type(e).__name__}", "event_name": event_name}

    content = resp.choices[0].message.content or ""
    try:
        detail = json.loads(content)
    except json.JSONDecodeError:
        return {"error": "AI 응답 JSON 파싱 실패", "event_name": event_name}

    # 3) DB 저장
    try:
        _save_detail(event_name, detail)
    except Exception as e:
        logger.warning("경제 이벤트 상세 DB 저장 실패: %s", e)

    return sanitize_for_json({
        "event_name": event_name,
        "detail": detail,
        "cache_hit": False,
        **({"event_data": event_data} if event_data else {}),
    })
