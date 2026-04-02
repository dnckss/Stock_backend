"""
종목별 AI 심층 분석 서비스.
해당 종목의 뉴스·기술적 지표·가격 변동을 종합하여
하락/상승 원인, 반등 가능성, 투자 전략을 분석한다.
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
from services.crud import sanitize_for_json

logger = logging.getLogger(__name__)

_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

_SYSTEM_PROMPT = """\
너는 월스트리트 최고 등급의 종목 전문 애널리스트야.
투자 조언 불가 같은 면책 조항은 절대 금지. 확신에 찬 전문가 톤으로 분석해.

## 제공 데이터
1. **quote**: 현재 시세 (가격, 등락, 거래량, 52주 고저, PER, 시총 등)
2. **technicals**: 기술적 지표 (RSI, MACD, 볼린저, 이동평균, ATR, 지지/저항, 거래량 비율)
3. **news**: 최근 관련 뉴스 헤드라인 + FinBERT 감성 분석 결과

## 분석 요구사항
- 현재 주가 움직임의 **원인 분석** (어떤 뉴스/이벤트가 주가에 영향을 줬는지)
- 기술적 지표 기반 **현재 위치 진단** (과매도/과매수, 추세, 지지/저항)
- **반등 가능성** 평가 (근거와 함께 high/medium/low)
- **핵심 리스크** (하락이 더 이어질 수 있는 요인)
- **단기 전략 제안** (진입 시점, 주의사항)
- 모든 내용은 **한국어**로 작성

## 응답 JSON
```json
{
  "price_action": {
    "trend": "downtrend|uptrend|sideways",
    "cause": "주가 움직임의 핵심 원인 2~3문장",
    "key_events": ["원인이 된 주요 뉴스/이벤트 1줄 요약", ...]
  },
  "technical_diagnosis": {
    "condition": "oversold|neutral|overbought",
    "summary": "기술적 지표 종합 진단 2~3문장 (RSI/MACD/MA/볼린저 언급)",
    "support_test": "현재 지지선 테스트 여부 및 의미"
  },
  "rebound_potential": {
    "rating": "high|medium|low",
    "reason": "반등 가능성 판단 근거 2~3문장",
    "catalysts": ["반등을 촉발할 수 있는 요인들"]
  },
  "risks": ["하락 지속 가능 요인 1줄씩", ...],
  "strategy": {
    "action": "BUY|HOLD|SELL|WAIT",
    "entry_condition": "진입 조건/시점 제안",
    "stop_loss_note": "손절 참고 사항",
    "summary": "최종 전략 한줄 요약"
  },
  "overall_summary": "전체 분석 핵심 3~4문장으로 요약"
}
```"""


async def analyze_stock(
    ticker: str,
    quote: dict[str, Any],
    technicals: dict[str, Any] | None,
    news: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    """
    종목별 AI 심층 분석을 수행한다.
    """
    if _client is None:
        return {"error": "OPENAI_API_KEY가 설정되지 않았습니다"}

    # 뉴스 다이제스트 (토큰 절약을 위해 핵심만)
    news_digest = []
    for item in (news or [])[:10]:
        news_digest.append({
            "title": item.get("title", ""),
            "sentiment": item.get("sentiment_polarity") or item.get("sentiment_label") or "neutral",
            "score": item.get("score", 0),
            "publisher": item.get("publisher", ""),
        })

    # 기술적 지표 요약 (필요한 것만)
    tech_summary = None
    if technicals:
        tech_summary = {
            "current_price": technicals.get("current_price"),
            "rsi_14": technicals.get("rsi_14"),
            "rsi_signal": technicals.get("rsi_signal"),
            "macd_signal": technicals.get("macd_signal"),
            "macd_histogram": technicals.get("macd_histogram"),
            "ma_position": technicals.get("ma_position"),
            "ma_20": technicals.get("ma_20"),
            "ma_50": technicals.get("ma_50"),
            "ma_200": technicals.get("ma_200"),
            "bollinger_position": technicals.get("bollinger_position"),
            "bb_upper": technicals.get("bb_upper"),
            "bb_lower": technicals.get("bb_lower"),
            "atr_14": technicals.get("atr_14"),
            "support": technicals.get("support"),
            "resistance": technicals.get("resistance"),
            "volume_ratio": technicals.get("volume_ratio"),
        }

    # 시세 요약
    quote_summary = {
        "price": quote.get("price"),
        "change": quote.get("change"),
        "change_pct": quote.get("change_pct"),
        "open": quote.get("open"),
        "day_high": quote.get("day_high"),
        "day_low": quote.get("day_low"),
        "prev_close": quote.get("prev_close"),
        "volume": quote.get("volume"),
        "avg_volume": quote.get("avg_volume"),
        "year_high": quote.get("year_high"),
        "year_low": quote.get("year_low"),
        "pe_ratio": quote.get("pe_ratio"),
        "market_cap": quote.get("market_cap"),
        "sector": quote.get("sector"),
        "name": quote.get("name"),
    }

    user_content = {
        "ticker": ticker,
        "quote": quote_summary,
        "technicals": tech_summary,
        "news": news_digest,
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
        return _client.chat.completions.create(**kwargs)

    try:
        resp = await asyncio.wait_for(
            asyncio.to_thread(_create),
            timeout=STRATEGIST_OPENAI_TIMEOUT_SEC + STRATEGIST_OPENAI_THREAD_BUFFER_SEC,
        )
    except asyncio.TimeoutError:
        return {"error": "AI 분석 타임아웃"}
    except Exception as e:
        logger.warning("종목 AI 분석 실패 (%s): %s", ticker, e)
        return {"error": f"AI 분석 실패: {type(e).__name__}"}

    content = resp.choices[0].message.content or ""
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        return {"error": "AI 응답 JSON 파싱 실패", "raw": content[:500]}

    # 기본값 보장
    parsed.setdefault("price_action", {"trend": "unknown", "cause": "", "key_events": []})
    parsed.setdefault("technical_diagnosis", {"condition": "neutral", "summary": "", "support_test": ""})
    parsed.setdefault("rebound_potential", {"rating": "medium", "reason": "", "catalysts": []})
    parsed.setdefault("risks", [])
    parsed.setdefault("strategy", {"action": "HOLD", "entry_condition": "", "stop_loss_note": "", "summary": ""})
    parsed.setdefault("overall_summary", "")

    parsed["ticker"] = ticker
    parsed["analyzed_at"] = datetime.now().isoformat()

    return sanitize_for_json(parsed)
