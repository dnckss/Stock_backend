"""
포트폴리오 빌더 서비스.
투자 금액 + 성향 + 기간을 입력받아 AI 전략 기반 포트폴리오를 구성한다.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
from datetime import datetime
from typing import Any

from openai import OpenAI

from config import (
    OPENAI_API_KEY,
    STRATEGIST_OPENAI_MODEL,
)
from services.crud import sanitize_for_json
from services.portfolio_agents import STYLE_CONFIG, PERIOD_CONFIG, _DEFENSIVE_ETFS
from services.stock_detail import fetch_quote

logger = logging.getLogger(__name__)

_client = OpenAI(
    api_key=OPENAI_API_KEY,
    max_retries=2,
    timeout=600,
) if OPENAI_API_KEY else None

# 단일 소스: portfolio_agents.py에서 import
_STYLE_CONFIG = STYLE_CONFIG
_PERIOD_CONFIG = PERIOD_CONFIG

_SYSTEM_PROMPT = """\
너는 월스트리트 최고 등급의 포트폴리오 매니저야.
면책 조항은 절대 금지. 확신에 찬 전문가 톤으로 분석해.

투자자의 금액, 성향, 기간에 맞춰 최적의 포트폴리오를 구성해.

## 제공 데이터
1. **budget**: 투자 가능 금액 (USD)
2. **style**: 투자 성향 (aggressive/balanced/conservative)
3. **period**: 투자 기간 (short/medium/long)
4. **candidates**: AI 전략실이 추천한 종목들 (기술적 지표 + 뉴스 감성 포함)
5. **defensive_etfs**: 방어주 ETF 시세 (보수적/균형 성향 시 배분용)
6. **allocation_guide**: 성향별 배분 가이드라인

## 응답 JSON
```json
{
  "allocations": [
    {
      "ticker": "NVDA",
      "shares": 10,
      "weight_pct": 35.0,
      "rationale": "배분 근거 2문장"
    }
  ],
  "portfolio_thesis": "전체 포트폴리오 투자 논리 3~4문장",
  "sector_exposure": {"Technology": 45, "Healthcare": 20},
  "risk_assessment": {
    "level": "medium",
    "max_drawdown_est": "-12%",
    "volatility_note": "변동성 관련 한줄"
  },
  "dca_plan": "분할 매수 제안 (해당 시만)",
  "rebalance_note": "리밸런싱 시점/조건 제안",
  "warnings": ["주의사항 목록"]
}
```

규칙:
- 각 종목의 현재가와 투자금을 고려해 **실제 매수 가능한 주수(정수)**를 계산해.
- 투자금을 초과하면 안 됨.
- 한국어로 작성."""


def _safe_round(v: Any, n: int = 2) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return round(f, n) if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _fetch_defensive_quotes() -> list[dict[str, Any]]:
    """방어주 ETF 시세를 가져온다."""
    results = []
    for ticker in _DEFENSIVE_ETFS:
        try:
            q = fetch_quote(ticker)
            if q.get("price"):
                results.append({
                    "ticker": ticker,
                    "name": q.get("name", ticker),
                    "price": q["price"],
                })
        except Exception:
            continue
    return results


async def build_portfolio(
    budget: float,
    style: str = "balanced",
    period: str = "medium",
    strategy_data: dict[str, Any] | None = None,
    exclude_tickers: list[str] | None = None,
) -> dict[str, Any]:
    """
    투자 금액 + 성향 + 기간을 기반으로 포트폴리오를 구성한다.

    Args:
        budget: 투자 금액 (USD)
        style: aggressive / balanced / conservative
        period: short / medium / long
        strategy_data: 전략실 /api/strategy 응답 데이터 (없으면 내부 호출)
        exclude_tickers: 제외할 종목 리스트
    """
    style_cfg = _STYLE_CONFIG.get(style, _STYLE_CONFIG["balanced"])
    period_cfg = _PERIOD_CONFIG.get(period, _PERIOD_CONFIG["medium"])
    excludes = set((t.upper() for t in (exclude_tickers or [])))

    # 전략 데이터 가져오기
    if not strategy_data:
        from services.strategist import get_cached_market_strategy
        strategy_data = await get_cached_market_strategy(None, None, None, None)

    recommendations = strategy_data.get("recommendations") or []

    # 제외 종목 필터
    candidates = [r for r in recommendations if r.get("ticker", "").upper() not in excludes]

    if not candidates:
        return sanitize_for_json({
            "error": "추천 종목이 없습니다. 전략실 데이터를 먼저 생성해주세요.",
            "budget": budget,
            "style": style,
            "period": period,
        })

    # 후보 종목 시세 조회 (최신 가격)
    candidate_quotes = []
    for rec in candidates[:style_cfg["max_picks"]]:
        ticker = rec.get("ticker", "").upper()
        try:
            q = fetch_quote(ticker)
            if q.get("price"):
                candidate_quotes.append({
                    "ticker": ticker,
                    "name": q.get("name", ticker),
                    "price": q["price"],
                    "sector": q.get("sector"),
                    "direction": rec.get("direction", "BUY"),
                    "confidence": rec.get("confidence", "medium"),
                    "rationale": rec.get("rationale", ""),
                    "technicals_summary": rec.get("technicals_summary", ""),
                    "stop_loss": rec.get("stop_loss"),
                    "targets": rec.get("targets", []),
                })
        except Exception:
            continue

    # 방어주 ETF (보수적/균형 성향 시)
    defensive_quotes = []
    if style_cfg["defensive_pct"] > 0:
        defensive_quotes = await asyncio.to_thread(_fetch_defensive_quotes)

    # LLM에 포트폴리오 구성 요청
    if _client is None:
        return {"error": "OPENAI_API_KEY가 설정되지 않았습니다"}

    user_content = {
        "budget": budget,
        "style": style,
        "style_ko": style_cfg["label_ko"],
        "period": period,
        "period_ko": period_cfg["label_ko"],
        "candidates": candidate_quotes,
        "defensive_etfs": defensive_quotes,
        "allocation_guide": {
            "top_concentration": style_cfg["top_concentration"],
            "defensive_pct": style_cfg["defensive_pct"],
            "cash_reserve_pct": style_cfg["cash_reserve_pct"],
            "max_picks": style_cfg["max_picks"],
            "dca_splits": period_cfg["dca_splits"],
        },
        "market_summary": strategy_data.get("market_summary", ""),
        "market_regime": strategy_data.get("market_regime", "unknown"),
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
        }
        model_lower = model.lower()
        if "gpt-5" not in model_lower and "o1" not in model_lower and "o3" not in model_lower:
            kwargs["temperature"] = 0.3
        return _client.chat.completions.create(**kwargs)

    try:
        resp = await asyncio.to_thread(_create)
    except Exception as e:
        logger.warning("포트폴리오 LLM 호출 실패: %s", e)
        return sanitize_for_json({
            "error": f"AI 포트폴리오 구성 실패: {type(e).__name__}",
            "budget": budget,
            "style": style,
            "period": period,
        })

    content = resp.choices[0].message.content or ""
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        return {"error": "AI 응답 JSON 파싱 실패", "raw": content[:500]}

    # allocations에 가격/금액 계산 보강
    allocations = parsed.get("allocations") or []
    total_invested = 0.0
    for alloc in allocations:
        ticker = alloc.get("ticker", "").upper()
        shares = int(alloc.get("shares", 0))
        # 후보에서 가격 찾기
        price = None
        for cq in candidate_quotes + defensive_quotes:
            if cq["ticker"] == ticker:
                price = cq["price"]
                break
        if price and shares > 0:
            amount = round(price * shares, 2)
            alloc["price"] = price
            alloc["amount"] = amount
            alloc["shares"] = shares
            total_invested += amount

    cash_remaining = round(budget - total_invested, 2)

    result = {
        "budget": budget,
        "currency": "USD",
        "style": style,
        "style_ko": style_cfg["label_ko"],
        "period": period,
        "period_ko": period_cfg["label_ko"],
        "allocations": allocations,
        "total_invested": round(total_invested, 2),
        "cash_remaining": max(0, cash_remaining),
        "portfolio_thesis": parsed.get("portfolio_thesis", ""),
        "sector_exposure": parsed.get("sector_exposure", {}),
        "risk_assessment": parsed.get("risk_assessment", {}),
        "dca_plan": parsed.get("dca_plan"),
        "rebalance_note": parsed.get("rebalance_note"),
        "warnings": parsed.get("warnings", []),
        "market_regime": strategy_data.get("market_regime", "unknown"),
        "generated_at": datetime.now().isoformat(),
    }

    return sanitize_for_json(result)
