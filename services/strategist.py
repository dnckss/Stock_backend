"""
AI 전략실 — 뉴스·경제일정·기술적 지표·섹터 분석을 종합하여 종목을 추천한다.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import yfinance as yf
from openai import OpenAI

from config import (
    OPENAI_API_KEY,
    STRATEGIST_CACHE_TTL_SEC,
    STRATEGIST_DIVERGENCE_FALLBACK,
    STRATEGIST_ECON_LOOKBACK_HOURS,
    STRATEGIST_ECON_MAX_SURPRISES,
    STRATEGIST_ECON_MAX_UPCOMING,
    STRATEGIST_ECON_MIN_IMPORTANCE,
    STRATEGIST_ECON_UPCOMING_HOURS,
    STRATEGIST_FALLBACK_TOP_PICKS_N,
    STRATEGIST_MAX_YFINANCE_SECTOR_CALLS_PER_REQUEST,
    STRATEGIST_NEWS_TOP_N,
    STRATEGIST_OPENAI_MODEL,
    STRATEGIST_OPENAI_THREAD_BUFFER_SEC,
    STRATEGIST_OPENAI_TIMEOUT_SEC,
    STRATEGIST_TEMPERATURE,
    STRATEGIST_TICKER_SECTOR_MAP,
    STRATEGIST_YFINANCE_SECTOR_TIMEOUT_SEC,
)
from services.crud import get_economic_events, get_latest_scan_records, sanitize_for_json, save_strategy_history

logger = logging.getLogger(__name__)

_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

_KST = timezone(timedelta(hours=9))

# ---------------------------------------------------------------------------
# System Prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
너는 월스트리트 최고 등급의 퀀트 전략가이자 포트폴리오 매니저야.
투자 조언 불가 같은 면책 조항은 절대 금지. 확신에 찬 전문가 톤으로 분석해.

## 제공 데이터
1. **sector_data**: 섹터별 평균 괴리율·감성·수익률 (스캔 기반)
2. **sector_etf**: 섹터 ETF 주간/월간 수익률 + 모멘텀 + 로테이션 방향
3. **macro_context**: VIX, 주요 지수, 환율, 시장 공포/탐욕 게이지(0~100)
4. **news_digest**: 최근 뉴스 헤드라인 감성 분석 — 전체 분포, 주요 헤드라인, 티커별 감성
5. **econ_digest**: 경제 캘린더 — 향후 48h 고임팩트 이벤트 + 최근 24h 서프라이즈
6. **technicals**: 추천 후보 종목들의 기술적 지표 (RSI, MACD, 볼린저, MA, ATR, 지지/저항, 거래량)

## 분석 규칙
- **market_regime**: 매크로 지표 + VIX + 섹터 ETF 흐름을 종합하여 시장 국면을 판단해.
- **news_themes**: 뉴스에서 시장을 움직이는 핵심 테마 2~4개를 도출해.
- **econ_analysis**: 경제 서프라이즈의 시장 영향 + 다가오는 리스크 이벤트를 분석해.
- **recommendations**: 3~5개 종목을 추천하되 반드시 아래 규칙을 지켜:
  - 각 종목에 **direction**(BUY/SELL), **confidence**(high/medium/low) 명시
  - **strategy_type**: scalp(당일) / swing(1~2주) / position(1개월+) 중 선택
  - **entry_zone**: 진입 가격대 (low ~ high)
  - **stop_loss**: 손절라인 가격 (기술적 지표 ATR, 지지선 기반)
  - **stop_loss_pct**: 현재가 대비 손절 비율(%)
  - **targets**: 목표가 2개 (TP1, TP2) + 각각 현재가 대비 %
  - **risk_reward_ratio**: 리스크 대비 리워드 비율
  - **rationale**: 뉴스·경제·기술적 근거를 구체적으로 통합하여 설명 (3~4문장)
  - **risk_factors**: 해당 종목의 구체적 리스크 요인
  - **technicals_summary**: 주요 기술적 지표 요약 (RSI/MACD/MA/볼린저 한줄)
- **risk_warnings**: 전체 포트폴리오 수준의 리스크 경고 1~3개
- 모든 내용은 **한국어**로 작성해.

## 응답 JSON (반드시 이 구조로)
```json
{
  "market_regime": "bullish|bearish|sideways|volatile",
  "market_regime_detail": "시장 국면 판단 근거 1~2문장",
  "market_summary": "시장 전체 상황 요약 3~5문장 (뉴스·경제·기술적 통합 분석)",
  "top_sector": {"name": "섹터명", "name_ko": "한국어 섹터명", "reason": "선택 이유 (뉴스/경제/기술적 근거)"},
  "news_themes": [
    {"theme": "테마명", "tickers": ["NVDA"], "sentiment": "positive|negative|neutral", "detail": "설명"}
  ],
  "econ_analysis": {
    "summary": "경제 이벤트 영향 분석 2~3문장",
    "recent_surprises": [
      {"event": "이벤트명", "actual": "값", "forecast": "값", "impact": "시장 영향 설명"}
    ],
    "upcoming_risks": [
      {"event": "이벤트명", "date": "YYYY-MM-DD", "risk_level": "high|medium|low", "scenario": "시나리오 설명"}
    ]
  },
  "recommendations": [
    {
      "ticker": "NVDA",
      "direction": "BUY|SELL",
      "confidence": "high|medium|low",
      "strategy_type": "swing",
      "holding_period": "1~2주",
      "entry_zone": {"low": 140.0, "high": 143.5},
      "stop_loss": 133.2,
      "stop_loss_pct": -6.5,
      "targets": [
        {"label": "TP1", "price": 152.0, "pct": 6.7},
        {"label": "TP2", "price": 160.0, "pct": 12.3}
      ],
      "risk_reward_ratio": 1.8,
      "rationale": "구체적 근거 3~4문장...",
      "risk_factors": "해당 종목 리스크...",
      "technicals_summary": "RSI 58(중립), MACD 골든크로스, 50일선 위, 거래량 135%"
    }
  ],
  "risk_warnings": ["경고1", "경고2"],
  "sector_rotation": "growth|defensive|cyclical|mixed"
}
```"""

_REQUIRED_KEYS = frozenset({"market_summary", "top_sector", "recommendations"})

_strategy_cache: dict[str, Any] | None = None
_strategy_cache_at: datetime | None = None
_strategy_lock = asyncio.Lock()

_sector_cache: dict[str, str] = {}


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _safe_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


# ---------------------------------------------------------------------------
# News Digest
# ---------------------------------------------------------------------------

def _build_news_digest(news_feed: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    if not news_feed:
        return None

    sentiment_dist: dict[str, int] = defaultdict(int)
    for item in news_feed:
        polarity = item.get("sentiment_polarity") or item.get("sentiment_label") or "neutral"
        sentiment_dist[polarity] += 1

    scored = []
    for item in news_feed:
        score = _safe_float(item.get("score")) or 0.0
        conf = _safe_float(item.get("confidence")) or 0.0
        scored.append((abs(score) * conf, item))
    scored.sort(key=lambda x: x[0], reverse=True)

    top_headlines = []
    for _, item in scored[:STRATEGIST_NEWS_TOP_N]:
        top_headlines.append({
            "title": item.get("title", ""),
            "ticker": item.get("ticker"),
            "sentiment": item.get("sentiment_polarity") or "neutral",
            "score": round(_safe_float(item.get("score")) or 0.0, 3),
        })

    ticker_agg: dict[str, dict[str, Any]] = defaultdict(lambda: {"sum": 0.0, "count": 0, "labels": []})
    for item in news_feed:
        t = item.get("ticker")
        if not t:
            continue
        s = _safe_float(item.get("score")) or 0.0
        ticker_agg[t]["sum"] += s
        ticker_agg[t]["count"] += 1
        ticker_agg[t]["labels"].append(item.get("sentiment_polarity") or "neutral")

    ticker_summary = {}
    for t, agg in ticker_agg.items():
        if agg["count"] == 0:
            continue
        avg = agg["sum"] / agg["count"]
        dominant = Counter(agg["labels"]).most_common(1)[0][0]
        ticker_summary[t] = {"avg_score": round(avg, 3), "count": agg["count"], "dominant": dominant}

    return {
        "overall_sentiment": dict(sentiment_dist),
        "high_confidence_headlines": top_headlines,
        "ticker_sentiment_summary": ticker_summary,
    }


# ---------------------------------------------------------------------------
# Economic Digest
# ---------------------------------------------------------------------------

def _build_econ_digest() -> dict[str, Any] | None:
    from services.economic_calendar import _translate_event

    now_kst = datetime.now(_KST)
    lookback_start = (now_kst - timedelta(hours=STRATEGIST_ECON_LOOKBACK_HOURS)).strftime("%Y-%m-%d")
    rows = get_economic_events(date_from=lookback_start, limit=200)
    if not rows:
        return None

    upcoming: list[dict[str, Any]] = []
    surprises: list[dict[str, Any]] = []
    upcoming_cutoff = now_kst + timedelta(hours=STRATEGIST_ECON_UPCOMING_HOURS)

    for row in rows:
        importance = row.get("importance", 0)
        if importance < STRATEGIST_ECON_MIN_IMPORTANCE:
            continue
        event_at_str = row.get("event_at")
        if not event_at_str:
            continue
        try:
            event_at = datetime.fromisoformat(event_at_str)
            if event_at.tzinfo is None:
                event_at = event_at.replace(tzinfo=_KST)
        except (ValueError, TypeError):
            continue

        entry = {
            "event": row.get("event") or "",
            "event_ko": _translate_event(row.get("event") or ""),
            "currency": row.get("currency"),
            "importance": importance,
            "date": event_at.strftime("%Y-%m-%d %H:%M"),
            "actual": row.get("actual"),
            "forecast": row.get("forecast"),
            "previous": row.get("previous"),
        }

        if event_at > now_kst:
            if event_at <= upcoming_cutoff and len(upcoming) < STRATEGIST_ECON_MAX_UPCOMING:
                upcoming.append(entry)
        else:
            actual = row.get("actual")
            forecast = row.get("forecast")
            if actual and forecast and actual != forecast and len(surprises) < STRATEGIST_ECON_MAX_SURPRISES:
                entry["surprise_direction"] = "above" if _compare_values(actual, forecast) > 0 else "below"
                surprises.append(entry)

    if not upcoming and not surprises:
        return None
    return {"upcoming_high_impact": upcoming, "recent_surprises": surprises}


def _compare_values(actual: str, forecast: str) -> int:
    def _parse(s: str) -> float | None:
        s = s.strip().replace(",", "").replace("%", "").replace("K", "e3").replace("M", "e6").replace("B", "e9")
        try:
            return float(s)
        except (ValueError, TypeError):
            return None
    a, f = _parse(actual), _parse(forecast)
    if a is not None and f is not None:
        return 1 if a > f else (-1 if a < f else 0)
    return 0


# ---------------------------------------------------------------------------
# Sector Computation
# ---------------------------------------------------------------------------

def _compute_sector_data(rows: list[dict[str, Any]], ticker_to_sector: dict[str, str]) -> list[dict[str, Any]]:
    aggregates: dict[str, dict[str, float]] = {}
    counts: dict[str, dict[str, int]] = {}

    for row in rows:
        ticker = (row.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        sector = ticker_to_sector.get(ticker) or "Unknown"
        aggregates.setdefault(sector, {"return_sum": 0.0, "sentiment_sum": 0.0, "divergence_sum": 0.0})
        counts.setdefault(sector, {"return_count": 0, "sentiment_count": 0, "divergence_count": 0})

        for key, col in [("return", "price_return"), ("sentiment", "sentiment"), ("divergence", "divergence")]:
            v = _safe_float(row.get(col))
            if v is not None:
                aggregates[sector][f"{key}_sum"] += v
                counts[sector][f"{key}_count"] += 1

    sector_data = []
    for sector, agg in aggregates.items():
        c = counts[sector]
        if c["divergence_count"] == 0:
            continue
        sector_data.append({
            "sector": sector,
            "avg_return": agg["return_sum"] / c["return_count"] if c["return_count"] > 0 else None,
            "avg_sentiment": agg["sentiment_sum"] / c["sentiment_count"] if c["sentiment_count"] > 0 else None,
            "avg_divergence": agg["divergence_sum"] / c["divergence_count"],
        })

    sector_data.sort(key=lambda x: x.get("avg_divergence") or STRATEGIST_DIVERGENCE_FALLBACK, reverse=True)
    return sector_data


# ---------------------------------------------------------------------------
# Sector Resolution
# ---------------------------------------------------------------------------

async def _resolve_sector_yfinance(ticker: str) -> str:
    def _fetch() -> Any:
        info = yf.Ticker(ticker).info or {}
        return info.get("sector")
    try:
        sector = await asyncio.wait_for(asyncio.to_thread(_fetch), timeout=STRATEGIST_YFINANCE_SECTOR_TIMEOUT_SEC)
        if isinstance(sector, str) and sector.strip():
            return sector.strip()
    except (asyncio.TimeoutError, Exception):
        pass
    return "Unknown"


async def _resolve_tickers_to_sectors(rows: list[dict[str, Any]]) -> dict[str, str]:
    unique = sorted({(r.get("ticker") or "").upper().strip() for r in rows if r.get("ticker")})
    unique = [t for t in unique if t]
    result: dict[str, str] = {}

    for t in unique:
        if t in STRATEGIST_TICKER_SECTOR_MAP:
            result[t] = STRATEGIST_TICKER_SECTOR_MAP[t]
        elif t in _sector_cache:
            result[t] = _sector_cache[t]

    remaining = [t for t in unique if t not in result][:STRATEGIST_MAX_YFINANCE_SECTOR_CALLS_PER_REQUEST]
    if remaining:
        tasks = [_resolve_sector_yfinance(t) for t in remaining]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for t, res in zip(remaining, results):
            sector = res if isinstance(res, str) and res.strip() else "Unknown"
            result[t] = sector
            _sector_cache[t] = sector

    for t in unique:
        result.setdefault(t, "Unknown")
    return result


# ---------------------------------------------------------------------------
# Technicals for Top Candidates
# ---------------------------------------------------------------------------

def _compute_candidate_technicals(rows: list[dict[str, Any]], max_tickers: int = 15) -> dict[str, dict[str, Any]]:
    """상위 후보 종목의 기술적 지표를 계산한다."""
    from services.technicals import compute_technicals, calc_stop_loss_and_targets

    tickers = []
    for row in rows:
        t = (row.get("ticker") or "").upper().strip()
        if t and t not in tickers:
            tickers.append(t)
        if len(tickers) >= max_tickers:
            break

    result: dict[str, dict[str, Any]] = {}
    for ticker in tickers:
        tech = compute_technicals(ticker)
        if not tech:
            continue

        # 손절/목표가 계산
        price = tech.get("current_price")
        atr = tech.get("atr_14")
        support = tech.get("support")
        resistance = tech.get("resistance")

        if price and atr:
            sl_data = calc_stop_loss_and_targets(
                current_price=price, atr=atr, support=support, resistance=resistance, direction="BUY"
            )
            tech.update(sl_data)

        result[ticker] = tech

    return result


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_strategy_json(data: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("AI 응답 JSON이 dict가 아닙니다")
    missing = _REQUIRED_KEYS - set(data.keys())
    if missing:
        raise ValueError(f"필수 키 누락: {sorted(missing)}")

    if not isinstance(data.get("market_summary"), str) or not data["market_summary"].strip():
        raise ValueError("market_summary가 비어 있습니다")

    top_sector = data.get("top_sector")
    if not isinstance(top_sector, dict) or not top_sector.get("name"):
        raise ValueError("top_sector 구조 오류")

    recs = data.get("recommendations")
    if not isinstance(recs, list) or not recs:
        # top_picks 호환
        recs = data.get("top_picks")
        if isinstance(recs, list) and recs:
            data["recommendations"] = recs
        else:
            raise ValueError("recommendations가 비어 있습니다")

    for i, pick in enumerate(data["recommendations"]):
        if not isinstance(pick, dict) or not pick.get("ticker"):
            raise ValueError(f"recommendations[{i}] 구조 오류")

    # 선택적 키 기본값
    data.setdefault("market_regime", "unknown")
    data.setdefault("market_regime_detail", "")
    data.setdefault("news_themes", [])
    data.setdefault("econ_analysis", {"summary": "", "recent_surprises": [], "upcoming_risks": []})
    data.setdefault("risk_warnings", [])
    data.setdefault("sector_rotation", "mixed")

    return data


# ---------------------------------------------------------------------------
# OpenAI Call
# ---------------------------------------------------------------------------

async def _call_openai_strategy(
    sector_data: list[dict[str, Any]],
    sector_etf: list[dict[str, Any]],
    macro: dict[str, Any] | None,
    market_gauge: int | None,
    vix: float | None,
    news_digest: dict[str, Any] | None,
    econ_digest: dict[str, Any] | None,
    technicals: dict[str, dict[str, Any]] | None,
) -> dict[str, Any]:
    if _client is None:
        raise RuntimeError("OPENAI_API_KEY가 설정되지 않았습니다")

    user_content: dict[str, Any] = {
        "sector_data": sector_data,
        "sector_etf": sector_etf,
        "macro_context": {"market_gauge": market_gauge, "vix": vix, "macro": macro},
    }
    if news_digest:
        user_content["news_digest"] = news_digest
    if econ_digest:
        user_content["econ_digest"] = econ_digest
    if technicals:
        user_content["technicals"] = technicals

    def _create() -> Any:
        kwargs: dict[str, Any] = {
            "model": STRATEGIST_OPENAI_MODEL,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(user_content, ensure_ascii=False)},
            ],
            "response_format": {"type": "json_object"},
            "timeout": STRATEGIST_OPENAI_TIMEOUT_SEC,
        }
        # gpt-5 등 temperature 미지원 모델 분기
        model_lower = (STRATEGIST_OPENAI_MODEL or "").lower()
        if "gpt-5" not in model_lower and "o1" not in model_lower and "o3" not in model_lower:
            kwargs["temperature"] = STRATEGIST_TEMPERATURE
        return _client.chat.completions.create(**kwargs)

    try:
        resp = await asyncio.wait_for(
            asyncio.to_thread(_create),
            timeout=STRATEGIST_OPENAI_TIMEOUT_SEC + STRATEGIST_OPENAI_THREAD_BUFFER_SEC,
        )
    except asyncio.TimeoutError as e:
        raise TimeoutError("OpenAI 전략가 호출 타임아웃") from e

    content = resp.choices[0].message.content or ""
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON 파싱 실패: {e}") from e

    return _validate_strategy_json(parsed)


# ---------------------------------------------------------------------------
# Response Assembly
# ---------------------------------------------------------------------------

_EMPTY_DEFAULTS: dict[str, Any] = {
    "market_regime": "unknown",
    "market_regime_detail": "",
    "news_themes": [],
    "econ_analysis": {"summary": "", "recent_surprises": [], "upcoming_risks": []},
    "risk_warnings": [],
    "sector_rotation": "mixed",
}


def _assemble_response(
    strategy_json: dict[str, Any],
    sector_data: list[dict[str, Any]],
    sector_etf: list[dict[str, Any]],
    technicals: dict[str, dict[str, Any]] | None,
    fear_greed: dict[str, Any],
) -> dict[str, Any]:
    result = {**_EMPTY_DEFAULTS, **strategy_json}

    # recommendations에 기술적 지표 데이터 보강
    if technicals:
        for rec in result.get("recommendations", []):
            ticker = (rec.get("ticker") or "").upper()
            tech = technicals.get(ticker)
            if tech:
                rec.setdefault("current_price", tech.get("current_price"))
                rec.setdefault("stop_loss", tech.get("stop_loss"))
                rec.setdefault("stop_loss_pct", tech.get("stop_loss_pct"))
                rec.setdefault("targets", tech.get("targets", []))
                rec.setdefault("risk_reward_ratio", tech.get("risk_reward_ratio"))
                rec["technicals"] = {
                    "rsi_14": tech.get("rsi_14"),
                    "rsi_signal": tech.get("rsi_signal"),
                    "macd_signal": tech.get("macd_signal"),
                    "ma_position": tech.get("ma_position"),
                    "bollinger_position": tech.get("bollinger_position"),
                    "atr_14": tech.get("atr_14"),
                    "volume_ratio": tech.get("volume_ratio"),
                    "support": tech.get("support"),
                    "resistance": tech.get("resistance"),
                    "ma_20": tech.get("ma_20"),
                    "ma_50": tech.get("ma_50"),
                    "ma_200": tech.get("ma_200"),
                    "bb_upper": tech.get("bb_upper"),
                    "bb_lower": tech.get("bb_lower"),
                }

    result["sector_data"] = sector_data
    result["sector_etf"] = sector_etf
    result["sector_chart"] = [{"sector": s["sector"], "avg_divergence": s.get("avg_divergence")} for s in sector_data]
    result["fear_greed"] = fear_greed
    result["generated_at"] = datetime.now().isoformat()

    return sanitize_for_json(result)


def _fallback_response(rows: list[dict[str, Any]], sector_data: list[dict[str, Any]], exc: BaseException) -> dict[str, Any]:
    logger.warning("OpenAI 전략가 호출 실패, fallback: %s", exc, exc_info=True)
    top_sector_name = (sector_data[0].get("sector") if sector_data else "Unknown") or "Unknown"
    n = max(0, STRATEGIST_FALLBACK_TOP_PICKS_N)
    recs = [
        {
            "ticker": (r.get("ticker") or "").upper(),
            "direction": r.get("signal", "HOLD"),
            "confidence": "low",
            "strategy_type": "swing",
            "holding_period": "-",
            "rationale": "전략가 모델 호출 실패로 스캔 데이터 기반 후보입니다.",
            "risk_factors": "AI 분석 불가 상태",
            "technicals_summary": "-",
        }
        for r in rows[:n] if (r.get("ticker") or "").strip()
    ]
    return {
        "market_regime": "unknown",
        "market_regime_detail": f"전략가 모델 호출 실패 ({type(exc).__name__})",
        "market_summary": "전략가 모델 호출에 실패해 섹터 집계 기반으로 요약합니다.",
        "top_sector": {"name": top_sector_name, "name_ko": "", "reason": "섹터별 평균 괴리율 기준 상위"},
        "recommendations": recs,
        **_EMPTY_DEFAULTS,
    }


def _empty_response() -> dict[str, Any]:
    return sanitize_for_json({
        "market_summary": "최근 스캔 데이터가 없습니다. 다음 업데이트를 기다려주세요.",
        "top_sector": {"name": "Unknown", "name_ko": "", "reason": "데이터 없음"},
        "recommendations": [],
        **_EMPTY_DEFAULTS,
        "sector_data": [],
        "sector_etf": [],
        "sector_chart": [],
        "fear_greed": {"gauge": None, "label": "-", "vix": None},
        "generated_at": datetime.now().isoformat(),
    })


def _fatal_response(exc: BaseException) -> dict[str, Any]:
    logger.exception("전략 브리핑 치명적 오류: %s", exc)
    return sanitize_for_json({
        "market_summary": f"전략 브리핑 생성 실패 ({type(exc).__name__})",
        "top_sector": {"name": "Unknown", "name_ko": "", "reason": "생성 불가"},
        "recommendations": [],
        **_EMPTY_DEFAULTS,
        "sector_data": [],
        "sector_etf": [],
        "sector_chart": [],
        "fear_greed": {"gauge": None, "label": "-", "vix": None},
        "generated_at": datetime.now().isoformat(),
    })


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _gauge_label(gauge: int | None) -> str:
    if gauge is None:
        return "-"
    if gauge >= 80:
        return "극도탐욕"
    if gauge >= 60:
        return "탐욕"
    if gauge >= 40:
        return "중립"
    if gauge >= 20:
        return "공포"
    return "극도공포"


def _is_cache_fresh(at: datetime | None) -> bool:
    if at is None:
        return False
    return (datetime.now() - at).total_seconds() < STRATEGIST_CACHE_TTL_SEC


async def build_market_strategy(
    macro: dict[str, Any] | None,
    market_gauge: int | None,
    vix: float | None,
    news_feed: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rows = await asyncio.to_thread(get_latest_scan_records)
    if not rows:
        return _empty_response()

    # 병렬 준비: 섹터 해석 + 경제 다이제스트 + 섹터 ETF + 기술적 지표
    from services.sector_tracker import fetch_sector_performance, determine_sector_rotation

    sector_task = _resolve_tickers_to_sectors(rows)
    econ_task = asyncio.to_thread(_build_econ_digest)
    etf_task = asyncio.to_thread(fetch_sector_performance)
    tech_task = asyncio.to_thread(_compute_candidate_technicals, rows)

    ticker_to_sector, econ_digest, sector_etf, technicals = await asyncio.gather(
        sector_task, econ_task, etf_task, tech_task
    )

    sector_data = _compute_sector_data(rows, ticker_to_sector)
    news_digest = _build_news_digest(news_feed)
    sector_rotation = determine_sector_rotation(sector_etf)

    fear_greed = {
        "gauge": market_gauge,
        "label": _gauge_label(market_gauge),
        "vix": vix,
    }

    try:
        strategy_json = await _call_openai_strategy(
            sector_data, sector_etf, macro, market_gauge, vix,
            news_digest, econ_digest, technicals,
        )
    except Exception as e:
        strategy_json = _fallback_response(rows, sector_data, e)

    strategy_json.setdefault("sector_rotation", sector_rotation)

    return _assemble_response(strategy_json, sector_data, sector_etf, technicals, fear_greed)


async def get_cached_market_strategy(
    macro: dict[str, Any] | None,
    market_gauge: int | None,
    vix: float | None,
    news_feed: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    global _strategy_cache, _strategy_cache_at

    if _is_cache_fresh(_strategy_cache_at) and _strategy_cache is not None:
        return _strategy_cache

    async with _strategy_lock:
        if _is_cache_fresh(_strategy_cache_at) and _strategy_cache is not None:
            return _strategy_cache

        try:
            result = await build_market_strategy(macro, market_gauge, vix, news_feed)
        except Exception as e:
            if _strategy_cache is not None:
                return _strategy_cache
            result = _fatal_response(e)

        _strategy_cache = result
        _strategy_cache_at = datetime.now()
        return _strategy_cache
