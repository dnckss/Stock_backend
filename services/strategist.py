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
    STRATEGIST_GAUGE_FEAR,
    STRATEGIST_HIGH_RISK_ECON_KEYWORDS,
    STRATEGIST_MAX_YFINANCE_SECTOR_CALLS_PER_REQUEST,
    STRATEGIST_NEWS_PER_TICKER_MAX,
    STRATEGIST_NEWS_TOP_N,
    STRATEGIST_OPENAI_MODEL,
    STRATEGIST_OPENAI_THREAD_BUFFER_SEC,
    STRATEGIST_OPENAI_TIMEOUT_SEC,
    STRATEGIST_TEMPERATURE,
    STRATEGIST_TICKER_SECTOR_MAP,
    STRATEGIST_VIX_ELEVATED,
    STRATEGIST_VIX_EXTREME,
    STRATEGIST_YFINANCE_SECTOR_TIMEOUT_SEC,
)
from services.crud import get_economic_events, get_latest_scan_records, sanitize_for_json, save_strategy_history

logger = logging.getLogger(__name__)

_client = OpenAI(
    api_key=OPENAI_API_KEY,
    max_retries=2,
    timeout=600,                # httpx 타임아웃 10분 (gpt-5 전문 분석 대기)
) if OPENAI_API_KEY else None

_KST = timezone(timedelta(hours=9))

# ---------------------------------------------------------------------------
# System Prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
너는 월스트리트 최고 등급의 퀀트 전략가이자 포트폴리오 매니저야.
전문적이고 자신감 있는 톤으로 분석하되, 데이터가 불충분하거나 신호가 혼재할 때는 그 불확실성을 솔직히 반영해.

## 제공 데이터
1. **analysis_context**: 분석 시점, 시장 상태, 리스크 플래그 — ⚠ risk_flags가 비어 있지 않으면 최우선 고려
2. **sector_data**: 섹터별 평균 괴리율·감성·수익률 (스캔 기반)
3. **sector_etf**: 섹터 ETF 주간/월간 수익률 + 모멘텀 + 로테이션 방향
4. **macro_context**: VIX + vix_regime, 시장 gauge + gauge_label, 주요 지수/환율 (_meta.freshness로 신선도 확인)
5. **news_digest**: 뉴스 감성 분석 — age_hours로 시간 경과 확인, 티커별 max 2건으로 중복 제거됨
6. **econ_digest**: 경제 캘린더 — hours_until/hours_ago로 시간 거리, surprise_magnitude로 서프라이즈 크기 확인
7. **technicals**: 후보 종목 기술적 지표 요약 (_meta.basis=daily, signal_summary + bias + 핵심 수치)

## 신호 우선순위 (충돌 시 상위가 하위를 override)
1. **거시 리스크**: vix_regime이 elevated/extreme이거나 gauge_label이 공포/극도공포 → 방어적 해석 우선, 공격적 BUY 최소화
2. **고임팩트 경제 이벤트**: hours_until ≤ 24인 importance=3 이벤트 → 해당 이벤트 전 공격적 진입 자제, 이벤트 리스크 명시
3. **섹터 ETF 모멘텀**: 실제 자금 흐름 반영 → 섹터 선택의 1차 근거
4. **뉴스 감성**: dominant_sentiment과 분포 비율 기반 판단 — 단일 헤드라인에 의존 금지
5. **기술적 지표**: bias + signal_summary 기반 진입점/손절/목표가 판단의 보조 근거

## 해석 제약
- 데이터가 부족하거나 신호가 모순될 때는 추천 종목 수를 2개 이하로 줄이고 confidence를 낮춰라.
- **단일 뉴스 헤드라인 하나만으로 방향성을 결정하지 마라.** 반드시 2개 이상의 데이터 소스를 교차 확인.
- 각 추천의 rationale에 반드시 **2개 이상의 데이터 카테고리**(매크로+기술적, 뉴스+섹터ETF 등)를 인용하라.
- risk_warnings에는 **현재 데이터에서 확인 가능한 구체적 리스크만** 기술하라 (일반론 금지).
- 6시간 이상 경과한 뉴스(age_hours ≥ 6)는 단독 근거로 사용하지 마라.
- analysis_context.risk_flags가 비어 있지 않으면, market_summary 첫 문장에서 해당 리스크를 언급하라.

## 분석 규칙
- **market_regime**: 매크로 지표 + VIX + 섹터 ETF 흐름을 종합하여 시장 국면을 판단. market_regime_conviction(0.0~1.0)으로 확신도 표현.
- **news_themes**: 뉴스에서 시장을 움직이는 핵심 테마 2~4개를 도출해.
- **econ_analysis**: 경제 서프라이즈의 시장 영향 + 다가오는 리스크 이벤트를 분석해.
- **recommendations**: 3~5개 종목 추천 (데이터 불충분 시 2개 이하 가능). 반드시 아래 규칙:
  - **direction**(BUY/SELL), **confidence**(high/medium/low), **confidence_score**(0.0~1.0)
  - **signal_drivers**: 이 추천을 뒷받침하는 데이터 소스 목록 (예: ["macd_bullish_cross", "sector_etf_up", "news_positive"])
  - **strategy_type**: scalp(당일) / swing(1~2주) / position(1개월+)
  - **entry_zone**: 진입 가격대 (low ~ high)
  - **stop_loss**: 손절라인 (ATR + 지지선 기반)
  - **stop_loss_pct**: 현재가 대비 손절 비율(%)
  - **targets**: 목표가 2개 (TP1, TP2) + 각각 %
  - **risk_reward_ratio**: 리스크 대비 리워드 비율
  - **rationale**: 2개 이상 데이터 카테고리를 인용한 구체적 근거 (3~4문장)
  - **risk_factors**: 해당 종목의 구체적 리스크
  - **technicals_summary**: 주요 기술적 지표 한줄 요약
- **risk_warnings**: 전체 포트폴리오 리스크 경고 1~3개 (구체적 데이터 근거 필수)
- 모든 내용은 **한국어**로 작성해.

## 응답 JSON (반드시 이 구조로)
```json
{
  "market_regime": "bullish|bearish|sideways|volatile",
  "market_regime_conviction": 0.75,
  "market_regime_detail": "시장 국면 판단 근거 1~2문장 (데이터 인용 포함)",
  "market_summary": "시장 전체 상황 요약 3~5문장 (리스크 플래그 있으면 첫 문장에서 언급)",
  "top_sector": {
    "name": "섹터명",
    "name_ko": "한국어 섹터명",
    "reason": "선택 이유 (ETF 모멘텀 + 추가 근거)",
    "signal_drivers": ["etf_momentum", "sector_divergence"]
  },
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
      "confidence_score": 0.82,
      "signal_drivers": ["macd_bullish_cross", "sector_etf_up", "news_positive"],
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
      "rationale": "2개 이상 데이터 소스를 인용한 구체적 근거 3~4문장",
      "risk_factors": "해당 종목 구체적 리스크",
      "technicals_summary": "RSI 58(중립), MACD 골든크로스, 50일선 위, 거래량 135%"
    }
  ],
  "risk_warnings": ["구체적 데이터 근거가 있는 경고만"]
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
# Numeric Parsing (K/M/B 접미사 지원)
# ---------------------------------------------------------------------------

def _parse_numeric(s: str) -> float | None:
    """숫자 문자열을 float로 파싱. K/M/B/% 접미사 자동 처리."""
    if not s:
        return None
    s = s.strip().replace(",", "").replace("%", "")
    s = s.replace("K", "e3").replace("M", "e6").replace("B", "e9")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Signal Classification Helpers
# ---------------------------------------------------------------------------

def _vix_regime(vix: float | None) -> str:
    """VIX 값을 시장 변동성 국면 라벨로 분류한다."""
    if vix is None:
        return "unknown"
    if vix >= STRATEGIST_VIX_EXTREME:
        return "extreme"
    if vix >= STRATEGIST_VIX_ELEVATED:
        return "elevated"
    if vix >= 15:
        return "normal"
    return "low"


def _detect_risk_flags(
    vix: float | None,
    market_gauge: int | None,
    econ_digest: dict[str, Any] | None,
) -> list[str]:
    """현재 데이터에서 주요 리스크 플래그를 추출한다."""
    flags: list[str] = []
    if vix is not None and vix >= STRATEGIST_VIX_ELEVATED:
        flags.append(f"VIX {vix:.1f} (elevated 이상)")
    if market_gauge is not None and market_gauge <= STRATEGIST_GAUGE_FEAR:
        flags.append(f"시장 공포 gauge {market_gauge}")
    if econ_digest:
        for ev in econ_digest.get("upcoming_high_impact", []):
            event_name = ev.get("event", "")
            hours = ev.get("hours_until")
            if hours is not None and hours <= 48:
                for kw in STRATEGIST_HIGH_RISK_ECON_KEYWORDS:
                    if kw.lower() in event_name.lower():
                        flags.append(f"{ev.get('event_ko') or event_name} ({hours:.0f}h 후)")
                        break
    return flags


def _build_analysis_context(
    vix: float | None,
    market_gauge: int | None,
    econ_digest: dict[str, Any] | None,
) -> dict[str, Any]:
    """LLM에 전달할 분석 컨텍스트(시점, 리스크 플래그)를 구성한다."""
    risk_flags = _detect_risk_flags(vix, market_gauge, econ_digest)
    now_kst = datetime.now(_KST)
    # 미국 시장 개장 시간 간이 판단 (KST 기준, EDT/EST 차이 무시)
    wd = now_kst.weekday()
    h = now_kst.hour
    if wd >= 5:
        market_hours = "weekend"
    elif 23 <= h or h < 6:
        market_hours = "us_regular"
    else:
        market_hours = "us_closed"
    return {
        "generated_at": now_kst.isoformat(),
        "market_hours": market_hours,
        "high_macro_risk": len(risk_flags) > 0,
        "risk_flags": risk_flags,
    }


# ---------------------------------------------------------------------------
# Technicals Summary Helpers
# ---------------------------------------------------------------------------

_MACD_KO = {
    "bullish_cross": "골든크로스", "bearish_cross": "데드크로스",
    "bullish": "강세", "bearish": "약세", "neutral": "중립",
}
_RSI_KO = {"oversold": "과매도", "overbought": "과매수", "neutral": "중립"}
_MA_KO = {
    "above_200": "200일선 위", "above_50": "50일선 위",
    "below_50": "50일선 아래", "below_200": "200일선 아래",
}


def _build_signal_summary(tech: dict[str, Any]) -> str:
    """기술적 지표를 한줄 요약 문자열로 압축한다."""
    parts: list[str] = []
    rsi = tech.get("rsi_14")
    rsi_sig = tech.get("rsi_signal", "neutral")
    if rsi is not None:
        parts.append(f"RSI {rsi:.0f}({_RSI_KO.get(rsi_sig, rsi_sig)})")
    macd = tech.get("macd_signal", "")
    if macd:
        parts.append(f"MACD {_MACD_KO.get(macd, macd)}")
    ma = tech.get("ma_position", "")
    if ma and ma in _MA_KO:
        parts.append(_MA_KO[ma])
    vr = tech.get("volume_ratio")
    if vr is not None:
        parts.append(f"거래량 {vr * 100:.0f}%")
    return " | ".join(parts) if parts else "-"


def _compute_tech_bias(tech: dict[str, Any]) -> str:
    """기술적 지표를 종합하여 bullish/bearish/neutral 편향을 산출한다."""
    score = 0
    rsi = tech.get("rsi_14")
    if rsi is not None:
        if rsi < 30:
            score += 1
        elif rsi > 70:
            score -= 1
    macd = tech.get("macd_signal", "")
    if macd in ("bullish", "bullish_cross"):
        score += 1
    elif macd in ("bearish", "bearish_cross"):
        score -= 1
    ma = tech.get("ma_position", "")
    if ma in ("above_200", "above_50"):
        score += 1
    elif ma in ("below_50", "below_200"):
        score -= 1
    if score >= 2:
        return "bullish"
    if score <= -2:
        return "bearish"
    return "neutral"


def _compress_technicals_for_llm(
    technicals: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """LLM에 전달할 기술적 지표를 요약형으로 압축한다 (토큰 절감)."""
    compressed: dict[str, Any] = {
        "_meta": {
            "basis": "daily",
            "period": "6mo",
            "as_of": datetime.now().strftime("%Y-%m-%d"),
        },
    }
    for ticker, tech in technicals.items():
        compressed[ticker] = {
            "signal_summary": _build_signal_summary(tech),
            "bias": _compute_tech_bias(tech),
            "current_price": tech.get("current_price"),
            "rsi_14": _safe_float(tech.get("rsi_14")),
            "macd_signal": tech.get("macd_signal"),
            "ma_position": tech.get("ma_position"),
            "volume_ratio": _safe_float(tech.get("volume_ratio")),
            "atr_14": _safe_float(tech.get("atr_14")),
            "support": _safe_float(tech.get("support")),
            "resistance": _safe_float(tech.get("resistance")),
        }
    return compressed


# ---------------------------------------------------------------------------
# News Digest
# ---------------------------------------------------------------------------

def _build_news_digest(news_feed: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    if not news_feed:
        return None

    now_ts = datetime.now(_KST).timestamp()

    sentiment_dist: dict[str, int] = defaultdict(int)
    confidence_sum = 0.0
    confidence_count = 0
    for item in news_feed:
        polarity = item.get("sentiment_polarity") or item.get("sentiment_label") or "neutral"
        sentiment_dist[polarity] += 1
        conf = _safe_float(item.get("confidence"))
        if conf is not None:
            confidence_sum += conf
            confidence_count += 1

    scored = []
    for item in news_feed:
        score = _safe_float(item.get("score")) or 0.0
        conf = _safe_float(item.get("confidence")) or 0.0
        scored.append((abs(score) * conf, item))
    scored.sort(key=lambda x: x[0], reverse=True)

    # 티커당 최대 N건으로 제한하여 단일 이벤트 과대 해석 방지
    top_headlines: list[dict[str, Any]] = []
    ticker_headline_count: dict[str, int] = defaultdict(int)
    for _, item in scored:
        if len(top_headlines) >= STRATEGIST_NEWS_TOP_N:
            break
        ticker = item.get("ticker") or ""
        if ticker and ticker_headline_count[ticker] >= STRATEGIST_NEWS_PER_TICKER_MAX:
            continue
        ts = _safe_float(item.get("timestamp"))
        age_hours = round((now_ts - ts) / 3600, 1) if ts else None
        top_headlines.append({
            "title": item.get("title", ""),
            "ticker": ticker or None,
            "sentiment": item.get("sentiment_polarity") or "neutral",
            "score": round(_safe_float(item.get("score")) or 0.0, 3),
            "age_hours": age_hours,
        })
        if ticker:
            ticker_headline_count[ticker] += 1

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

    # 전체 지배적 감성 판별
    dominant_overall = max(sentiment_dist, key=sentiment_dist.get) if sentiment_dist else "neutral"

    return {
        "_meta": {
            "as_of": datetime.now(_KST).isoformat(),
            "total_articles": len(news_feed),
            "avg_confidence": round(confidence_sum / confidence_count, 2) if confidence_count > 0 else None,
        },
        "overall_sentiment": dict(sentiment_dist),
        "dominant_sentiment": dominant_overall,
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
                entry["hours_until"] = round((event_at - now_kst).total_seconds() / 3600, 1)
                upcoming.append(entry)
        else:
            actual = row.get("actual")
            forecast = row.get("forecast")
            if actual and forecast and actual != forecast and len(surprises) < STRATEGIST_ECON_MAX_SURPRISES:
                entry["surprise_direction"] = "above" if _compare_values(actual, forecast) > 0 else "below"
                entry["hours_ago"] = round((now_kst - event_at).total_seconds() / 3600, 1)
                # 서프라이즈 크기 판정
                a_val = _parse_numeric(actual)
                f_val = _parse_numeric(forecast)
                if a_val is not None and f_val is not None and f_val != 0:
                    pct_diff = abs(a_val - f_val) / abs(f_val) * 100
                    if pct_diff >= 20:
                        entry["surprise_magnitude"] = "large"
                    elif pct_diff >= 10:
                        entry["surprise_magnitude"] = "moderate"
                    else:
                        entry["surprise_magnitude"] = "small"
                surprises.append(entry)

    if not upcoming and not surprises:
        return None
    return {
        "_meta": {"as_of": now_kst.isoformat()},
        "upcoming_high_impact": upcoming,
        "recent_surprises": surprises,
    }


def _compare_values(actual: str, forecast: str) -> int:
    a, f = _parse_numeric(actual), _parse_numeric(forecast)
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
        from services.yf_limiter import throttled
        info = throttled(lambda: yf.Ticker(ticker).info or {})
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
    data.setdefault("market_regime_conviction", 0.5)
    data.setdefault("market_regime_detail", "")
    data.setdefault("news_themes", [])
    data.setdefault("econ_analysis", {"summary": "", "recent_surprises": [], "upcoming_risks": []})
    data.setdefault("risk_warnings", [])
    data.setdefault("sector_rotation", "mixed")

    # 추천 종목별 선택적 필드 기본값
    for rec in data["recommendations"]:
        rec.setdefault("confidence_score", None)
        rec.setdefault("signal_drivers", [])

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
    technicals: dict[str, Any] | None,
) -> dict[str, Any]:
    if _client is None:
        raise RuntimeError("OPENAI_API_KEY가 설정되지 않았습니다")

    analysis_context = _build_analysis_context(vix, market_gauge, econ_digest)

    macro_context: dict[str, Any] = {
        "_meta": {"as_of": datetime.now(_KST).isoformat(), "freshness": "realtime"},
        "market_gauge": market_gauge,
        "gauge_label": _gauge_label(market_gauge),
        "vix": vix,
        "vix_regime": _vix_regime(vix),
    }
    if macro:
        macro_context["indicators"] = macro

    user_content: dict[str, Any] = {
        "analysis_context": analysis_context,
        "sector_data": sector_data,
        "sector_etf": sector_etf,
        "macro_context": macro_context,
    }
    if news_digest:
        user_content["news_digest"] = news_digest
    if econ_digest:
        user_content["econ_digest"] = econ_digest
    if technicals:
        user_content["technicals"] = technicals

    input_json = json.dumps(user_content, ensure_ascii=False)
    t_start = datetime.now()

    def _create() -> Any:
        kwargs: dict[str, Any] = {
            "model": STRATEGIST_OPENAI_MODEL,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": input_json},
            ],
            "response_format": {"type": "json_object"},
        }
        # gpt-5 등 temperature 미지원 모델 분기
        model_lower = (STRATEGIST_OPENAI_MODEL or "").lower()
        if "gpt-5" not in model_lower and "o1" not in model_lower and "o3" not in model_lower:
            kwargs["temperature"] = STRATEGIST_TEMPERATURE
        return _client.chat.completions.create(**kwargs)

    # 타임아웃 제한 없이 응답 완료까지 대기 (전문적 분석 품질 우선)
    resp = await asyncio.to_thread(_create)
    elapsed = (datetime.now() - t_start).total_seconds()

    # 운영 로깅: 토큰 사용량 + 응답 시간
    usage = getattr(resp, "usage", None)
    if usage:
        logger.info(
            "전략가 OpenAI 완료: %.1fs | 입력 %d tok, 출력 %d tok, 합계 %d tok",
            elapsed,
            getattr(usage, "prompt_tokens", 0),
            getattr(usage, "completion_tokens", 0),
            getattr(usage, "total_tokens", 0),
        )
    else:
        logger.info("전략가 OpenAI 완료: %.1fs (usage 미제공)", elapsed)

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
    "market_regime_conviction": 0.5,
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


# 이전 저장 추천 시그니처 (프로세스 내 캐시). 동일 추천 중복 저장 방지.
_last_saved_rec_signature: str | None = None


def _recommendations_signature(recs: list[dict[str, Any]], market_regime: str | None) -> str:
    """추천 리스트의 핵심 식별 필드로 해시 생성 — 동일하면 재저장 스킵."""
    parts: list[str] = [str(market_regime or "")]
    for rec in recs or []:
        ticker = (rec.get("ticker") or "").upper()
        direction = rec.get("direction") or ""
        confidence = rec.get("confidence") or ""
        stop_loss = rec.get("stop_loss")
        entry = rec.get("entry_zone") or {}
        entry_low = entry.get("low")
        entry_high = entry.get("high")
        parts.append(f"{ticker}|{direction}|{confidence}|{stop_loss}|{entry_low}|{entry_high}")
    import hashlib
    return hashlib.sha1("||".join(parts).encode("utf-8")).hexdigest()


async def _persist_recommendations_if_changed(response: dict[str, Any]) -> None:
    """이전 저장분과 내용이 다를 때만 strategy_history 에 insert."""
    global _last_saved_rec_signature
    recs = response.get("recommendations") or []
    if not recs:
        return
    # ticker + direction 최소 필드가 있어야 백테스트 대상
    valid = [r for r in recs if (r.get("ticker") or "").strip() and r.get("direction")]
    if not valid:
        return
    market_regime = response.get("market_regime")
    signature = _recommendations_signature(valid, market_regime)
    if signature == _last_saved_rec_signature:
        return
    try:
        await asyncio.to_thread(save_strategy_history, valid, market_regime)
        _last_saved_rec_signature = signature
        logger.info("strategy_history 저장 완료: %d건 (regime=%s)", len(valid), market_regime)
    except Exception as e:
        logger.warning("strategy_history 저장 실패: %s", e, exc_info=True)


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

    technicals_compressed = _compress_technicals_for_llm(technicals) if technicals else None

    is_fallback = False
    try:
        strategy_json = await _call_openai_strategy(
            sector_data, sector_etf, macro, market_gauge, vix,
            news_digest, econ_digest, technicals_compressed,
        )
    except Exception as e:
        strategy_json = _fallback_response(rows, sector_data, e)
        is_fallback = True

    # 코드 계산값을 단일 소스로 사용 (모델 출력 override)
    strategy_json["sector_rotation"] = sector_rotation
    strategy_json["data_quality"] = {
        "macro_fresh": macro is not None,
        "news_count": len(news_feed) if news_feed else 0,
        "econ_events_upcoming": len((econ_digest or {}).get("upcoming_high_impact", [])),
        "technicals_coverage": len(technicals) if technicals else 0,
    }

    response = _assemble_response(strategy_json, sector_data, sector_etf, technicals, fear_greed)

    # AI 추천을 strategy_history 에 영구 저장 (백테스트용).
    # fallback 응답은 품질이 낮아 저장하지 않는다.
    if not is_fallback:
        await _persist_recommendations_if_changed(response)

    return response


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
