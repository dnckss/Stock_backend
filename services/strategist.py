from __future__ import annotations

import asyncio
import json
import logging
import math
from collections import defaultdict
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
from services.crud import get_economic_events, get_latest_scan_records, sanitize_for_json

logger = logging.getLogger(__name__)

_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ---------------------------------------------------------------------------
# System Prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "너는 월스트리트 최고 등급의 퀀트 애널리스트이자 매크로 전략가야.\n"
    "투자 조언 불가 같은 면책 조항은 절대 금지.\n\n"
    "## 제공 데이터\n"
    "1. **sector_data**: 섹터별 평균 괴리율·감성·수익률 (스캔 기반)\n"
    "2. **macro_context**: VIX, 주요 지수, 환율, 시장 공포/탐욕 게이지(0~100)\n"
    "3. **news_digest**: 최근 뉴스 헤드라인 감성 분석 — 전체 감성 분포, 주요 헤드라인, 티커별 감성\n"
    "4. **econ_digest**: 경제 캘린더 — 향후 48h 고임팩트 이벤트 + 최근 24h 서프라이즈(실제 vs 예측 괴리)\n\n"
    "## 분석 규칙\n"
    "- 뉴스 헤드라인에서 시장을 움직이는 **핵심 테마 2~4개**를 도출해. 각 테마에 관련 티커와 감성 방향을 매핑해.\n"
    "- 경제 이벤트 **서프라이즈(실제 vs 예측 괴리)**가 시장/섹터에 미치는 즉각적 영향을 분석해.\n"
    "- 다가오는 고임팩트 이벤트(금리 결정, 고용지표 등)가 단기 시장 방향에 미칠 **리스크**를 경고해.\n"
    "- 종목 추천(top_picks)에는 반드시 **뉴스·경제 데이터 기반 구체적 근거**를 포함해.\n"
    "  - 단순히 '괴리율이 높다'가 아니라, 어떤 뉴스/이벤트가 해당 종목에 어떤 영향을 주는지 설명해.\n"
    "- top_picks는 **3~5개** 종목을 추천하되, 각각 BUY/SELL 방향과 확신도(high/medium/low)를 명시해.\n"
    "- 모든 내용은 **한국어**로 작성해.\n\n"
    "## 응답 JSON 형식 (반드시 이 구조로)\n"
    "```json\n"
    "{\n"
    '  "market_summary": "시장 전체 상황 요약 (3~5문장, 뉴스·경제·기술적 지표 통합 분석)",\n'
    '  "top_sector": {"name": "섹터명", "reason": "해당 섹터를 선택한 이유 (뉴스/경제 근거 포함)"},\n'
    '  "top_picks": [\n'
    '    {"ticker": "NVDA", "direction": "BUY", "confidence": "high", "rationale": "구체적 근거..."},\n'
    "    ...\n"
    "  ],\n"
    '  "news_themes": [\n'
    '    {"theme": "테마명", "tickers": ["NVDA","AMD"], "sentiment": "positive", "detail": "설명..."},\n'
    "    ...\n"
    "  ],\n"
    '  "econ_impact": "최근 경제 이벤트 결과가 시장에 미치는 영향 분석 (2~3문장)",\n'
    '  "risk_events": [\n'
    '    {"event": "이벤트명", "date": "2026-04-01", "risk_level": "high", "detail": "리스크 설명..."}\n'
    "  ]\n"
    "}\n"
    "```"
)

_REQUIRED_KEYS = frozenset({"market_summary", "top_sector", "top_picks"})

_strategy_cache: dict[str, Any] | None = None
_strategy_cache_at: datetime | None = None
_strategy_lock = asyncio.Lock()

# 프로세스 내 yfinance 섹터 캐시
_sector_cache: dict[str, str] = {}

_KST = timezone(timedelta(hours=9))


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
    if not math.isfinite(v):
        return None
    return v


# ---------------------------------------------------------------------------
# News Digest Builder
# ---------------------------------------------------------------------------

def _build_news_digest(news_feed: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    """뉴스 피드를 LLM 컨텍스트용 압축 다이제스트로 변환한다."""
    if not news_feed:
        return None

    # 1) 전체 감성 분포
    sentiment_dist: dict[str, int] = defaultdict(int)
    for item in news_feed:
        polarity = item.get("sentiment_polarity") or item.get("sentiment_label") or "neutral"
        sentiment_dist[polarity] += 1

    # 2) 주요 헤드라인 (abs(score) * confidence 상위 N개)
    scored = []
    for item in news_feed:
        score = _safe_float(item.get("score")) or 0.0
        conf = _safe_float(item.get("confidence")) or 0.0
        impact = abs(score) * conf
        scored.append((impact, item))
    scored.sort(key=lambda x: x[0], reverse=True)

    top_headlines = []
    for _, item in scored[:STRATEGIST_NEWS_TOP_N]:
        top_headlines.append({
            "title": item.get("title", ""),
            "ticker": item.get("ticker"),
            "sentiment": item.get("sentiment_polarity") or item.get("sentiment_label") or "neutral",
            "score": round(_safe_float(item.get("score")) or 0.0, 3),
        })

    # 3) 티커별 감성 집계
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
        from collections import Counter
        dominant = Counter(agg["labels"]).most_common(1)[0][0]
        ticker_summary[t] = {
            "avg_score": round(avg, 3),
            "count": agg["count"],
            "dominant": dominant,
        }

    return {
        "overall_sentiment": dict(sentiment_dist),
        "high_confidence_headlines": top_headlines,
        "ticker_sentiment_summary": ticker_summary,
    }


# ---------------------------------------------------------------------------
# Economic Calendar Digest Builder
# ---------------------------------------------------------------------------

def _build_econ_digest() -> dict[str, Any] | None:
    """경제 캘린더에서 예정 고임팩트 이벤트 + 최근 서프라이즈를 추출한다."""
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

        event_name = row.get("event") or ""
        event_ko = _translate_event(event_name)
        actual = row.get("actual")
        forecast = row.get("forecast")
        previous = row.get("previous")

        entry = {
            "event": event_name,
            "event_ko": event_ko,
            "currency": row.get("currency"),
            "importance": importance,
            "date": event_at.strftime("%Y-%m-%d %H:%M"),
            "actual": actual,
            "forecast": forecast,
            "previous": previous,
        }

        if event_at > now_kst:
            # 예정 이벤트
            if event_at <= upcoming_cutoff and len(upcoming) < STRATEGIST_ECON_MAX_UPCOMING:
                upcoming.append(entry)
        else:
            # 지난 이벤트 — 서프라이즈 체크
            if actual and forecast and actual != forecast and len(surprises) < STRATEGIST_ECON_MAX_SURPRISES:
                entry["surprise_direction"] = "above" if _compare_values(actual, forecast) > 0 else "below"
                surprises.append(entry)

    if not upcoming and not surprises:
        return None

    return {
        "upcoming_high_impact": upcoming,
        "recent_surprises": surprises,
    }


def _compare_values(actual: str, forecast: str) -> int:
    """실제값과 예측값을 비교하여 양수(상회)/음수(하회)/0(동일)을 반환한다."""
    def _parse_num(s: str) -> float | None:
        s = s.strip().replace(",", "").replace("%", "").replace("K", "e3").replace("M", "e6").replace("B", "e9")
        try:
            return float(s)
        except (ValueError, TypeError):
            return None

    a = _parse_num(actual)
    f = _parse_num(forecast)
    if a is not None and f is not None:
        if a > f:
            return 1
        elif a < f:
            return -1
    return 0


# ---------------------------------------------------------------------------
# Sector Computation (기존 유지)
# ---------------------------------------------------------------------------

def _compute_sector_data(rows: list[dict[str, Any]], ticker_to_sector: dict[str, str]) -> list[dict[str, Any]]:
    aggregates: dict[str, dict[str, float]] = {}
    counts: dict[str, dict[str, int]] = {}

    for row in rows:
        ticker = (row.get("ticker") or "").upper().strip()
        if not ticker:
            continue

        sector = ticker_to_sector.get(ticker) or "Unknown"

        aggregates.setdefault(
            sector,
            {"return_sum": 0.0, "sentiment_sum": 0.0, "divergence_sum": 0.0},
        )
        counts.setdefault(
            sector,
            {"return_count": 0, "sentiment_count": 0, "divergence_count": 0},
        )

        r = _safe_float(row.get("price_return"))
        s = _safe_float(row.get("sentiment"))
        d = _safe_float(row.get("divergence"))

        if r is not None:
            aggregates[sector]["return_sum"] += r
            counts[sector]["return_count"] += 1
        if s is not None:
            aggregates[sector]["sentiment_sum"] += s
            counts[sector]["sentiment_count"] += 1
        if d is not None:
            aggregates[sector]["divergence_sum"] += d
            counts[sector]["divergence_count"] += 1

    sector_data: list[dict[str, Any]] = []
    for sector, agg in aggregates.items():
        c = counts[sector]
        if c["divergence_count"] == 0:
            continue

        avg_return = agg["return_sum"] / c["return_count"] if c["return_count"] > 0 else None
        avg_sentiment = (
            agg["sentiment_sum"] / c["sentiment_count"] if c["sentiment_count"] > 0 else None
        )
        avg_divergence = agg["divergence_sum"] / c["divergence_count"]

        sector_data.append(
            {
                "sector": sector,
                "avg_return": avg_return,
                "avg_sentiment": avg_sentiment,
                "avg_divergence": avg_divergence,
            }
        )

    sector_data.sort(
        key=lambda x: x.get("avg_divergence") or STRATEGIST_DIVERGENCE_FALLBACK,
        reverse=True,
    )
    return sector_data


def _sector_chart(sector_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"sector": s["sector"], "avg_divergence": s.get("avg_divergence")}
        for s in sector_data
    ]


# ---------------------------------------------------------------------------
# Response Assembly
# ---------------------------------------------------------------------------

_NEW_FIELD_DEFAULTS: dict[str, Any] = {
    "news_themes": [],
    "econ_impact": None,
    "risk_events": [],
}


def _assemble_strategy_response(
    strategy_json: dict[str, Any],
    sector_data: list[dict[str, Any]],
) -> dict[str, Any]:
    result = {**_NEW_FIELD_DEFAULTS, **strategy_json}
    result["sector_data"] = sector_data
    result["sector_chart"] = _sector_chart(sector_data)
    result["generated_at"] = datetime.now().isoformat()
    return sanitize_for_json(result)


def _empty_scan_response() -> dict[str, Any]:
    return {
        "market_summary": "최근 스캔 데이터가 없습니다. 다음 업데이트를 기다려주세요.",
        "top_sector": {"name": "Unknown", "reason": "데이터가 없어 분석을 수행할 수 없습니다."},
        "top_picks": [],
        **_NEW_FIELD_DEFAULTS,
        "sector_data": [],
        "sector_chart": [],
        "generated_at": datetime.now().isoformat(),
    }


def _heuristic_strategy_on_llm_failure(
    rows: list[dict[str, Any]],
    sector_data: list[dict[str, Any]],
    exc: BaseException,
) -> dict[str, Any]:
    logger.warning("OpenAI 전략가 호출 실패, 섹터 집계 기반 fallback 사용: %s", exc, exc_info=True)
    top_sector_name = (sector_data[0].get("sector") if sector_data else "Unknown") or "Unknown"
    n = max(0, STRATEGIST_FALLBACK_TOP_PICKS_N)
    top_picks = [
        {
            "ticker": (r.get("ticker") or "").upper(),
            "direction": r.get("signal", "HOLD"),
            "confidence": "low",
            "rationale": "전략가 모델 호출 실패로 스캔 데이터 기반 후보 종목입니다.",
        }
        for r in rows[:n]
        if (r.get("ticker") or "").strip()
    ]
    return {
        "market_summary": (
            "전략가 모델 호출에 실패해 섹터 집계 기반으로 요약합니다. "
            f"(원인: {type(exc).__name__})"
        ),
        "top_sector": {
            "name": top_sector_name,
            "reason": "최근 스캔 데이터의 섹터별 평균 괴리율 기준 상위 섹터입니다.",
        },
        "top_picks": top_picks,
        **_NEW_FIELD_DEFAULTS,
        "econ_impact": "전략가 모델 호출 실패로 경제 이벤트 분석을 수행할 수 없습니다.",
    }


def _fatal_build_response(exc: BaseException) -> dict[str, Any]:
    logger.exception("전략 브리핑 생성 중 치명적 오류(캐시 없음): %s", exc)
    return sanitize_for_json(
        {
            "market_summary": f"전략 브리핑 생성에 실패했습니다. (원인: {type(exc).__name__})",
            "top_sector": {"name": "Unknown", "reason": "현재 브리핑을 생성할 수 없습니다."},
            "top_picks": [],
            **_NEW_FIELD_DEFAULTS,
            "sector_data": [],
            "sector_chart": [],
            "generated_at": datetime.now().isoformat(),
        }
    )


# ---------------------------------------------------------------------------
# Sector Resolution (기존 유지)
# ---------------------------------------------------------------------------

async def _resolve_sector_yfinance(ticker: str) -> str:
    def _fetch() -> Any:
        info = yf.Ticker(ticker).info or {}
        return info.get("sector")

    try:
        sector = await asyncio.wait_for(
            asyncio.to_thread(_fetch),
            timeout=STRATEGIST_YFINANCE_SECTOR_TIMEOUT_SEC,
        )
        if isinstance(sector, str) and sector.strip():
            return sector.strip()
    except asyncio.TimeoutError:
        return "Unknown"
    except Exception:
        return "Unknown"
    return "Unknown"


async def _resolve_tickers_to_sectors(rows: list[dict[str, Any]]) -> dict[str, str]:
    unique_tickers = sorted(
        {(r.get("ticker") or "").upper().strip() for r in rows if r.get("ticker")}
    )
    unique_tickers = [t for t in unique_tickers if t]

    ticker_to_sector: dict[str, str] = {}

    for t in unique_tickers:
        if t in STRATEGIST_TICKER_SECTOR_MAP:
            ticker_to_sector[t] = STRATEGIST_TICKER_SECTOR_MAP[t]
        elif t in _sector_cache:
            ticker_to_sector[t] = _sector_cache[t]

    remaining = [t for t in unique_tickers if t not in ticker_to_sector]
    remaining = remaining[:STRATEGIST_MAX_YFINANCE_SECTOR_CALLS_PER_REQUEST]

    if remaining:
        tasks = [_resolve_sector_yfinance(t) for t in remaining]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for t, res in zip(remaining, results):
            sector = "Unknown"
            if isinstance(res, str) and res.strip():
                sector = res.strip()
            ticker_to_sector[t] = sector
            _sector_cache[t] = sector

    for t in unique_tickers:
        if t not in ticker_to_sector:
            ticker_to_sector[t] = "Unknown"

    return ticker_to_sector


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_strategy_json(data: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("AI 응답 JSON이 dict가 아닙니다")
    missing = _REQUIRED_KEYS - set(data.keys())
    if missing:
        raise ValueError(f"AI 응답 JSON에 필수 키가 없습니다: {sorted(missing)}")

    if not isinstance(data.get("market_summary"), str) or not data["market_summary"].strip():
        raise ValueError("market_summary가 문자열이 아닙니다")

    top_sector = data.get("top_sector")
    if not isinstance(top_sector, dict):
        raise ValueError("top_sector가 객체가 아닙니다")
    if not isinstance(top_sector.get("name"), str) or not top_sector["name"].strip():
        raise ValueError("top_sector.name이 문자열이 아닙니다")
    if not isinstance(top_sector.get("reason"), str) or not top_sector["reason"].strip():
        raise ValueError("top_sector.reason이 문자열이 아닙니다")

    top_picks = data.get("top_picks")
    if not isinstance(top_picks, list) or not top_picks:
        raise ValueError("top_picks가 배열이 아니거나 비어 있습니다")
    for i, pick in enumerate(top_picks):
        if not isinstance(pick, dict):
            raise ValueError(f"top_picks[{i}]가 객체가 아닙니다")
        if not isinstance(pick.get("ticker"), str) or not pick["ticker"].strip():
            raise ValueError(f"top_picks[{i}].ticker가 문자열이 아닙니다")
        if not isinstance(pick.get("rationale"), str) or not pick["rationale"].strip():
            raise ValueError(f"top_picks[{i}].rationale이 문자열이 아닙니다")

    # 선택적 키 검증 (있으면 구조만 확인)
    if "news_themes" in data and not isinstance(data["news_themes"], list):
        data["news_themes"] = []
    if "risk_events" in data and not isinstance(data["risk_events"], list):
        data["risk_events"] = []
    if "econ_impact" in data and not isinstance(data["econ_impact"], str):
        data["econ_impact"] = None

    return data


# ---------------------------------------------------------------------------
# OpenAI Call
# ---------------------------------------------------------------------------

async def _call_openai_strategy(
    sector_data: list[dict[str, Any]],
    macro: dict[str, Any] | None,
    market_gauge: int | None,
    vix: float | None,
    news_digest: dict[str, Any] | None,
    econ_digest: dict[str, Any] | None,
) -> dict[str, Any]:
    if _client is None:
        raise RuntimeError("OPENAI_API_KEY가 설정되지 않았습니다")

    user_content: dict[str, Any] = {
        "sector_data": sector_data,
        "macro_context": {
            "market_gauge": market_gauge,
            "vix": vix,
            "macro": macro,
        },
    }
    if news_digest:
        user_content["news_digest"] = news_digest
    if econ_digest:
        user_content["econ_digest"] = econ_digest

    def _create() -> Any:
        return _client.chat.completions.create(
            model=STRATEGIST_OPENAI_MODEL,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(user_content, ensure_ascii=False)},
            ],
            temperature=STRATEGIST_TEMPERATURE,
            response_format={"type": "json_object"},
            timeout=STRATEGIST_OPENAI_TIMEOUT_SEC,
        )

    try:
        resp = await asyncio.wait_for(
            asyncio.to_thread(_create),
            timeout=STRATEGIST_OPENAI_TIMEOUT_SEC + STRATEGIST_OPENAI_THREAD_BUFFER_SEC,
        )
    except asyncio.TimeoutError as e:
        raise TimeoutError("OpenAI 전략가 호출이 타임아웃되었습니다") from e

    content = resp.choices[0].message.content or ""
    try:
        parsed: Any = json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"AI 응답을 JSON으로 파싱하지 못했습니다: {e}") from e

    return _validate_strategy_json(parsed)


# ---------------------------------------------------------------------------
# Cache & Orchestration
# ---------------------------------------------------------------------------

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
        return sanitize_for_json(_empty_scan_response())

    # 병렬: 섹터 해석 + 경제 다이제스트
    sector_task = _resolve_tickers_to_sectors(rows)
    econ_task = asyncio.to_thread(_build_econ_digest)
    ticker_to_sector, econ_digest = await asyncio.gather(sector_task, econ_task)

    sector_data = _compute_sector_data(rows, ticker_to_sector)
    news_digest = _build_news_digest(news_feed)

    try:
        strategy_json = await _call_openai_strategy(
            sector_data, macro, market_gauge, vix, news_digest, econ_digest
        )
    except Exception as e:
        strategy_json = _heuristic_strategy_on_llm_failure(rows, sector_data, e)

    return _assemble_strategy_response(strategy_json, sector_data)


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
            result = _fatal_build_response(e)

        _strategy_cache = result
        _strategy_cache_at = datetime.now()
        return _strategy_cache
