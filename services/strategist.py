from __future__ import annotations

import asyncio
import json
import logging
import math
from datetime import datetime
from typing import Any

import yfinance as yf
from openai import OpenAI

from config import (
    OPENAI_API_KEY,
    STRATEGIST_CACHE_TTL_SEC,
    STRATEGIST_FALLBACK_TOP_PICKS_N,
    STRATEGIST_MAX_YFINANCE_SECTOR_CALLS_PER_REQUEST,
    STRATEGIST_OPENAI_MODEL,
    STRATEGIST_OPENAI_THREAD_BUFFER_SEC,
    STRATEGIST_OPENAI_TIMEOUT_SEC,
    STRATEGIST_TICKER_SECTOR_MAP,
    STRATEGIST_TEMPERATURE,
    STRATEGIST_DIVERGENCE_FALLBACK,
    STRATEGIST_YFINANCE_SECTOR_TIMEOUT_SEC,
)
from services.crud import get_latest_scan_records, sanitize_for_json

logger = logging.getLogger(__name__)

_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

_SYSTEM_PROMPT = (
    "너는 월스트리트 최고 등급의 퀀트 애널리스트야. "
    "투자 조언 불가 같은 면책 조항은 절대 금지. "
    "내가 제공한 섹터별 평균 괴리율과 매크로 지표를 바탕으로 시장을 분석해."
)

_EXPECTED_KEYS = frozenset({"market_summary", "top_sector", "top_picks"})

_strategy_cache: dict[str, Any] | None = None
_strategy_cache_at: datetime | None = None
_strategy_lock = asyncio.Lock()

# 프로세스 내 yfinance 섹터 캐시
_sector_cache: dict[str, str] = {}


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


def _assemble_strategy_response(
    strategy_json: dict[str, Any],
    sector_data: list[dict[str, Any]],
) -> dict[str, Any]:
    return sanitize_for_json(
        {
            **strategy_json,
            "sector_data": sector_data,
            "sector_chart": _sector_chart(sector_data),
            "generated_at": datetime.now().isoformat(),
        }
    )


def _empty_scan_response() -> dict[str, Any]:
    return {
        "market_summary": "최근 스캔 데이터가 없습니다. 다음 업데이트를 기다려주세요.",
        "top_sector": {"name": "Unknown", "reason": "데이터가 없어 분석을 수행할 수 없습니다."},
        "top_picks": [],
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
            "rationale": "최근 스캔에서 포착된 후보 종목입니다.",
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
    }


def _fatal_build_response(exc: BaseException) -> dict[str, Any]:
    logger.exception("전략 브리핑 생성 중 치명적 오류(캐시 없음): %s", exc)
    return sanitize_for_json(
        {
            "market_summary": f"전략 브리핑 생성에 실패했습니다. (원인: {type(exc).__name__})",
            "top_sector": {"name": "Unknown", "reason": "현재 브리핑을 생성할 수 없습니다."},
            "top_picks": [],
            "sector_data": [],
            "sector_chart": [],
            "generated_at": datetime.now().isoformat(),
        }
    )


async def _resolve_sector_yfinance(ticker: str) -> str:
    """yfinance Ticker.info의 sector 조회(타임아웃으로 hang 방지)."""

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


def _validate_strategy_json(data: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("AI 응답 JSON이 dict가 아닙니다")
    missing = _EXPECTED_KEYS - set(data.keys())
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

    return data


async def _call_openai_strategy(
    sector_data: list[dict[str, Any]],
    macro: dict[str, Any] | None,
    market_gauge: int | None,
    vix: float | None,
) -> dict[str, Any]:
    if _client is None:
        raise RuntimeError("OPENAI_API_KEY가 설정되지 않았습니다")

    user_content = {
        "sector_data": sector_data,
        "macro_context": {
            "market_gauge": market_gauge,
            "vix": vix,
            "macro": macro,
        },
    }

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


def _is_cache_fresh(at: datetime | None) -> bool:
    if at is None:
        return False
    return (datetime.now() - at).total_seconds() < STRATEGIST_CACHE_TTL_SEC


async def build_market_strategy(
    macro: dict[str, Any] | None,
    market_gauge: int | None,
    vix: float | None,
) -> dict[str, Any]:
    rows = await asyncio.to_thread(get_latest_scan_records)
    if not rows:
        return sanitize_for_json(_empty_scan_response())

    ticker_to_sector = await _resolve_tickers_to_sectors(rows)
    sector_data = _compute_sector_data(rows, ticker_to_sector)

    try:
        strategy_json = await _call_openai_strategy(sector_data, macro, market_gauge, vix)
    except Exception as e:
        strategy_json = _heuristic_strategy_on_llm_failure(rows, sector_data, e)

    return _assemble_strategy_response(strategy_json, sector_data)


async def get_cached_market_strategy(
    macro: dict[str, Any] | None,
    market_gauge: int | None,
    vix: float | None,
) -> dict[str, Any]:
    global _strategy_cache, _strategy_cache_at

    if _is_cache_fresh(_strategy_cache_at) and _strategy_cache is not None:
        return _strategy_cache

    async with _strategy_lock:
        if _is_cache_fresh(_strategy_cache_at) and _strategy_cache is not None:
            return _strategy_cache

        try:
            result = await build_market_strategy(macro, market_gauge, vix)
        except Exception as e:
            if _strategy_cache is not None:
                return _strategy_cache
            result = _fatal_build_response(e)

        _strategy_cache = result
        _strategy_cache_at = datetime.now()
        return _strategy_cache
