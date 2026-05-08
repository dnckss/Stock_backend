"""종목 검색용 universe.

S&P 500 구성종목 + strategy_history(AI 추천 이력) + analysis_results(스캐너 이력)
를 합집합으로 묶어 검색 가능한 종목 풀을 단일 소스로 제공한다.

기존에는 프런트가 latest_cache.top_picks/radar 처럼 좁은 source 만 보고 있어
백테스트 결과(AI 추천)에서 본 NCLH/ADM/HAL/OXY 같은 종목이 검색되지 않는
mismatch 가 발생했다. 이 모듈로 mismatch 를 해소한다.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Any

from config import (
    STOCK_UNIVERSE_CACHE_TTL_SEC,
    STOCK_UNIVERSE_DB_FETCH_LIMIT,
)
from services.crud import (
    get_analysis_results_tickers,
    get_strategy_history_tickers,
)
from services.scanner import get_sp500_constituents

logger = logging.getLogger(__name__)

_cache: dict[str, Any] | None = None
_cache_at: float = 0.0


def _is_fresh() -> bool:
    return _cache is not None and (time.time() - _cache_at) < STOCK_UNIVERSE_CACHE_TTL_SEC


def _build_universe_sync() -> dict[str, Any]:
    """동기 빌더 — to_thread 로 호출. 외부 IO(Wikipedia + Supabase) 합산."""
    sp500 = get_sp500_constituents()
    sp500_map: dict[str, dict[str, Any]] = {}
    for c in sp500:
        ticker = (c.get("ticker") or "").upper()
        if not ticker:
            continue
        sp500_map[ticker] = {
            "ticker": ticker,
            "name": c.get("name") or None,
            "sector": c.get("sector") or None,
            "in_sp500": True,
        }

    extra_tickers: set[str] = set()
    try:
        extra_tickers.update(
            get_strategy_history_tickers(limit=STOCK_UNIVERSE_DB_FETCH_LIMIT)
        )
    except Exception as e:
        logger.warning("strategy_history universe 조회 실패: %s", e)
    try:
        extra_tickers.update(
            get_analysis_results_tickers(limit=STOCK_UNIVERSE_DB_FETCH_LIMIT)
        )
    except Exception as e:
        logger.warning("analysis_results universe 조회 실패: %s", e)

    # S&P 500 미포함 ticker 는 ticker 만 채우고 name/sector 는 unknown
    for t in extra_tickers:
        if t and t not in sp500_map:
            sp500_map[t] = {
                "ticker": t,
                "name": None,
                "sector": None,
                "in_sp500": False,
            }

    items = sorted(sp500_map.values(), key=lambda x: x["ticker"])
    return {
        "items": items,
        "count": len(items),
        "sp500_count": sum(1 for v in sp500_map.values() if v["in_sp500"]),
        "extra_count": sum(1 for v in sp500_map.values() if not v["in_sp500"]),
    }


async def get_stock_universe(refresh: bool = False) -> dict[str, Any]:
    """종목 검색 universe 를 반환한다. 1시간 메모리 캐시(기본).

    Args:
        refresh: True 면 캐시 무시 후 재구성.
    """
    global _cache, _cache_at

    if not refresh and _is_fresh() and _cache is not None:
        return {
            **_cache,
            "cache_hit": True,
            "cache_ttl_sec": STOCK_UNIVERSE_CACHE_TTL_SEC,
            "fetched_at": datetime.fromtimestamp(_cache_at).isoformat(),
        }

    try:
        built = await asyncio.to_thread(_build_universe_sync)
    except Exception as e:
        logger.warning("종목 universe 빌드 실패: %s", e, exc_info=True)
        # 직전 캐시가 있으면 stale 로라도 내려보냄
        if _cache is not None:
            return {
                **_cache,
                "cache_hit": True,
                "stale": True,
                "cache_ttl_sec": STOCK_UNIVERSE_CACHE_TTL_SEC,
                "fetched_at": datetime.fromtimestamp(_cache_at).isoformat(),
                "error": {"code": "build_failed", "message": str(e)},
            }
        return {
            "items": [],
            "count": 0,
            "sp500_count": 0,
            "extra_count": 0,
            "cache_hit": False,
            "stale": False,
            "cache_ttl_sec": STOCK_UNIVERSE_CACHE_TTL_SEC,
            "fetched_at": datetime.now().isoformat(),
            "error": {"code": "build_failed", "message": str(e)},
        }

    _cache = built
    _cache_at = time.time()
    logger.info(
        "종목 universe 빌드 완료: %d개 (S&P500 %d + extra %d)",
        built["count"], built["sp500_count"], built["extra_count"],
    )
    return {
        **built,
        "cache_hit": False,
        "cache_ttl_sec": STOCK_UNIVERSE_CACHE_TTL_SEC,
        "fetched_at": datetime.fromtimestamp(_cache_at).isoformat(),
    }
