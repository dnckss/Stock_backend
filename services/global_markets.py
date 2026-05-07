"""
글로벌 마켓 오버뷰 — 국제 지수, 원자재, 환율 일괄 조회.

GET /api/markets/global 응답을 만든다. 기존 services.scanner._fetch_macro_value 를
재사용 → ticker 별 stale fallback 캐시도 자동으로 적용된다.
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any

from config import (
    GLOBAL_COMMODITIES,
    GLOBAL_CURRENCIES,
    GLOBAL_INDICES,
    GLOBAL_MARKETS_CACHE_TTL_SEC,
)
from services.crud import sanitize_for_json
from services.scanner import _fetch_macro_value

logger = logging.getLogger(__name__)


_cache: dict[str, Any] | None = None
_cache_at: float = 0.0
_cache_lock = threading.Lock()


def _is_fresh() -> bool:
    return _cache is not None and (time.time() - _cache_at) < GLOBAL_MARKETS_CACHE_TTL_SEC


def _format_change_pct(pct_ratio: float | None) -> float | None:
    """_fetch_macro_value 의 pct(비율, 0.0085) → 응답용 퍼센트(0.85)."""
    if pct_ratio is None:
        return None
    return round(pct_ratio * 100, 2)


async def _fetch_one(spec: dict[str, Any]) -> dict[str, Any]:
    """ticker 1개 조회 → 응답용 dict. 실패 시 value=None + stale=False."""
    ticker = spec["ticker"]
    decimals = int(spec.get("decimals", 2))
    data = await asyncio.to_thread(_fetch_macro_value, ticker, decimals)
    out: dict[str, Any] = {
        "symbol": spec["symbol"],
        "name": spec["name"],
        "value": data.get("value"),
        "change_pct": _format_change_pct(data.get("pct")),
        "stale": data.get("stale", False),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if "country" in spec:
        out["country"] = spec["country"]
    return out


async def _fetch_category(specs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """카테고리 내 ticker 들을 병렬 조회 (yf_limiter 가 동시성 제어)."""
    results = await asyncio.gather(
        *[_fetch_one(s) for s in specs], return_exceptions=True,
    )
    out: list[dict[str, Any]] = []
    for r in results:
        if isinstance(r, Exception):
            logger.debug("global_markets ticker fetch 실패: %s", r)
            continue
        out.append(r)
    return out


async def fetch_global_markets(refresh: bool = False) -> dict[str, Any]:
    """
    indices / commodities / currencies 카테고리별 yfinance 일괄 조회.
    5분 메모리 캐시 + ticker 별 stale fallback (scanner 의 _macro_value_cache 활용).
    """
    global _cache, _cache_at

    if not refresh and _is_fresh():
        return _cache  # type: ignore[return-value]

    indices, commodities, currencies = await asyncio.gather(
        _fetch_category(GLOBAL_INDICES),
        _fetch_category(GLOBAL_COMMODITIES),
        _fetch_category(GLOBAL_CURRENCIES),
    )

    payload = {
        "indices": indices,
        "commodities": commodities,
        "currencies": currencies,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    sanitized = sanitize_for_json(payload)

    with _cache_lock:
        _cache = sanitized
        _cache_at = time.time()
    return sanitized
