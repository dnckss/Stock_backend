"""S&P 500 섹터별 히트맵 데이터 — Wikipedia 구성종목 + price_history(DB) + yfinance fallback."""
from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import yfinance as yf

from config import (
    HEATMAP_CACHE_TTL_SEC,
    HEATMAP_MCAP_CACHE_TTL_SEC,
    HEATMAP_MCAP_CONCURRENCY,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

_heatmap_cache: dict[str, Any] | None = None
_heatmap_cache_at: float = 0.0
_heatmap_lock = asyncio.Lock()
_refresh_task: asyncio.Task[None] | None = None

_constituents: list[dict[str, Any]] = []
_constituents_at: float = 0.0


# ---------------------------------------------------------------------------
# Market Cap (parallel fast_info)
# ---------------------------------------------------------------------------

async def _fetch_market_caps(tickers: list[str]) -> dict[str, float | None]:
    """fast_info로 시가총액을 병렬 조회한다 (글로벌 속도 제한 적용)."""
    from services.yf_limiter import throttled

    sem = asyncio.Semaphore(HEATMAP_MCAP_CONCURRENCY)

    async def _one(t: str) -> tuple[str, float | None]:
        async with sem:
            def _get() -> float | None:
                try:
                    mc = throttled(lambda: yf.Ticker(t).fast_info["marketCap"])
                    return float(mc) if mc and not math.isnan(mc) else None
                except Exception:
                    return None
            return (t, await asyncio.to_thread(_get))

    results = await asyncio.gather(
        *[_one(t) for t in tickers], return_exceptions=True,
    )
    out: dict[str, float | None] = {}
    for res in results:
        if isinstance(res, tuple):
            out[res[0]] = res[1]
    return out


# ---------------------------------------------------------------------------
# Price / Change (batch download)
# ---------------------------------------------------------------------------

def _fetch_prices(tickers: list[str]) -> dict[str, dict[str, float | None]]:
    """
    price_history DB 우선 조회 + 누락 시 yfinance fallback.
    각 ticker 의 마지막·직전 종가로 일일 등락률 계산.
    """
    if not tickers:
        return {}

    # 최근 7일치 (주말·공휴일 고려) 가져와서 마지막 두 거래일을 사용
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=10)

    try:
        from services.price_store import fetch_close_prices
        close_df = fetch_close_prices(tickers, start, end)
    except Exception as e:
        logger.warning("히트맵 price_store 조회 실패: %s", e)
        return {}

    if close_df is None or close_df.empty:
        return {}

    result: dict[str, dict[str, float | None]] = {}
    for t in tickers:
        if t not in close_df.columns:
            continue
        series = close_df[t].dropna()
        if series.empty:
            continue
        try:
            price = float(series.iloc[-1])
            if not math.isfinite(price):
                continue
            prev = float(series.iloc[-2]) if len(series) >= 2 else None
            if prev is not None and not math.isfinite(prev):
                prev = None
            change = round((price - prev) / prev * 100, 2) if prev and prev != 0 else None
            result[t] = {"price": round(price, 2), "change_pct": change}
        except (IndexError, TypeError, ValueError):
            continue
    return result


# ---------------------------------------------------------------------------
# Constituent Refresh
# ---------------------------------------------------------------------------

async def _refresh_constituents() -> list[dict[str, Any]]:
    """Wikipedia 구성종목 + 시가총액을 갱신한다."""
    global _constituents, _constituents_at

    from services.scanner import get_sp500_constituents
    wiki = await asyncio.to_thread(get_sp500_constituents)
    tickers = [d["ticker"] for d in wiki]
    mcaps = await _fetch_market_caps(tickers)

    updated: list[dict[str, Any]] = []
    for d in wiki:
        updated.append({
            "ticker": d["ticker"],
            "name": d["name"],
            "sector": d["sector"],
            "market_cap": mcaps.get(d["ticker"]),
        })

    _constituents = updated
    _constituents_at = time.time()
    logger.info("S&P 500 구성종목 %d건 + 시가총액 갱신 완료", len(updated))
    return updated


# ---------------------------------------------------------------------------
# Heatmap Build / Cache
# ---------------------------------------------------------------------------

async def build_sp500_heatmap() -> dict[str, Any]:
    """S&P 500 히트맵 데이터를 구성한다."""
    now = time.time()
    constituents = _constituents

    # 구성종목 + 시가총액 장기 캐시 갱신
    if not constituents or (now - _constituents_at > HEATMAP_MCAP_CACHE_TTL_SEC):
        constituents = await _refresh_constituents()

    # 가격 일괄 조회
    tickers = [c["ticker"] for c in constituents]
    prices = await asyncio.to_thread(_fetch_prices, tickers)

    # 섹터별 그룹핑
    sectors_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for c in constituents:
        t = c["ticker"]
        p_data = prices.get(t)
        if not p_data:
            continue
        sectors_map[c["sector"]].append({
            "ticker": t,
            "name": c["name"],
            "market_cap": c["market_cap"],
            "change_pct": p_data["change_pct"],
            "price": p_data["price"],
        })

    # 섹터 내 시가총액 내림차순 정렬
    sectors_list: list[dict[str, Any]] = []
    for name in sorted(sectors_map.keys()):
        stocks = sectors_map[name]
        stocks.sort(key=lambda s: s.get("market_cap") or 0, reverse=True)
        sectors_list.append({"name": name, "stocks": stocks})

    return {
        "sectors": sectors_list,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _heatmap_has_content(result: dict[str, Any] | None) -> bool:
    """결과에 의미 있는 stocks 가 있는지 — yfinance 폭주로 빈 sectors 만 잡혔을 때 가드."""
    if not result:
        return False
    sectors = result.get("sectors") or []
    if not sectors:
        return False
    total_stocks = sum(len(s.get("stocks") or []) for s in sectors)
    return total_stocks > 0


async def _background_refresh() -> None:
    """백그라운드에서 히트맵을 갱신 → 메모리 + DB 저장. 빈 결과는 DB save 스킵."""
    global _heatmap_cache, _heatmap_cache_at
    try:
        result = await build_sp500_heatmap()
        if not _heatmap_has_content(result):
            logger.warning(
                "히트맵 갱신 결과가 비어있어 DB save 를 건너뜀 — 기존 스냅샷 유지 (yfinance 차단 의심)",
            )
            return
        _heatmap_cache = result
        _heatmap_cache_at = time.time()
        from services.crud import save_heatmap_snapshot
        await asyncio.to_thread(save_heatmap_snapshot, result)
        sectors = result.get("sectors") or []
        total_stocks = sum(len(s.get("stocks") or []) for s in sectors)
        logger.info(
            "히트맵 백그라운드 갱신 완료: sectors=%d, stocks=%d", len(sectors), total_stocks,
        )
    except Exception as e:
        logger.error("히트맵 백그라운드 갱신 실패: %s", e, exc_info=True)


async def get_cached_sp500_heatmap() -> dict[str, Any]:
    """Stale-While-Revalidate: DB/메모리 캐시를 즉시 반환, 백그라운드 갱신."""
    global _heatmap_cache, _heatmap_cache_at, _refresh_task

    now = time.time()

    # 1) 메모리 캐시 신선 → 즉시 반환
    if _heatmap_cache and (now - _heatmap_cache_at < HEATMAP_CACHE_TTL_SEC):
        return _heatmap_cache

    need_refresh = not _refresh_task or _refresh_task.done()

    # 2) 메모리 캐시 stale → 즉시 반환 + 백그라운드 갱신
    if _heatmap_cache:
        if need_refresh:
            _refresh_task = asyncio.create_task(_background_refresh())
        return _heatmap_cache

    # 3) 메모리 없음 → DB 스냅샷 로드
    from services.crud import get_heatmap_snapshot
    db_data = await asyncio.to_thread(get_heatmap_snapshot)
    if db_data:
        _heatmap_cache = db_data
        _heatmap_cache_at = 0.0  # stale 표시 → 다음 요청에서 갱신 트리거
        if need_refresh:
            _refresh_task = asyncio.create_task(_background_refresh())
        return db_data

    # 4) 어디에도 없음 → 동기 빌드 (최초 1회)
    async with _heatmap_lock:
        if _heatmap_cache:
            return _heatmap_cache
        result = await build_sp500_heatmap()
        _heatmap_cache = result
        _heatmap_cache_at = time.time()
        from services.crud import save_heatmap_snapshot
        await asyncio.to_thread(save_heatmap_snapshot, result)
        return result
