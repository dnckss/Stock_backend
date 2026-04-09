"""S&P 500 섹터별 히트맵 데이터 — Wikipedia 구성종목 + yfinance 시세/시가총액."""
from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import defaultdict
from datetime import datetime, timezone
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
    """yfinance batch download로 현재가 + 일일 등락률을 조회한다."""
    if not tickers:
        return {}
    try:
        df = yf.download(
            " ".join(tickers),
            period="2d",
            interval="1d",
            group_by="ticker",
            progress=False,
            threads=True,
        )
    except Exception as e:
        logger.warning("히트맵 가격 다운로드 실패: %s", e)
        return {}

    result: dict[str, dict[str, float | None]] = {}
    single = len(tickers) == 1

    for t in tickers:
        try:
            closes = (df["Close"] if single else df[t]["Close"]).dropna()
            if len(closes) < 1:
                continue
            price = float(closes.iloc[-1])
            prev = float(closes.iloc[-2]) if len(closes) >= 2 else None
            change = round((price - prev) / prev * 100, 2) if prev and prev != 0 else None
            result[t] = {"price": round(price, 2), "change_pct": change}
        except (KeyError, IndexError, TypeError, ValueError):
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


async def _background_refresh() -> None:
    """백그라운드에서 히트맵을 갱신 → 메모리 + DB 저장."""
    global _heatmap_cache, _heatmap_cache_at
    try:
        result = await build_sp500_heatmap()
        _heatmap_cache = result
        _heatmap_cache_at = time.time()
        from services.crud import save_heatmap_snapshot
        await asyncio.to_thread(save_heatmap_snapshot, result)
        logger.info("히트맵 백그라운드 갱신 완료")
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
