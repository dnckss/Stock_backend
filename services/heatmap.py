"""S&P 500 섹터별 히트맵 데이터 — Wikipedia 구성종목 + yfinance 시세/시가총액."""
from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import defaultdict
from datetime import datetime, timezone
from io import StringIO
from typing import Any

import pandas as pd
import requests
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

# 구성종목 + 시가총액 장기 캐시 (30분)
_constituents: list[dict[str, Any]] = []
_constituents_at: float = 0.0

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_WIKI_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}


# ---------------------------------------------------------------------------
# Wikipedia S&P 500 구성종목
# ---------------------------------------------------------------------------

def _fetch_sp500_from_wiki() -> list[dict[str, str]]:
    """Wikipedia에서 S&P 500 구성종목(ticker, name, sector)을 가져온다."""
    resp = requests.get(_WIKI_URL, headers=_WIKI_HEADERS, timeout=15)
    resp.raise_for_status()
    df = pd.read_html(StringIO(resp.text))[0]
    rows: list[dict[str, str]] = []
    for _, row in df.iterrows():
        ticker = str(row.get("Symbol", "")).strip().replace(".", "-")
        name = str(row.get("Security", "")).strip()
        sector = str(row.get("GICS Sector", "")).strip()
        if ticker and sector:
            rows.append({"ticker": ticker, "name": name, "sector": sector})
    return rows


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
            period="5d",
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

    wiki = await asyncio.to_thread(_fetch_sp500_from_wiki)
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


async def get_cached_sp500_heatmap() -> dict[str, Any]:
    """캐시된 히트맵을 반환한다. TTL 만료 시 갱신."""
    global _heatmap_cache, _heatmap_cache_at

    now = time.time()
    if _heatmap_cache and (now - _heatmap_cache_at < HEATMAP_CACHE_TTL_SEC):
        return _heatmap_cache

    async with _heatmap_lock:
        # 더블체크
        now = time.time()
        if _heatmap_cache and (now - _heatmap_cache_at < HEATMAP_CACHE_TTL_SEC):
            return _heatmap_cache

        try:
            result = await build_sp500_heatmap()
        except Exception as e:
            logger.error("S&P 500 히트맵 생성 실패: %s", e, exc_info=True)
            if _heatmap_cache:
                return _heatmap_cache
            raise

        _heatmap_cache = result
        _heatmap_cache_at = time.time()
        return result
