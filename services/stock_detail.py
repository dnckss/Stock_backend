"""
종목 상세 페이지용 데이터 서비스.
증권사 앱 수준의 시세/차트/호가/기업정보를 제공한다.
"""
from __future__ import annotations

import logging
import math
import threading
import time
from datetime import datetime
from typing import Any

import pandas as pd
import yfinance as yf

from config import (
    STOCK_CHART_DAILY_TTL_SEC,
    STOCK_CHART_INTRADAY_TTL_SEC,
    STOCK_QUOTE_CACHE_TTL_SEC,
)

logger = logging.getLogger(__name__)

# 회사명 프로세스 내 캐시 (info 호출 실패 시 매번 재시도 방지)
_company_info_cache: dict[str, dict[str, Any]] = {}
# fetch_quote stale fallback — yfinance 차단 시 직전 성공 응답을 stale=True 로 노출.
_quote_stale_store: dict[str, dict[str, Any]] = {}

# fetch_quote / fetch_chart 인메모리 TTL 캐시 — 프런트 polling 폭주에서 yfinance 보호.
# key 는 ticker(quote) / (ticker, period_key)(chart). 모든 접근은 lock 하에 수행.
_quote_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_quote_cache_lock = threading.Lock()
_chart_cache: dict[tuple[str, str], tuple[float, list[dict[str, Any]]]] = {}
_chart_cache_lock = threading.Lock()


_CHART_INTRADAY_KEYS = frozenset({"1min", "5min", "30min", "60min"})


def _chart_ttl_for(period_key: str) -> int:
    """차트 TTL — 분봉은 짧게(stale 회피), 일봉 이상은 길게(부하 절감)."""
    return (
        STOCK_CHART_INTRADAY_TTL_SEC
        if period_key in _CHART_INTRADAY_KEYS
        else STOCK_CHART_DAILY_TTL_SEC
    )

# 차트 인터벌별 기간/yfinance 인터벌 매핑
# key = 프론트가 보내는 값, value = (yfinance period, yfinance interval)
_CHART_PRESETS: dict[str, tuple[str, str]] = {
    # 분봉 (yfinance 자체 제한 — 분봉은 짧은 기간만 제공)
    "1min": ("7d", "1m"),        # 1분봉 — 최대 7일
    "5min": ("60d", "5m"),       # 5분봉 — 최대 60일
    "30min": ("60d", "30m"),     # 30분봉 — 최대 60일
    "60min": ("60d", "60m"),     # 60분봉 — 최대 60일
    # 일봉 이상 — 상장일부터 전체 history (yfinance 가 종목별 상장일까지 알아서 끊음).
    # 일봉 max 는 종목에 따라 1k~12k bars (AAPL 1980 상장 ≈ 11.5k). orjson + GZip 으로
    # wire 비용은 ~100KB 수준. 응답 캐시는 STOCK_CHART_DAILY_TTL_SEC(기본 30분)로 재호출 회피.
    "day": ("max", "1d"),        # 일봉 — 상장 이후 전체
    "week": ("max", "1wk"),      # 주봉 — 상장 이후 전체
    "month": ("max", "1mo"),     # 월봉 — 전체
    "year": ("max", "3mo"),      # 분기봉 — 전체
}


def _safe(v: Any, decimals: int = 2) -> Any:
    if v is None:
        return None
    try:
        f = float(v)
        return round(f, decimals) if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _pct(current: Any, base: Any) -> float | None:
    c = _safe(current, 6)
    b = _safe(base, 6)
    if c is None or b is None or b == 0:
        return None
    return _safe((c - b) / b * 100, 2)


def _fi_get(fi: Any, key: str, default: Any = None) -> Any:
    """yfinance LazyDict 키 접근 — lazy fetch 단계에서 raise 되어도 default 로."""
    if fi is None:
        return default
    try:
        v = fi.get(key) if hasattr(fi, "get") else None
        return v if v is not None else default
    except Exception as e:
        logger.debug("fast_info[%s] 접근 실패: %s", key, e)
        return default


def fetch_quote(ticker: str) -> dict[str, Any]:
    """
    실시간 시세 + 기본 정보. yfinance 차단으로 핵심 가격이 없으면
    직전 성공 응답을 stale=True 로 반환해 화면이 비지 않게 한다.

    프런트가 초당 N회 polling 해도 yfinance 호출은 STOCK_QUOTE_CACHE_TTL_SEC
    동안 1회로 합쳐진다. 같은 ticker 동시 요청은 cache hit 으로 즉시 응답.
    """
    from services.yf_limiter import throttled

    upper = (ticker or "").upper().strip()
    now = time.time()
    with _quote_cache_lock:
        cached = _quote_cache.get(upper)
        if cached is not None and now - cached[0] < STOCK_QUOTE_CACHE_TTL_SEC:
            return cached[1]

    t = yf.Ticker(ticker)

    try:
        fi = throttled(lambda: t.fast_info)
    except Exception as e:
        logger.warning("fast_info 조회 실패 (%s): %s", ticker, e)
        fi = None

    # 회사 정보 캐시 활용 (info 호출은 느리고 실패할 수 있음)
    if ticker in _company_info_cache:
        info = _company_info_cache[ticker]
    else:
        info = {}
        try:
            info = throttled(lambda: t.info or {})
            if info.get("longName") or info.get("shortName"):
                _company_info_cache[ticker] = info
        except Exception as e:
            logger.debug("Ticker.info 조회 실패 (%s): %s", ticker, e)

    price = _safe(_fi_get(fi, "lastPrice"))
    prev_close = _safe(_fi_get(fi, "previousClose"))

    # 핵심 가격 둘 다 없으면 stale fallback 시도 — 화면이 0 으로 비지 않게
    if price is None and prev_close is None:
        cached = _quote_stale_store.get(ticker)
        if cached is not None:
            logger.debug("fetch_quote stale fallback (%s)", ticker)
            return {**cached, "stale": True, "as_of": datetime.now().isoformat()}

    change = _safe(price - prev_close, 2) if price is not None and prev_close is not None else None
    change_pct = _pct(price, prev_close)

    result = {
        # 현재가
        "price": price,
        "change": change,
        "change_pct": change_pct,
        "currency": _fi_get(fi, "currency", "USD"),

        # 당일 시세
        "open": _safe(_fi_get(fi, "open")),
        "day_high": _safe(_fi_get(fi, "dayHigh")),
        "day_low": _safe(_fi_get(fi, "dayLow")),
        "prev_close": prev_close,
        "volume": _fi_get(fi, "lastVolume"),
        "avg_volume": _fi_get(fi, "tenDayAverageVolume"),

        # 52주
        "year_high": _safe(_fi_get(fi, "yearHigh")),
        "year_low": _safe(_fi_get(fi, "yearLow")),

        # 기업 정보
        "name": info.get("longName") or info.get("shortName") or ticker,
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "market_cap": _fi_get(fi, "marketCap"),
        "pe_ratio": _safe(info.get("trailingPE")),
        "forward_pe": _safe(info.get("forwardPE")),
        "dividend_yield": _safe(info.get("dividendYield"), 4),
        "beta": _safe(info.get("beta")),
        "shares": _fi_get(fi, "shares"),

        # 이동평균
        "ma_50": _safe(_fi_get(fi, "fiftyDayAverage")),
        "ma_200": _safe(_fi_get(fi, "twoHundredDayAverage")),

        # 호가
        "bid": _safe(info.get("bid")),
        "ask": _safe(info.get("ask")),
        "bid_size": info.get("bidSize"),
        "ask_size": info.get("askSize"),

        "stale": False,
        "as_of": datetime.now().isoformat(),
    }

    # 핵심 가격이 채워졌으면 stale store 갱신
    if price is not None or prev_close is not None:
        _quote_stale_store[ticker] = result
    # 다음 동일 ticker 요청은 캐시 히트로 즉시 응답.
    with _quote_cache_lock:
        _quote_cache[upper] = (time.time(), result)
    return result


def fetch_chart(ticker: str, period: str = "1D") -> list[dict[str, Any]]:
    """차트 데이터(OHLCV)를 가져온다.

    같은 (ticker, period) 가 인터벌별 TTL(분봉 30s, 일봉+ 5분) 안에 재요청되면
    캐시 히트로 즉시 응답한다. 분봉의 stale 가시성과 일봉의 부하 절감을 균형.
    """
    upper = (ticker or "").upper().strip()
    key = period.lower().strip()
    # 축약 키 호환 매핑 (프론트에서 1m, 5m 등으로 보낼 수 있음)
    _ALIAS: dict[str, str] = {
        "1m": "1min", "5m": "5min", "30m": "30min", "60m": "60min",
        "1d": "day", "1w": "week", "1mo": "month", "1y": "year",
        "5d": "day", "3m": "day", "6m": "day", "5y": "week",
    }
    key = _ALIAS.get(key, key)
    preset = _CHART_PRESETS.get(key)
    if not preset:
        preset = _CHART_PRESETS["day"]
        key = "day"

    cache_key = (upper, key)
    ttl = _chart_ttl_for(key)
    now = time.time()
    with _chart_cache_lock:
        cached = _chart_cache.get(cache_key)
        if cached is not None and now - cached[0] < ttl:
            return cached[1]

    yf_period, yf_interval = preset
    try:
        df = yf.download(ticker, period=yf_period, interval=yf_interval, progress=False)
    except Exception as e:
        logger.warning("차트 다운로드 실패 (%s %s): %s", ticker, period, e)
        return []

    if df.empty:
        return []

    # MultiIndex 처리
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    bars: list[dict[str, Any]] = []
    for idx, row in df.iterrows():
        try:
            ts = idx if hasattr(idx, "isoformat") else pd.Timestamp(idx)
            o = _safe(row.get("Open"))
            h = _safe(row.get("High"))
            l_ = _safe(row.get("Low"))
            c = _safe(row.get("Close"))
            v = row.get("Volume")
            if c is None:
                continue
            bars.append({
                "timestamp": ts.isoformat(),
                "open": o,
                "high": h,
                "low": l_,
                "close": c,
                "volume": int(v) if v is not None and not pd.isna(v) else 0,
            })
        except Exception:
            continue

    with _chart_cache_lock:
        _chart_cache[cache_key] = (time.time(), bars)
    return bars


def format_market_cap(cap: Any) -> str | None:
    """시가총액을 읽기 쉬운 문자열로 변환한다."""
    if cap is None:
        return None
    try:
        v = float(cap)
    except (TypeError, ValueError):
        return None
    if v >= 1e12:
        return f"${v / 1e12:.2f}T"
    if v >= 1e9:
        return f"${v / 1e9:.2f}B"
    if v >= 1e6:
        return f"${v / 1e6:.1f}M"
    return f"${v:,.0f}"
