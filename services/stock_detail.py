"""
종목 상세 페이지용 데이터 서비스.
증권사 앱 수준의 시세/차트/호가/기업정보를 제공한다.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Any

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# 차트 인터벌별 기간/yfinance 인터벌 매핑
# key = 프론트가 보내는 값, value = (yfinance period, yfinance interval)
_CHART_PRESETS: dict[str, tuple[str, str]] = {
    # 분봉
    "1m": ("7d", "1m"),       # 1분봉 — 최대 7일
    "5m": ("60d", "5m"),      # 5분봉 — 최대 60일
    "30m": ("60d", "30m"),    # 30분봉 — 최대 60일
    "60m": ("60d", "60m"),    # 60분봉 — 최대 60일
    # 일봉 이상
    "1D": ("1y", "1d"),       # 일봉 — 1년
    "1W": ("5y", "1wk"),      # 주봉 — 5년
    "1M": ("10y", "1mo"),     # 월봉 — 10년
    "1Y": ("max", "3mo"),     # 년봉(분기) — 전체
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


def fetch_quote(ticker: str) -> dict[str, Any]:
    """실시간 시세 + 기본 정보를 가져온다."""
    t = yf.Ticker(ticker)

    fi = t.fast_info
    info = {}
    try:
        info = t.info or {}
    except Exception:
        pass

    price = _safe(fi.get("lastPrice"))
    prev_close = _safe(fi.get("previousClose"))
    change = _safe(price - prev_close, 2) if price is not None and prev_close is not None else None
    change_pct = _pct(price, prev_close)

    return {
        # 현재가
        "price": price,
        "change": change,
        "change_pct": change_pct,
        "currency": fi.get("currency", "USD"),

        # 당일 시세
        "open": _safe(fi.get("open")),
        "day_high": _safe(fi.get("dayHigh")),
        "day_low": _safe(fi.get("dayLow")),
        "prev_close": prev_close,
        "volume": fi.get("lastVolume"),
        "avg_volume": fi.get("tenDayAverageVolume"),

        # 52주
        "year_high": _safe(fi.get("yearHigh")),
        "year_low": _safe(fi.get("yearLow")),

        # 기업 정보
        "name": info.get("longName") or info.get("shortName") or ticker,
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "market_cap": fi.get("marketCap"),
        "pe_ratio": _safe(info.get("trailingPE")),
        "forward_pe": _safe(info.get("forwardPE")),
        "dividend_yield": _safe(info.get("dividendYield"), 4),
        "beta": _safe(info.get("beta")),
        "shares": fi.get("shares"),

        # 이동평균
        "ma_50": _safe(fi.get("fiftyDayAverage")),
        "ma_200": _safe(fi.get("twoHundredDayAverage")),

        # 호가
        "bid": _safe(info.get("bid")),
        "ask": _safe(info.get("ask")),
        "bid_size": info.get("bidSize"),
        "ask_size": info.get("askSize"),

        "as_of": datetime.now().isoformat(),
    }


def fetch_chart(ticker: str, period: str = "1D") -> list[dict[str, Any]]:
    """차트 데이터(OHLCV)를 가져온다."""
    # 분봉(1m, 5m 등)은 소문자, 일봉 이상(1D, 1W 등)은 대문자
    preset = _CHART_PRESETS.get(period) or _CHART_PRESETS.get(period.upper()) or _CHART_PRESETS.get(period.lower())
    if not preset:
        preset = _CHART_PRESETS["1D"]

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
