"""
종목 펀더멘털 데이터 서비스.
기업 개요, 투자 지표, 수익성, 성장성, 안정성, 실적 데이터를 제공한다.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any

import pandas as pd
import yfinance as yf

from config import (
    FUNDAMENTALS_CACHE_TTL_SEC,
    FUNDAMENTALS_MAX_EARNINGS_HISTORY,
    FUNDAMENTALS_MAX_OFFICERS,
    FUNDAMENTALS_MAX_QUARTERS,
)
from services.stock_detail import _safe, _pct, format_market_cap

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 프로세스 내 TTL 캐시 (fetch_all_fundamentals 전용)
# ---------------------------------------------------------------------------
_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_cache_lock = threading.Lock()


def _get_cached(ticker: str) -> dict[str, Any] | None:
    with _cache_lock:
        entry = _cache.get(ticker)
        if entry and (time.time() - entry[0]) < FUNDAMENTALS_CACHE_TTL_SEC:
            return entry[1]
    return None


def _set_cached(ticker: str, data: dict[str, Any]) -> None:
    with _cache_lock:
        _cache[ticker] = (time.time(), data)


# ---------------------------------------------------------------------------
# DataFrame 헬퍼
# ---------------------------------------------------------------------------

def _df_val(df: pd.DataFrame, row: str, col_idx: int = 0) -> Any:
    """DataFrame에서 (row_name, col_index) 값을 안전하게 추출한다."""
    if df.empty or row not in df.index:
        return None
    try:
        if col_idx >= len(df.columns):
            return None
        v = df.loc[row].iloc[col_idx]
        if pd.isna(v):
            return None
        return v
    except Exception:
        return None


def _df_quarter_dates(df: pd.DataFrame) -> list[str]:
    """DataFrame 컬럼(날짜)을 오래된 순서로 ISO 문자열 리스트로 반환한다."""
    if df.empty:
        return []
    dates = []
    for col in reversed(df.columns):
        try:
            ts = pd.Timestamp(col)
            dates.append(ts.strftime("%Y-%m-%d"))
        except Exception:
            continue
    return dates


def _calc_margin(numerator: Any, denominator: Any) -> float | None:
    """(numerator / denominator * 100) 마진율을 안전하게 계산한다."""
    n = _safe(numerator, 6)
    d = _safe(denominator, 6)
    if n is None or d is None or d == 0:
        return None
    return _safe(n / d * 100, 2)


def _calc_yoy(current: Any, prior: Any) -> float | None:
    """YoY 성장률 = (current - prior) / |prior| * 100."""
    c = _safe(current, 6)
    p = _safe(prior, 6)
    if c is None or p is None or p == 0:
        return None
    return _safe((c - p) / abs(p) * 100, 2)


# ---------------------------------------------------------------------------
# Raw 데이터 fetcher (각각 throttled + try/except)
# ---------------------------------------------------------------------------

def _fetch_info(t: yf.Ticker) -> dict[str, Any]:
    from services.yf_limiter import throttled
    try:
        return throttled(lambda: t.info or {})
    except Exception as e:
        logger.debug("info 조회 실패 (%s): %s", t.ticker, e)
        return {}


def _fetch_quarterly_financials(t: yf.Ticker) -> pd.DataFrame:
    from services.yf_limiter import throttled
    try:
        df = throttled(lambda: t.quarterly_financials)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        logger.debug("quarterly_financials 조회 실패 (%s): %s", t.ticker, e)
        return pd.DataFrame()


def _fetch_quarterly_balance_sheet(t: yf.Ticker) -> pd.DataFrame:
    from services.yf_limiter import throttled
    try:
        df = throttled(lambda: t.quarterly_balance_sheet)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        logger.debug("quarterly_balance_sheet 조회 실패 (%s): %s", t.ticker, e)
        return pd.DataFrame()


def _fetch_earnings_dates(t: yf.Ticker) -> pd.DataFrame:
    from services.yf_limiter import throttled
    try:
        df = throttled(lambda: t.earnings_dates)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        logger.debug("earnings_dates 조회 실패 (%s): %s", t.ticker, e)
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Builder 함수 (순수 변환 — API 호출 없음)
# ---------------------------------------------------------------------------

def _build_profile(info: dict[str, Any]) -> dict[str, Any]:
    """기업 개요."""
    officers = []
    for o in (info.get("companyOfficers") or [])[:FUNDAMENTALS_MAX_OFFICERS]:
        name = o.get("name")
        title = o.get("title")
        if name:
            officers.append({"name": name, "title": title})

    city = info.get("city") or ""
    state = info.get("state") or ""
    headquarters = ", ".join(filter(None, [city, state])) or None

    return {
        "name": info.get("longName") or info.get("shortName"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "description": info.get("longBusinessSummary"),
        "website": info.get("website"),
        "employees": info.get("fullTimeEmployees"),
        "officers": officers,
        "market_cap": info.get("marketCap"),
        "market_cap_display": format_market_cap(info.get("marketCap")),
        "shares_outstanding": info.get("sharesOutstanding"),
        "country": info.get("country"),
        "headquarters": headquarters,
    }


def _build_indicators(
    info: dict[str, Any],
    qf: pd.DataFrame,
    qbs: pd.DataFrame,
) -> dict[str, Any]:
    """투자 지표 — 밸류에이션, 수익, 배당, 재무건전성."""
    # 재무건전성: 최신 분기 기준
    total_debt = _safe(_df_val(qbs, "Total Debt"), 0)
    equity = _safe(_df_val(qbs, "Stockholders Equity"), 0)
    current_assets = _safe(_df_val(qbs, "Current Assets"), 0)
    current_liabilities = _safe(_df_val(qbs, "Current Liabilities"), 0)
    operating_income = _safe(_df_val(qf, "Operating Income"), 0)
    interest_expense = _safe(
        _df_val(qf, "Interest Expense") or _df_val(qf, "Interest Expense Non Operating"),
        0,
    )

    debt_ratio = _calc_margin(total_debt, equity)
    current_ratio = (
        _safe(current_assets / current_liabilities, 2)
        if current_assets and current_liabilities and current_liabilities != 0
        else None
    )
    interest_coverage = (
        _safe(operating_income / abs(interest_expense), 2)
        if operating_income is not None and interest_expense and interest_expense != 0
        else None
    )

    # 배당 ex-date
    ex_date_raw = info.get("exDividendDate")
    ex_dividend_date = None
    if ex_date_raw:
        try:
            ex_dividend_date = pd.Timestamp(ex_date_raw, unit="s").strftime("%Y-%m-%d")
        except Exception:
            pass

    return {
        "valuation": {
            "per": _safe(info.get("trailingPE")),
            "forward_per": _safe(info.get("forwardPE")),
            "psr": _safe(info.get("priceToSalesTrailing12Months")),
            "pbr": _safe(info.get("priceToBook")),
        },
        "per_share": {
            "eps": _safe(info.get("trailingEps")),
            "bps": _safe(info.get("bookValue")),
            "roe": _safe(
                (info.get("returnOnEquity") or 0) * 100
                if info.get("returnOnEquity") is not None
                else None,
                2,
            ),
        },
        "dividends": {
            "dividend_yield": _safe(info.get("dividendYield")),
            "dividend_rate": _safe(info.get("dividendRate")),
            "payout_ratio": _safe(
                (info.get("payoutRatio") or 0) * 100
                if info.get("payoutRatio") is not None
                else None,
                2,
            ),
            "ex_dividend_date": ex_dividend_date,
        },
        "financial_health": {
            "debt_ratio": debt_ratio,
            "current_ratio": current_ratio,
            "interest_coverage_ratio": interest_coverage,
        },
    }


def _build_profitability(qf: pd.DataFrame) -> dict[str, Any]:
    """수익성 — 분기별 매출, 순이익, 순이익률, 순이익 YoY."""
    if qf.empty:
        return {"quarters": []}

    n_cols = min(len(qf.columns), FUNDAMENTALS_MAX_QUARTERS)
    quarters: list[dict[str, Any]] = []

    for i in range(n_cols - 1, -1, -1):  # 오래된 순
        date_str = _quarter_date_str(qf, i)
        revenue = _df_val(qf, "Total Revenue", i)
        net_income = _df_val(qf, "Net Income", i)
        net_margin = _calc_margin(net_income, revenue)

        # YoY: 4분기 전 데이터와 비교
        prior_idx = i + 4
        prior_net = _df_val(qf, "Net Income", prior_idx) if prior_idx < len(qf.columns) else None
        net_income_yoy = _calc_yoy(net_income, prior_net)

        quarters.append({
            "date": date_str,
            "revenue": _safe(revenue, 0),
            "net_income": _safe(net_income, 0),
            "net_margin": net_margin,
            "net_income_yoy": net_income_yoy,
        })

    return {"quarters": quarters}


def _build_growth(qf: pd.DataFrame) -> dict[str, Any]:
    """성장성 — 분기별 영업이익, 영업이익률, YoY 성장률."""
    if qf.empty:
        return {"quarters": []}

    n_cols = min(len(qf.columns), FUNDAMENTALS_MAX_QUARTERS)
    quarters: list[dict[str, Any]] = []

    for i in range(n_cols - 1, -1, -1):
        date_str = _quarter_date_str(qf, i)
        revenue = _df_val(qf, "Total Revenue", i)
        op_income = _df_val(qf, "Operating Income", i)
        op_margin = _calc_margin(op_income, revenue)

        prior_idx = i + 4
        prior_op = _df_val(qf, "Operating Income", prior_idx) if prior_idx < len(qf.columns) else None
        op_income_yoy = _calc_yoy(op_income, prior_op)

        quarters.append({
            "date": date_str,
            "operating_income": _safe(op_income, 0),
            "operating_margin": op_margin,
            "operating_income_yoy": op_income_yoy,
        })

    return {"quarters": quarters}


def _build_stability(qbs: pd.DataFrame) -> dict[str, Any]:
    """안정성 — 분기별 자본, 부채, 부채비율."""
    if qbs.empty:
        return {"quarters": []}

    n_cols = min(len(qbs.columns), FUNDAMENTALS_MAX_QUARTERS)
    quarters: list[dict[str, Any]] = []

    for i in range(n_cols - 1, -1, -1):
        date_str = _quarter_date_str(qbs, i)
        equity = _df_val(qbs, "Stockholders Equity", i)
        debt = _df_val(qbs, "Total Debt", i)
        debt_ratio = _calc_margin(debt, equity)

        quarters.append({
            "date": date_str,
            "total_equity": _safe(equity, 0),
            "total_debt": _safe(debt, 0),
            "debt_ratio": debt_ratio,
        })

    return {"quarters": quarters}


def _build_earnings(
    info: dict[str, Any],
    ed: pd.DataFrame,
) -> dict[str, Any]:
    """실적 — 다음 실적 발표일, EPS 히스토리, 애널리스트 목표가."""
    # 다음 실적 발표일
    next_date = None
    history: list[dict[str, Any]] = []

    if not ed.empty:
        now = pd.Timestamp.now(tz="UTC")
        for idx in ed.index:
            try:
                ts = pd.Timestamp(idx)
                if ts.tzinfo is None:
                    ts = ts.tz_localize("UTC")
                date_str = ts.strftime("%Y-%m-%d")

                actual = ed.loc[idx].get("Reported EPS")
                estimate = ed.loc[idx].get("EPS Estimate")
                surprise = ed.loc[idx].get("Surprise(%)")

                if ts > now and next_date is None:
                    next_date = date_str

                if actual is not None and not pd.isna(actual):
                    history.append({
                        "date": date_str,
                        "eps_actual": _safe(actual),
                        "eps_estimate": _safe(estimate),
                        "surprise_pct": _safe(surprise),
                    })
            except Exception:
                continue

        history = history[:FUNDAMENTALS_MAX_EARNINGS_HISTORY]

    return {
        "next_earnings_date": next_date,
        "history": history,
        "analyst_count": info.get("numberOfAnalystOpinions"),
        "target_mean_price": _safe(info.get("targetMeanPrice")),
        "target_high_price": _safe(info.get("targetHighPrice")),
        "target_low_price": _safe(info.get("targetLowPrice")),
        "recommendation": info.get("recommendationKey"),
    }


# ---------------------------------------------------------------------------
# 헬퍼
# ---------------------------------------------------------------------------

def _quarter_date_str(df: pd.DataFrame, col_idx: int) -> str | None:
    """DataFrame 컬럼 인덱스를 ISO 날짜 문자열로 변환한다."""
    try:
        ts = pd.Timestamp(df.columns[col_idx])
        return ts.strftime("%Y-%m-%d")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_all_fundamentals(ticker: str) -> dict[str, Any]:
    """전체 펀더멘털 데이터를 한 번에 조회한다 (캐시 적용)."""
    cached = _get_cached(ticker)
    if cached is not None:
        return cached

    t = yf.Ticker(ticker)
    info = _fetch_info(t)
    qf = _fetch_quarterly_financials(t)
    qbs = _fetch_quarterly_balance_sheet(t)
    ed = _fetch_earnings_dates(t)

    data = {
        "profile": _build_profile(info),
        "indicators": _build_indicators(info, qf, qbs),
        "profitability": _build_profitability(qf),
        "growth": _build_growth(qf),
        "stability": _build_stability(qbs),
        "earnings": _build_earnings(info, ed),
    }

    _set_cached(ticker, data)
    return data


def fetch_profile(ticker: str) -> dict[str, Any]:
    """기업 개요만 조회한다."""
    t = yf.Ticker(ticker)
    info = _fetch_info(t)
    return {"profile": _build_profile(info)}


def fetch_indicators(ticker: str) -> dict[str, Any]:
    """투자 지표만 조회한다."""
    t = yf.Ticker(ticker)
    info = _fetch_info(t)
    qf = _fetch_quarterly_financials(t)
    qbs = _fetch_quarterly_balance_sheet(t)
    return {"indicators": _build_indicators(info, qf, qbs)}


def fetch_profitability(ticker: str) -> dict[str, Any]:
    """수익성만 조회한다."""
    t = yf.Ticker(ticker)
    qf = _fetch_quarterly_financials(t)
    return {"profitability": _build_profitability(qf)}


def fetch_growth(ticker: str) -> dict[str, Any]:
    """성장성만 조회한다."""
    t = yf.Ticker(ticker)
    qf = _fetch_quarterly_financials(t)
    return {"growth": _build_growth(qf)}


def fetch_stability(ticker: str) -> dict[str, Any]:
    """안정성만 조회한다."""
    t = yf.Ticker(ticker)
    qbs = _fetch_quarterly_balance_sheet(t)
    return {"stability": _build_stability(qbs)}


def fetch_earnings(ticker: str) -> dict[str, Any]:
    """실적만 조회한다."""
    t = yf.Ticker(ticker)
    info = _fetch_info(t)
    ed = _fetch_earnings_dates(t)
    return {"earnings": _build_earnings(info, ed)}


# 섹션 이름 → fetch 함수 매핑 (라우터에서 사용)
SECTION_FETCHERS: dict[str, Any] = {
    "profile": fetch_profile,
    "indicators": fetch_indicators,
    "profitability": fetch_profitability,
    "growth": fetch_growth,
    "stability": fetch_stability,
    "earnings": fetch_earnings,
}
