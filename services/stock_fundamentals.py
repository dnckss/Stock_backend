"""
종목 펀더멘털 데이터 서비스.
기업 개요, 투자 지표, 수익성, 성장성, 안정성, 실적 데이터를 제공한다.
"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
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
# yfinance 차단/rate limit 시 fallback 으로 쓰는 마지막 성공 데이터(영구 보관).
_stale_store: dict[str, dict[str, Any]] = {}
_cache_lock = threading.Lock()

# single-flight — 같은 ticker 의 동시 cache-miss 요청을 1회 조회로 합친다.
# (펀더멘털 7개 섹션 엔드포인트가 동시에 fetch_all_fundamentals 를 호출해도
#  yfinance 조회는 한 번만 일어나도록. 티커별 락 + 이중검사 캐시.)
_inflight_locks: dict[str, threading.Lock] = {}
_inflight_master = threading.Lock()


def _ticker_fetch_lock(ticker: str) -> threading.Lock:
    with _inflight_master:
        lock = _inflight_locks.get(ticker)
        if lock is None:
            lock = threading.Lock()
            _inflight_locks[ticker] = lock
        return lock


def _get_cached(ticker: str) -> dict[str, Any] | None:
    with _cache_lock:
        entry = _cache.get(ticker)
        if entry and (time.time() - entry[0]) < FUNDAMENTALS_CACHE_TTL_SEC:
            return entry[1]
    return None


def _set_cached(ticker: str, data: dict[str, Any]) -> None:
    with _cache_lock:
        _cache[ticker] = (time.time(), data)
        _stale_store[ticker] = data


def _get_stale(ticker: str) -> dict[str, Any] | None:
    with _cache_lock:
        return _stale_store.get(ticker)


def _is_empty_fundamentals(data: dict[str, Any]) -> bool:
    """
    yfinance 차단으로 사실상 빈 응답인지 판정.
    profile.description 은 yfinance info 외엔 못 채워지므로 그게 비어있으면
    info 자체가 차단된 것으로 간주 (Wikipedia 폴백으로 name/sector 만 있어도 빈 것으로 봄
    → 다음 호출에서 yfinance 재시도 + 진짜 데이터 들어오면 캐시 갱신).
    """
    profile = (data or {}).get("profile") or {}
    return not profile.get("description") and not profile.get("market_cap")


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

def _wiki_constituent_lookup(ticker: str) -> dict[str, Any] | None:
    """SP500 Wikipedia 구성종목에서 ticker 정보 조회 — yfinance 차단 시 fallback."""
    if not ticker:
        return None
    try:
        from services.scanner import get_sp500_constituents
        constituents = get_sp500_constituents()
    except Exception:
        return None
    upper = ticker.upper()
    for c in constituents or []:
        if (c.get("ticker") or "").upper() == upper:
            return c
    return None


def _build_profile(info: dict[str, Any], ticker: str | None = None) -> dict[str, Any]:
    """
    기업 개요. yfinance info 차단 시 SP500 Wikipedia constituent 로 name/sector
    최소값 폴백 — 사용자 화면이 완전히 비지 않도록.
    """
    officers = []
    for o in (info.get("companyOfficers") or [])[:FUNDAMENTALS_MAX_OFFICERS]:
        name = o.get("name")
        title = o.get("title")
        if name:
            officers.append({"name": name, "title": title})

    city = info.get("city") or ""
    state = info.get("state") or ""
    headquarters = ", ".join(filter(None, [city, state])) or None

    name = info.get("longName") or info.get("shortName")
    sector = info.get("sector")
    industry = info.get("industry")

    # yfinance info 가 빈 경우 SP500 constituent 로 보강
    if (not name or not sector) and ticker:
        wc = _wiki_constituent_lookup(ticker)
        if wc:
            name = name or wc.get("name")
            sector = sector or wc.get("sector")

    return {
        "name": name or (ticker.upper() if ticker else None),
        "sector": sector,
        "industry": industry,
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
        except Exception as e:
            logger.debug("ex_dividend_date 변환 실패 (raw=%r): %s", ex_date_raw, e)

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


# 기간 라벨 → 최신 거래일 기준 달력 오프셋 (price_performance 공용)
_PRICE_PERF_OFFSETS: list[tuple[str, pd.DateOffset]] = [
    ("1D", pd.DateOffset(days=1)),
    ("5D", pd.DateOffset(days=7)),
    ("1W", pd.DateOffset(weeks=1)),
    ("1M", pd.DateOffset(months=1)),
    ("1Y", pd.DateOffset(years=1)),
]
# 1Y 등락률을 산출하려면 DB 가 이만큼 과거를 덮어야 한다. 미달이면 yfinance fallback.
_PRICE_PERF_DB_MIN_SPAN_DAYS = 360
# DB 조회 범위 — 13개월 여유 (1Y + 영업일 갭 흡수)
_PRICE_PERF_LOOKBACK_DAYS = 400


def _price_performance_from_db(ticker: str) -> pd.DataFrame | None:
    """price_history(DB)에서 최근 ~13개월 OHLCV. 1Y 커버 못 하면 None → yfinance fallback.

    차트(fetch_chart)와 동일한 DB-first 패턴. 부트스트랩된 S&P 500 은 여기서 끝나
    yfinance 13mo 다운로드(가장 무거운 호출)가 사라진다.
    """
    try:
        from services.price_store import get_ohlcv_db
        start = date.today() - timedelta(days=_PRICE_PERF_LOOKBACK_DAYS)
        df = get_ohlcv_db(ticker, start=start)
    except Exception as e:
        logger.debug("price_performance DB 조회 실패 (%s): %s", ticker, e)
        return None
    if df is None or df.empty or len(df) < 2:
        return None
    span_days = (df.index.max() - df.index.min()).days
    if span_days < _PRICE_PERF_DB_MIN_SPAN_DAYS:
        return None  # 1Y 미커버(미보유/young/shallow) → yfinance 로
    return df


def _price_performance_from_yf(ticker: str) -> pd.DataFrame | None:
    """yfinance 13mo 일봉 — DB 미보유/얕은 종목 fallback. 컬럼 소문자 정규화."""
    from services.yf_limiter import throttled
    try:
        df = throttled(
            lambda: yf.download(ticker, period="13mo", interval="1d", progress=False)
        )
    except Exception as e:
        logger.debug("price_performance 다운로드 실패 (%s): %s", ticker, e)
        return None
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    # MultiIndex 해제 후 중복 컬럼 발생 시 첫 번째만 사용
    df = df.loc[:, ~df.columns.duplicated()]
    return df.rename(columns={
        "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume",
    })


def _compute_price_periods(df: pd.DataFrame | None) -> list[dict[str, Any]]:
    """소문자 OHLCV df → 기간별 등락률·거래량·거래대금. DB·yfinance 경로 공용."""
    if df is None or df.empty or len(df) < 2 or "close" not in df.columns:
        return []
    df = df.sort_index()
    latest_date = df.index[-1]
    current_close = _safe(df["close"].iloc[-1], 6)
    if current_close is None:
        return []
    has_vol = "volume" in df.columns

    periods: list[dict[str, Any]] = []
    for label, offset in _PRICE_PERF_OFFSETS:
        target_date = latest_date - offset
        mask = df.index >= target_date  # target 이후 가장 오래된 거래일
        if not mask.any():
            periods.append({"label": label, "change_pct": None, "volume": None, "trading_value": None})
            continue

        period_df = df.loc[mask]
        past_close = _safe(period_df["close"].iloc[0], 6)
        change_pct = _safe((current_close - past_close) / past_close * 100, 2) if past_close else None

        vol = period_df["volume"].sum() if has_vol else None
        tv = (period_df["close"] * period_df["volume"]).sum() if has_vol else None
        periods.append({
            "label": label,
            "change_pct": change_pct,
            "volume": int(vol) if vol is not None and not pd.isna(vol) else None,
            "trading_value": _safe(tv, 0) if tv is not None else None,
        })

    return periods


def _build_price_performance(ticker: str) -> dict[str, Any]:
    """기간별 등락률·거래량·거래대금 (1D/5D/1W/1M/1Y).

    price_history(DB) 우선 — 차트와 동일한 DB-first. DB 가 1Y 를 못 덮으면
    (미보유/young/shallow) yfinance 13mo 일봉으로 fallback.
    """
    df = _price_performance_from_db(ticker)
    if df is None:
        df = _price_performance_from_yf(ticker)
    return {"periods": _compute_price_periods(df)}


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
    """
    전체 펀더멘털 데이터를 한 번에 조회한다.
    - TTL 캐시 hit 시 즉시 반환
    - cache-miss 시 single-flight: 같은 ticker 동시 요청은 한 번만 실제 조회
    - yfinance 차단으로 빈 응답이 오면 캐시 저장 안 하고 직전 성공 데이터(stale) 반환
    """
    cached = _get_cached(ticker)
    if cached is not None:
        return cached

    # single-flight: 락 대기 중 다른 스레드가 채웠으면 그 결과를 공유(이중검사).
    lock = _ticker_fetch_lock(ticker)
    with lock:
        cached = _get_cached(ticker)
        if cached is not None:
            return cached
        return _fetch_all_fundamentals_uncached(ticker)


def _fetch_all_fundamentals_uncached(ticker: str) -> dict[str, Any]:
    """실제 조회 — 독립적인 yfinance 호출 5개를 병렬 실행 후 조립.

    info / quarterly_financials / quarterly_balance_sheet / earnings_dates /
    price_performance 는 서로 의존성이 없어 동시에 받는다. 각 호출은 yf_limiter
    의 throttled(세마포어=YF_GLOBAL_CONCURRENCY, 최소간격)로 rate limit 이 제어되므로
    벽시계 시간이 '합'에서 '최댓값'으로 줄어든다. (Ticker 객체는 호출별로 분리해 동시
    속성 접근 경합을 피한다.)
    """
    with ThreadPoolExecutor(max_workers=5) as ex:
        f_info = ex.submit(_fetch_info, yf.Ticker(ticker))
        f_qf = ex.submit(_fetch_quarterly_financials, yf.Ticker(ticker))
        f_qbs = ex.submit(_fetch_quarterly_balance_sheet, yf.Ticker(ticker))
        f_ed = ex.submit(_fetch_earnings_dates, yf.Ticker(ticker))
        f_pp = ex.submit(_build_price_performance, ticker)
        info = f_info.result()
        qf = f_qf.result()
        qbs = f_qbs.result()
        ed = f_ed.result()
        price_performance = f_pp.result()

    data = {
        "profile": _build_profile(info, ticker=ticker),
        "indicators": _build_indicators(info, qf, qbs),
        "profitability": _build_profitability(qf),
        "growth": _build_growth(qf),
        "stability": _build_stability(qbs),
        "earnings": _build_earnings(info, ed),
        "price_performance": price_performance,
    }

    if _is_empty_fundamentals(data):
        stale = _get_stale(ticker)
        if stale is not None:
            return {**stale, "stale": True}
        return data

    data["stale"] = False
    _set_cached(ticker, data)
    return data


def _section_from_full(ticker: str, key: str) -> dict[str, Any]:
    """
    섹션별 fetcher 공통 로직 — fetch_all_fundamentals 의 결과에서 한 섹션만 슬라이스.
    같은 캐시·stale_store 를 공유해 yfinance 차단 시에도 직전 데이터(stale=True) 노출.
    """
    full = fetch_all_fundamentals(ticker)
    return {
        key: full.get(key) or {},
        "stale": full.get("stale", False),
    }


def fetch_profile(ticker: str) -> dict[str, Any]:
    return _section_from_full(ticker, "profile")


def fetch_indicators(ticker: str) -> dict[str, Any]:
    return _section_from_full(ticker, "indicators")


def fetch_profitability(ticker: str) -> dict[str, Any]:
    return _section_from_full(ticker, "profitability")


def fetch_growth(ticker: str) -> dict[str, Any]:
    return _section_from_full(ticker, "growth")


def fetch_stability(ticker: str) -> dict[str, Any]:
    return _section_from_full(ticker, "stability")


def fetch_earnings(ticker: str) -> dict[str, Any]:
    return _section_from_full(ticker, "earnings")


def fetch_price_performance(ticker: str) -> dict[str, Any]:
    return _section_from_full(ticker, "price_performance")


# 섹션 이름 → fetch 함수 매핑 (라우터에서 사용)
SECTION_FETCHERS: dict[str, Any] = {
    "profile": fetch_profile,
    "indicators": fetch_indicators,
    "profitability": fetch_profitability,
    "growth": fetch_growth,
    "stability": fetch_stability,
    "earnings": fetch_earnings,
    "price_performance": fetch_price_performance,
}
