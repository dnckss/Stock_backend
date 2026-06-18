"""
종목 상세 페이지용 데이터 서비스.
증권사 앱 수준의 시세/차트/호가/기업정보를 제공한다.
"""
from __future__ import annotations

import logging
import math
import threading
import time
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import yfinance as yf

from config import (
    PRICE_BACKFILL_FULL_HISTORY_ENABLED,
    PRICE_HISTORY_COVERAGE_MIN_DAYS,
    STOCK_CHART_DAILY_TTL_SEC,
    STOCK_CHART_DB_LOOKBACK_DAY_DAYS,
    STOCK_CHART_DB_LOOKBACK_MONTH_DAYS,
    STOCK_CHART_DB_LOOKBACK_WEEK_DAYS,
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

# 리샘플 키별 DB 조회 lookback(일). 상세페이지가 매번 상장 이후 전체를 읽어 느리던 문제
# 방지 — 키가 커버하는 최대 뷰에 맞춰 범위 제한. year(연봉)은 봉 수가 적어 전체(None).
_DB_CHART_LOOKBACK_DAYS: dict[str, int | None] = {
    "day": STOCK_CHART_DB_LOOKBACK_DAY_DAYS,
    "week": STOCK_CHART_DB_LOOKBACK_WEEK_DAYS,
    "month": STOCK_CHART_DB_LOOKBACK_MONTH_DAYS,
    "year": None,
}

# lookback(일) → yfinance period 문자열 (DB 미보유/빈 종목 fallback 도 전체 대신 범위 제한).
_YF_PERIOD_THRESHOLDS: list[tuple[int, str]] = [
    (7, "5d"), (31, "1mo"), (95, "3mo"), (185, "6mo"),
    (370, "1y"), (740, "2y"), (1830, "5y"), (3700, "10y"),
]


def _yf_period_for_lookback(days: int | None) -> str:
    """lookback 일수를 yfinance period 문자열로 변환. None/초과면 'max'."""
    if days is None:
        return "max"
    for limit, period in _YF_PERIOD_THRESHOLDS:
        if days <= limit:
            return period
    return "max"

# 온디맨드 풀히스토리 백필 — 차트 요청 시 DB 히스토리가 얕으면(상장 이후 전체가 아직
# 안 채워졌으면) 해당 종목만 1회 백필한다. 프로세스 단위로 종목당 1회만 시도해
# yfinance 재호출/young-stock 무한재시도를 막는다. (전체 부트스트랩의 lazy 버전)
_full_backfill_attempted: set[str] = set()
_full_backfill_lock = threading.Lock()


def _maybe_backfill_full_history(ticker: str) -> None:
    """일봉 이상 차트 요청 시, DB 히스토리가 얕으면 해당 종목 풀히스토리를 1회 백필.

    **백그라운드 스레드에서 수행해 차트 응답을 막지 않는다(첫 진입 지연 제거).**
    호출부는 DB 에 있는 만큼을 즉시 반환하고, 더 깊은 히스토리는 다음 방문에 반영된다.
    DB 의 최초 일자가 COVERAGE_MIN_DAYS 보다 최근이면 '상장 이후 전체가 아직 없음'으로
    보고 backfill_full_history([ticker]) 를 실행한다. upsert 라 idempotent.
    프로세스 단위로 종목당 1회만 시도(attempted set)해 재호출/무한재시도를 막는다.
    """
    if not PRICE_BACKFILL_FULL_HISTORY_ENABLED:
        return
    upper = (ticker or "").upper().strip()
    if not upper:
        return
    with _full_backfill_lock:
        if upper in _full_backfill_attempted:
            return
        _full_backfill_attempted.add(upper)  # 동시 요청/재요청 모두 1회로 제한

    def _run() -> None:
        try:
            from services.price_store import get_ohlcv_db, backfill_full_history
            df = get_ohlcv_db(upper)
            if df is not None and not df.empty:
                earliest = pd.Timestamp(df.index.min())
                if earliest.tzinfo is not None:
                    earliest = earliest.tz_localize(None)
                threshold = pd.Timestamp.now().normalize() - pd.Timedelta(
                    days=PRICE_HISTORY_COVERAGE_MIN_DAYS
                )
                if earliest <= threshold:
                    return  # 이미 충분한 과거까지 보유 → 백필 불필요
            logger.info("온디맨드 풀히스토리 백필 시작(백그라운드): %s", upper)
            result = backfill_full_history([upper])
            logger.info("온디맨드 풀히스토리 백필 완료: %s — %s", upper, result)
        except Exception as e:
            logger.warning("온디맨드 풀히스토리 백필 실패 (%s): %s", upper, e)

    # 차트 응답을 막지 않도록 데몬 스레드로 분리(요청 스레드는 즉시 반환).
    threading.Thread(target=_run, name=f"chart-backfill-{upper}", daemon=True).start()


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


# 일봉을 주/월/분기봉으로 리샘플하는 규칙. 차트 엔드포인트가 DB(price_history) 에서
# 일봉을 읽어 직접 변환하므로 yfinance "1wk"/"1mo"/"3mo" 재호출이 사라진다.
_RESAMPLE_RULES = {
    "week": "W-MON",   # 주봉 — 월요일 시작
    "month": "MS",     # 월봉 — 월 1일 시작
    "year": "YS",      # 년봉 — 연 1월 1일 시작 (프론트 라벨 '년'과 일치)
}


def _resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """일봉 OHLCV → 주/월/분기봉 리샘플 (open=first, high=max, low=min, close=last, volume=sum)."""
    if df is None or df.empty:
        return df
    agg = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    return df.resample(rule).agg(agg).dropna(subset=["close"])


def _df_to_bars(df: pd.DataFrame) -> list[dict[str, Any]]:
    """소문자 OHLCV 컬럼 DataFrame → 차트 bar dict 리스트 (DB/yfinance fallback 공용)."""
    if df is None or df.empty:
        return []
    bars: list[dict[str, Any]] = []
    for idx, row in df.iterrows():
        close = _safe(row.get("close"))
        if close is None:
            continue
        v = row.get("volume")
        bars.append({
            "timestamp": (
                idx.isoformat() if hasattr(idx, "isoformat") else pd.Timestamp(idx).isoformat()
            ),
            "open": _safe(row.get("open")),
            "high": _safe(row.get("high")),
            "low": _safe(row.get("low")),
            "close": close,
            "volume": int(v) if v is not None and not pd.isna(v) else 0,
        })
    return bars


def _db_chart_fresh(ticker: str) -> bool:
    """차트용 DB(price_history) 일봉이 신선한지 — stale 하면 yfinance 로 다시 받게 한다."""
    try:
        from services.price_store import is_ticker_db_fresh
        return is_ticker_db_fresh(ticker)
    except Exception as e:
        logger.debug("DB 차트 신선도 확인 실패 (%s): %s", ticker, e)
        return True  # 확인 불가 시 기존 동작(있는 DB 사용) 유지


def _chart_bars_from_db(ticker: str, key: str) -> list[dict[str, Any]]:
    """일봉~년봉 차트를 price_history 에서 직접 만들어 반환.

    DB 가 비어 있으면 빈 리스트 (호출부가 yfinance fallback). 주/월/년봉은 일봉 리샘플.
    부트스트랩이 끝난 뒤엔 차트 1회 호출 = DB 조회 1회 (yfinance 호출 0회).

    조회 범위는 키별 lookback 으로 제한해 상장 이후 전체(예: AAPL ~11k 봉/1.2MB)를
    매번 읽지 않게 한다 — 응답 시간·페이로드를 크게 줄인다.
    """
    from services.price_store import get_ohlcv_db
    lookback = _DB_CHART_LOOKBACK_DAYS.get(key)
    start = (date.today() - timedelta(days=lookback)) if lookback else None
    df = get_ohlcv_db(ticker, start=start)
    if df is None or df.empty:
        return []
    if key != "day":
        rule = _RESAMPLE_RULES.get(key)
        if rule:
            df = _resample_ohlcv(df, rule)
    return _df_to_bars(df)


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

    # 일봉 이상(day/week/month/year)은 DB(price_history) 에서 직접 — bootstrap 후 yfinance 미사용.
    # DB 가 비어 있을 때(bootstrap 전 또는 non-S&P 종목)만 아래 yfinance 경로로 fallback.
    db_bars: list[dict[str, Any]] = []
    if key not in _CHART_INTRADAY_KEYS:
        # DB 에 있는 만큼 즉시 반환하고, 히스토리가 얕으면 풀히스토리 백필은 백그라운드로
        # 돌린다(응답 비차단 — 첫 진입 지연 제거). 더 깊은 과거는 다음 방문에 반영.
        # 단, DB 최신 거래일이 오래됐으면(증분 backfill 누락) 마지막 봉이 며칠 전 종가라
        # 차트·현재가가 실제와 어긋난다 → 아래 yfinance 경로로 신선하게 다시 받는다.
        _maybe_backfill_full_history(upper)
        db_bars = _chart_bars_from_db(upper, key)
        if db_bars and _db_chart_fresh(upper):
            with _chart_cache_lock:
                _chart_cache[cache_key] = (time.time(), db_bars)
            return db_bars

    # 주/월/년봉은 일봉을 받아 DB 경로와 동일 규칙으로 리샘플 → 일관성 보장.
    # (yfinance 에는 '년' 인터벌이 없어 1wk/1mo/3mo 직접 호출 대신 일봉 리샘플로 통일)
    # auto_adjust=False — DB(price_history)가 raw 종가로 저장되므로 값 일관성을 맞춘다.
    # 분봉은 preset 그대로, 일봉~연봉은 lookback 으로 기간 제한(전체 'max' 회피 → 응답 가속).
    rule = _RESAMPLE_RULES.get(key)
    if key in _CHART_INTRADAY_KEYS:
        yf_period, yf_interval = preset
    else:
        yf_period, yf_interval = _yf_period_for_lookback(_DB_CHART_LOOKBACK_DAYS.get(key)), "1d"
    try:
        df = yf.download(
            ticker, period=yf_period, interval=yf_interval, progress=False, auto_adjust=False
        )
    except Exception as e:
        logger.warning("차트 다운로드 실패 (%s %s): %s", ticker, period, e)
        return db_bars  # 신선한 데이터를 못 받으면 stale 라도 비우지 않는다

    if df.empty:
        return db_bars

    # MultiIndex 처리 + 컬럼명 소문자 정규화 (DB 스키마와 통일)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.rename(columns={
        "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume",
    })
    if rule:
        df = _resample_ohlcv(df, rule)

    bars = _df_to_bars(df)
    if not bars:
        return db_bars

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
