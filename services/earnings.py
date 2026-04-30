from __future__ import annotations

import logging
import math
import threading
import time

import yfinance as yf

from config import (
    EARNINGS_CACHE_FAILURES,
    EARNINGS_CACHE_TTL_SEC,
    EARNINGS_FAILURE_CACHE_TTL_SEC,
    EARNINGS_INTER_REQUEST_DELAY_SEC,
)

logger = logging.getLogger(__name__)


# Ticker 별 실적 결과 메모리 캐시.
# 성공: TTL = EARNINGS_CACHE_TTL_SEC (24h)
# 실패(None): TTL = EARNINGS_FAILURE_CACHE_TTL_SEC (1h, 옵션 비활성화 시 캐시 안 함)
# 분기 실적은 자주 바뀌지 않으므로 사이클마다 yfinance 를 다시 부를 필요가 없다.
_earnings_cache: dict[str, tuple[float, dict | None]] = {}
_earnings_cache_lock = threading.Lock()


def _cache_get(ticker: str) -> tuple[bool, dict | None]:
    """
    캐시 조회. (hit, value) 반환.
    hit=False 면 다시 yfinance 호출 필요.
    """
    with _earnings_cache_lock:
        entry = _earnings_cache.get(ticker)
        if not entry:
            return False, None
        ts, data = entry
        ttl = EARNINGS_CACHE_TTL_SEC if data is not None else EARNINGS_FAILURE_CACHE_TTL_SEC
        if time.time() - ts > ttl:
            _earnings_cache.pop(ticker, None)
            return False, None
        return True, data


def _cache_put(ticker: str, value: dict | None) -> None:
    if value is None and not EARNINGS_CACHE_FAILURES:
        return
    with _earnings_cache_lock:
        _earnings_cache[ticker] = (time.time(), value)


def _fetch_earnings_surprise(ticker: str) -> dict | None:
    """yfinance 실호출 — 캐시 없이 직접 조회."""
    from services.yf_limiter import throttled

    try:
        t = yf.Ticker(ticker)
        df = throttled(t.get_earnings_history)

        if df is None or df.empty:
            return None

        latest = df.iloc[-1]
        actual = latest.get("epsActual")
        estimate = latest.get("epsEstimate")
        surprise = latest.get("surprisePercent")

        if actual is None or estimate is None:
            return None
        if not math.isfinite(actual) or not math.isfinite(estimate):
            return None

        if surprise is not None and math.isfinite(surprise):
            surprise_pct = round(surprise, 4)
        elif estimate != 0:
            surprise_pct = round((actual - estimate) / abs(estimate), 4)
        else:
            surprise_pct = 0.0

        return {
            "ticker": ticker,
            "eps_actual": round(actual, 4),
            "eps_estimate": round(estimate, 4),
            "surprise_pct": surprise_pct,
        }
    except Exception as e:
        msg = str(e)
        if "HTTP Error" in msg or "quoteSummary" in msg:
            logger.debug("실적 조회 실패 (%s): %s", ticker, msg)
        else:
            logger.warning("실적 조회 실패 (%s): %s", ticker, msg)
        return None


def get_earnings_surprise(ticker: str) -> dict | None:
    """
    yfinance get_earnings_history()로 최신 분기 실적 서프라이즈를 조회한다.
    캐시(24h) 적용 — 동일 ticker 재호출은 yfinance 다시 부르지 않는다.

    Returns:
        {
            "ticker": str,
            "eps_actual": float,
            "eps_estimate": float,
            "surprise_pct": float,  # (actual - estimate) / |estimate|, 소수
        }
        또는 데이터 없으면 None.
    """
    if not ticker:
        return None
    hit, cached = _cache_get(ticker)
    if hit:
        return cached
    result = _fetch_earnings_surprise(ticker)
    _cache_put(ticker, result)
    return result


def get_earnings_surprises(tickers: list[str]) -> list[dict | None]:
    """
    티커 목록에 대해 순서대로 최신 실적 서프라이즈를 조회한다.
    실패한 티커는 None으로 채운다. 캐시 hit이면 즉시 반환되어 inter-request delay 도 스킵.
    """
    results: list[dict | None] = []
    delay = EARNINGS_INTER_REQUEST_DELAY_SEC
    for i, ticker in enumerate(tickers):
        # 캐시 확인 — hit이면 sleep 생략(yfinance 호출이 없으니)
        hit, cached = _cache_get(ticker)
        if hit:
            results.append(cached)
            continue
        # 실호출 직전에만 inter-request 간격 적용
        if i > 0 and delay > 0:
            time.sleep(delay)
        result = _fetch_earnings_surprise(ticker)
        _cache_put(ticker, result)
        results.append(result)
    return results
