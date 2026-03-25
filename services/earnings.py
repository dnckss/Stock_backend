from __future__ import annotations

import logging
import math
import time

import yfinance as yf

from config import EARNINGS_INTER_REQUEST_DELAY_SEC

logger = logging.getLogger(__name__)


def get_earnings_surprise(ticker: str) -> dict | None:
    """
    yfinance get_earnings_history()로 최신 분기 실적 서프라이즈를 조회한다.

    Returns:
        {
            "ticker": str,
            "eps_actual": float,
            "eps_estimate": float,
            "surprise_pct": float,  # (actual - estimate) / |estimate|, 소수
        }
        또는 데이터 없으면 None.
    """
    try:
        t = yf.Ticker(ticker)
        df = t.get_earnings_history()

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
            logger.warning("실적 조회 실패 (%s): %s", ticker, msg, exc_info=True)
        return None


def get_earnings_surprises(tickers: list[str]) -> list[dict | None]:
    """
    티커 목록에 대해 순서대로 최신 실적 서프라이즈를 조회한다.
    실패한 티커는 None으로 채운다.
    """
    results: list[dict | None] = []
    delay = EARNINGS_INTER_REQUEST_DELAY_SEC
    for i, ticker in enumerate(tickers):
        if i > 0 and delay > 0:
            time.sleep(delay)
        results.append(get_earnings_surprise(ticker))
    return results
