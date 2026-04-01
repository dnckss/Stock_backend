"""
섹터 ETF 기반 섹터 로테이션/모멘텀 추적 모듈.
SPDR 섹터 ETF 11종의 주간 수익률을 계산하여 섹터 흐름을 파악한다.
"""
from __future__ import annotations

import logging
import math
from typing import Any

import yfinance as yf

logger = logging.getLogger(__name__)

# SPDR 섹터 ETF 매핑
SECTOR_ETFS: list[dict[str, str]] = [
    {"ticker": "XLK", "sector": "Technology", "sector_ko": "기술"},
    {"ticker": "XLF", "sector": "Financials", "sector_ko": "금융"},
    {"ticker": "XLV", "sector": "Healthcare", "sector_ko": "헬스케어"},
    {"ticker": "XLC", "sector": "Communication Services", "sector_ko": "커뮤니케이션"},
    {"ticker": "XLY", "sector": "Consumer Discretionary", "sector_ko": "임의소비재"},
    {"ticker": "XLP", "sector": "Consumer Staples", "sector_ko": "필수소비재"},
    {"ticker": "XLE", "sector": "Energy", "sector_ko": "에너지"},
    {"ticker": "XLI", "sector": "Industrials", "sector_ko": "산업재"},
    {"ticker": "XLB", "sector": "Materials", "sector_ko": "소재"},
    {"ticker": "XLRE", "sector": "Real Estate", "sector_ko": "부동산"},
    {"ticker": "XLU", "sector": "Utilities", "sector_ko": "유틸리티"},
]


def _safe_round(v: Any, n: int = 4) -> float | None:
    try:
        f = float(v)
        return round(f, n) if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def fetch_sector_performance() -> list[dict[str, Any]]:
    """
    섹터 ETF의 1주/1개월 수익률을 계산한다.
    """
    etf_tickers = [e["ticker"] for e in SECTOR_ETFS]
    try:
        data = yf.download(etf_tickers, period="1mo", interval="1d", group_by="ticker", progress=False, threads=True)
    except Exception as e:
        logger.warning("섹터 ETF 다운로드 실패: %s", e)
        return []

    if data.empty:
        return []

    results: list[dict[str, Any]] = []
    is_single = len(etf_tickers) == 1

    for etf in SECTOR_ETFS:
        ticker = etf["ticker"]
        try:
            if is_single:
                close = data["Close"].dropna()
            else:
                if ticker not in data.columns.get_level_values(0):
                    continue
                close = data[ticker]["Close"].dropna()

            if len(close) < 2:
                continue

            current = float(close.iloc[-1])

            # 1주 수익률 (5거래일)
            week_idx = min(5, len(close) - 1)
            week_base = float(close.iloc[-week_idx - 1])
            weekly_return = (current - week_base) / week_base if week_base != 0 else None

            # 1개월 수익률
            month_base = float(close.iloc[0])
            monthly_return = (current - month_base) / month_base if month_base != 0 else None

            # 모멘텀 판단
            momentum = "neutral"
            if weekly_return is not None:
                if weekly_return > 0.02:
                    momentum = "strong_up"
                elif weekly_return > 0.005:
                    momentum = "up"
                elif weekly_return < -0.02:
                    momentum = "strong_down"
                elif weekly_return < -0.005:
                    momentum = "down"

            results.append({
                "ticker": ticker,
                "sector": etf["sector"],
                "sector_ko": etf["sector_ko"],
                "price": _safe_round(current, 2),
                "weekly_return": _safe_round(weekly_return),
                "monthly_return": _safe_round(monthly_return),
                "momentum": momentum,
            })
        except Exception:
            continue

    results.sort(key=lambda x: x.get("weekly_return") or 0, reverse=True)
    return results


def determine_sector_rotation(sector_perf: list[dict[str, Any]]) -> str:
    """
    섹터 수익률 패턴으로 로테이션 방향을 추정한다.
    - 기술/임의소비재 강세 → growth (성장주 선호)
    - 유틸/필수소비재/헬스케어 강세 → defensive (방어주 선호)
    - 에너지/소재 강세 → cyclical (경기순환주 선호)
    """
    if not sector_perf:
        return "unknown"

    sector_map = {s["sector"]: s.get("weekly_return") or 0 for s in sector_perf}

    growth_avg = _avg([sector_map.get("Technology", 0), sector_map.get("Consumer Discretionary", 0), sector_map.get("Communication Services", 0)])
    defensive_avg = _avg([sector_map.get("Utilities", 0), sector_map.get("Consumer Staples", 0), sector_map.get("Healthcare", 0)])
    cyclical_avg = _avg([sector_map.get("Energy", 0), sector_map.get("Materials", 0), sector_map.get("Industrials", 0)])

    scores = {"growth": growth_avg, "defensive": defensive_avg, "cyclical": cyclical_avg}
    top = max(scores, key=scores.get)

    if top == "growth" and growth_avg > defensive_avg + 0.005:
        return "growth"
    elif top == "defensive" and defensive_avg > growth_avg + 0.005:
        return "defensive"
    elif top == "cyclical" and cyclical_avg > growth_avg + 0.005:
        return "cyclical"
    return "mixed"


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0
