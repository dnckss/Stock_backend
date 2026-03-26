import asyncio

import yfinance as yf
from fastapi import APIRouter, HTTPException

from config import STOCK_PROFILE_TIMEOUT_SEC
from services.crud import get_latest_report, get_history
from services.news_feed import build_stock_news_feed

router = APIRouter(prefix="/api", tags=["Stock"])
_company_name_cache: dict[str, str] = {}


async def _get_company_name(ticker: str) -> str:
    upper = (ticker or "").upper().strip()
    if not upper:
        return ""
    if upper in _company_name_cache:
        return _company_name_cache[upper]

    def _fetch() -> str:
        info = yf.Ticker(upper).info or {}
        name = (
            info.get("longName")
            or info.get("shortName")
            or info.get("displayName")
            or upper
        )
        return str(name).strip() or upper

    try:
        name = await asyncio.wait_for(
            asyncio.to_thread(_fetch),
            timeout=STOCK_PROFILE_TIMEOUT_SEC,
        )
    except Exception:
        name = upper

    _company_name_cache[upper] = name
    return name


@router.get("/stock/{ticker}")
async def api_stock_detail(ticker: str, news_limit: int = 10, news_refresh: int = 0):
    """
    종목 상세 분석 페이지용 엔드포인트.
    - latest_report: 가장 최근 AI 리포트 전문 1건
    - history: 최근 30일치 (수익률, 감성, 괴리율) 시계열 데이터
    """
    upper = ticker.upper()
    company_name = await _get_company_name(upper)
    latest = get_latest_report(upper)
    history = get_history(upper, days=30)
    stock_news = await build_stock_news_feed(upper, limit=news_limit, refresh=bool(news_refresh))

    if not latest and not history and not stock_news:
        raise HTTPException(status_code=404, detail=f"{upper} 데이터 없음")

    return {
        "ticker": upper,
        "company_name": company_name,
        "latest_report": latest,
        "history": history,
        "stock_news": stock_news,
        "stock_news_meta": {
            "refresh": bool(news_refresh),
        },
    }
