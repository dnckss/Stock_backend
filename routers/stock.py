import asyncio
import logging

from fastapi import APIRouter, HTTPException

from services.crud import get_latest_report, get_history, sanitize_for_json
from services.news_feed import build_stock_news_feed
from services.stock_detail import fetch_quote, fetch_chart, format_market_cap

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["Stock"])


# 구체적 경로를 먼저 등록 (FastAPI는 선언 순서로 매칭)

@router.get("/stock/{ticker}/chart")
async def api_stock_chart(ticker: str, period: str = "day"):
    """
    차트 데이터만 별도 조회 (기간 전환 시 사용).
    period: 1D / 5D / 1M / 3M / 6M / 1Y / 5Y
    """
    upper = ticker.upper()
    chart = await asyncio.to_thread(fetch_chart, upper, period)
    if not chart:
        raise HTTPException(status_code=404, detail=f"{upper} 차트 데이터 없음")
    return sanitize_for_json({
        "ticker": upper,
        "period": period.upper(),
        "bars": chart,
        "count": len(chart),
    })


@router.get("/stock/{ticker}/quote")
async def api_stock_quote(ticker: str):
    """실시간 시세만 별도 조회 (polling 갱신용)."""
    upper = ticker.upper()
    try:
        quote = await asyncio.to_thread(fetch_quote, upper)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return sanitize_for_json({
        "ticker": upper,
        **quote,
        "market_cap_display": format_market_cap(quote.get("market_cap")),
    })


@router.get("/stock/{ticker}")
async def api_stock_detail(
    ticker: str,
    chart_period: str = "day",
    news_limit: int = 10,
    news_refresh: int = 0,
):
    """
    종목 상세 페이지.
    - quote: 실시간 시세 + 기업정보 + 호가
    - chart: OHLCV 차트 데이터 (기간: 1D/5D/1M/3M/6M/1Y/5Y)
    - news: 관련 뉴스
    - analysis: AI 분석 리포트 + 히스토리
    """
    upper = ticker.upper()

    # 병렬 실행: 시세 + 차트 + 뉴스
    quote_task = asyncio.to_thread(fetch_quote, upper)
    chart_task = asyncio.to_thread(fetch_chart, upper, chart_period)
    news_task = build_stock_news_feed(upper, limit=news_limit, refresh=bool(news_refresh))

    try:
        quote, chart, stock_news = await asyncio.gather(quote_task, chart_task, news_task)
    except Exception as e:
        logger.exception("종목 상세 조회 실패 (%s): %s", upper, e)
        raise HTTPException(status_code=500, detail=f"데이터 조회 실패: {e}")

    # AI 분석 (동기 DB 호출)
    latest = get_latest_report(upper)
    history = get_history(upper, days=30)

    if not quote.get("price") and not chart and not stock_news:
        raise HTTPException(status_code=404, detail=f"{upper} 데이터 없음")

    return sanitize_for_json({
        "ticker": upper,
        "quote": {
            **quote,
            "market_cap_display": format_market_cap(quote.get("market_cap")),
        },
        "chart": {
            "period": chart_period.upper(),
            "bars": chart,
            "count": len(chart),
        },
        "news": stock_news,
        "analysis": {
            "latest_report": latest,
            "history": history,
        },
    })
