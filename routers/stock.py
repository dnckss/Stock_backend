import asyncio
import logging

from fastapi import APIRouter, HTTPException

from services.crud import get_latest_report, get_history, sanitize_for_json
from services.news_feed import build_stock_news_feed
from services.stock_detail import fetch_quote, fetch_chart, format_market_cap
from services.stock_analysis import analyze_stock
from services.stock_fundamentals import fetch_all_fundamentals, SECTION_FETCHERS
from services.technicals import compute_technicals

from config import FUNDAMENTALS_VALID_SECTIONS

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


@router.get("/stock/{ticker}/analysis")
async def api_stock_analysis(ticker: str):
    """
    종목별 AI 심층 분석.
    뉴스·기술적 지표·가격 변동을 종합하여 원인 분석, 반등 가능성, 전략을 제공한다.
    """
    upper = ticker.upper()

    # 병렬: 시세 + 기술적 지표 + 뉴스
    quote_task = asyncio.to_thread(fetch_quote, upper)
    tech_task = asyncio.to_thread(compute_technicals, upper)
    news_task = build_stock_news_feed(upper, limit=10, refresh=True)

    try:
        quote, technicals, stock_news = await asyncio.gather(quote_task, tech_task, news_task)
    except Exception as e:
        logger.exception("종목 분석 데이터 수집 실패 (%s): %s", upper, e)
        raise HTTPException(status_code=500, detail=f"데이터 수집 실패: {e}")

    result = await analyze_stock(upper, quote, technicals, stock_news)

    if result.get("error"):
        return sanitize_for_json({"ticker": upper, "analysis": None, "error": result["error"]})

    return sanitize_for_json({"ticker": upper, "analysis": result})


@router.get("/stock/{ticker}/fundamentals/{section}")
async def api_stock_fundamentals_section(ticker: str, section: str):
    """펀더멘털 개별 섹션 조회 (탭 전환 / lazy-loading)."""
    upper = ticker.upper()
    if section not in FUNDAMENTALS_VALID_SECTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"유효한 섹션: {', '.join(sorted(FUNDAMENTALS_VALID_SECTIONS))}",
        )
    try:
        data = await asyncio.to_thread(SECTION_FETCHERS[section], upper)
    except Exception as e:
        logger.exception("펀더멘털 섹션 조회 실패 (%s/%s): %s", upper, section, e)
        raise HTTPException(status_code=500, detail=f"펀더멘털 조회 실패: {e}")
    return sanitize_for_json({"ticker": upper, **data})


@router.get("/stock/{ticker}/fundamentals")
async def api_stock_fundamentals(ticker: str):
    """종목 펀더멘털 전체 조회 (기업개요, 투자지표, 수익성, 성장성, 안정성, 실적)."""
    upper = ticker.upper()
    try:
        data = await asyncio.to_thread(fetch_all_fundamentals, upper)
    except Exception as e:
        logger.exception("펀더멘털 조회 실패 (%s): %s", upper, e)
        raise HTTPException(status_code=500, detail=f"펀더멘털 조회 실패: {e}")
    return sanitize_for_json({"ticker": upper, **data})


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
