from fastapi import APIRouter, HTTPException

from services.news_article import get_news_article
from services.economic_calendar import fetch_economic_calendar

router = APIRouter(prefix="/api", tags=["News"])


@router.get("/news")
async def api_news_detail(url: str, refresh: int = 0, analyze: int = 1):
    """
    뉴스 상세(원문 URL 크롤링):
    - url: 원문 URL (yfinance news의 url을 그대로 전달)
    """
    if not url or not url.strip():
        raise HTTPException(status_code=400, detail="url 쿼리 파라미터가 필요합니다.")
    return await get_news_article(url, refresh=bool(refresh), analyze=bool(analyze))


@router.get("/economic-calendar")
async def api_economic_calendar(refresh: int = 0, limit: int = 500):
    """
    경제 일정 캘린더 조회(myfxbook 크롤링 + TTL 캐시).
    - refresh=1: 캐시 무시 후 재수집
    - limit: 반환 이벤트 개수(최대 config.ECON_CALENDAR_MAX_ITEMS)
    """
    if limit < 1:
        raise HTTPException(status_code=400, detail="limit은 1 이상이어야 합니다.")
    return await fetch_economic_calendar(refresh=bool(refresh), limit=limit)

