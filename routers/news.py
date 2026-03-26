from fastapi import APIRouter, HTTPException

from services.news_article import get_news_article

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

