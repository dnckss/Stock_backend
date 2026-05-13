from fastapi import APIRouter, HTTPException

from services.crud import count_news_items, get_news_items, sanitize_for_json
from services.news_article import get_news_article
from services.news_feed import enrich_feed_with_llm
from services.economic_calendar import fetch_economic_calendar
from services.econ_detail import get_econ_event_detail

router = APIRouter(prefix="/api", tags=["News"])


@router.get("/news/list")
async def api_news_list(
    limit: int = 50,
    offset: int = 0,
    ticker: str | None = None,
    with_count: int = 0,
):
    """
    DB 에 저장된 뉴스 목록 조회 (페이지네이션).
    - limit: 반환 개수 (기본 50, 최대 500)
    - offset: 시작 오프셋 (기본 0)
    - ticker: 특정 종목 필터 (선택)
    - with_count=1: total(전체 행 수) 동봉. 부담이 있으니 첫 페이지에서만 권장.
    """
    safe_limit = max(1, min(limit, 500))
    safe_offset = max(0, offset)
    items = get_news_items(limit=safe_limit, ticker=ticker, offset=safe_offset)
    items = enrich_feed_with_llm(items)
    payload: dict = {
        "items": items,
        "count": len(items),
        "limit": safe_limit,
        "offset": safe_offset,
    }
    if with_count:
        payload["total"] = count_news_items(ticker=ticker)
    return sanitize_for_json(payload)


@router.get("/news")
async def api_news_detail(url: str, refresh: int = 0, analyze: int = 1):
    """
    뉴스 상세(원문 URL 크롤링):
    - url: 원문 URL (yfinance news의 url을 그대로 전달)
    """
    if not url or not url.strip():
        raise HTTPException(status_code=400, detail="url 쿼리 파라미터가 필요합니다.")
    return sanitize_for_json(await get_news_article(url, refresh=bool(refresh), analyze=bool(analyze)))


@router.get("/economic-calendar")
async def api_economic_calendar(refresh: int = 0):
    """
    경제 일정 캘린더 조회(myfxbook 크롤링 + TTL 캐시).
    DB에 저장된 모든 일정을 시간순으로 반환한다.
    - refresh=1: 캐시 무시 후 재수집
    """
    return sanitize_for_json(await fetch_economic_calendar(refresh=bool(refresh)))


@router.get("/economic-calendar/detail")
async def api_econ_event_detail(
    event: str,
    country: str | None = None,
    currency: str | None = None,
    importance: int | None = None,
    actual: str | None = None,
    forecast: str | None = None,
    previous: str | None = None,
):
    """
    경제 이벤트 상세 정보 조회.
    - event: 이벤트 영문명 (필수)
    - 나머지: 부가 컨텍스트 (선택)

    첫 호출 시 AI 생성 (30~60초), 이후 DB 캐시 히트로 즉시 응답.
    """
    if not event or not event.strip():
        raise HTTPException(status_code=400, detail="event 파라미터가 필요합니다.")

    event_data = {}
    if country:
        event_data["country_name"] = country
    if currency:
        event_data["currency"] = currency
    if importance is not None:
        event_data["importance"] = importance
    if actual:
        event_data["actual"] = actual
    if forecast:
        event_data["forecast"] = forecast
    if previous:
        event_data["previous"] = previous

    return await get_econ_event_detail(event.strip(), event_data or None)

