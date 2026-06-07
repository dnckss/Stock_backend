import asyncio
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from config import (
    NEWS_IMPACT_HALF_LIFE_HOURS,
    NEWS_TOP_DEFAULT_LIMIT,
    NEWS_TOP_DEFAULT_WINDOW_HOURS,
    NEWS_TOP_MAX_WINDOW_HOURS,
    NEWS_TOP_SCAN_MAX_ITEMS,
)
from services.crud import count_news_items, get_news_items, sanitize_for_json
from services.news_article import get_news_article
from services.news_feed import attach_impact_scores, enrich_feed_with_llm
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
    # Supabase sync 호출은 to_thread 로 감싸 이벤트 루프 비차단
    items = await asyncio.to_thread(
        get_news_items, limit=safe_limit, ticker=ticker, offset=safe_offset,
    )
    items = enrich_feed_with_llm(items)
    # score(경로 통일) + impact(0~1) 부여 — 라이브 종목 피드와 필드 일치.
    items = attach_impact_scores(items)
    payload: dict = {
        "items": items,
        "count": len(items),
        "limit": safe_limit,
        "offset": safe_offset,
    }
    if with_count:
        payload["total"] = await asyncio.to_thread(count_news_items, ticker=ticker)
    return sanitize_for_json(payload)


@router.get("/news/top")
async def api_news_top(
    limit: int = NEWS_TOP_DEFAULT_LIMIT,
    window_hours: int = NEWS_TOP_DEFAULT_WINDOW_HOURS,
    ticker: str | None = None,
    half_life_hours: float = NEWS_IMPACT_HALF_LIFE_HOURS,
):
    """
    최근 구간 뉴스 중 시장 영향도 상위 항목을 반환한다(전체 코퍼스 기준 랭킹).
    - limit: 상위 개수 (기본 NEWS_TOP_DEFAULT_LIMIT, 최대 50)
    - window_hours: 랭킹 대상 최근 구간 (기본 NEWS_TOP_DEFAULT_WINDOW_HOURS, 최대 NEWS_TOP_MAX_WINDOW_HOURS)
    - ticker: 특정 종목 필터 (선택)
    - half_life_hours: 시간 감쇠 반감기 (기본 NEWS_IMPACT_HALF_LIFE_HOURS)
    응답 items 는 /api/news/list 와 동일 형식 + impact 필드를 포함하며 impact 내림차순 정렬.
    """
    safe_limit = max(1, min(limit, 50))
    safe_window = max(1, min(window_hours, NEWS_TOP_MAX_WINDOW_HOURS))
    safe_half_life = half_life_hours if half_life_hours and half_life_hours > 0 else NEWS_IMPACT_HALF_LIFE_HOURS
    since_ts = int(datetime.now(timezone.utc).timestamp() - safe_window * 3600)

    items = await asyncio.to_thread(
        get_news_items, limit=NEWS_TOP_SCAN_MAX_ITEMS, ticker=ticker, since_ts=since_ts,
    )
    items = enrich_feed_with_llm(items)
    items = attach_impact_scores(items, half_life_hours=safe_half_life)
    items.sort(key=lambda x: x.get("impact", 0.0), reverse=True)
    top = items[:safe_limit]
    return sanitize_for_json({
        "items": top,
        "count": len(top),
        "limit": safe_limit,
        "window_hours": safe_window,
        "half_life_hours": safe_half_life,
    })


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

