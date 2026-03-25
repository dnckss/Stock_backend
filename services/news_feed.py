from __future__ import annotations

from datetime import datetime
from typing import Any

import yfinance as yf

from config import NEWS_FEED_MAX_ITEMS, NEWS_FEED_TTL_SEC
from services.finbert import analyze_batch

_cache: list[dict[str, Any]] = []
_cache_at: datetime | None = None
_stock_news_cache: dict[str, tuple[datetime, list[dict[str, Any]]]] = {}


def _is_fresh() -> bool:
    if _cache_at is None:
        return False
    return (datetime.now() - _cache_at).total_seconds() < NEWS_FEED_TTL_SEC


def _is_stock_news_fresh(ticker: str) -> bool:
    entry = _stock_news_cache.get(ticker)
    if entry is None:
        return False
    cached_at, _ = entry
    return (datetime.now() - cached_at).total_seconds() < NEWS_FEED_TTL_SEC


def _parse_news_item(item: dict, fallback_ticker: str) -> dict[str, Any] | None:
    """
    yfinance get_news() 응답 1건을 파싱한다.
    FinBERT 감성 분석은 배치로 별도 처리하므로 여기서는 텍스트만 추출한다.
    """
    content = item.get("content")
    if not content or not isinstance(content, dict):
        return None

    title = (content.get("title") or "").strip()
    if not title:
        return None

    pub_date = content.get("pubDate", "")
    try:
        dt = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
        timestamp = int(dt.timestamp())
    except (ValueError, TypeError, AttributeError):
        timestamp = 0

    provider_obj = content.get("provider") or {}
    publisher = provider_obj.get("displayName", "") if isinstance(provider_obj, dict) else str(provider_obj)

    url_field = content.get("clickThroughUrl") or content.get("canonicalUrl") or ""
    url = url_field.get("url", "") if isinstance(url_field, dict) else str(url_field)

    return {
        "title": title,
        "publisher": publisher,
        "timestamp": timestamp,
        "ticker": fallback_ticker,
        "url": url,
    }


async def build_news_feed(tickers: list[str]) -> list[dict[str, Any]]:
    """
    yfinance Ticker.get_news()로 뉴스를 수집하고
    FinBERT 배치 감성 분석으로 score/sentiment_label을 부여해
    최신순으로 정렬한 뒤 최대 NEWS_FEED_MAX_ITEMS 개를 반환한다.
    TTL 캐시 적용.
    """
    global _cache, _cache_at

    if _is_fresh():
        return _cache

    raw_items: list[dict[str, Any]] = []

    for ticker in tickers[:15]:
        try:
            t = yf.Ticker(ticker)
            news_items = t.get_news(count=5)
        except Exception as e:
            print(f"뉴스 수집 실패 ({ticker}): {e}")
            continue

        if not news_items:
            continue

        for raw_item in news_items:
            parsed = _parse_news_item(raw_item, ticker)
            if parsed:
                raw_items.append(parsed)

    if not raw_items:
        _cache = []
        _cache_at = datetime.now()
        return _cache

    titles = [item["title"] for item in raw_items]
    finbert_results = analyze_batch(titles)

    feed: list[dict[str, Any]] = []
    for item, fb in zip(raw_items, finbert_results):
        item["score"] = fb["score"]
        item["sentiment_label"] = fb["label"]
        item["confidence"] = fb["confidence"]
        feed.append(item)

    feed.sort(key=lambda x: x["timestamp"], reverse=True)

    seen_titles: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for item in feed:
        if item["title"] not in seen_titles:
            seen_titles.add(item["title"])
            deduped.append(item)

    feed = deduped[:NEWS_FEED_MAX_ITEMS]

    _cache = feed
    _cache_at = datetime.now()
    return feed


async def build_stock_news_feed(ticker: str, limit: int = 10) -> list[dict[str, Any]]:
    """
    단일 종목 상세 페이지용 뉴스 피드를 생성한다.
    - yfinance Ticker.get_news() 수집
    - FinBERT 배치 감성 분석 점수 부여
    - 최신순 정렬 + 제목 중복 제거
    - 티커별 TTL 캐시 적용
    """
    upper = (ticker or "").upper().strip()
    if not upper:
        return []

    safe_limit = max(1, min(int(limit), 30))
    if _is_stock_news_fresh(upper):
        return _stock_news_cache[upper][1][:safe_limit]

    fetch_count = max(safe_limit, 10)
    try:
        t = yf.Ticker(upper)
        news_items = t.get_news(count=fetch_count)
    except Exception:
        _stock_news_cache[upper] = (datetime.now(), [])
        return []

    raw_items: list[dict[str, Any]] = []
    for raw_item in news_items or []:
        parsed = _parse_news_item(raw_item, upper)
        if parsed:
            raw_items.append(parsed)

    if not raw_items:
        _stock_news_cache[upper] = (datetime.now(), [])
        return []

    titles = [item["title"] for item in raw_items]
    finbert_results = analyze_batch(titles)

    feed: list[dict[str, Any]] = []
    for item, fb in zip(raw_items, finbert_results):
        item["score"] = fb["score"]
        item["sentiment_label"] = fb["label"]
        item["confidence"] = fb["confidence"]
        feed.append(item)

    feed.sort(key=lambda x: x["timestamp"], reverse=True)

    seen_titles: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for item in feed:
        if item["title"] not in seen_titles:
            seen_titles.add(item["title"])
            deduped.append(item)

    result = deduped[:safe_limit]
    _stock_news_cache[upper] = (datetime.now(), result)
    return result
