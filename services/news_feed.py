from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime
from typing import Any

import yfinance as yf

from config import NEWS_FEED_MAX_ITEMS, NEWS_FEED_TTL_SEC, NEWS_CRAWL_MAX_CONCURRENT
from services.finbert import analyze_batch
from services.news_sentiment import normalize_to_polarity, polarity_to_ko

logger = logging.getLogger(__name__)

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
        p = normalize_to_polarity(fb.get("label"))
        item["sentiment_polarity"] = p
        item["sentiment_ko"] = polarity_to_ko(p)
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

    # DB에 저장 (url_hash 추가)
    for item in feed:
        url = (item.get("url") or "").strip()
        if url:
            item["url_hash"] = _hash_url(url)
    try:
        from services.crud import upsert_news_items
        upsert_news_items(feed)
    except Exception as e:
        logger.warning("뉴스 피드 DB 저장 실패: %s", e)

    _cache = feed
    _cache_at = datetime.now()
    return feed


async def build_stock_news_feed(ticker: str, limit: int = 10, refresh: bool = False) -> list[dict[str, Any]]:
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
    if not refresh and _is_stock_news_fresh(upper):
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
        p = normalize_to_polarity(fb.get("label"))
        item["sentiment_polarity"] = p
        item["sentiment_ko"] = polarity_to_ko(p)
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


# ---------------------------------------------------------------------------
# 본문 프리페치: 뉴스 피드의 URL을 백그라운드에서 미리 크롤링하여 DB에 캐싱
# ---------------------------------------------------------------------------

def _hash_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


async def prefetch_news_articles(feed: list[dict[str, Any]]) -> None:
    """
    뉴스 피드의 URL들을 백그라운드에서 크롤링하여 DB에 저장한다.
    이미 캐시된 기사는 건너뛴다. 사용자가 상세보기를 눌렀을 때 즉시 응답할 수 있도록 한다.
    """
    from services.article_crawler import fetch_and_extract
    from services.crud import get_cached_news_article, upsert_news_article, mark_news_item_has_article

    urls_to_fetch: list[dict[str, Any]] = []
    for item in feed:
        url = (item.get("url") or "").strip()
        if not url:
            continue
        url_hash = _hash_url(url)
        cached = get_cached_news_article(url_hash)
        if cached:
            continue
        urls_to_fetch.append({"url": url, "url_hash": url_hash, "title": item.get("title"), "publisher": item.get("publisher"), "ticker": item.get("ticker")})

    if not urls_to_fetch:
        return

    sem = asyncio.Semaphore(NEWS_CRAWL_MAX_CONCURRENT)

    async def _crawl_one(entry: dict[str, Any]) -> None:
        async with sem:
            try:
                crawled = await fetch_and_extract(entry["url"])
                row = {
                    "url_hash": entry["url_hash"],
                    "url": entry["url"],
                    "final_url": crawled.get("final_url") or entry["url"],
                    "http_status": crawled.get("http_status"),
                    "extraction_status": crawled.get("extraction_status"),
                    "error_reason": crawled.get("error_reason"),
                    "title": crawled.get("title") or entry.get("title"),
                    "publisher": crawled.get("publisher") or entry.get("publisher"),
                    "author": crawled.get("author"),
                    "ticker": entry.get("ticker"),
                    "timestamp": crawled.get("timestamp"),
                    "canonical_url": crawled.get("canonical_url"),
                    "article_text": crawled.get("article_text") or "",
                    "article_markdown": crawled.get("article_markdown") or "",
                    "media": crawled.get("media") or [],
                    "domains": crawled.get("domains") or {"article": "", "media": []},
                }
                upsert_news_article(row)
                try:
                    mark_news_item_has_article(entry["url_hash"])
                except Exception:
                    pass
            except Exception as e:
                logger.debug("프리페치 실패 (%s): %s", entry["url"][:60], e)

    tasks = [_crawl_one(entry) for entry in urls_to_fetch]
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("뉴스 본문 프리페치 완료: %d/%d건 처리", len(urls_to_fetch), len(feed))
