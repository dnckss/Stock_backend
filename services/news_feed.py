from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any

import yfinance as yf

from config import (
    NEWS_FEED_MAX_ITEMS,
    NEWS_FEED_TTL_SEC,
    NEWS_CRAWL_MAX_CONCURRENT,
    NEWS_IMPACT_HALF_LIFE_HOURS,
)
from services.finbert import analyze_batch
from services.news_sentiment import (
    llm_polarity_from_analysis,
    normalize_to_polarity,
    polarity_to_ko,
)
from services.utils import spawn_logged

logger = logging.getLogger(__name__)

_cache: list[dict[str, Any]] = []
_cache_at: datetime | None = None
_stock_news_cache: dict[str, tuple[datetime, list[dict[str, Any]]]] = {}


def _is_fresh() -> bool:
    if _cache_at is None:
        return False
    return (datetime.now() - _cache_at).total_seconds() < NEWS_FEED_TTL_SEC


def enrich_feed_with_llm(feed: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    뉴스 피드 항목에 LLM 분석 결과(news_articles.analysis_json) 가 있으면
    그 polarity 로 sentiment_polarity/label/ko 를 덮어쓰고 sentiment_source 표시.
    LLM 분석 없는 항목은 FinBERT 라벨 그대로 + sentiment_source="finbert".
    """
    if not feed:
        return feed
    hashes = [item.get("url_hash") for item in feed if item.get("url_hash")]
    analysis_map: dict[str, Any] = {}
    if hashes:
        try:
            from services.crud import get_news_articles_analysis_by_hashes
            analysis_map = get_news_articles_analysis_by_hashes(hashes)
        except Exception as e:
            logger.warning("LLM 분석 enrich 조회 실패: %s", e)

    for item in feed:
        h = item.get("url_hash")
        a = analysis_map.get(h) if h else None
        llm_polarity = llm_polarity_from_analysis(a) if a is not None else None
        if llm_polarity is None:
            item.setdefault("sentiment_source", "finbert")
            continue
        item["sentiment_polarity"] = llm_polarity
        item["sentiment_ko"] = polarity_to_ko(llm_polarity)
        item["sentiment_label"] = llm_polarity
        item["sentiment_source"] = "llm"
    return feed


def _coerce_score(item: dict[str, Any]) -> float:
    """라이브 경로(score)와 DB 목록 경로(sentiment_score) 의 감성 점수 필드를 통일한다.

    라이브 종목 상세 피드는 FinBERT 가 item["score"] 를 부여하지만,
    DB(news_items) 경로는 컬럼명이 sentiment_score 라 score 가 비어 있어
    프런트 영향도 계산이 0 이 되던 불일치를 흡수한다.
    """
    raw = item.get("score")
    if raw is None:
        raw = item.get("sentiment_score")
    try:
        return float(raw) if raw is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def attach_impact_scores(
    feed: list[dict[str, Any]],
    *,
    half_life_hours: float = NEWS_IMPACT_HALF_LIFE_HOURS,
) -> list[dict[str, Any]]:
    """피드 항목에 score(경로 통일) + impact(0.0~1.0) 를 부여한다(단일 출처).

    impact = clamp01( |score| × confidence × 0.5^(age_hours / half_life_hours) )
      - age_hours = (now − timestamp) / 3600, timestamp 는 epoch seconds.
      - half_life_hours 이하/0/음수는 기본값으로 폴백한다.
    """
    if not feed:
        return feed
    hl = half_life_hours if half_life_hours and half_life_hours > 0 else NEWS_IMPACT_HALF_LIFE_HOURS
    now_ts = datetime.now(timezone.utc).timestamp()
    for item in feed:
        score = _coerce_score(item)
        item["score"] = score

        conf_raw = item.get("confidence")
        try:
            confidence = float(conf_raw) if conf_raw is not None else 0.0
        except (TypeError, ValueError):
            confidence = 0.0

        impact = abs(score) * confidence
        ts = item.get("timestamp") or 0
        if impact > 0 and ts:
            try:
                age_hours = max(0.0, (now_ts - float(ts)) / 3600.0)
                impact *= 0.5 ** (age_hours / hl)
            except (TypeError, ValueError):
                pass
        item["impact"] = max(0.0, min(1.0, impact))
    return feed


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


def _db_item_to_feed_item(db_item: dict[str, Any]) -> dict[str, Any]:
    """DB(news_items) 행을 피드 항목 형태로 변환한다(라이브 피드와 동일 필드, 단일 출처)."""
    return {
        "title": db_item.get("title", ""),
        "publisher": db_item.get("publisher", ""),
        "timestamp": db_item.get("timestamp", 0),
        "ticker": db_item.get("ticker"),
        "url": db_item.get("url", ""),
        "url_hash": db_item.get("url_hash", ""),
        "score": db_item.get("sentiment_score", 0.0),
        "sentiment_label": db_item.get("sentiment_label", "neutral"),
        "sentiment_polarity": db_item.get("sentiment_polarity", "neutral"),
        "sentiment_ko": db_item.get("sentiment_ko", "중립"),
        "confidence": db_item.get("confidence", 0.0),
        "has_article": db_item.get("has_article", False),
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

    # ticker 별 yf.Ticker.get_news 를 병렬 호출 (직렬 루프 시 N×수초 누적).
    target_tickers = tickers[:15]

    def _fetch_one(tk: str) -> tuple[str, list]:
        from services.yf_limiter import throttled
        try:
            return tk, throttled(lambda _tk=tk: yf.Ticker(_tk).get_news(count=5)) or []
        except Exception as e:
            logger.debug("뉴스 수집 실패 (%s): %s", tk, e)
            return tk, []

    fetch_tasks = [asyncio.to_thread(_fetch_one, tk) for tk in target_tickers]
    fetched = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    for entry in fetched:
        if isinstance(entry, Exception):
            continue
        tk, news_items = entry
        for raw_item in news_items:
            parsed = _parse_news_item(raw_item, tk)
            if parsed:
                raw_items.append(parsed)

    if not raw_items:
        # yfinance 차단 등으로 신규 수집이 0건이면 DB(news_items) 최신값으로 폴백 —
        # 매크로처럼 뉴스도 '없으면 DB의 직전 정보라도' 보여주기 위함.
        try:
            from services.crud import get_news_items
            db_items = get_news_items(limit=NEWS_FEED_MAX_ITEMS)
            if db_items:
                feed = enrich_feed_with_llm([_db_item_to_feed_item(d) for d in db_items])
                _cache = feed
                _cache_at = datetime.now()
                logger.info("뉴스 신규 수집 0건 — DB 최신 %d건으로 폴백", len(feed))
                return _cache
        except Exception as e:
            logger.warning("뉴스 DB 폴백 실패: %s", e)
        _cache = []
        _cache_at = datetime.now()
        return _cache

    titles = [item["title"] for item in raw_items]
    # FinBERT 추론(CPU 바운드 또는 OpenAI 호출) 도 비차단으로 — 대량일 때 효과 큼.
    finbert_results = await asyncio.to_thread(analyze_batch, titles)

    feed: list[dict[str, Any]] = []
    for item, fb in zip(raw_items, finbert_results):
        item["score"] = fb["score"]
        item["sentiment_label"] = fb["label"]
        p = normalize_to_polarity(fb.get("label"))
        item["sentiment_polarity"] = p
        item["sentiment_ko"] = polarity_to_ko(p)
        item["confidence"] = fb["confidence"]
        feed.append(item)

    # 최신순 정렬 + 제목 중복 제거를 한 번에 — 기존엔 두 번 정렬했다.
    # raw_items 는 ticker 병렬 fetch 순서라 단일 sort 가 결정적이다.
    feed.sort(key=lambda x: x["timestamp"], reverse=True)

    seen_titles: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for item in feed:
        if item["title"] not in seen_titles:
            seen_titles.add(item["title"])
            deduped.append(item)

    feed = deduped[:NEWS_FEED_MAX_ITEMS]

    # url_hash 일괄 부여 (DB upsert + 후속 enrich_feed_with_llm 키)
    for item in feed:
        url = (item.get("url") or "").strip()
        if url:
            item["url_hash"] = _hash_url(url)

    # DB upsert + 보충 머지 — 티커 변동으로 뉴스가 사라지는 것을 방지한다.
    # 최적화: fresh feed 가 이미 max items 에 도달했으면 DB 보충은 의미가 없다(슬라이스에서 빠짐).
    # 다만 ticker 변동 보존을 위해 보충은 "부족할 때만" 수행한다.
    try:
        from services.crud import upsert_news_items, get_news_items
        upsert_news_items(feed)

        if len(feed) < NEWS_FEED_MAX_ITEMS:
            db_items = get_news_items(limit=NEWS_FEED_MAX_ITEMS * 2)
            seen_hashes: set[str] = {item.get("url_hash", "") for item in feed}
            for db_item in db_items:
                h = db_item.get("url_hash", "")
                if not h or h in seen_hashes:
                    continue
                seen_hashes.add(h)
                feed.append(_db_item_to_feed_item(db_item))
                if len(feed) >= NEWS_FEED_MAX_ITEMS:
                    break
            feed.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
            feed = feed[:NEWS_FEED_MAX_ITEMS]
    except Exception as e:
        logger.warning("뉴스 피드 DB 저장/병합 실패: %s", e)

    feed = enrich_feed_with_llm(feed)
    _cache = feed
    _cache_at = datetime.now()

    # 본문 전체를 백그라운드로 미리 크롤링해 news_articles DB 에 저장 → 사용자가 뉴스를
    # 눌렀을 때 on-demand 크롤링 없이 DB 캐시로 즉시 응답. (이미 캐시된 건 건너뜀)
    spawn_logged(prefetch_news_articles(feed), name="prefetch_news_articles")
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
        # 동기 yfinance 호출 — 이벤트 루프 비차단을 위해 to_thread.
        news_items = await asyncio.to_thread(
            lambda: yf.Ticker(upper).get_news(count=fetch_count),
        )
    except Exception as e:
        logger.debug("종목별 뉴스 수집 실패 (%s): %s", upper, e)
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
    finbert_results = await asyncio.to_thread(analyze_batch, titles)

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

    # 종목 뉴스에도 url_hash 부여 후 DB 영구 저장 (전체 뉴스 페이지에서 재사용)
    for item in deduped:
        url = (item.get("url") or "").strip()
        if url and not item.get("url_hash"):
            item["url_hash"] = _hash_url(url)
    try:
        from services.crud import upsert_news_items
        upsert_news_items(deduped)
    except Exception as e:
        logger.warning("종목 뉴스 DB 저장 실패 (%s): %s", upper, e)

    result = enrich_feed_with_llm(deduped[:safe_limit])
    _stock_news_cache[upper] = (datetime.now(), result)

    # 종목 상세 뉴스도 본문을 미리 크롤링해 DB 에 저장 → 클릭 시 즉시 응답.
    # (기존엔 종목 뉴스 경로에 본문 프리페치가 없어 클릭마다 on-demand 크롤링 → 느렸다.)
    spawn_logged(prefetch_news_articles(result), name="prefetch_stock_news_articles")
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
                except Exception as e:
                    logger.debug("mark_news_item_has_article 실패: %s", e)
            except Exception as e:
                logger.debug("프리페치 실패 (%s): %s", entry["url"][:60], e)

    tasks = [_crawl_one(entry) for entry in urls_to_fetch]
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("뉴스 본문 프리페치 완료: %d/%d건 처리", len(urls_to_fetch), len(feed))
