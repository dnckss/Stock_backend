from __future__ import annotations

import hashlib
from typing import Any

from services.article_crawler import fetch_and_extract
from services.crud import get_cached_news_article, upsert_news_article, sanitize_for_json
from config import NEWS_ARTICLE_CACHE_TTL_SEC
from services.finbert import analyze_text
from services.news_sentiment import normalize_to_polarity, polarity_to_ko
from services.news_analysis import analyze_news_korean


def _hash_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


async def get_news_article(url: str, refresh: bool = False, analyze: bool = True) -> dict[str, Any]:
    """
    url 기반으로 본문 텍스트를 반환한다.
    - SQLite 캐시 조회 (TTL 적용)
    - 없으면 크롤링 후 캐시 저장
    """
    clean_url = (url or "").strip()
    if not clean_url:
        return {
            "url": clean_url,
            "article_text": "",
            "article_markdown": "",
            "media": [],
            "domains": {"article": "", "media": []},
            "extraction_status": "empty",
            "error_reason": "empty_url",
            "cache_hit": False,
            "cache_ttl_sec": NEWS_ARTICLE_CACHE_TTL_SEC,
        }

    url_hash = _hash_url(clean_url)
    if not refresh:
        cached = get_cached_news_article(url_hash)
        if cached:
            # 리스트(FinBERT)와 상세(LLM) 비교가 가능하도록 상세 응답에도 FinBERT를 포함한다.
            try:
                title = cached.get("title") or ""
                fb = analyze_text(title) if title else {"label": "neutral", "score": 0.0, "confidence": 0.0}
                p = normalize_to_polarity(fb.get("label"))
                cached["finbert"] = {**fb, "polarity": p, "polarity_ko": polarity_to_ko(p)}
            except Exception:
                cached["finbert"] = {"label": "neutral", "score": 0.0, "confidence": 0.0, "polarity": "neutral", "polarity_ko": "중립"}
            if analyze and cached.get("analysis") is None and (cached.get("article_markdown") or cached.get("article_text")):
                try:
                    analysis = await analyze_news_korean(
                        title=cached.get("title"),
                        publisher=cached.get("publisher"),
                        article_markdown=cached.get("article_markdown") or cached.get("article_text"),
                        url=cached.get("final_url") or cached.get("url"),
                    )
                    cached["analysis"] = analysis
                    upsert_news_article(cached)
                except Exception:
                    pass
            return sanitize_for_json({**cached, "cache_hit": True, "cache_ttl_sec": NEWS_ARTICLE_CACHE_TTL_SEC})

    crawled = await fetch_and_extract(clean_url)
    item = {
        "url_hash": url_hash,
        "url": crawled.get("url") or clean_url,
        "final_url": crawled.get("final_url") or clean_url,
        "http_status": crawled.get("http_status"),
        "extraction_status": crawled.get("extraction_status"),
        "error_reason": crawled.get("error_reason"),
        "title": crawled.get("title"),
        "publisher": crawled.get("publisher"),
        "author": crawled.get("author"),
        "ticker": None,
        "timestamp": crawled.get("timestamp"),
        "canonical_url": crawled.get("canonical_url"),
        "article_text": crawled.get("article_text") or "",
        "article_markdown": crawled.get("article_markdown") or "",
        "media": crawled.get("media") or [],
        "domains": crawled.get("domains") or {"article": "", "media": []},
        "finbert": None,
        "analysis": None,
    }

    try:
        title = item.get("title") or ""
        fb = analyze_text(title) if title else {"label": "neutral", "score": 0.0, "confidence": 0.0}
        p = normalize_to_polarity(fb.get("label"))
        item["finbert"] = {**fb, "polarity": p, "polarity_ko": polarity_to_ko(p)}
    except Exception:
        item["finbert"] = {"label": "neutral", "score": 0.0, "confidence": 0.0, "polarity": "neutral", "polarity_ko": "중립"}

    if analyze and (item.get("article_markdown") or item.get("article_text")) and item.get("extraction_status") == "ok":
        try:
            item["analysis"] = await analyze_news_korean(
                title=item.get("title"),
                publisher=item.get("publisher"),
                article_markdown=item.get("article_markdown") or item.get("article_text"),
                url=item.get("final_url") or item.get("url"),
            )
        except Exception:
            item["analysis"] = None

    upsert_news_article(item)
    return sanitize_for_json({**item, "cache_hit": False, "cache_ttl_sec": NEWS_ARTICLE_CACHE_TTL_SEC})

