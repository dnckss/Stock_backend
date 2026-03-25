from __future__ import annotations

import hashlib
from typing import Any

from services.article_crawler import fetch_and_extract
from services.crud import get_cached_news_article, upsert_news_article, sanitize_for_json


def _hash_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


async def get_news_article(url: str) -> dict[str, Any]:
    """
    url 기반으로 본문 텍스트를 반환한다.
    - SQLite 캐시 조회 (TTL 적용)
    - 없으면 크롤링 후 캐시 저장
    """
    clean_url = (url or "").strip()
    if not clean_url:
        return {"url": clean_url, "article_text": ""}

    url_hash = _hash_url(clean_url)
    cached = get_cached_news_article(url_hash)
    if cached:
        return sanitize_for_json(cached)

    crawled = await fetch_and_extract(clean_url)
    item = {
        "url_hash": url_hash,
        "url": clean_url,
        "title": None,
        "publisher": None,
        "ticker": None,
        "timestamp": None,
        "article_text": crawled.get("article_text") or "",
    }
    upsert_news_article(item)
    return sanitize_for_json(item)

