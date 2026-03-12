import asyncio
from datetime import datetime
from typing import Any, Dict, List

import yfinance as yf

from config import NEWS_FEED_MAX_ITEMS, NEWS_PER_TICKER, NEWS_FEED_TTL_SEC
from services.sentiment import headline_score

_cache: List[Dict[str, Any]] = []
_cache_at: datetime | None = None


def _is_fresh() -> bool:
    if _cache_at is None:
        return False
    return (datetime.now() - _cache_at).total_seconds() < NEWS_FEED_TTL_SEC


def _fetch_ticker_news(ticker: str) -> List[Dict[str, Any]]:
    """
    yfinance의 yf.Ticker(ticker).news 를 사용해 뉴스 리스트를 가져온다.
    반환 형태는 raw list(dict) 그대로라 예외 방어가 중요.
    """
    try:
        items = yf.Ticker(ticker).news or []
        if not isinstance(items, list):
            return []
        return items[: NEWS_PER_TICKER * 2]
    except Exception:
        return []


async def build_news_feed(tickers: List[str]) -> List[Dict[str, Any]]:
    """
    candidates 티커들의 최신 뉴스를 수집하고 headline_score로 감성 점수를 부여해
    최신순으로 정렬한 뒤 최대 NEWS_FEED_MAX_ITEMS 개를 반환한다.
    TTL 캐시 적용.
    """
    global _cache, _cache_at

    if _is_fresh():
        return _cache

    sem = asyncio.Semaphore(6)

    async def _one(t: str) -> List[Dict[str, Any]]:
        async with sem:
            return await asyncio.to_thread(_fetch_ticker_news, t)

    raw_lists = await asyncio.gather(*[_one(t) for t in tickers])

    feed: List[Dict[str, Any]] = []
    for ticker, raw in zip(tickers, raw_lists):
        for item in raw[:NEWS_PER_TICKER]:
            try:
                title = item.get("title") or ""
                publisher = item.get("publisher") or ""
                ts = item.get("providerPublishTime")
                if not title or ts is None:
                    continue
                timestamp = int(ts)
                feed.append(
                    {
                        "title": title,
                        "publisher": publisher,
                        "timestamp": timestamp,
                        "ticker": ticker,
                        "score": headline_score(title),
                    }
                )
            except Exception:
                continue

    feed.sort(key=lambda x: x["timestamp"], reverse=True)
    feed = feed[:NEWS_FEED_MAX_ITEMS]

    _cache = feed
    _cache_at = datetime.now()
    return feed

