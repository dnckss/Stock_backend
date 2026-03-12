import asyncio
import httpx
from bs4 import BeautifulSoup
from config import (
    POSITIVE_KEYWORDS,
    NEGATIVE_KEYWORDS,
    SENTIMENT_WEIGHT,
    SENTIMENT_MAX_HEADLINES,
)

def headline_score(title: str) -> float:
    """
    개별 뉴스 제목 단위 감성 점수 산출.
    - POSITIVE_KEYWORDS / NEGATIVE_KEYWORDS 기반 키워드 매칭
    - -1.0 ~ 1.0 범위로 클리핑
    """
    if not title:
        return 0.0
    t = title.lower()
    pos = sum(1 for kw in POSITIVE_KEYWORDS if kw in t)
    neg = sum(1 for kw in NEGATIVE_KEYWORDS if kw in t)
    score = (pos - neg) * SENTIMENT_WEIGHT
    return round(max(min(score, 1.0), -1.0), 2)


async def _fetch_one(client: httpx.AsyncClient, ticker: str) -> float:
    """Finviz 뉴스 헤드라인을 스크래핑하여 감성 점수를 산출한다."""
    url = f"https://finviz.com/quote.ashx?t={ticker}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = await client.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
        news_table = soup.find(id="news-table")
        if not news_table:
            return 0.0

        headlines = [
            row.a.text.lower()
            for row in news_table.find_all("tr")[:SENTIMENT_MAX_HEADLINES]
            if row.a
        ]

        score = 0.0
        for h in headlines:
            if any(kw in h for kw in POSITIVE_KEYWORDS):
                score += SENTIMENT_WEIGHT
            if any(kw in h for kw in NEGATIVE_KEYWORDS):
                score -= SENTIMENT_WEIGHT

        return round(max(min(score, 1.0), -1.0), 2)
    except Exception:
        return 0.0


async def analyze_sentiments(tickers: list) -> list:
    """티커 목록에 대해 비동기 병렬로 감성 점수 리스트를 반환한다."""
    async with httpx.AsyncClient() as client:
        tasks = [_fetch_one(client, t) for t in tickers]
        return list(await asyncio.gather(*tasks))
