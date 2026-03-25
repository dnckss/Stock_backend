from __future__ import annotations

import asyncio
import logging
import random

import httpx
from bs4 import BeautifulSoup

from config import (
    SENTIMENT_FINVIZ_DELAY_SEC,
    SENTIMENT_FINVIZ_MAX_CONCURRENT,
    SENTIMENT_FINVIZ_MAX_RETRIES,
    SENTIMENT_FINVIZ_RETRY_BASE_SEC,
    SENTIMENT_MAX_HEADLINES,
)
from services.finbert import analyze_text, analyze_batch

logger = logging.getLogger(__name__)

_FINVIZ_BASE = "https://finviz.com/quote.ashx?t={}"
_FINVIZ_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

# Finviz 동시 연결 수를 httpx 레벨에서도 제한
_HTTP_LIMITS = httpx.Limits(
    max_connections=SENTIMENT_FINVIZ_MAX_CONCURRENT,
    max_keepalive_connections=SENTIMENT_FINVIZ_MAX_CONCURRENT,
)


def headline_score(title: str) -> float:
    """
    단일 헤드라인에 대해 FinBERT 감성 점수를 반환한다.
    -1.0(부정) ~ 1.0(긍정) 범위.
    """
    result = analyze_text(title)
    return result["score"]


def headline_analysis(title: str) -> dict:
    """
    단일 헤드라인에 대해 FinBERT 전체 분석 결과를 반환한다.
    {"label": "positive"|"negative"|"neutral", "score": float, "confidence": float}
    """
    return analyze_text(title)


async def _scrape_headlines(client: httpx.AsyncClient, ticker: str) -> list[str]:
    """Finviz에서 종목 뉴스 헤드라인을 스크래핑한다. 429 시 지수 백오프 후 재시도."""
    url = _FINVIZ_BASE.format(ticker)
    for attempt in range(SENTIMENT_FINVIZ_MAX_RETRIES):
        try:
            resp = await client.get(url, headers=_FINVIZ_HEADERS, timeout=httpx.Timeout(15.0))

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_sec = float(retry_after)
                else:
                    wait_sec = SENTIMENT_FINVIZ_RETRY_BASE_SEC * (2**attempt) + random.uniform(0, 0.5)
                logger.warning(
                    "Finviz 429 (%s), %.1f초 후 재시도 (%s/%s)",
                    ticker,
                    wait_sec,
                    attempt + 1,
                    SENTIMENT_FINVIZ_MAX_RETRIES,
                )
                await asyncio.sleep(wait_sec)
                continue

            if resp.status_code != 200:
                logger.debug("Finviz HTTP %s (%s)", resp.status_code, ticker)
                return []

            soup = BeautifulSoup(resp.text, "html.parser")
            news_table = soup.find(id="news-table")
            if not news_table:
                return []

            return [
                row.a.text
                for row in news_table.find_all("tr")[:SENTIMENT_MAX_HEADLINES]
                if row.a and row.a.text.strip()
            ]
        except (httpx.HTTPError, asyncio.TimeoutError) as e:
            wait_sec = SENTIMENT_FINVIZ_RETRY_BASE_SEC * (2**attempt) + random.uniform(0, 0.3)
            logger.debug("Finviz 요청 실패 (%s): %s — %.1f초 후 재시도", ticker, e, wait_sec)
            if attempt == SENTIMENT_FINVIZ_MAX_RETRIES - 1:
                return []
            await asyncio.sleep(wait_sec)
        except Exception as e:
            logger.debug("Finviz 파싱 실패 (%s): %s", ticker, e)
            return []

    return []


async def _analyze_one(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    ticker: str,
) -> float:
    """
    Finviz에서 헤드라인을 가져온 뒤 FinBERT 배치 추론으로 종합 감성 점수를 산출한다.
    """
    async with semaphore:
        await asyncio.sleep(SENTIMENT_FINVIZ_DELAY_SEC)
        headlines = await _scrape_headlines(client, ticker)

    if not headlines:
        return 0.0

    results = analyze_batch(headlines)
    scores = [r["score"] for r in results]
    avg = sum(scores) / len(scores)
    return round(max(min(avg, 1.0), -1.0), 4)


async def analyze_sentiments(tickers: list[str]) -> list[float]:
    """
    티커 목록에 대해 감성 점수 리스트를 반환한다.
    Finviz 동시 접속은 제한하고, 요청 간 짧은 간격을 둔다.
    """
    if not tickers:
        return []

    sem = asyncio.Semaphore(SENTIMENT_FINVIZ_MAX_CONCURRENT)

    async with httpx.AsyncClient(limits=_HTTP_LIMITS) as client:
        tasks = [_analyze_one(client, sem, t) for t in tickers]
        return list(await asyncio.gather(*tasks))
