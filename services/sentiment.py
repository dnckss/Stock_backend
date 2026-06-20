from __future__ import annotations

import asyncio
import logging
import random

import httpx
from bs4 import BeautifulSoup

from config import (
    BROWSER_HEADERS,
    FINVIZ_QUOTE_URL,
    FINVIZ_TIMEOUT_SEC,
    SENTIMENT_FINVIZ_DELAY_SEC,
    SENTIMENT_FINVIZ_MAX_CONCURRENT,
    SENTIMENT_FINVIZ_MAX_RETRIES,
    SENTIMENT_FINVIZ_RETRY_BASE_SEC,
    SENTIMENT_MAX_HEADLINES,
)
from services.finbert import analyze_text, analyze_batch

logger = logging.getLogger(__name__)

# Finviz 가 2026년 quote.ashx → quote 로 영구 리다이렉트(301).
# 새 URL 을 직접 사용해 redirect 비용을 피하고, 만약을 위해 client 에서도 follow_redirects=True 로 안전망.
_FINVIZ_BASE = FINVIZ_QUOTE_URL
_FINVIZ_HEADERS = BROWSER_HEADERS

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
            resp = await client.get(url, headers=_FINVIZ_HEADERS, timeout=httpx.Timeout(FINVIZ_TIMEOUT_SEC))

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


def _yf_news_headlines(ticker: str) -> list[str]:
    """yfinance 뉴스 제목 — Finviz 가 빈 결과(HF 데이터센터 IP throttle/차단)일 때 폴백.

    Finviz 는 데이터센터 IP를 강하게 throttle 해서 HF 에선 대부분 빈 헤드라인이 온다.
    yfinance 뉴스는 HF 에서 안정적으로 동작하므로 헤드라인 수급을 보강한다(감성 0 방지).
    """
    try:
        import yfinance as yf
        from services.yf_limiter import throttled
        news = throttled(lambda: yf.Ticker(ticker).get_news(count=SENTIMENT_MAX_HEADLINES)) or []
    except Exception as e:
        logger.debug("yfinance 뉴스 헤드라인 폴백 실패 (%s): %s", ticker, e)
        return []
    out: list[str] = []
    for it in news or []:
        title = (it.get("content") or {}).get("title") or it.get("title")
        if title and str(title).strip():
            out.append(str(title).strip())
    return out


async def _analyze_one(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    ticker: str,
) -> float:
    """
    헤드라인(Finviz 우선, 비면 yfinance 폴백)을 가져온 뒤 FinBERT/LLM 배치 추론으로
    종합 감성 점수를 산출한다.
    """
    async with semaphore:
        await asyncio.sleep(SENTIMENT_FINVIZ_DELAY_SEC)
        headlines = await _scrape_headlines(client, ticker)
        # Finviz 가 비면(HF IP throttle 등) yfinance 뉴스로 폴백 — HF 에서도 감성이 0 안 되게.
        if not headlines:
            headlines = await asyncio.to_thread(_yf_news_headlines, ticker)
        if not headlines:
            return 0.0
        # analyze_batch 는 동기 OpenAI/FinBERT 호출 — to_thread 로 이벤트 루프 비차단.
        # (기존엔 async 함수 안에서 동기 호출이 이벤트 루프를 막아, 스캔(503종목) 동안
        #  모든 API 요청이 수 초씩 멈췄다. semaphore 안에 둬 동시 OpenAI 호출도 바운드.)
        results = await asyncio.to_thread(analyze_batch, headlines)

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

    async with httpx.AsyncClient(limits=_HTTP_LIMITS, follow_redirects=True) as client:
        tasks = [_analyze_one(client, sem, t) for t in tickers]
        return list(await asyncio.gather(*tasks))
