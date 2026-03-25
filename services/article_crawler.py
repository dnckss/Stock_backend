from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

import httpx
from bs4 import BeautifulSoup

from config import NEWS_ARTICLE_MAX_CHARS, NEWS_CRAWL_MAX_CONCURRENT, NEWS_CRAWL_TIMEOUT_SEC

logger = logging.getLogger(__name__)

_semaphore = asyncio.Semaphore(max(1, NEWS_CRAWL_MAX_CONCURRENT))

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}


def _clean_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_article_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    # 제거: nav/script/style 등 노이즈
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]):
        tag.decompose()

    root = soup.find("article") or soup.find("main") or soup.body
    if root is None:
        return ""

    paragraphs = []
    for p in root.find_all("p"):
        txt = p.get_text(" ", strip=True)
        txt = _clean_text(txt)
        if len(txt) < 30:
            continue
        paragraphs.append(txt)

    text = "\n\n".join(paragraphs)
    text = text.strip()
    if not text:
        # fallback: root 전체 텍스트
        text = _clean_text(root.get_text(" ", strip=True))

    if len(text) > NEWS_ARTICLE_MAX_CHARS:
        text = text[:NEWS_ARTICLE_MAX_CHARS].rstrip()
    return text


async def fetch_and_extract(url: str) -> dict[str, Any]:
    """
    URL을 가져와 HTML에서 본문 텍스트를 추출한다.
    실패 시 article_text는 빈 문자열로 반환한다.
    """
    if not url or not url.strip():
        return {"url": url, "article_text": ""}

    async with _semaphore:
        try:
            timeout = httpx.Timeout(NEWS_CRAWL_TIMEOUT_SEC)
            async with httpx.AsyncClient(follow_redirects=True, timeout=timeout, headers=_HEADERS) as client:
                resp = await client.get(url)
                if resp.status_code != 200:
                    logger.warning("뉴스 크롤링 실패 HTTP %s: %s", resp.status_code, url)
                    return {"url": url, "article_text": ""}

                html = resp.text or ""
                article_text = _extract_article_text(html)
                return {"url": url, "article_text": article_text}
        except Exception as e:
            logger.warning("뉴스 크롤링 예외: %s (%s)", e, url)
            return {"url": url, "article_text": ""}

