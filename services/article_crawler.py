from __future__ import annotations

import asyncio
import logging
import re
from typing import Any
from urllib.parse import urljoin, urlparse

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

_STRIP_TOKENS = ("HTML_TAG_START", "HTML_TAG_END")


def _strip_markers(text: str) -> str:
    if not text:
        return ""
    for t in _STRIP_TOKENS:
        text = text.replace(t, "")
    # 토큰 제거 후 공백 정리
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _clean_text(text: str) -> str:
    text = _strip_markers(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _domain(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def _safe_url(base_url: str, maybe_url: str) -> str:
    u = (maybe_url or "").strip()
    if not u:
        return ""
    try:
        return urljoin(base_url, u)
    except Exception:
        return u


def _extract_meta(soup: BeautifulSoup) -> dict[str, Any]:
    # OpenGraph / basic meta
    def _meta(name: str, attr: str = "property") -> str:
        tag = soup.find("meta", attrs={attr: name})
        if tag and tag.get("content"):
            return str(tag.get("content")).strip()
        return ""

    og_title = _meta("og:title")
    og_site = _meta("og:site_name")
    og_author = _meta("article:author")
    og_published = _meta("article:published_time")
    canonical = ""
    link_can = soup.find("link", rel="canonical")
    if link_can and link_can.get("href"):
        canonical = str(link_can.get("href")).strip()

    title = og_title or (soup.title.string.strip() if soup.title and soup.title.string else "")
    publisher = og_site
    author = og_author

    return {
        "title": title or None,
        "publisher": publisher or None,
        "author": author or None,
        "published_time": og_published or None,
        "canonical_url": canonical or None,
    }


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
    return _strip_markers(text)


def _extract_media(soup: BeautifulSoup, base_url: str) -> list[dict[str, Any]]:
    media: list[dict[str, Any]] = []

    # images
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or ""
        if not src:
            # srcset의 첫 URL을 사용
            srcset = img.get("srcset") or ""
            if srcset:
                src = srcset.split(",")[0].strip().split(" ")[0]
        src = _safe_url(base_url, src)
        if not src:
            continue

        # 로고/아이콘/트래킹 픽셀 필터링
        alt_raw = (img.get("alt") or "").strip()
        class_raw = " ".join(img.get("class") or []).strip()
        id_raw = (img.get("id") or "").strip()
        src_l = src.lower()
        meta_l = f"{alt_raw} {class_raw} {id_raw}".lower()

        def _as_int(x: Any) -> int | None:
            try:
                return int(str(x).strip())
            except Exception:
                return None

        w = _as_int(img.get("width"))
        h = _as_int(img.get("height"))

        # 1) 명시적으로 로고/아이콘/스프라이트/파비콘류
        if any(k in src_l for k in ["logo", "favicon", "sprite", "icon", "brand", "masthead"]):
            continue
        if any(k in meta_l for k in ["logo", "favicon", "sprite", "icon", "brand", "masthead"]):
            continue

        # 2) 트래킹 픽셀/비콘
        if any(k in src_l for k in ["pixel", "tracking", "beacon"]):
            continue

        # 3) 너무 작은 이미지(대개 로고/아이콘). width/height가 있으면 기준 적용
        if (w is not None and w <= 96) or (h is not None and h <= 96):
            continue

        alt = alt_raw or None
        media.append(
            {
                "type": "image",
                "url": src,
                "caption": alt,
                "thumbnail_url": None,
                "provider": None,
                "start_time": None,
            }
        )

    # iframes (embeds)
    for iframe in soup.find_all("iframe"):
        src = iframe.get("src") or ""
        src = _safe_url(base_url, src)
        if not src:
            continue
        d = _domain(src)
        provider = None
        if "youtube.com" in d or "youtu.be" in d:
            provider = "youtube"
        elif "vimeo.com" in d:
            provider = "vimeo"
        elif "twitter.com" in d or "x.com" in d:
            provider = "twitter"

        media.append(
            {
                "type": "embed",
                "url": src,
                "caption": None,
                "thumbnail_url": None,
                "provider": provider,
                "start_time": None,
            }
        )

    # video tags
    for video in soup.find_all("video"):
        src = video.get("src") or ""
        if not src:
            source = video.find("source")
            if source and source.get("src"):
                src = source.get("src")
        src = _safe_url(base_url, src)
        if not src:
            continue
        media.append(
            {
                "type": "video",
                "url": src,
                "caption": None,
                "thumbnail_url": None,
                "provider": None,
                "start_time": None,
            }
        )

    # de-dup by url
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for m in media:
        u = m.get("url") or ""
        if not u or u in seen:
            continue
        seen.add(u)
        deduped.append(m)
    return deduped


def _tag_to_markdown(node: Any, base_url: str) -> str:
    if node is None:
        return ""
    if isinstance(node, str):
        return _clean_text(node)

    name = getattr(node, "name", None)
    if not name:
        return _clean_text(getattr(node, "string", "") or "")

    if name in {"h1", "h2", "h3"}:
        level = {"h1": "#", "h2": "##", "h3": "###"}[name]
        text = _clean_text(node.get_text(" ", strip=True))
        return f"{level} {text}" if text else ""

    if name == "p":
        parts: list[str] = []
        for child in node.children:
            parts.append(_tag_to_markdown(child, base_url))
        text = " ".join([p for p in parts if p]).strip()
        return text

    if name in {"ul", "ol"}:
        items = []
        for i, li in enumerate(node.find_all("li", recursive=False), start=1):
            li_text = _clean_text(li.get_text(" ", strip=True))
            if not li_text:
                continue
            prefix = "-" if name == "ul" else f"{i}."
            items.append(f"{prefix} {li_text}")
        return "\n".join(items)

    if name in {"strong", "b"}:
        text = _clean_text(node.get_text(" ", strip=True))
        return f"**{text}**" if text else ""

    if name in {"em", "i"}:
        text = _clean_text(node.get_text(" ", strip=True))
        return f"*{text}*" if text else ""

    if name == "code":
        text = node.get_text("", strip=True)
        text = text.replace("`", "\\`")
        return f"`{text}`" if text else ""

    if name == "a":
        href = _safe_url(base_url, node.get("href") or "")
        text = _clean_text(node.get_text(" ", strip=True))
        if href and text:
            return f"[{text}]({href})"
        return text

    # default: text
    return _clean_text(node.get_text(" ", strip=True))


def _extract_article_markdown(html: str, base_url: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]):
        tag.decompose()

    root = soup.find("article") or soup.find("main") or soup.body
    if root is None:
        return ""

    blocks: list[str] = []
    for el in root.find_all(["h1", "h2", "h3", "p", "ul", "ol"], recursive=True):
        md = _tag_to_markdown(el, base_url)
        if md:
            blocks.append(md)

    # 빈 줄로 구분
    markdown = "\n\n".join(blocks).strip()
    if len(markdown) > NEWS_ARTICLE_MAX_CHARS:
        markdown = markdown[:NEWS_ARTICLE_MAX_CHARS].rstrip()
    return _strip_markers(markdown)


async def fetch_and_extract(url: str) -> dict[str, Any]:
    """
    URL을 가져와 HTML에서 본문 텍스트를 추출한다.
    실패 시 article_text는 빈 문자열로 반환한다.
    """
    if not url or not url.strip():
        return {
            "url": url,
            "final_url": url,
            "http_status": None,
            "extraction_status": "empty",
            "error_reason": "empty_url",
            "title": None,
            "publisher": None,
            "author": None,
            "timestamp": None,
            "canonical_url": None,
            "article_text": "",
            "article_markdown": "",
            "media": [],
            "domains": {"article": "", "media": []},
        }

    async with _semaphore:
        try:
            timeout = httpx.Timeout(NEWS_CRAWL_TIMEOUT_SEC)
            async with httpx.AsyncClient(follow_redirects=True, timeout=timeout, headers=_HEADERS) as client:
                resp = await client.get(url)
                if resp.status_code != 200:
                    logger.warning("뉴스 크롤링 실패 HTTP %s: %s", resp.status_code, url)
                    return {
                        "url": url,
                        "final_url": str(resp.url),
                        "http_status": resp.status_code,
                        "extraction_status": "blocked" if resp.status_code in (401, 403, 429) else "error",
                        "error_reason": f"http_{resp.status_code}",
                        "title": None,
                        "publisher": None,
                        "author": None,
                        "timestamp": None,
                        "canonical_url": None,
                        "article_text": "",
                        "article_markdown": "",
                        "media": [],
                        "domains": {"article": _domain(str(resp.url)), "media": []},
                    }

                html = resp.text or ""
                soup = BeautifulSoup(html, "html.parser")
                meta = _extract_meta(soup)
                article_text = _extract_article_text(html)
                article_markdown = _extract_article_markdown(html, str(resp.url))
                media = _extract_media(soup, str(resp.url))
                media_domains = sorted({d for d in (_domain(m.get("url") or "") for m in media) if d})

                status = "ok" if (article_text or article_markdown) else "empty"
                return {
                    "url": url,
                    "final_url": str(resp.url),
                    "http_status": resp.status_code,
                    "extraction_status": status,
                    "error_reason": None,
                    "title": meta.get("title"),
                    "publisher": meta.get("publisher"),
                    "author": meta.get("author"),
                    "timestamp": None,
                    "canonical_url": meta.get("canonical_url"),
                    "article_text": article_text,
                    "article_markdown": article_markdown,
                    "media": media,
                    "domains": {"article": _domain(str(resp.url)), "media": media_domains},
                }
        except Exception as e:
            logger.warning("뉴스 크롤링 예외: %s (%s)", e, url)
            return {
                "url": url,
                "final_url": url,
                "http_status": None,
                "extraction_status": "timeout" if "timeout" in str(e).lower() else "error",
                "error_reason": str(e),
                "title": None,
                "publisher": None,
                "author": None,
                "timestamp": None,
                "canonical_url": None,
                "article_text": "",
                "article_markdown": "",
                "media": [],
                "domains": {"article": _domain(url), "media": []},
            }

