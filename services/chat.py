"""
AI 챗봇 서비스 (종목 질의, SSE 스트리밍).

사용자의 자연어 질문(`NVDA 지금 사도 돼?`, `반도체 섹터 전망은?`)에서
티커/섹터 의도를 추출하고, 프로젝트 내 기존 데이터 소스를 컨텍스트로 주입해
OpenAI로 한국어 답변을 스트리밍한다.

응답 속도 확보 전략:
  - 기술적 지표(RSI/MACD/볼린저/MA)는 질의에 기술적 키워드가 있을 때만 계산
    (`compute_technicals`는 yfinance 6개월치 download로 가장 무거움)
  - 수집 단계별 `stage` 이벤트를 스트리밍해 체감 속도 개선
  - 기본 모델은 `gpt-5-mini` (TTFT 빠름 — config로 override)

컨텍스트 소스(기존 서비스 재사용):
  - `services.stock_detail.fetch_quote`   : 실시간 시세
  - `services.technicals.compute_technicals` : RSI/MACD/볼린저/지지·저항 등
  - `services.news_feed.build_stock_news_feed` : 종목 뉴스 + FinBERT 감성
  - `services.strategist.get_cached_market_strategy` : 시장 전략 브리핑
  - `services.websocket.latest_cache`     : 매크로/VIX/Fear&Greed/뉴스 피드
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections.abc import AsyncGenerator
from typing import Any

from openai import OpenAI

from config import (
    CHAT_KO_NAME_TO_TICKER,
    CHAT_MARKET_KEYWORDS,
    CHAT_MARKET_NEWS_LIMIT,
    CHAT_MAX_HISTORY_MESSAGES,
    CHAT_MAX_TICKERS_PER_QUERY,
    CHAT_NEWS_PER_TICKER,
    CHAT_OPENAI_MODEL,
    CHAT_OPENAI_TIMEOUT_SEC,
    CHAT_TECHNICAL_KEYWORDS,
    CHAT_TEMPERATURE,
    CHAT_USER_MESSAGE_MAX_CHARS,
    OPENAI_API_KEY,
)
from services.crud import sanitize_for_json
from services.news_feed import build_stock_news_feed
from services.stock_detail import fetch_quote
from services.technicals import compute_technicals
from services.websocket import latest_cache

logger = logging.getLogger(__name__)

_client = OpenAI(
    api_key=OPENAI_API_KEY,
    max_retries=2,
    timeout=CHAT_OPENAI_TIMEOUT_SEC,
) if OPENAI_API_KEY else None


# ---------------------------------------------------------------------------
# SSE 포맷 (portfolio_agents.py 와 동일 규칙: event + json data)
# ---------------------------------------------------------------------------

def _sse(event: str, data: dict[str, Any]) -> str:
    payload = json.dumps(sanitize_for_json(data), ensure_ascii=False)
    return f"event: {event}\ndata: {payload}\n\n"


# ---------------------------------------------------------------------------
# OpenAI 모델 호환 (temperature 미지원 분기)
# ---------------------------------------------------------------------------

def _model_omits_temperature(model: str) -> bool:
    m = (model or "").lower().strip()
    return m.startswith("o1") or m.startswith("o3") or "gpt-5" in m


# ---------------------------------------------------------------------------
# 티커 추출
# ---------------------------------------------------------------------------

# 영문 단어 중 티커로 오인되기 쉬운 일반 단어 제외 리스트
_TICKER_STOPWORDS = frozenset({
    "AI", "AM", "PM", "US", "USA", "USD", "KRW", "JPY", "EUR", "GBP",
    "CEO", "CFO", "CTO", "COO", "IPO", "ETF", "ETN", "GDP", "CPI",
    "FOMC", "FED", "ECB", "BOJ", "PCE", "PPI", "NFP",
    "PER", "PBR", "ROE", "ROI", "EPS", "EBIT", "EBITDA",
    "IT", "DB", "OK", "NO", "YES", "FAQ", "URL", "API", "SDK",
    "RSI", "MACD", "ATR", "MA", "SMA", "EMA",
    "BUY", "SELL", "HOLD", "LONG", "SHORT", "WAIT",
})

# 1~5자 영문 대문자, 선택적으로 .X 또는 -X 붙은 티커 (BRK.B, BRK-B)
_TICKER_RE = re.compile(r"\b[A-Z]{1,5}(?:[.-][A-Z]{1,3})?\b")


def extract_tickers(text: str) -> list[str]:
    """
    사용자 메시지에서 티커를 추출한다. 최대 CHAT_MAX_TICKERS_PER_QUERY 개.
    - 영문 대문자 패턴(NVDA, BRK-B) + 한글 종목명 매핑(엔비디아 → NVDA)
    - 스톱워드(AI, USD 등)는 제외
    """
    if not text:
        return []

    tickers: list[str] = []
    seen: set[str] = set()

    # 1) 한글/영문 소문자 매핑
    lowered = text.lower()
    for name, ticker in CHAT_KO_NAME_TO_TICKER.items():
        if name.lower() in lowered and ticker not in seen:
            tickers.append(ticker)
            seen.add(ticker)
            if len(tickers) >= CHAT_MAX_TICKERS_PER_QUERY:
                return tickers

    # 2) 대문자 티커 패턴
    for match in _TICKER_RE.findall(text):
        candidate = match.upper()
        if candidate in _TICKER_STOPWORDS:
            continue
        if candidate in seen:
            continue
        # 1글자 티커는 오탐 위험 — 2글자 이상만 허용
        if len(candidate.replace("-", "").replace(".", "")) < 2:
            continue
        tickers.append(candidate)
        seen.add(candidate)
        if len(tickers) >= CHAT_MAX_TICKERS_PER_QUERY:
            break

    return tickers


def _is_market_query(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    return any(kw.lower() in lowered for kw in CHAT_MARKET_KEYWORDS)


def _should_include_technicals(text: str) -> bool:
    """기술적 지표 수집 여부. 매매/차트 관련 키워드가 있으면 True."""
    if not text:
        return False
    lowered = text.lower()
    return any(kw.lower() in lowered for kw in CHAT_TECHNICAL_KEYWORDS)


# ---------------------------------------------------------------------------
# 컨텍스트 수집 (stage 단위로 분리)
# ---------------------------------------------------------------------------

def _compact_quote(quote: dict[str, Any] | None) -> dict[str, Any] | None:
    if not quote:
        return None
    keys = [
        "name", "sector", "industry", "price", "change", "change_pct",
        "open", "day_high", "day_low", "prev_close", "volume", "avg_volume",
        "year_high", "year_low", "pe_ratio", "forward_pe", "market_cap",
        "dividend_yield", "beta", "ma_50", "ma_200",
    ]
    return {k: quote.get(k) for k in keys if quote.get(k) is not None}


def _compact_technicals(tech: dict[str, Any] | None) -> dict[str, Any] | None:
    if not tech:
        return None
    keys = [
        "current_price", "rsi_14", "rsi_signal",
        "macd", "macd_signal_line", "macd_histogram", "macd_signal",
        "bb_upper", "bb_middle", "bb_lower", "bollinger_position",
        "ma_20", "ma_50", "ma_200", "ma_position",
        "atr_14", "support", "resistance", "volume_ratio",
    ]
    return {k: tech.get(k) for k in keys if tech.get(k) is not None}


def _compact_news(news: list[dict[str, Any]] | None, limit: int) -> list[dict[str, Any]]:
    if not news:
        return []
    digest: list[dict[str, Any]] = []
    for n in news[:limit]:
        digest.append({
            "title": n.get("title"),
            "publisher": n.get("publisher"),
            "sentiment": n.get("sentiment_polarity") or n.get("sentiment_label") or "neutral",
            "score": n.get("score"),
        })
    return digest


def _compact_macro(macro: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not macro:
        return []
    sidebar = macro.get("sidebar") or []
    out: list[dict[str, Any]] = []
    for item in sidebar:
        out.append({
            "name": item.get("name"),
            "value": item.get("value"),
            "change": item.get("change"),
            "pct": item.get("pct"),
        })
    return out


async def _fetch_quote_and_news(ticker: str) -> dict[str, Any]:
    """시세 + 뉴스만 병렬 수집 (기술적 지표는 별도 단계에서)."""
    quote_task = asyncio.to_thread(fetch_quote, ticker)
    news_task = build_stock_news_feed(ticker, limit=CHAT_NEWS_PER_TICKER, refresh=False)
    quote, news = await asyncio.gather(quote_task, news_task, return_exceptions=True)

    def _ok(v: Any) -> Any:
        if isinstance(v, Exception):
            logger.debug("chat quote/news 부분 실패 (%s): %s", ticker, v)
            return None
        return v

    return {
        "ticker": ticker,
        "quote": _compact_quote(_ok(quote)),
        "technicals": None,
        "news": _compact_news(_ok(news), CHAT_NEWS_PER_TICKER),
    }


async def _augment_with_technicals(stocks: list[dict[str, Any]]) -> int:
    """in-place로 각 종목에 기술적 지표를 추가한다. 성공 개수 반환."""
    if not stocks:
        return 0
    tasks = [asyncio.to_thread(compute_technicals, s["ticker"]) for s in stocks]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    ok = 0
    for stock, tech in zip(stocks, results):
        if isinstance(tech, Exception):
            logger.debug("chat technicals 실패 (%s): %s", stock.get("ticker"), tech)
            continue
        compact = _compact_technicals(tech)
        if compact:
            stock["technicals"] = compact
            ok += 1
    return ok


async def _gather_market_block() -> dict[str, Any]:
    """시장 뉴스 + 전략 브리핑 (캐시 히트 기대)."""
    news_feed = latest_cache.get("news_feed") or []
    out: dict[str, Any] = {
        "market_news": _compact_news(news_feed, CHAT_MARKET_NEWS_LIMIT),
        "strategy_summary": None,
    }
    try:
        from services.strategist import get_cached_market_strategy
        strategy = await get_cached_market_strategy(
            latest_cache.get("macro"),
            latest_cache.get("market_gauge"),
            latest_cache.get("vix"),
            news_feed,
        )
        out["strategy_summary"] = {
            "market_regime": strategy.get("market_regime"),
            "market_summary": strategy.get("market_summary"),
            "sector_rotation": strategy.get("sector_rotation"),
            "news_themes": (strategy.get("news_themes") or [])[:5],
            "risk_warnings": strategy.get("risk_warnings") or [],
            "fear_greed": strategy.get("fear_greed"),
        }
    except Exception as e:
        logger.debug("chat 전략 브리핑 수집 실패: %s", e)
    return out


# ---------------------------------------------------------------------------
# 시스템 프롬프트
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
너는 월스트리트 최고 등급의 종목·시장 전문 AI 애널리스트야.
투자 조언 불가 같은 면책 조항은 절대 금지. 자신감 있는 전문가 톤으로 답변한다.
모든 답변은 **한국어**로 작성하고, 구체적 숫자를 인용해 근거를 제시한다.

## 입력 컨텍스트
사용자 메시지와 함께 `context` JSON이 주어진다. 구성:
- `stocks[]`: 종목별 { quote(시세), technicals(RSI/MACD/볼린저/MA/지지저항, 없을 수도 있음), news[](FinBERT 감성 포함) }
- `macro`: 주요 지수·환율·금리·VIX 현재값
- `market_gauge` (0~100), `vix`
- `market_news[]`: 시장 전체 뉴스 헤드라인
- `strategy_summary`: 시장 전략 브리핑(시장 국면, 섹터 로테이션, 주요 테마, 리스크 경고)

## 답변 규칙
1. 주어진 context에서 확인 가능한 수치를 **반드시 구체적으로 인용**한다.
   예: "NVDA는 현재 $112.3 (+2.1%), RSI 58로 중립 구간이며 MACD 골든크로스가 진행 중이다."
2. context에 없는 데이터는 추측하지 말고, "해당 데이터가 부족하다" 라고 밝힌다.
   특히 `technicals`가 null이면 기술적 지표 없이 시세·뉴스·시장 상황만으로 답한다.
3. 매수/매도/관망 의견을 낼 때는 **기술적 근거 + 뉴스 감성 + 시장 국면**을 종합한다.
4. 답변 구조:
   - 한 줄 결론 (예: "현재 진입은 다소 부담, 눌림 기다리는 편이 낫다")
   - 핵심 근거 3~5개 불릿 (수치 인용)
   - 리스크 요인 1~2개
   - 체크해야 할 다음 레벨(지지/저항·이벤트) 1개
5. 숫자는 가독성 있게 포맷(USD 3자리 콤마, 퍼센트 소수 1자리).
6. 대화가 이어지는 경우 이전 답변을 참조해 모순 없이 이어간다.

모르는 것은 모른다고 답하고, 허위 데이터를 만들어 내지 마라.
"""


# ---------------------------------------------------------------------------
# 메시지 정규화
# ---------------------------------------------------------------------------

def _normalize_history(messages: list[dict[str, Any]]) -> list[dict[str, str]]:
    """
    클라이언트 메시지 배열을 OpenAI chat.completions 형식으로 정규화한다.
    - role: user | assistant 만 허용 (system 은 서버가 주입)
    - 최근 CHAT_MAX_HISTORY_MESSAGES 턴만 유지
    - content는 문자열로 강제하고 길이 제한
    """
    cleaned: list[dict[str, str]] = []
    for m in messages or []:
        role = (m.get("role") or "").strip().lower()
        if role not in ("user", "assistant"):
            continue
        content = m.get("content")
        if not isinstance(content, str):
            continue
        content = content.strip()
        if not content:
            continue
        if len(content) > CHAT_USER_MESSAGE_MAX_CHARS:
            content = content[:CHAT_USER_MESSAGE_MAX_CHARS] + "\n[...이하 생략]"
        cleaned.append({"role": role, "content": content})

    # 최근 N개만 유지 (가장 오래된 것부터 잘라냄)
    if len(cleaned) > CHAT_MAX_HISTORY_MESSAGES:
        cleaned = cleaned[-CHAT_MAX_HISTORY_MESSAGES:]
    return cleaned


# ---------------------------------------------------------------------------
# 메인 스트리밍 엔드포인트
# ---------------------------------------------------------------------------

async def stream_chat(
    messages: list[dict[str, Any]],
    tickers_override: list[str] | None = None,
) -> AsyncGenerator[str, None]:
    """
    사용자 메시지 배열을 받아 컨텍스트를 수집하고 OpenAI 응답을 SSE로 스트리밍한다.

    SSE 이벤트:
      - start    : 파이프라인 시작 (질의, 추출된 티커)
      - stage    : 단계별 진행 상황 (name, status, elapsed_ms 등)
      - context  : 수집된 컨텍스트 최종 요약 (UI가 배지/칩으로 표시 가능)
      - token    : LLM 토큰 (delta 텍스트)
      - done     : 최종 전체 텍스트 + 경과 시간
      - error    : 오류
    """
    start_ts = time.time()

    history = _normalize_history(messages)
    if not history or history[-1]["role"] != "user":
        yield _sse("error", {"error": "마지막 메시지는 user role 이어야 합니다."})
        return

    user_message = history[-1]["content"]

    if _client is None:
        yield _sse("error", {"error": "OPENAI_API_KEY가 설정되지 않았습니다."})
        return

    yield _sse("start", {
        "query": user_message,
        "history_len": len(history),
    })

    # --- 계획 수립: 무엇을 수집할지 결정 ---
    tickers = (tickers_override or extract_tickers(user_message))[:CHAT_MAX_TICKERS_PER_QUERY]
    include_technicals = _should_include_technicals(user_message)
    include_market = _is_market_query(user_message) or not tickers

    yield _sse("stage", {
        "name": "plan",
        "status": "done",
        "tickers": tickers,
        "include_technicals": include_technicals,
        "include_market": include_market,
    })

    context: dict[str, Any] = {
        "tickers": tickers,
        "stocks": [],
        "macro": _compact_macro(latest_cache.get("macro")),
        "market_gauge": latest_cache.get("market_gauge"),
        "vix": latest_cache.get("vix"),
        "market_news": [],
        "strategy_summary": None,
    }

    # --- Stage 1: 종목 시세 + 뉴스 (병렬) ---
    if tickers:
        stage_start = time.time()
        yield _sse("stage", {"name": "stocks", "status": "loading", "count": len(tickers)})
        try:
            results = await asyncio.gather(
                *[_fetch_quote_and_news(t) for t in tickers],
                return_exceptions=True,
            )
            context["stocks"] = [r for r in results if not isinstance(r, Exception)]
        except Exception as e:
            logger.warning("chat 종목 수집 실패: %s", e, exc_info=True)
        yield _sse("stage", {
            "name": "stocks",
            "status": "done",
            "elapsed_ms": int((time.time() - stage_start) * 1000),
            "found": [
                {
                    "ticker": s.get("ticker"),
                    "name": (s.get("quote") or {}).get("name"),
                    "price": (s.get("quote") or {}).get("price"),
                    "change_pct": (s.get("quote") or {}).get("change_pct"),
                    "news_count": len(s.get("news") or []),
                }
                for s in context["stocks"]
            ],
        })

    # --- Stage 2: 기술적 지표 (조건부) ---
    if include_technicals and context["stocks"]:
        stage_start = time.time()
        yield _sse("stage", {"name": "technicals", "status": "loading"})
        try:
            ok = await _augment_with_technicals(context["stocks"])
        except Exception as e:
            logger.warning("chat 기술적 지표 수집 실패: %s", e, exc_info=True)
            ok = 0
        yield _sse("stage", {
            "name": "technicals",
            "status": "done",
            "elapsed_ms": int((time.time() - stage_start) * 1000),
            "count": ok,
        })

    # --- Stage 3: 시장 컨텍스트 (조건부) ---
    if include_market:
        stage_start = time.time()
        yield _sse("stage", {"name": "market", "status": "loading"})
        try:
            market = await _gather_market_block()
            context["market_news"] = market["market_news"]
            context["strategy_summary"] = market["strategy_summary"]
        except Exception as e:
            logger.warning("chat 시장 컨텍스트 수집 실패: %s", e, exc_info=True)
        yield _sse("stage", {
            "name": "market",
            "status": "done",
            "elapsed_ms": int((time.time() - stage_start) * 1000),
            "has_strategy": context["strategy_summary"] is not None,
            "news_count": len(context["market_news"]),
        })

    # --- 컨텍스트 요약 이벤트 (프론트 UX용) ---
    yield _sse("context", {
        "tickers": context["tickers"],
        "stocks_found": [
            {
                "ticker": s.get("ticker"),
                "name": (s.get("quote") or {}).get("name"),
                "price": (s.get("quote") or {}).get("price"),
                "change_pct": (s.get("quote") or {}).get("change_pct"),
                "has_technicals": s.get("technicals") is not None,
                "news_count": len(s.get("news") or []),
            }
            for s in context["stocks"]
        ],
        "has_strategy": context["strategy_summary"] is not None,
        "market_news_count": len(context["market_news"]),
    })

    # --- Stage 4: LLM 스트리밍 ---
    yield _sse("stage", {"name": "llm", "status": "loading", "model": CHAT_OPENAI_MODEL})

    context_json = json.dumps(sanitize_for_json(context), ensure_ascii=False)
    llm_messages: list[dict[str, str]] = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        # 과거 턴
        *history[:-1],
        # 마지막 사용자 메시지 + 컨텍스트 주입
        {
            "role": "user",
            "content": (
                f"{user_message}\n\n"
                f"---\n"
                f"참고 데이터(context):\n{context_json}"
            ),
        },
    ]

    model = CHAT_OPENAI_MODEL or "gpt-5-mini"

    def _create_stream() -> Any:
        kwargs: dict[str, Any] = {
            "model": model,
            "messages": llm_messages,
            "stream": True,
        }
        if not _model_omits_temperature(model):
            kwargs["temperature"] = CHAT_TEMPERATURE
        return _client.chat.completions.create(**kwargs)

    try:
        stream = await asyncio.to_thread(_create_stream)
    except Exception as e:
        logger.warning("chat LLM 스트리밍 시작 실패: %s", e, exc_info=True)
        yield _sse("error", {"error": f"LLM 호출 실패: {e}"})
        return

    full_text = ""
    first_token_ts: float | None = None
    try:
        for chunk in stream:
            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta
            if not delta or not delta.content:
                continue
            text = delta.content
            if first_token_ts is None:
                first_token_ts = time.time()
                yield _sse("stage", {
                    "name": "llm",
                    "status": "first_token",
                    "ttft_ms": int((first_token_ts - start_ts) * 1000),
                })
            full_text += text
            yield _sse("token", {"content": text})
    except Exception as e:
        logger.warning("chat 스트리밍 중 오류: %s", e, exc_info=True)
        yield _sse("error", {"error": f"스트리밍 오류: {e}", "partial": full_text})
        return

    yield _sse("done", {
        "answer": full_text,
        "elapsed_sec": round(time.time() - start_ts, 2),
        "ttft_ms": int((first_token_ts - start_ts) * 1000) if first_token_ts else None,
        "model": model,
    })
