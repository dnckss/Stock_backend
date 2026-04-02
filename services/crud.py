from __future__ import annotations

import math
import json
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from supabase import create_client, Client

from config import SUPABASE_URL, SUPABASE_KEY, STRATEGIST_LATEST_SCAN_WINDOW_MINUTES, NEWS_ARTICLE_CACHE_TTL_SEC

logger = logging.getLogger(__name__)

_supabase: Client | None = None


def _get_client() -> Client:
    global _supabase
    if _supabase is None:
        _supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _supabase


def sanitize_for_json(obj):
    """dict/list 내 float NaN·Inf를 None으로 치환해 JSON 직렬화 시 500 방지."""
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def init_db():
    """Supabase 연결 확인. 테이블은 Supabase 대시보드/SQL에서 미리 생성."""
    client = _get_client()
    logger.info("Supabase 연결 완료: %s", SUPABASE_URL)


def save_candidates(candidates: list):
    client = _get_client()
    rows = []
    for item in candidates:
        daily = item.get("daily")
        rows.append({
            "ticker": item["ticker"],
            "price": _safe_value(item.get("price")),
            "volume": item.get("volume"),
            "daily_json": json.dumps(daily, ensure_ascii=False) if daily else None,
            "price_return": _safe_value(item.get("return")),
            "sentiment": _safe_value(item.get("sentiment")),
            "divergence": _safe_value(item.get("divergence")),
            "signal": item.get("signal"),
            "signal_source": item.get("signal_source"),
            "eps_actual": _safe_value(item.get("eps_actual")),
            "eps_estimate": _safe_value(item.get("eps_estimate")),
            "earnings_surprise_pct": _safe_value(item.get("earnings_surprise_pct")),
            "report": item.get("report"),
        })
    client.table("analysis_results").insert(rows).execute()


def _safe_value(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if hasattr(v, 'isoformat'):
        return v.isoformat()
    return v


def _sanitize(records: list) -> list:
    return [{k: _safe_value(v) for k, v in row.items()} for row in records]


def get_latest_report(ticker: str) -> dict | None:
    client = _get_client()
    resp = (
        client.table("analysis_results")
        .select("*")
        .eq("ticker", ticker.upper())
        .not_.is_("report", "null")
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    rows = _sanitize(resp.data)
    return rows[0] if rows else None


def get_history(ticker: str, days: int = 30) -> list:
    client = _get_client()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    resp = (
        client.table("analysis_results")
        .select("price_return, sentiment, divergence, signal, signal_source, eps_actual, eps_estimate, earnings_surprise_pct, created_at")
        .eq("ticker", ticker.upper())
        .gte("created_at", cutoff)
        .order("created_at", desc=True)
        .execute()
    )
    return _sanitize(resp.data)


def get_all_records(limit: int = 100) -> list:
    client = _get_client()
    resp = (
        client.table("analysis_results")
        .select("*")
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )
    return _sanitize(resp.data)


def get_cached_news_article(url_hash: str) -> dict | None:
    """
    url_hash로 뉴스 본문 캐시를 조회한다.
    캐시 TTL(NEWS_ARTICLE_CACHE_TTL_SEC)이 지나면 None 처리한다.
    """
    if not url_hash:
        return None
    client = _get_client()
    resp = (
        client.table("news_articles")
        .select("*")
        .eq("url_hash", url_hash)
        .limit(1)
        .execute()
    )
    if not resp.data:
        return None

    row = _sanitize(resp.data)[0]

    # json decode (best-effort) — Supabase jsonb 컬럼은 이미 dict/list로 반환될 수 있음
    media_val = row.get("media_json")
    if isinstance(media_val, str):
        try:
            row["media"] = json.loads(media_val)
        except Exception:
            row["media"] = []
    else:
        row["media"] = media_val if media_val is not None else []

    domains_val = row.get("domains_json")
    if isinstance(domains_val, str):
        try:
            row["domains"] = json.loads(domains_val)
        except Exception:
            row["domains"] = {}
    else:
        row["domains"] = domains_val if domains_val is not None else {}

    analysis_val = row.get("analysis_json")
    if isinstance(analysis_val, str):
        try:
            row["analysis"] = json.loads(analysis_val)
        except Exception:
            row["analysis"] = None
    else:
        row["analysis"] = analysis_val

    fetched_at = row.get("fetched_at")
    try:
        fetched_dt = pd.to_datetime(fetched_at, errors="coerce")
    except Exception:
        fetched_dt = pd.NaT

    if fetched_dt is pd.NaT:
        return row

    age_sec = (datetime.now(timezone.utc) - fetched_dt.to_pydatetime().replace(tzinfo=timezone.utc)).total_seconds()
    if age_sec > NEWS_ARTICLE_CACHE_TTL_SEC:
        return None
    return row


def upsert_news_article(item: dict) -> None:
    """
    news_articles에 url_hash 기준 upsert.
    """
    client = _get_client()
    now = datetime.now(timezone.utc).isoformat()
    row = {
        "url_hash": item.get("url_hash"),
        "url": item.get("url"),
        "title": item.get("title"),
        "publisher": item.get("publisher"),
        "author": item.get("author"),
        "ticker": item.get("ticker"),
        "timestamp": item.get("timestamp"),
        "article_text": item.get("article_text"),
        "article_markdown": item.get("article_markdown"),
        "media_json": json.dumps(item.get("media") or [], ensure_ascii=False),
        "domains_json": json.dumps(item.get("domains") or {}, ensure_ascii=False),
        "extraction_status": item.get("extraction_status"),
        "error_reason": item.get("error_reason"),
        "http_status": item.get("http_status"),
        "final_url": item.get("final_url"),
        "canonical_url": item.get("canonical_url"),
        "analysis_json": json.dumps(item.get("analysis"), ensure_ascii=False) if item.get("analysis") is not None else None,
        "analysis_at": item.get("analysis_at"),
        "fetched_at": now,
    }
    client.table("news_articles").upsert(row, on_conflict="url_hash").execute()


def get_latest_scan_records(
    window_minutes: int = STRATEGIST_LATEST_SCAN_WINDOW_MINUTES,
) -> list[dict]:
    """
    analysis_results에서 가장 최근에 기록된 스캔 시점(max(created_at))을 기준으로
    window_minutes 범위 내 기록을 가져온 뒤,
    티커별로 created_at이 가장 최신 1건만 남긴다.
    """
    client = _get_client()

    # 1) 가장 최근 created_at 조회
    max_resp = (
        client.table("analysis_results")
        .select("created_at")
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    if not max_resp.data:
        return []

    max_ts_str = max_resp.data[0]["created_at"]
    max_ts = pd.to_datetime(max_ts_str, errors="coerce")
    if max_ts is pd.NaT:
        return []

    cutoff_dt = max_ts - timedelta(minutes=window_minutes)
    cutoff = cutoff_dt.isoformat()

    # 2) cutoff 이후 레코드 조회
    resp = (
        client.table("analysis_results")
        .select("*")
        .gte("created_at", cutoff)
        .order("created_at", desc=True)
        .execute()
    )

    if not resp.data:
        return []

    df = pd.DataFrame(resp.data)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df = df.sort_values("created_at", ascending=False).drop_duplicates(
        subset=["ticker"], keep="first"
    )
    records = _sanitize(df.to_dict(orient="records"))

    # daily_json → daily 복원, DB 컬럼명 → 프론트 필드명 매핑
    for row in records:
        daily_raw = row.pop("daily_json", None)
        if isinstance(daily_raw, str):
            try:
                row["daily"] = json.loads(daily_raw)
            except Exception:
                row["daily"] = []
        else:
            row["daily"] = daily_raw if daily_raw else []

        # price_return → return (프론트 호환)
        if "price_return" in row and "return" not in row:
            row["return"] = row.pop("price_return")

    return records


# ---------------------------------------------------------------------------
# Economic Calendar
# ---------------------------------------------------------------------------

def upsert_economic_events(events: list[dict]) -> None:
    """경제 일정 이벤트를 DB에 upsert한다. 배치 내 중복 키는 마지막 값만 유지."""
    if not events:
        return
    client = _get_client()
    # 동일 (event_date, event_time, event, currency) 중복 제거 — 마지막 값 우선
    seen: dict[tuple, dict] = {}
    for e in events:
        key = (e.get("event_date"), e.get("time_label") or None, e.get("event"), e.get("currency"))
        seen[key] = {
            "event_date": e.get("event_date"),
            "event_time": e.get("time_label") or None,
            "event_at": e.get("event_at"),
            "country_code": e.get("country_code"),
            "country_name": e.get("country_name"),
            "currency": e.get("currency"),
            "importance": e.get("importance", 0),
            "event": e.get("event"),
            "actual": e.get("actual"),
            "forecast": e.get("forecast"),
            "previous": e.get("previous"),
        }
    rows = list(seen.values())
    client.table("economic_events").upsert(
        rows, on_conflict="event_date,event_time,event,currency"
    ).execute()


def get_economic_events(date_from: str | None = None, limit: int = 500) -> list[dict]:
    """경제 일정을 시간순으로 조회한다. date_from이 None이면 전체 조회."""
    client = _get_client()
    query = client.table("economic_events").select("*").order("event_at", desc=False).limit(limit)
    if date_from:
        query = query.gte("event_date", date_from)
    resp = query.execute()
    return _sanitize(resp.data)


# ---------------------------------------------------------------------------
# News Items (뉴스 피드 항목)
# ---------------------------------------------------------------------------

def upsert_news_items(items: list[dict]) -> None:
    """뉴스 피드 항목을 DB에 upsert한다."""
    if not items:
        return
    client = _get_client()
    seen: dict[str, dict] = {}
    for item in items:
        url_hash = item.get("url_hash")
        if not url_hash:
            continue
        seen[url_hash] = {
            "url_hash": url_hash,
            "url": item.get("url"),
            "title": item.get("title"),
            "publisher": item.get("publisher"),
            "ticker": item.get("ticker"),
            "timestamp": item.get("timestamp"),
            "sentiment_score": _safe_value(item.get("score")),
            "sentiment_label": item.get("sentiment_label"),
            "sentiment_polarity": item.get("sentiment_polarity"),
            "sentiment_ko": item.get("sentiment_ko"),
            "confidence": _safe_value(item.get("confidence")),
            "has_article": item.get("has_article", False),
        }
    rows = list(seen.values())
    client.table("news_items").upsert(rows, on_conflict="url_hash").execute()


def get_news_items(limit: int = 50, ticker: str | None = None) -> list[dict]:
    """뉴스 피드 항목을 최신순으로 조회한다."""
    client = _get_client()
    query = client.table("news_items").select("*").order("timestamp", desc=True).limit(limit)
    if ticker:
        query = query.eq("ticker", ticker.upper())
    resp = query.execute()
    return _sanitize(resp.data)


def mark_news_item_has_article(url_hash: str) -> None:
    """뉴스 아이템의 has_article을 True로 업데이트한다."""
    client = _get_client()
    client.table("news_items").update({"has_article": True}).eq("url_hash", url_hash).execute()


# ---------------------------------------------------------------------------
# Strategy History (전략 추천 이력)
# ---------------------------------------------------------------------------

def save_strategy_history(recommendations: list[dict], market_regime: str | None = None) -> None:
    """AI 전략 추천 이력을 DB에 저장한다."""
    if not recommendations:
        return
    client = _get_client()
    rows = []
    for rec in recommendations:
        entry_zone = rec.get("entry_zone") or {}
        targets = rec.get("targets") or []
        rows.append({
            "ticker": (rec.get("ticker") or "").upper(),
            "direction": rec.get("direction"),
            "confidence": rec.get("confidence"),
            "strategy_type": rec.get("strategy_type"),
            "entry_low": _safe_value(entry_zone.get("low")),
            "entry_high": _safe_value(entry_zone.get("high")),
            "stop_loss": _safe_value(rec.get("stop_loss")),
            "stop_loss_pct": _safe_value(rec.get("stop_loss_pct")),
            "target1_price": _safe_value(targets[0].get("price")) if len(targets) > 0 else None,
            "target2_price": _safe_value(targets[1].get("price")) if len(targets) > 1 else None,
            "risk_reward_ratio": _safe_value(rec.get("risk_reward_ratio")),
            "rationale": rec.get("rationale"),
            "market_regime": market_regime,
        })
    client.table("strategy_history").insert(rows).execute()


def get_strategy_history(limit: int = 20, ticker: str | None = None) -> list[dict]:
    """전략 추천 이력을 최신순으로 조회한다."""
    client = _get_client()
    query = client.table("strategy_history").select("*").order("created_at", desc=True).limit(limit)
    if ticker:
        query = query.eq("ticker", ticker.upper())
    resp = query.execute()
    return _sanitize(resp.data)
