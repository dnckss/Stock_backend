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


def _reset_client() -> None:
    """httpx 연결 오류 등 이후 다음 호출에서 새 client/커넥션을 만들도록 리셋."""
    global _supabase
    _supabase = None


# sanitize_for_json 핫 패스 최적화:
#   - 모든 API 응답·WS broadcast 마다 호출 → ms 단위 누적.
#   - 503행 페이로드(S&P 500) 기준 ~3ms → 변경 없는 케이스(NaN/Inf 無)는 0.x ms 로 단축.
#   - 핵심 아이디어: 자식이 모두 그대로면 컨테이너도 재생성하지 않고 원본 식별자 그대로 반환.
#     이로써 broadcast 마다 발생하던 dict/list 재할당이 사라진다.
#   - `type(obj) is X` 가 `isinstance` 보다 빠르고, 99% dict/list 케이스가 압도적.
#   - `math.isfinite` 1회 호출이 `isnan or isinf` 2회 호출보다 빠르다.
_isfinite = math.isfinite


def sanitize_for_json(obj):
    """dict/list 내 float NaN·Inf를 None으로 치환해 JSON 직렬화 시 500 방지.

    NaN/Inf 가 없는 경우(거의 모든 핫 패스)는 원본 객체를 그대로 반환해
    불필요한 dict/list 재할당을 피한다. 테스트(`test_sanitize.py`) 호환.
    """
    t = type(obj)

    if t is dict:
        new_d: dict | None = None
        for k, v in obj.items():
            sv = sanitize_for_json(v)
            if sv is not v:
                if new_d is None:
                    new_d = dict(obj)
                new_d[k] = sv
        return new_d if new_d is not None else obj

    if t is list:
        new_l: list | None = None
        for i, v in enumerate(obj):
            sv = sanitize_for_json(v)
            if sv is not v:
                if new_l is None:
                    new_l = list(obj)
                new_l[i] = sv
        return new_l if new_l is not None else obj

    if t is float and not _isfinite(obj):
        return None

    # bool/int/str/None/tuple/datetime 등 — 원본 그대로 통과.
    # dict/list 서브클래스(예: pandas Series 가 list 로 cast 되는 경우)는
    # 위 type() 체크에서 매칭이 안되므로 isinstance 폴백을 명시한다.
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
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


# Supabase statement timeout(57014) 회피: 페이지당 행 수를 줄여 각 쿼리가 더 빨리 끝나게 한다.
# 1000 → 500 → 250 으로 계속 줄여, 페이지가 깊어져도 각 쿼리 자체는 timeout 회피.
# (offset 방식 — _paginate. 백테스트 레코드 읽기는 _paginate_keyset 으로 전환됨.)
_BACKTEST_PAGE_SIZE = 250
_BACKTEST_MAX_PAGES = 200  # 최대 50,000건 유지 (페이지 사이즈 1/4 → 페이지 상한 4배)
_BACKTEST_PAGE_MAX_RETRIES = 3
_BACKTEST_PAGE_RETRY_BASE_SEC = 0.5
# keyset(커서) 페이지네이션 — 깊은 offset 재스캔이 없어 페이지를 크게 잡아도 안전(타임아웃 회피).
_BACKTEST_KEYSET_PAGE_SIZE = 1000
# 단일 cursor 값 tie 가 페이지를 넘을 때 페이지를 키우는 상한(데이터 누락 방지 안전망).
_BACKTEST_KEYSET_MAX_PAGE = 50000


def _paginate(query_builder_fn, *, select_cols: str) -> list[dict]:
    """
    PostgREST 기본 1000건 제한을 피해 .range()로 페이지네이션해 전체 반환.
    query_builder_fn(client) 는 .select()/.order() 까지 체이닝된 쿼리를 반환해야 한다.

    httpx.RemoteProtocolError/ReadError 등 일시적 네트워크 오류에는 지수 백오프로
    재시도하고, 최종 실패 시 지금까지 수집된 부분 결과를 반환한다(전체 실패 대신).
    """
    import time
    import httpx

    collected: list[dict] = []
    for page in range(_BACKTEST_MAX_PAGES):
        start = page * _BACKTEST_PAGE_SIZE
        end = start + _BACKTEST_PAGE_SIZE - 1

        rows: list[dict] | None = None
        last_exc: Exception | None = None
        for attempt in range(_BACKTEST_PAGE_MAX_RETRIES):
            try:
                client = _get_client()
                resp = query_builder_fn(client).range(start, end).execute()
                rows = resp.data or []
                break
            except (
                httpx.RemoteProtocolError,
                httpx.ReadError,
                httpx.WriteError,
                httpx.ConnectError,
                httpx.ReadTimeout,
                httpx.ConnectTimeout,
            ) as e:
                last_exc = e
                logger.warning(
                    "paginate page=%d attempt=%d 네트워크 오류(%s): %s",
                    page, attempt, type(e).__name__, e,
                )
                _reset_client()
                time.sleep(_BACKTEST_PAGE_RETRY_BASE_SEC * (2 ** attempt))
            except Exception as e:
                # PostgREST APIError 등 — 재시도해도 동일하면 바로 중단
                last_exc = e
                logger.warning("paginate page=%d attempt=%d API 오류: %s", page, attempt, e)
                time.sleep(_BACKTEST_PAGE_RETRY_BASE_SEC * (2 ** attempt))

        if rows is None:
            logger.error(
                "paginate page=%d 최종 실패 — 지금까지 수집 %d건으로 진행: %s",
                page, len(collected), last_exc,
            )
            break

        collected.extend(rows)
        if len(rows) < _BACKTEST_PAGE_SIZE:
            break
    return collected


def _paginate_keyset(
    build_fn,
    *,
    page_size: int = _BACKTEST_KEYSET_PAGE_SIZE,
    cursor_col: str = "created_at",
    dedup_key=lambda r: r.get("id"),
) -> list[dict]:
    """
    keyset(커서) 페이지네이션 — offset 의 '깊은 페이지 재스캔/정렬' 비용을 제거한다.

    build_fn(client, cursor) 는 ``.gte(cursor_col, cursor or cutoff)`` + 보조정렬까지
    체이닝된 쿼리를 반환해야 한다(여기서 .limit 만 덧붙임).

    dedup_key(row): 행의 '전역 고유 식별자'를 반환(경계 tie 재조회분 중복 제거용).
      - id 컬럼이 있으면 ``lambda r: r["id"]``.
      - id 가 없어도 (created_at, ticker) 처럼 행을 유일하게 식별하는 조합이면 그것을 사용
        (analysis_results 는 스캔 1사이클이 ticker 당 1행이라 (created_at, ticker) 가 유일).

    같은 cursor_col 값이 한 배치(예: 스캔 1사이클 ≈ S&P500 ~503행)로 묶여 동률(tie)이 많으므로,
    커서를 마지막 행의 cursor_col 로 '포함(gte)' 전진시키고 dedup_key 로 중복을 제거한다 —
    경계의 tie 배치를 다음 페이지가 다시 받아 dedup 하므로 누락이 없다.

    한 페이지가 전부 동일 cursor 값(tie ≥ effective page)이면 gte 로 전진할 수 없으므로,
    그 tie 를 한 번에 포섭할 때까지 페이지 크기를 키워 재조회한다(데이터 누락 방지).
    실제 tie 는 ~503 < 1000 이라 이 경로는 거의 타지 않지만, 안전망으로 둔다.

    httpx 일시 오류는 _paginate 와 동일하게 지수 백오프 재시도하고, 최종 실패 시 부분 결과 반환.
    """
    import time
    import httpx

    collected: list[dict] = []
    seen_keys: set = set()
    cursor: str | None = None
    effective_page = page_size

    for _ in range(_BACKTEST_MAX_PAGES):
        rows: list[dict] | None = None
        last_exc: Exception | None = None
        for attempt in range(_BACKTEST_PAGE_MAX_RETRIES):
            try:
                client = _get_client()
                resp = build_fn(client, cursor).limit(effective_page).execute()
                rows = resp.data or []
                break
            except (
                httpx.RemoteProtocolError,
                httpx.ReadError,
                httpx.WriteError,
                httpx.ConnectError,
                httpx.ReadTimeout,
                httpx.ConnectTimeout,
            ) as e:
                last_exc = e
                logger.warning(
                    "paginate_keyset 네트워크 오류(%s): %s", type(e).__name__, e,
                )
                _reset_client()
                time.sleep(_BACKTEST_PAGE_RETRY_BASE_SEC * (2 ** attempt))
            except Exception as e:
                last_exc = e
                logger.warning("paginate_keyset API 오류: %s", e)
                time.sleep(_BACKTEST_PAGE_RETRY_BASE_SEC * (2 ** attempt))

        if rows is None:
            logger.error(
                "paginate_keyset 최종 실패 — 지금까지 수집 %d건으로 진행: %s",
                len(collected), last_exc,
            )
            break
        if not rows:
            break

        fresh = [r for r in rows if dedup_key(r) not in seen_keys]
        for r in fresh:
            seen_keys.add(dedup_key(r))
        collected.extend(fresh)

        if len(rows) < effective_page:
            break  # 마지막 페이지

        first_cursor = rows[0].get(cursor_col)
        last_cursor = rows[-1].get(cursor_col)
        if first_cursor == last_cursor:
            # 페이지 전체가 단일 cursor 값(tie ≥ effective_page) → 커서 전진 불가.
            # 페이지를 키워 tie 전체를 한 번에 받는다(누락 방지). 상한 도달 시 중단.
            if effective_page >= _BACKTEST_KEYSET_MAX_PAGE:
                logger.warning(
                    "paginate_keyset: 단일 cursor tie 가 %d행 초과 — 중단",
                    _BACKTEST_KEYSET_MAX_PAGE,
                )
                break
            effective_page = min(effective_page * 2, _BACKTEST_KEYSET_MAX_PAGE)
            continue  # cursor 유지, 더 크게 재조회

        effective_page = page_size  # tie 해소 — 기본 페이지로 복귀
        cursor = last_cursor

    return collected


def get_analysis_records_for_backtest(
    days: int,
    directions: list[str] | None = None,
) -> list[dict]:
    """
    백테스팅용 — 과거 N일 이내 analysis_results 중 시그널/가격이 유효한 레코드만.
    오래된 순으로 정렬(누적 곡선 계산 용이). 페이지네이션으로 1000-row 제한 우회.

    directions: ["BUY"], ["BUY","SELL"] 등으로 DB 단계에서 필터링 — 50K 안전상한
    cap 에 더 일찍 부딪히지 않게 한다 (BUY 만이면 약 7K 건 정도라 cap 무관).
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    cols = (
        "ticker, price, volume, divergence, sentiment, price_return, "
        "signal, signal_source, created_at"
    )
    allowed = (
        sorted({(d or "").upper() for d in directions if d})
        if directions else None
    )

    def _build(client, cursor):
        q = (
            client.table("analysis_results")
            .select(cols)
            .gte("created_at", cursor or cutoff)
            .not_.is_("signal", "null")
            .not_.is_("price", "null")
        )
        if allowed:
            q = q.in_("signal", allowed)
        # id 컬럼 의존 없이 (created_at, ticker) 로 보조정렬·dedup — 스캔 1사이클은
        # ticker 당 1행이라 이 조합이 전역 유일.
        return q.order("created_at", desc=False).order("ticker", desc=False)

    return _sanitize(_paginate_keyset(
        _build, dedup_key=lambda r: (r.get("created_at"), r.get("ticker")),
    ))


def get_strategy_records_for_backtest(days: int) -> list[dict]:
    """
    백테스팅용 — 과거 N일 이내 strategy_history 중 direction/entry 유효 레코드만.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    cols = (
        "id, ticker, direction, confidence, strategy_type, "
        "entry_low, entry_high, stop_loss, stop_loss_pct, "
        "target1_price, target2_price, risk_reward_ratio, "
        "market_regime, created_at"
    )

    def _build(client, cursor):
        return (
            client.table("strategy_history")
            .select(cols)
            .gte("created_at", cursor or cutoff)
            .not_.is_("direction", "null")
            .order("created_at", desc=False)
            .order("id", desc=False)
        )

    return _sanitize(_paginate_keyset(_build))


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


def get_news_articles_analysis_by_hashes(hashes: list[str]) -> dict[str, Any]:
    """
    url_hash 목록에 대해 news_articles.analysis_json 을 batch 조회.
    LLM 분석이 있는 항목만 {url_hash: analysis_json} 매핑으로 반환한다.
    피드 응답에서 LLM polarity 우선 노출에 사용한다.
    """
    if not hashes:
        return {}
    unique = list({h for h in hashes if h})
    if not unique:
        return {}
    client = _get_client()
    try:
        resp = (
            client.table("news_articles")
            .select("url_hash, analysis_json")
            .in_("url_hash", unique)
            .execute()
        )
    except Exception as e:
        logger.warning("news_articles analysis 조회 실패: %s", e)
        return {}
    out: dict[str, Any] = {}
    for r in resp.data or []:
        h = r.get("url_hash")
        a = r.get("analysis_json")
        if h and a is not None:
            out[h] = a
    return out


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

_ECON_PRESERVE_FIELDS = ("actual", "forecast", "previous")


def upsert_economic_events(events: list[dict]) -> None:
    """
    경제 일정 이벤트를 DB에 upsert한다. 배치 내 중복 키는 마지막 값만 유지.

    actual/forecast/previous 보존: 신규 응답에서 이 필드들이 빈값(None/"")이면
    DB의 기존 값을 유지한다. myfxbook/ForexFactory 가 일시적으로 같은 이벤트의
    actual 을 비워서 내려보내도, 한 번 발표된 값이 사라지지 않게 한다.
    """
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
            "actual": e.get("actual") or None,
            "forecast": e.get("forecast") or None,
            "previous": e.get("previous") or None,
        }
    rows = list(seen.values())

    # 보존 필드 중 하나라도 비어 있는 행만 DB 기존값 조회
    needs_check = [r for r in rows if any(not r.get(f) for f in _ECON_PRESERVE_FIELDS)]
    preserved_count = 0
    if needs_check:
        dates = sorted({r["event_date"] for r in needs_check if r.get("event_date")})
        if dates:
            try:
                resp = (
                    client.table("economic_events")
                    .select("event_date,event_time,event,currency,actual,forecast,previous")
                    .gte("event_date", dates[0])
                    .lte("event_date", dates[-1])
                    .execute()
                )
                existing_map: dict[tuple, dict] = {
                    (
                        row.get("event_date"),
                        row.get("event_time") or None,
                        row.get("event"),
                        row.get("currency"),
                    ): row
                    for row in (resp.data or [])
                }
            except Exception as e:
                logger.warning("경제 일정 기존값 조회 실패 — 보존 스킵: %s", e)
                existing_map = {}

            for r in needs_check:
                key = (r["event_date"], r["event_time"] or None, r["event"], r["currency"])
                existing = existing_map.get(key)
                if not existing:
                    continue
                for f in _ECON_PRESERVE_FIELDS:
                    if not r.get(f) and existing.get(f):
                        r[f] = existing[f]
                        preserved_count += 1

    if preserved_count:
        logger.info("경제 일정 upsert: %d개 필드 기존값 보존", preserved_count)

    client.table("economic_events").upsert(
        rows, on_conflict="event_date,event_time,event,currency"
    ).execute()


def get_economic_events(date_from: str | None = None, limit: int = 0) -> list[dict]:
    """
    경제 일정을 시간순으로 조회한다. date_from이 None이면 전체, limit=0이면 무제한.
    PostgREST 기본 1000-row cap 을 페이지네이션으로 우회 — 5월 신규가 빠지던 문제 해결.
    """
    cols = "*"

    def _build(client):
        q = client.table("economic_events").select(cols).order("event_at", desc=False)
        if date_from:
            q = q.gte("event_date", date_from)
        return q

    rows = _paginate(_build, select_cols=cols)
    if limit > 0:
        rows = rows[:limit]
    return _sanitize(rows)


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


def get_news_items(
    limit: int = 50,
    ticker: str | None = None,
    offset: int = 0,
    since_ts: int | None = None,
) -> list[dict]:
    """뉴스 피드 항목을 최신순(timestamp DESC)으로 조회. offset 으로 페이지네이션.

    since_ts(epoch seconds) 지정 시 timestamp >= since_ts 인 최근 구간만 조회한다
    (영향도 TOP 랭킹 후보 수집용).
    """
    client = _get_client()
    safe_offset = max(0, offset)
    safe_limit = max(1, limit)
    query = client.table("news_items").select("*").order("timestamp", desc=True)
    if ticker:
        query = query.eq("ticker", ticker.upper())
    if since_ts is not None:
        query = query.gte("timestamp", int(since_ts))
    resp = query.range(safe_offset, safe_offset + safe_limit - 1).execute()
    return _sanitize(resp.data)


def count_news_items(ticker: str | None = None) -> int:
    """news_items 전체 행 수 (ticker 필터 가능). PostgREST count='exact' 사용."""
    client = _get_client()
    query = client.table("news_items").select("url_hash", count="exact").limit(1)
    if ticker:
        query = query.eq("ticker", ticker.upper())
    try:
        resp = query.execute()
    except Exception as e:
        logger.warning("news_items count 실패: %s", e)
        return 0
    return int(getattr(resp, "count", 0) or 0)


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


def get_strategy_history_tickers(limit: int = 5000) -> set[str]:
    """strategy_history 테이블의 distinct ticker 집합 (최신순 limit 행 기준).

    종목 검색 universe 구성용 — 백테스트 등에 등장한 AI 추천 종목까지 검색되도록 한다.
    """
    client = _get_client()
    resp = (
        client.table("strategy_history")
        .select("ticker")
        .order("created_at", desc=True)
        .limit(max(1, limit))
        .execute()
    )
    return {
        (row.get("ticker") or "").upper()
        for row in (resp.data or [])
        if row.get("ticker")
    }


def get_analysis_results_tickers(limit: int = 5000) -> set[str]:
    """analysis_results 테이블의 distinct ticker 집합 (최신순 limit 행 기준).

    종목 검색 universe 구성용 — 스캐너에 잡혔던 종목 전체를 포함시킨다.
    """
    client = _get_client()
    resp = (
        client.table("analysis_results")
        .select("ticker")
        .order("created_at", desc=True)
        .limit(max(1, limit))
        .execute()
    )
    return {
        (row.get("ticker") or "").upper()
        for row in (resp.data or [])
        if row.get("ticker")
    }


# ---------------------------------------------------------------------------
# S&P 500 Heatmap Snapshot
# ---------------------------------------------------------------------------
# Supabase 테이블 생성 SQL:
#   CREATE TABLE sp500_heatmap (
#       id INTEGER PRIMARY KEY DEFAULT 1,
#       data_json TEXT NOT NULL,
#       updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
#   );

def save_heatmap_snapshot(data: dict) -> None:
    """sp500_heatmap 테이블에 히트맵 스냅샷을 upsert한다."""
    client = _get_client()
    row = {
        "id": 1,
        "data_json": json.dumps(sanitize_for_json(data), ensure_ascii=False),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        client.table("sp500_heatmap").upsert(row, on_conflict="id").execute()
    except Exception as e:
        logger.warning("히트맵 DB 저장 실패: %s", e)


def save_backtest_cache(cache_key: str, payload: dict, updated_at_iso: str | None = None) -> None:
    """backtest_cache 테이블에 결과 1건을 cache_key 기준 upsert 한다(영속 캐시).

    payload 는 이미 sanitize 된 결과 dict. 실패해도 best-effort(메모리 캐시로 동작).
    """
    if not cache_key:
        return
    client = _get_client()
    row = {
        "cache_key": cache_key,
        "payload_json": json.dumps(sanitize_for_json(payload), ensure_ascii=False),
        "updated_at": updated_at_iso or datetime.now(timezone.utc).isoformat(),
    }
    try:
        client.table("backtest_cache").upsert(row, on_conflict="cache_key").execute()
    except Exception as e:
        logger.warning("백테스트 캐시 DB 저장 실패 (%s): %s", cache_key, e)


def get_all_backtest_cache() -> list[dict]:
    """backtest_cache 전체 행을 읽어 [{cache_key, payload(dict), updated_at}] 로 반환.

    재시작 후 인메모리 SWR 캐시 복원용. 실패 시 빈 리스트(동기 재계산으로 폴백).
    """
    client = _get_client()
    try:
        resp = client.table("backtest_cache").select("cache_key, payload_json, updated_at").execute()
    except Exception as e:
        logger.warning("백테스트 캐시 DB 조회 실패: %s", e)
        return []
    out: list[dict] = []
    for row in resp.data or []:
        raw = row.get("payload_json")
        payload = None
        if isinstance(raw, str):
            try:
                payload = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                payload = None
        elif isinstance(raw, dict):
            payload = raw
        if payload is None:
            continue
        out.append({
            "cache_key": row.get("cache_key"),
            "payload": payload,
            "updated_at": row.get("updated_at"),
        })
    return out


def save_cache_entry(cache_key: str, payload: dict, updated_at_iso: str | None = None) -> None:
    """backtest_cache 테이블을 범용 KV 영속 캐시로 재사용 — cache_key 기준 upsert.

    전용 테이블(DDL) 추가 없이 펀더멘털 등 다른 영속 캐시도 공유한다.
    충돌 방지를 위해 키는 네임스페이스를 둔다(예: 'fund:AAPL'). best-effort.
    """
    if not cache_key:
        return
    client = _get_client()
    row = {
        "cache_key": cache_key,
        "payload_json": json.dumps(sanitize_for_json(payload), ensure_ascii=False),
        "updated_at": updated_at_iso or datetime.now(timezone.utc).isoformat(),
    }
    try:
        client.table("backtest_cache").upsert(row, on_conflict="cache_key").execute()
    except Exception as e:
        logger.warning("KV 캐시 DB 저장 실패 (%s): %s", cache_key, e)


def get_cache_entry(cache_key: str) -> dict | None:
    """범용 KV 캐시(backtest_cache)에서 cache_key 1건 조회.

    반환: {"payload": dict, "updated_at": iso_str} 또는 None(없음/실패).
    """
    if not cache_key:
        return None
    client = _get_client()
    try:
        resp = (
            client.table("backtest_cache")
            .select("payload_json, updated_at")
            .eq("cache_key", cache_key)
            .limit(1)
            .execute()
        )
    except Exception as e:
        logger.warning("KV 캐시 DB 조회 실패 (%s): %s", cache_key, e)
        return None
    rows = resp.data or []
    if not rows:
        return None
    raw = rows[0].get("payload_json")
    payload = None
    if isinstance(raw, str):
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            payload = None
    elif isinstance(raw, dict):
        payload = raw
    if payload is None:
        return None
    return {"payload": payload, "updated_at": rows[0].get("updated_at")}


def get_heatmap_snapshot() -> dict | None:
    """sp500_heatmap 테이블에서 최신 스냅샷을 읽는다. 없으면 None."""
    client = _get_client()
    try:
        resp = (
            client.table("sp500_heatmap")
            .select("data_json")
            .eq("id", 1)
            .limit(1)
            .execute()
        )
    except Exception as e:
        logger.warning("히트맵 DB 조회 실패: %s", e)
        return None
    if not resp.data:
        return None
    raw = resp.data[0].get("data_json")
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None
    return raw if isinstance(raw, dict) else None
