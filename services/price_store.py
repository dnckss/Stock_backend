"""
가격 시계열 영구 저장 (Supabase price_history).

백테스트·기술적 지표·차트가 yfinance 를 매번 호출하지 않게 한 번 받은 OHLCV 를
DB 에 누적해두고 거기서 먼저 조회한다.

흐름:
  fetch_close_prices(tickers, start, end)
    1) DB 에서 (ticker, date) 범위 조회
    2) 누락(또는 stale) ticker 만 yfinance fallback
    3) 받은 OHLCV 를 price_history 에 upsert (다음 호출은 DB 만으로 끝)
"""
from __future__ import annotations

import logging
import math
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import yfinance as yf

from config import (
    PRICE_BACKFILL_FULL_HISTORY_BATCH_DELAY_SEC,
    PRICE_BACKFILL_FULL_HISTORY_BATCH_SIZE,
    PRICE_BACKFILL_LOOKBACK_DAYS,
    PRICE_BACKFILL_RECENT_USE_SP500,
    PRICE_DB_COVERAGE_THRESHOLD,
    PRICE_DB_STALE_DAYS,
    PRICE_HISTORY_COVERAGE_MIN_DAYS,
    PRICE_HISTORY_COVERAGE_MIN_TICKERS,
    PRICE_HISTORY_MAX_PAGES,
)
from services.crud import _get_client
from services.yf_limiter import throttled

logger = logging.getLogger(__name__)


_PRICE_PAGE_SIZE = 1000
_TABLE = "price_history"


# ---------------------------------------------------------------------------
# 신선도(staleness) 판정 — DB-first 경로가 오래된 종가를 현재가로 오인하지 않게.
# ---------------------------------------------------------------------------

def is_ohlcv_fresh(df: pd.DataFrame, max_stale_days: int | None = None) -> bool:
    """DB OHLCV 의 최신 거래일이 충분히 최근인지 판정.

    DB-first 경로(차트·기간 등락률)가 며칠 오래된 종가를 '현재가'로 오인해
    잘못된 가격/등락률을 내보내는 것을 막는다. 주말·공휴일을 흡수하도록
    기본 허용치는 PRICE_DB_STALE_DAYS (달력 기준). 비거나 인덱스가 비정상이면 stale.
    """
    if df is None or getattr(df, "empty", True):
        return False
    max_stale = PRICE_DB_STALE_DAYS if max_stale_days is None else max_stale_days
    try:
        last = df.index.max()
        last_date = last.date() if hasattr(last, "date") else pd.Timestamp(last).date()
    except (AttributeError, ValueError, TypeError):
        return False
    return (date.today() - last_date).days <= max_stale


def latest_price_date(ticker: str) -> date | None:
    """price_history 에서 해당 ticker 의 가장 최근 거래일 (1 row 조회)."""
    if not ticker:
        return None
    client = _get_client()
    try:
        resp = (
            client.table(_TABLE)
            .select("date")
            .eq("ticker", ticker.upper().strip())
            .order("date", desc=True)
            .limit(1)
            .execute()
        )
    except Exception as e:
        logger.warning("latest_price_date 조회 실패 (%s): %s", ticker, e)
        return None
    rows = resp.data or []
    if not rows:
        return None
    try:
        return pd.Timestamp(rows[0]["date"]).date()
    except (KeyError, ValueError, TypeError):
        return None


def is_ticker_db_fresh(ticker: str, max_stale_days: int | None = None) -> bool:
    """특정 ticker 의 DB 최신 거래일이 충분히 최근인지 (전체 히스토리 로드 없이 1 row)."""
    d = latest_price_date(ticker)
    if d is None:
        return False
    max_stale = PRICE_DB_STALE_DAYS if max_stale_days is None else max_stale_days
    return (date.today() - d).days <= max_stale


# ---------------------------------------------------------------------------
# 변환 유틸
# ---------------------------------------------------------------------------

def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _safe_volume(v: Any) -> int | None:
    f = _safe_float(v)
    if f is None:
        return None
    try:
        return int(f)
    except (TypeError, ValueError, OverflowError):
        return None


def _yf_to_rows(df: pd.DataFrame, tickers: list[str]) -> list[dict]:
    """yf.download 결과 → price_history row 리스트."""
    if df is None or df.empty:
        return []

    rows: list[dict] = []

    if isinstance(df.columns, pd.MultiIndex):
        # 여러 ticker — Open/High/Low/Close/Volume × ticker
        for t in tickers:
            try:
                t_df = df.xs(t, level=1, axis=1)
            except (KeyError, ValueError):
                continue
            for idx, row in t_df.iterrows():
                close = _safe_float(row.get("Close"))
                if close is None:
                    continue
                d_iso = idx.date().isoformat() if hasattr(idx, "date") else str(idx)[:10]
                rows.append({
                    "ticker": t,
                    "date": d_iso,
                    "open": _safe_float(row.get("Open")),
                    "high": _safe_float(row.get("High")),
                    "low": _safe_float(row.get("Low")),
                    "close": close,
                    "volume": _safe_volume(row.get("Volume")),
                })
    else:
        # 단일 ticker
        if "Close" not in df.columns:
            return []
        t = tickers[0] if tickers else None
        if not t:
            return []
        for idx, row in df.iterrows():
            close = _safe_float(row.get("Close"))
            if close is None:
                continue
            d_iso = idx.date().isoformat() if hasattr(idx, "date") else str(idx)[:10]
            rows.append({
                "ticker": t,
                "date": d_iso,
                "open": _safe_float(row.get("Open")),
                "high": _safe_float(row.get("High")),
                "low": _safe_float(row.get("Low")),
                "close": close,
                "volume": _safe_volume(row.get("Volume")),
            })
    return rows


def _rows_to_close_df(rows: list[dict]) -> pd.DataFrame:
    """price_history row 리스트 → 컬럼=ticker, index=DatetimeIndex 인 close DataFrame."""
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    pivot = df.pivot_table(index="date", columns="ticker", values="close", aggfunc="last")
    pivot.index.name = None
    return pivot.sort_index()


# ---------------------------------------------------------------------------
# DB read / write
# ---------------------------------------------------------------------------

def get_close_prices_db(
    tickers: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    """price_history 에서 (ticker, date) 범위의 종가 시계열을 가져온다."""
    if not tickers:
        return pd.DataFrame()

    client = _get_client()
    collected: list[dict] = []
    for page in range(PRICE_HISTORY_MAX_PAGES):
        start_idx = page * _PRICE_PAGE_SIZE
        end_idx = start_idx + _PRICE_PAGE_SIZE - 1
        try:
            resp = (
                client.table(_TABLE)
                .select("ticker, date, close")
                .in_("ticker", list(tickers))
                .gte("date", start.isoformat())
                .lte("date", end.isoformat())
                .order("date", desc=False)
                .range(start_idx, end_idx)
                .execute()
            )
        except Exception as e:
            logger.warning("price_history 조회 실패 (page %d): %s", page, e)
            break
        rows = resp.data or []
        collected.extend(rows)
        if len(rows) < _PRICE_PAGE_SIZE:
            break

    return _rows_to_close_df(collected)


def get_ohlcv_db(
    ticker: str,
    start: date | None = None,
    end: date | None = None,
) -> pd.DataFrame:
    """단일 ticker 의 OHLCV 시계열 — index=DatetimeIndex, columns=open/high/low/close/volume.

    차트 엔드포인트가 yfinance 재호출 없이 일봉을 가져올 수 있게 한다.
    """
    if not ticker:
        return pd.DataFrame()

    client = _get_client()
    upper = ticker.upper().strip()
    collected: list[dict] = []
    for page in range(PRICE_HISTORY_MAX_PAGES):
        start_idx = page * _PRICE_PAGE_SIZE
        end_idx = start_idx + _PRICE_PAGE_SIZE - 1
        try:
            q = (
                client.table(_TABLE)
                .select("date, open, high, low, close, volume")
                .eq("ticker", upper)
            )
            if start is not None:
                q = q.gte("date", start.isoformat())
            if end is not None:
                q = q.lte("date", end.isoformat())
            resp = q.order("date", desc=False).range(start_idx, end_idx).execute()
        except Exception as e:
            logger.warning("price_history OHLCV 조회 실패 (%s, page %d): %s", upper, page, e)
            break
        rows = resp.data or []
        collected.extend(rows)
        if len(rows) < _PRICE_PAGE_SIZE:
            break

    if not collected:
        return pd.DataFrame()
    df = pd.DataFrame(collected)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    return df[["open", "high", "low", "close", "volume"]]


def get_ohlc_prices_db(
    tickers: list[str],
    start: date,
    end: date,
) -> dict[str, pd.DataFrame]:
    """price_history 에서 (ticker, date) 범위의 OHLCV 를 ticker별 DataFrame 으로 반환한다.

    반환: {TICKER: DataFrame(index=DatetimeIndex, columns=[open, high, low, close, volume])}.
    종가 전용 경로(get_close_prices_db)와 분리한 이유 — 손절/목표가 intrabar 청산
    평가에는 고가(high)·저가(low)가, 스캐너 VOL 백필에는 volume 이 필요하기 때문.
    (백테스트 _planned_exit 는 volume 컬럼을 무시한다.) 조회 실패/빈 결과면 빈 dict.
    """
    if not tickers:
        return {}

    uppers = sorted({(t or "").upper().strip() for t in tickers if t})
    if not uppers:
        return {}

    client = _get_client()
    collected: list[dict] = []
    for page in range(PRICE_HISTORY_MAX_PAGES):
        start_idx = page * _PRICE_PAGE_SIZE
        end_idx = start_idx + _PRICE_PAGE_SIZE - 1
        try:
            resp = (
                client.table(_TABLE)
                .select("ticker, date, open, high, low, close, volume")
                .in_("ticker", uppers)
                .gte("date", start.isoformat())
                .lte("date", end.isoformat())
                .order("date", desc=False)
                .range(start_idx, end_idx)
                .execute()
            )
        except Exception as e:
            logger.warning("price_history OHLC 배치 조회 실패 (page %d): %s", page, e)
            break
        rows = resp.data or []
        collected.extend(rows)
        if len(rows) < _PRICE_PAGE_SIZE:
            break

    if not collected:
        return {}

    df = pd.DataFrame(collected)
    df["date"] = pd.to_datetime(df["date"])
    out: dict[str, pd.DataFrame] = {}
    for ticker, grp in df.groupby("ticker"):
        frame = (
            grp.drop_duplicates(subset="date", keep="last")
            .set_index("date")
            .sort_index()[["open", "high", "low", "close", "volume"]]
        )
        out[str(ticker).upper()] = frame
    return out


def upsert_price_rows(rows: list[dict]) -> int:
    """ticker+date 충돌 시 갱신. chunk 500 으로 나눠 호출."""
    if not rows:
        return 0
    client = _get_client()
    written = 0
    chunk = 500
    for i in range(0, len(rows), chunk):
        batch = rows[i:i + chunk]
        try:
            client.table(_TABLE).upsert(batch, on_conflict="ticker,date").execute()
            written += len(batch)
        except Exception as e:
            logger.warning("price_history upsert 실패 (chunk start=%d): %s", i, e)
    return written


def _last_dates_from_df(db_df: pd.DataFrame, tickers: list[str]) -> dict[str, date]:
    """이미 가져온 db_df 에서 ticker 별 마지막 거래일을 추출 (DB 추가 쿼리 없음)."""
    out: dict[str, date] = {}
    if db_df is None or db_df.empty:
        return out
    for t in tickers:
        if t not in db_df.columns:
            continue
        series = db_df[t].dropna()
        if series.empty:
            continue
        last_idx = series.index[-1]
        if hasattr(last_idx, "date"):
            out[t] = last_idx.date()
    return out


# ---------------------------------------------------------------------------
# Public — DB 우선 + yfinance fallback
# ---------------------------------------------------------------------------

def fetch_close_prices(
    tickers: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    """
    DB price_history 에서 종가 시계열을 가져오고, 누락/stale 한 ticker 만
    yfinance 로 보강한 뒤 DB 에 upsert. 반환은 컬럼=ticker, index=DatetimeIndex.
    """
    if not tickers:
        return pd.DataFrame()

    tickers = sorted({(t or "").upper() for t in tickers if t})

    # 1) DB 조회
    db_df = get_close_prices_db(tickers, start, end)

    # 2) 누락 / stale / sparse 판정
    missing_tickers: list[str] = []
    for t in tickers:
        if t not in db_df.columns or db_df[t].dropna().empty:
            missing_tickers.append(t)

    non_missing = [t for t in tickers if t not in missing_tickers]
    last_dates = _last_dates_from_df(db_df, non_missing)
    stale_cutoff = end - timedelta(days=PRICE_DB_STALE_DAYS)
    stale_tickers = [t for t, last_d in last_dates.items() if last_d < stale_cutoff]

    # 범위 전체 커버리지 — 최신은 있어도 과거 거래일이 듬성듬성하면 sparse 로 보고
    # yfinance fallback 으로 빈칸을 채운다 (예: 90일 lookback 에 5일만 있는 경우).
    span_days = max(1, (end - start).days)
    expected_trading_days = max(1, int(span_days * 5 / 7))
    sparse_tickers: list[str] = []
    for t in non_missing:
        actual = db_df[t].dropna().shape[0] if t in db_df.columns else 0
        if actual < expected_trading_days * PRICE_DB_COVERAGE_THRESHOLD:
            sparse_tickers.append(t)

    fetch_targets = sorted(set(missing_tickers) | set(stale_tickers) | set(sparse_tickers))

    # 3) yfinance fallback — DB upsert 는 best-effort, 실패해도 yfinance 결과는 그대로 사용
    if fetch_targets:
        end_plus = end + timedelta(days=1)
        try:
            yf_df = throttled(
                yf.download,
                fetch_targets,
                start=start.isoformat(),
                end=end_plus.isoformat(),
                progress=False,
                auto_adjust=False,
                threads=True,
            )
            rows = _yf_to_rows(yf_df, fetch_targets)
            if rows:
                # DB upsert 시도 (테이블 없거나 실패해도 응답엔 영향 없게)
                try:
                    n_written = upsert_price_rows(rows)
                    logger.info(
                        "price_history yfinance fallback: %d targets → %d rows upserted",
                        len(fetch_targets), n_written,
                    )
                except Exception as e:
                    logger.warning("price_history upsert 실패 (DB 미적용 가능성): %s", e)

                # yfinance 결과 자체를 DataFrame 으로 즉시 사용 — DB 의존 X
                yf_close_df = _rows_to_close_df(rows)
                if db_df.empty:
                    db_df = yf_close_df
                elif not yf_close_df.empty:
                    # yfinance 결과를 우선시하되, DB 에만 있던 과거 데이터는 보존
                    db_df = yf_close_df.combine_first(db_df)
        except Exception as e:
            logger.warning("yfinance fallback 실패: %s — DB 부분 결과로 진행", e)

    return db_df


# ---------------------------------------------------------------------------
# 백그라운드 backfill
# ---------------------------------------------------------------------------

def _active_tickers_recent(days: int = 2, limit: int = 10000) -> list[str]:
    """최근 N일 analysis_results 의 unique ticker — 활성 종목 후보."""
    client = _get_client()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    try:
        resp = (
            client.table("analysis_results")
            .select("ticker")
            .gte("created_at", cutoff)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
    except Exception as e:
        logger.warning("active_tickers 조회 실패: %s", e)
        return []
    return sorted({(r.get("ticker") or "").upper() for r in (resp.data or []) if r.get("ticker")})


def backfill_recent(days: int | None = None) -> dict[str, Any]:
    """
    최근 N일 active ticker OHLCV 를 yfinance 에서 받아 price_history 에 upsert.
    반환: {'tickers': N, 'rows_written': M, 'elapsed_sec': X}.
    """
    start_ts = time.time()
    lookback = days or PRICE_BACKFILL_LOOKBACK_DAYS

    tickers = _active_tickers_recent()
    if PRICE_BACKFILL_RECENT_USE_SP500:
        # S&P 500 전체를 일일 증분 대상에 포함 — 차트가 DB 만으로 동작하도록 한다.
        try:
            from services.scanner import get_all_tickers
            sp500 = get_all_tickers()
            tickers = sorted({(t or "").upper() for t in (tickers + sp500) if t})
        except Exception as e:
            logger.warning("backfill_recent S&P 500 보강 실패 (active만 사용): %s", e)
    if not tickers:
        return {"tickers": 0, "rows_written": 0, "elapsed_sec": 0.0, "message": "ticker 없음"}

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=lookback)
    end_plus = end + timedelta(days=1)

    rows_written = 0
    try:
        yf_df = throttled(
            yf.download,
            tickers,
            start=start.isoformat(),
            end=end_plus.isoformat(),
            progress=False,
            auto_adjust=False,
            threads=True,
        )
        rows = _yf_to_rows(yf_df, tickers)
        rows_written = upsert_price_rows(rows)
    except Exception as e:
        logger.warning("backfill yfinance 실패: %s", e)

    return {
        "tickers": len(tickers),
        "rows_written": rows_written,
        "elapsed_sec": round(time.time() - start_ts, 2),
    }


# ---------------------------------------------------------------------------
# 풀히스토리 부트스트랩 (1회) — period="max" 일봉을 S&P 500 전체에 대해 저장.
# ---------------------------------------------------------------------------

def backfill_full_history(tickers: list[str]) -> dict[str, Any]:
    """모든 ticker 의 period='max' 일봉 OHLCV 를 yfinance 에서 받아 price_history 에 upsert.

    한 번만 실행하면 충분하다 — 이후엔 backfill_recent 가 일일 증분을 갱신한다.
    upsert(ticker,date on_conflict) 라 중복 실행해도 데이터 손상이 없다(idempotent).
    """
    if not tickers:
        return {"tickers": 0, "rows_written": 0, "elapsed_sec": 0.0, "message": "ticker 없음"}

    start_ts = time.time()
    cleaned = sorted({(t or "").upper().strip() for t in tickers if t})
    batch_size = max(1, PRICE_BACKFILL_FULL_HISTORY_BATCH_SIZE)
    delay = max(0.0, PRICE_BACKFILL_FULL_HISTORY_BATCH_DELAY_SEC)
    total_rows = 0
    total_batches = (len(cleaned) + batch_size - 1) // batch_size
    logger.info(
        "price_history 풀히스토리 부트스트랩 시작: %d tickers, %d batches (size=%d)",
        len(cleaned), total_batches, batch_size,
    )

    for i in range(0, len(cleaned), batch_size):
        batch = cleaned[i : i + batch_size]
        batch_no = i // batch_size + 1
        if i > 0 and delay > 0:
            time.sleep(delay)  # batch 간 대기 — rate limit 분산
        try:
            yf_df = throttled(
                yf.download,
                batch,
                period="max",
                interval="1d",
                progress=False,
                auto_adjust=False,
                threads=True,
            )
            rows = _yf_to_rows(yf_df, batch)
            if rows:
                written = upsert_price_rows(rows)
                total_rows += written
                logger.info(
                    "부트스트랩 batch %d/%d: %d tickers, %d rows upserted (누적 %d)",
                    batch_no, total_batches, len(batch), written, total_rows,
                )
            else:
                logger.warning(
                    "부트스트랩 batch %d/%d: rows 없음 (예: %s)",
                    batch_no, total_batches, batch[:3],
                )
        except Exception as e:
            logger.warning("부트스트랩 batch %d/%d 실패: %s", batch_no, total_batches, e)

    elapsed = round(time.time() - start_ts, 2)
    logger.info(
        "price_history 풀히스토리 부트스트랩 완료: %d rows, %.1fs",
        total_rows, elapsed,
    )
    return {"tickers": len(cleaned), "rows_written": total_rows, "elapsed_sec": elapsed}


def check_price_history_coverage() -> dict[str, Any]:
    """price_history 가 부트스트랩이 필요할 만큼 부족한지 점검 (전체 row 수 기반).

    임계치 = MIN_TICKERS × MIN_DAYS × 5/7(주말 제외) — 예: 400 × 365 × 5/7 ≈ 104,286 rows.
    DB row 수가 이 임계를 넘으면 풀히스토리가 충분하다고 보고 부트스트랩을 스킵한다.
    """
    min_rows = int(
        PRICE_HISTORY_COVERAGE_MIN_TICKERS * PRICE_HISTORY_COVERAGE_MIN_DAYS * 5 / 7
    )
    client = _get_client()
    try:
        # estimated: pg_class.reltuples 기반 추정 — count(*) 전체 스캔/타임아웃 없이 즉시.
        # 대형 테이블(수백만 행)에서 exact count 는 500("JSON could not be generated") 으로
        # 실패하기 쉬운데, 그 실패를 0 으로 단정하면 매 기동마다 전체 재부트스트랩이 돌았다.
        resp = (
            client.table(_TABLE).select("ticker", count="estimated", head=True).execute()
        )
        total = getattr(resp, "count", None)
        if not total:
            # 통계 미수집(reltuples 0/None) 가능성 — 소형/빈 테이블이면 exact 도 가벼움.
            resp = (
                client.table(_TABLE).select("ticker", count="exact", head=True).execute()
            )
            total = getattr(resp, "count", None) or 0
    except Exception as e:
        # 조회 실패를 0 으로 단정하면 503종목 풀 재부트스트랩이 돌아 Supabase/yfinance 를
        # 과부하시키고 연결 종료(ConnectionTerminated) 악순환을 만든다(HF 재기동 빈번).
        # 알 수 없을 땐 '충분함'으로 보아 스킵 — 증분 backfill·self-heal 이 빈칸을 메운다.
        logger.warning("price_history coverage 조회 실패: %s — 부트스트랩 스킵(기존 데이터 보존)", e)
        return {"ok": True, "total_rows": -1, "min_rows": min_rows, "error": str(e)}
    return {"ok": total >= min_rows, "total_rows": int(total), "min_rows": min_rows}
