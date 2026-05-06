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
    PRICE_BACKFILL_LOOKBACK_DAYS,
    PRICE_DB_STALE_DAYS,
    PRICE_HISTORY_MAX_PAGES,
)
from services.crud import _get_client
from services.yf_limiter import throttled

logger = logging.getLogger(__name__)


_PRICE_PAGE_SIZE = 1000
_TABLE = "price_history"


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

    # 2) 누락 / stale 판정
    missing_tickers: list[str] = []
    for t in tickers:
        if t not in db_df.columns or db_df[t].dropna().empty:
            missing_tickers.append(t)

    last_dates = _last_dates_from_df(db_df, [t for t in tickers if t not in missing_tickers])
    stale_cutoff = end - timedelta(days=PRICE_DB_STALE_DAYS)
    stale_tickers = [
        t for t, last_d in last_dates.items() if last_d < stale_cutoff
    ]

    fetch_targets = sorted(set(missing_tickers) | set(stale_tickers))

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
    if not tickers:
        return {"tickers": 0, "rows_written": 0, "elapsed_sec": 0.0, "message": "active ticker 없음"}

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
