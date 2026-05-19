from __future__ import annotations

import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from io import StringIO
from typing import Any

import pandas as pd
import requests
import yfinance as yf

from config import (
    MIN_VOLUME,
    SCAN_TOP_N,
    SCAN_TRADING_DAYS,
    SCAN_DOWNLOAD_BATCH_PARALLELISM,
    MACRO_MARQUEE,
    MACRO_SIDEBAR,
    MACRO_FALLBACK,
    MIN_VIX,
    MAX_VIX,
    PRICE_FAST_INFO_FALLBACK_MAX_SYMBOLS,
    PRICE_DOWNLOAD_BATCH_SIZE,
    PRICE_INTRADAY_INTERVAL,
    SP500_WIKI_URL,
    SP500_WIKI_HEADERS,
    SP500_CONSTITUENTS_CACHE_TTL_SEC,
)

logger = logging.getLogger(__name__)

_sp500_constituents_cache: list[dict[str, str]] = []
_sp500_constituents_cache_at: float = 0.0


# ---------------------------------------------------------------------------
# 1) 티커 수집 — Wikipedia S&P 500 구성종목
# ---------------------------------------------------------------------------

def get_sp500_constituents(refresh: bool = False) -> list[dict[str, str]]:
    """Wikipedia에서 S&P 500 구성종목(ticker, name, sector)을 가져온다.

    Returns:
        [{"ticker": "AAPL", "name": "Apple Inc.", "sector": "Information Technology"}, ...]
        실패 시 빈 리스트.
    """
    global _sp500_constituents_cache, _sp500_constituents_cache_at

    now = time.time()
    if (
        not refresh
        and _sp500_constituents_cache
        and now - _sp500_constituents_cache_at < SP500_CONSTITUENTS_CACHE_TTL_SEC
    ):
        return [dict(row) for row in _sp500_constituents_cache]

    try:
        resp = requests.get(SP500_WIKI_URL, headers=SP500_WIKI_HEADERS, timeout=15)
        resp.raise_for_status()
        df = pd.read_html(StringIO(resp.text))[0]
        rows: list[dict[str, str]] = []
        for _, row in df.iterrows():
            ticker = str(row.get("Symbol", "")).strip().replace(".", "-")
            name = str(row.get("Security", "")).strip()
            sector = str(row.get("GICS Sector", "")).strip()
            if ticker and sector:
                rows.append({"ticker": ticker, "name": name, "sector": sector})
        print(f"S&P 500 구성종목 {len(rows)}개 수집 완료")
        _sp500_constituents_cache = rows
        _sp500_constituents_cache_at = now
        return [dict(row) for row in rows]
    except requests.RequestException as e:
        print(f"Wikipedia 네트워크 에러: {e}")
    except (ValueError, KeyError) as e:
        print(f"Wikipedia 파싱 에러: {e}")
    except Exception as e:
        print(f"티커 수집 실패: {e}")
    return []


def get_all_tickers() -> list[str]:
    """S&P 500 구성종목 티커 목록을 반환한다. 실패 시 빈 리스트."""
    return sorted({c["ticker"] for c in get_sp500_constituents()})


def _placeholder_stock(
    ticker: str,
    *,
    name: str | None = None,
    sector: str | None = None,
    source: str = "sp500",
) -> dict:
    return {
        "ticker": ticker,
        "name": name,
        "sector": sector,
        "in_sp500": source == "sp500",
        "return": None,
        "price": None,
        "volume": None,
        "liquidity_ok": None,
        "daily": [],
        "price_available": False,
        "scan_missing": True,
        "universe_source": source,
    }


def ensure_sp500_coverage(candidates: list[dict]) -> list[dict]:
    """
    기존 스캔/DB 스냅샷에 없는 S&P 500 구성종목을 placeholder 로 보강한다.
    가격 틱 루프가 다음 주기에서 전 종목 가격을 채울 수 있게 하는 목적이다.
    """
    rows = list(candidates or [])
    constituents = get_sp500_constituents()
    if not constituents:
        return rows

    by_ticker = {
        (row.get("ticker") or "").upper().strip(): row
        for row in rows
        if isinstance(row, dict) and row.get("ticker")
    }

    for c in constituents:
        ticker = (c.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        existing = by_ticker.get(ticker)
        if existing is not None:
            existing.setdefault("name", c.get("name"))
            existing.setdefault("sector", c.get("sector"))
            existing.setdefault("in_sp500", True)
            existing.setdefault("price_available", existing.get("price") is not None)
            continue
        row = _placeholder_stock(
            ticker,
            name=c.get("name"),
            sector=c.get("sector"),
            source="sp500",
        )
        rows.append(row)
        by_ticker[ticker] = row

    return rows


# ---------------------------------------------------------------------------
# 2) 종목 스캔 — yf.download() 배치 다운로드
# ---------------------------------------------------------------------------

_DOWNLOAD_BATCH_SIZE = 100


def _compute_return(ohlc: pd.DataFrame) -> float | None:
    """Google Finance "지난 N일" 정의로 등락률 계산.

    base = (today - (N-1) trading days) 의 OPEN
    last = today CLOSE
    SCAN_TRADING_DAYS=5 → 5 포인트 윈도우의 첫 점 OPEN vs 마지막 점 CLOSE.

    OPEN 이 base 인 이유: Google Finance "지난 N일" 표기가 거래 시작가 기준이라
    어닝 직후 점프(DDOG 5/7 +31%) 같은 경우에도 외부 사이트 수치와 일치한다.
    """
    if "Close" not in ohlc.columns or "Open" not in ohlc.columns:
        return None
    close = ohlc["Close"].dropna()
    open_ = ohlc["Open"].dropna()
    if close.empty or open_.empty:
        return None
    n = min(SCAN_TRADING_DAYS, len(close), len(open_))
    if n < 1:
        return None
    last_close = float(close.iloc[-1])
    first_open = float(open_.iloc[-n])
    if first_open == 0 or not math.isfinite(first_open) or not math.isfinite(last_close):
        return None
    return (last_close - first_open) / first_open


def _download_batch(batch: list[str]) -> pd.DataFrame | None:
    """yf.download 1회 — 예외/빈 결과는 None 반환."""
    if not batch:
        return None
    try:
        data = yf.download(
            batch,
            period="10d",
            interval="1d",
            group_by="ticker",
            progress=False,
            threads=True,
        )
    except Exception as e:
        logger.warning("yf.download batch (size=%d) 실패: %s", len(batch), e)
        return None
    if data is None or data.empty:
        return None
    return data


def _parse_batch_candidates(data: pd.DataFrame, batch: list[str]) -> list[dict]:
    """yf.download 결과 1건에서 ticker 별 candidate dict 리스트를 추출한다."""
    out: list[dict] = []
    is_single = len(batch) == 1

    # MultiIndex 의 level 0 (ticker) 집합을 1회만 계산.
    if not is_single and isinstance(data.columns, pd.MultiIndex):
        level0_tickers = set(data.columns.get_level_values(0))
    else:
        level0_tickers = set(batch)

    for ticker in batch:
        try:
            if is_single:
                ticker_data = data
            else:
                if ticker not in level0_tickers:
                    continue
                ticker_data = data[ticker]

            close_series = ticker_data["Close"].dropna()
            volume_series = ticker_data["Volume"].dropna()

            if close_series.empty or volume_series.empty:
                continue

            last_volume = int(volume_series.iloc[-1])
            liquidity_ok = last_volume >= MIN_VOLUME

            # 등락률을 못 구해도(데이터 1개 등) 가격·일봉이 있으면 candidates 에 포함.
            # 그래야 ensure_sp500_coverage 가 placeholder 로 채우지 않고, 등락률 컬럼만
            # 비어 있는 정상 row 가 화면에 나타난다(가격·거래량·일봉은 모두 표시됨).
            ret = _compute_return(ticker_data)

            daily_bars: list[dict] = []
            for idx, row in ticker_data.iterrows():
                try:
                    ts = idx if hasattr(idx, "strftime") else pd.Timestamp(idx)
                    date_str = ts.strftime("%Y-%m-%d")
                    o = row.get("Open")
                    h = row.get("High")
                    l_ = row.get("Low")
                    c = row.get("Close")
                    v = row.get("Volume")
                    if c is None or (pd.isna(c)):
                        continue
                    daily_bars.append({
                        "date": date_str,
                        "open": round(float(o), 2) if o is not None and not pd.isna(o) else None,
                        "high": round(float(h), 2) if h is not None and not pd.isna(h) else None,
                        "low": round(float(l_), 2) if l_ is not None and not pd.isna(l_) else None,
                        "close": round(float(c), 2),
                        "volume": int(v) if v is not None and not pd.isna(v) else 0,
                    })
                except Exception:
                    continue

            # _compute_return 와 동일한 5 포인트 윈도우로 통일.
            # merge_intraday_into_candidates 가 daily[0].close 를 base 로 사용하므로
            # 같은 첫 종가가 잡히도록 keep == SCAN_TRADING_DAYS.
            keep = SCAN_TRADING_DAYS
            trimmed_bars = daily_bars[-keep:] if len(daily_bars) > keep else daily_bars

            out.append({
                "ticker": ticker,
                "return": round(ret, 6) if ret is not None else None,
                "price": round(float(close_series.iloc[-1]), 2),
                "volume": last_volume,
                "liquidity_ok": liquidity_ok,
                "daily": trimmed_bars,
            })
        except Exception:
            continue
    return out


def scan_stocks(tickers: list[str]) -> list[dict]:
    """
    yf.download()로 10캘린더일(≥5거래일)치 종가·거래량을 배치 조회하고,
    거래량 기준은 liquidity_ok 로 표시만 한 뒤, 변동률 기준으로 정렬된
    전체 유효 종목 리스트를 반환한다.

    배치 동시 다운로드: yf.download 자체가 threads=True 로 종목 내부 병렬을 하지만
    batch(=100종목) 5개를 직렬로 도는 게 그 사이클 시간의 대부분이었다.
    ``SCAN_DOWNLOAD_BATCH_PARALLELISM`` 만큼 batch 를 동시에 다운로드해 사이클을
    수배~5배 단축한다. yfinance 자체 connection pool + 한 IP 당 동시 요청 한도를
    고려해 기본 3 으로 보수화.
    """
    if not tickers:
        return []

    # batch 분할
    batches = [
        tickers[i : i + _DOWNLOAD_BATCH_SIZE]
        for i in range(0, len(tickers), _DOWNLOAD_BATCH_SIZE)
    ]

    candidates: list[dict] = []
    workers = max(1, min(SCAN_DOWNLOAD_BATCH_PARALLELISM, len(batches)))

    if workers == 1 or len(batches) == 1:
        # 단일 batch 또는 동시성 비활성 — 직렬 폴백
        for batch in batches:
            data = _download_batch(batch)
            if data is not None:
                candidates.extend(_parse_batch_candidates(data, batch))
    else:
        # ThreadPoolExecutor 로 batch 다운로드 병렬화.
        # 각 batch 의 _parse_batch_candidates 는 CPU 바운드이지만 작아서 thread 로 충분.
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="scan-batch") as ex:
            futures = {ex.submit(_download_batch, batch): batch for batch in batches}
            for fut in as_completed(futures):
                batch = futures[fut]
                try:
                    data = fut.result()
                except Exception as e:
                    logger.warning("batch (size=%d) thread 실패: %s", len(batch), e)
                    continue
                if data is None:
                    continue
                candidates.extend(_parse_batch_candidates(data, batch))

    # 누락 ticker 는 placeholder 보강 (가격 틱 루프가 추후 채움).
    present = {(c.get("ticker") or "").upper() for c in candidates}
    for ticker in tickers:
        key = (ticker or "").upper().strip()
        if key and key not in present:
            candidates.append(_placeholder_stock(key, source="requested"))
            present.add(key)

    candidates = ensure_sp500_coverage(candidates)

    def _sort_return(item: dict) -> float:
        ret = item.get("return")
        try:
            ret_f = float(ret)
            return abs(ret_f) if math.isfinite(ret_f) else -1.0
        except (TypeError, ValueError):
            return -1.0

    candidates.sort(key=_sort_return, reverse=True)
    priced_count = sum(1 for c in candidates if c.get("price") is not None)
    logger.info("스캔 결과: %d/%d개 가격 확보 (batch=%d, workers=%d)",
                priced_count, len(candidates), len(batches), workers)
    return candidates


def _ticker_frame_from_download(data: pd.DataFrame, ticker: str, batch: list[str]) -> pd.DataFrame | None:
    """yf.download 결과에서 ticker 1개의 OHLCV 프레임을 안전하게 추출한다."""
    if data is None or data.empty:
        return None

    if isinstance(data.columns, pd.MultiIndex):
        levels0 = set(str(v) for v in data.columns.get_level_values(0))
        levels1 = set(str(v) for v in data.columns.get_level_values(1))
        if ticker in levels0:
            try:
                return data[ticker]
            except Exception:
                return None
        if ticker in levels1:
            try:
                return data.xs(ticker, level=1, axis=1)
            except Exception:
                return None

    if len(batch) == 1:
        return data

    return None


def _format_quote_as_of(idx: Any, fallback: str) -> str:
    """pandas timestamp/index 값을 API 응답용 문자열로 변환한다."""
    try:
        if hasattr(idx, "to_pydatetime"):
            return idx.to_pydatetime().isoformat()
        if hasattr(idx, "isoformat"):
            return idx.isoformat()
    except Exception:
        pass
    return fallback


def _refresh_intraday_prices_batch(tickers: list[str]) -> dict[str, dict]:
    """yf.download 분봉 batch 조회로 최신 가격을 가져온다."""
    if not tickers:
        return {}

    from services.yf_limiter import throttled

    out: dict[str, dict] = {}
    now_str = datetime.now().isoformat()
    batch_size = max(1, PRICE_DOWNLOAD_BATCH_SIZE)

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        try:
            data = throttled(
                yf.download,
                batch,
                period="1d",
                interval=PRICE_INTRADAY_INTERVAL,
                group_by="ticker",
                progress=False,
                threads=True,
                prepost=True,
                auto_adjust=False,
            )
        except Exception as e:
            logger.debug("분봉 batch 조회 실패 (batch=%d, size=%d): %s", i, len(batch), e)
            continue

        if data is None or data.empty:
            continue

        for ticker in batch:
            try:
                frame = _ticker_frame_from_download(data, ticker, batch)
                if frame is None or frame.empty or "Close" not in frame:
                    continue

                close_series = frame["Close"].dropna()
                if close_series.empty:
                    continue

                price = float(close_series.iloc[-1])
                if not math.isfinite(price):
                    continue

                volume_int: int | None = None
                if "Volume" in frame:
                    volume_series = frame["Volume"].dropna()
                    if not volume_series.empty:
                        try:
                            volume = float(volume_series.iloc[-1])
                            volume_int = int(volume) if math.isfinite(volume) else None
                        except (TypeError, ValueError, OverflowError):
                            volume_int = None

                last_idx = close_series.index[-1]
                out[ticker] = {
                    "price": round(price, 2),
                    "volume": volume_int,
                    "as_of": _format_quote_as_of(last_idx, now_str),
                    "source": f"yf_download_{PRICE_INTRADAY_INTERVAL}",
                }
            except Exception:
                continue

    return out


def _refresh_fast_info_prices(tickers: list[str]) -> dict[str, dict]:
    """분봉 batch 누락분을 fast_info 로 제한적으로 보강한다."""
    if not tickers:
        return {}

    out: dict[str, dict] = {}
    now_str = datetime.now().isoformat()

    from services.yf_limiter import throttled

    for ticker in tickers[:PRICE_FAST_INFO_FALLBACK_MAX_SYMBOLS]:
        key = (ticker or "").upper().strip()
        if not key:
            continue
        try:
            fi = throttled(lambda _k=key: yf.Ticker(_k).fast_info)
            price = fi.get("lastPrice")
            if price is None or not math.isfinite(price):
                continue
            volume = fi.get("lastVolume")
            vol_int: int | None = None
            if volume is not None:
                try:
                    vol_int = int(volume) if math.isfinite(float(volume)) else None
                except (TypeError, ValueError):
                    vol_int = None
            out[key] = {
                "price": round(float(price), 2),
                "volume": vol_int,
                "as_of": now_str,
                "source": "fast_info",
            }
        except Exception:
            continue

    return out


def refresh_intraday_prices(tickers: list[str]) -> dict[str, dict]:
    """
    yfinance 분봉 batch 조회로 종목별 최신 가격을 가져오고,
    누락된 일부 심볼만 fast_info 로 보강한다.

    Returns:
        { "AAPL": {"price": float, "volume": int|None, "as_of": str}, ... }
    """
    if not tickers:
        return {}

    normalized = []
    seen: set[str] = set()
    for ticker in tickers:
        key = (ticker or "").upper().strip()
        if key and key not in seen:
            seen.add(key)
            normalized.append(key)

    out = _refresh_intraday_prices_batch(normalized)
    missing = [t for t in normalized if t not in out]
    if missing and PRICE_FAST_INFO_FALLBACK_MAX_SYMBOLS > 0:
        out.update(_refresh_fast_info_prices(missing))

    return out


def merge_intraday_into_candidates(candidates: list[dict], live: dict[str, dict]) -> None:
    """
    live 시세를 top_picks/radar 항목에 제자리 반영한다.
    현재가가 바뀌면 5일 등락률(return)도 함께 재계산해 기준 시점을 맞춘다.
    """
    for c in candidates:
        t = (c.get("ticker") or "").upper().strip()
        if not t or t not in live:
            continue
        u = live[t]
        c["price"] = u["price"]
        c["price_available"] = True
        c["quote_source"] = u.get("source")
        if u.get("volume") is not None:
            c["volume"] = u["volume"]
            if c.get("liquidity_ok") is None:
                c["liquidity_ok"] = u["volume"] >= MIN_VOLUME
        c["quote_as_of"] = u["as_of"]

        # Google "지난 5일" 정의에 맞춘 base = 5포인트 윈도우 첫 점의 OPEN.
        # 최신 분봉 price 와 결합해 등락률을 재계산한다.
        try:
            daily = c.get("daily") or []
            if daily and isinstance(daily, list):
                base_open = daily[0].get("open")
                if base_open is None:
                    # OPEN 이 누락된 경우 close 로 fallback (편차 미미한 종목엔 충분).
                    base_open = daily[0].get("close")
                if base_open is not None:
                    base = float(base_open)
                    now_px = float(u["price"])
                    if math.isfinite(base) and math.isfinite(now_px) and base != 0:
                        c["return"] = round((now_px - base) / base, 6)
        except Exception:
            # return 재계산 실패는 무시하고 기존 값을 유지한다.
            pass


def backfill_missing_returns(candidates: list[dict]) -> int:
    """
    return 이 비어있고 price 가 있는 종목들에 대해, price_history DB 에서
    SCAN_TRADING_DAYS 거래일 전 종가를 가져와 5일 등락률을 채운다.

    ``_preserve_or_restore_snapshot`` 으로 placeholder(daily=[]) 가 유지되는 경우,
    `merge_intraday_into_candidates` 는 daily 가 비어있어 return 재계산을 skip 한다.
    이 함수가 다음 price tick 사이클에서 DB 데이터로 5D RETURN 컬럼의 누락을 메운다.

    Returns:
        backfill 된 종목 수.
    """
    if not candidates:
        return 0

    missing = [
        c for c in candidates
        if c.get("return") is None and c.get("price") is not None
    ]
    if not missing:
        return 0

    # price_history 조회 — yfinance 직접 호출은 fetch_close_prices 가 알아서 처리.
    tickers = sorted({(c.get("ticker") or "").upper().strip() for c in missing if c.get("ticker")})
    if not tickers:
        return 0

    # 영업일 5일 + 주말/공휴일 여유 (BACKTEST_PRICE_LOOKAHEAD_DAYS 정의 안 가져옴 — 14일 직접)
    end = datetime.now().date()
    start = end - timedelta(days=14)

    try:
        from services.price_store import fetch_close_prices
        close_df = fetch_close_prices(tickers, start, end)
    except Exception as e:
        logger.warning("backfill_missing_returns: fetch_close_prices 실패: %s", e)
        return 0

    if close_df is None or close_df.empty:
        return 0

    filled = 0
    for c in missing:
        try:
            t = (c.get("ticker") or "").upper().strip()
            if t not in close_df.columns:
                continue
            series = close_df[t].dropna()
            if len(series) < 2:
                continue
            # SCAN_TRADING_DAYS=5 일 전 close. 부족하면 가장 오래된 점 사용.
            base_idx = min(SCAN_TRADING_DAYS, len(series))
            base = float(series.iloc[-base_idx])
            now_px = float(c["price"])
            if not (math.isfinite(base) and math.isfinite(now_px)) or base == 0:
                continue
            c["return"] = round((now_px - base) / base, 6)
            filled += 1
        except Exception:
            continue

    return filled


# ---------------------------------------------------------------------------
# 3) 매크로 지표 — yfinance fast_info
# ---------------------------------------------------------------------------

# Ticker 별 마지막 성공값 메모리 캐시 — Yahoo 일시 차단 시 stale fallback 으로 사용.
_macro_value_cache: dict[str, dict] = {}


def _fetch_macro_value(ticker: str, decimals: int) -> dict:
    """
    yfinance fast_info 로 지표 현재값·변동·변동률을 조회한다.
    실패(SSL 에러·차단·NoneType 등)하면 마지막 성공값을 stale=True 로 반환해
    화면에서 직전 값이 사라지지 않게 한다.
    """
    from services.yf_limiter import throttled

    try:
        fi = throttled(lambda: yf.Ticker(ticker).fast_info)
        if fi is None:
            raise ValueError("fast_info returned None (yfinance 차단 의심)")

        # fast_info 는 LazyDict — 키 접근 자체가 lazy fetch 라 try 안에서 보호
        price = fi.get("lastPrice")
        prev_close = fi.get("previousClose")

        if price is None or not math.isfinite(price):
            raise ValueError("invalid lastPrice")

        value = round(price, decimals)
        change = None
        pct = None

        if prev_close is not None and math.isfinite(prev_close) and prev_close != 0:
            raw_change = price - prev_close
            change = round(raw_change, decimals)
            pct = round(raw_change / prev_close, 4)

        result = {"value": value, "change": change, "pct": pct, "stale": False}
        _macro_value_cache[ticker] = result
        return result
    except Exception as e:
        cached = _macro_value_cache.get(ticker)
        if cached is not None:
            logger.warning(
                "매크로 지표 조회 실패 (%s): %s — 직전 성공값(stale)으로 대체", ticker, e,
            )
            return {**cached, "stale": True}
        logger.warning("매크로 지표 조회 실패 (%s, 캐시 없음): %s", ticker, e)
        return {"value": None, "change": None, "pct": None, "stale": False}


def _fetch_macro_values_batch(ticker_decimals: dict[str, int]) -> dict[str, dict]:
    """
    매크로 지표 ticker 들을 1회 yf.download batch 로 모두 조회한다.

    개선 전: 14개 ticker × fast_info 호출 = 14회 yfinance 왕복 (각 ~수백 ms).
    개선 후: 1회 batch download (~1초 미만) + 종가/직전 종가 계산.

    fast_info 의 lastPrice 는 분봉 마지막 값에 가깝지만, 매크로는 1분 주기로 갱신되므로
    daily 종가로도 사용자 체감 차이는 미미하다. batch 실패 시 ticker 별 _fetch_macro_value
    폴백으로 가용성 유지.
    """
    if not ticker_decimals:
        return {}

    from services.yf_limiter import throttled

    tickers = list(ticker_decimals.keys())
    out: dict[str, dict] = {}

    try:
        data = throttled(
            yf.download,
            tickers,
            period="5d",
            interval="1d",
            group_by="ticker",
            progress=False,
            threads=True,
            auto_adjust=False,
        )
    except Exception as e:
        logger.warning("매크로 batch download 실패 — ticker 별 폴백: %s", e)
        # batch 실패 시 ticker 별 fast_info 폴백
        for ticker, decimals in ticker_decimals.items():
            out[ticker] = _fetch_macro_value(ticker, decimals)
        return out

    if data is None or data.empty:
        # batch 결과가 비어있으면 ticker 별 폴백
        for ticker, decimals in ticker_decimals.items():
            out[ticker] = _fetch_macro_value(ticker, decimals)
        return out

    is_single = len(tickers) == 1

    for ticker, decimals in ticker_decimals.items():
        try:
            if is_single:
                ticker_data = data
            else:
                if isinstance(data.columns, pd.MultiIndex):
                    level0 = data.columns.get_level_values(0)
                    if ticker not in set(level0):
                        out[ticker] = _fetch_macro_value(ticker, decimals)
                        continue
                ticker_data = data[ticker]

            close_series = ticker_data["Close"].dropna()
            if close_series.empty:
                out[ticker] = _fetch_macro_value(ticker, decimals)
                continue

            price = float(close_series.iloc[-1])
            if not math.isfinite(price):
                out[ticker] = _fetch_macro_value(ticker, decimals)
                continue

            value = round(price, decimals)
            change = None
            pct = None
            if len(close_series) >= 2:
                prev_close = float(close_series.iloc[-2])
                if math.isfinite(prev_close) and prev_close != 0:
                    raw_change = price - prev_close
                    change = round(raw_change, decimals)
                    pct = round(raw_change / prev_close, 4)

            result = {"value": value, "change": change, "pct": pct, "stale": False}
            _macro_value_cache[ticker] = result
            out[ticker] = result
        except Exception as e:
            logger.debug("매크로 batch parse 실패 (%s): %s — ticker 별 폴백", ticker, e)
            out[ticker] = _fetch_macro_value(ticker, decimals)

    return out


def fetch_macro_indicators() -> dict:
    """
    yfinance로 글로벌 매크로 지표를 수집하여 marquee/sidebar 구조로 반환한다.

    Hot path: 1분 주기 macro loop 에서 호출. 14개 ticker 를 1회 yf.download batch 로
    묶어 호출 비용을 14배 줄였다.
    """
    # 모든 unique ticker 와 decimals 매핑
    ticker_decimals: dict[str, int] = {}
    for group_def in (MACRO_MARQUEE, MACRO_SIDEBAR):
        for ind in group_def:
            ticker_decimals.setdefault(ind["ticker"], ind["decimals"])

    fetched = _fetch_macro_values_batch(ticker_decimals)

    result: dict = {"marquee": [], "sidebar": []}
    for group_def, key in [(MACRO_MARQUEE, "marquee"), (MACRO_SIDEBAR, "sidebar")]:
        for ind in group_def:
            data = fetched.get(ind["ticker"]) or {"value": None, "change": None, "pct": None, "stale": False}
            result[key].append({
                "name": ind["name"],
                "value": data["value"],
                "change": data["change"],
                "pct": data["pct"],
                "stale": data.get("stale", False),
            })

    if not any(item["value"] is not None for items in result.values() for item in items):
        return MACRO_FALLBACK

    return result


# ---------------------------------------------------------------------------
# 4) 시장 분위기 게이지 (VIX 기반 0~100)
# ---------------------------------------------------------------------------

def get_market_gauge(macro: dict) -> dict:
    """
    macro의 VIX 값을 사용해 시장 분위기 게이지를 계산한다.

    - market_gauge: 0(공포) ~ 100(과열/탐욕). VIX 낮을수록 게이지 높음.
    - 공식:
        score = 100 - (log(vix_clamped / MIN_VIX) / log(MAX_VIX / MIN_VIX)) * 100
      (MIN_VIX~MAX_VIX로 클램프한 뒤 로그 스케일로 0~100으로 매핑)

    Returns:
        {"market_gauge": int | None, "vix": float | None}
    """
    vix_val = None
    for item in (macro.get("sidebar") or []) + (macro.get("marquee") or []):
        if item.get("name") == "VIX" and item.get("value") is not None:
            try:
                vix_val = float(item["value"])
                break
            except (TypeError, ValueError):
                pass

    if vix_val is None or not math.isfinite(vix_val):
        return {"market_gauge": None, "vix": None}

    try:
        vix_clamped = max(float(MIN_VIX), min(float(MAX_VIX), float(vix_val)))
        denom = math.log(float(MAX_VIX) / float(MIN_VIX))
        if denom == 0:
            return {"market_gauge": None, "vix": round(vix_val, 2)}

        score = 100.0 - (math.log(vix_clamped / float(MIN_VIX)) / denom) * 100.0
        gauge = int(round(max(0.0, min(100.0, score))))
        return {"market_gauge": gauge, "vix": round(vix_val, 2)}
    except Exception:
        return {"market_gauge": None, "vix": round(vix_val, 2)}
