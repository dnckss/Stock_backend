from __future__ import annotations

import logging
import math
from io import StringIO

import pandas as pd
import requests
import yfinance as yf

from config import (
    MIN_VOLUME,
    SCAN_TOP_N,
    MACRO_MARQUEE,
    MACRO_SIDEBAR,
    MACRO_FALLBACK,
    MIN_VIX,
    MAX_VIX,
    PRICE_DOWNLOAD_BATCH_SIZE,
    PRICE_INTRADAY_INTERVAL,
)

logger = logging.getLogger(__name__)

_WIKI_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_WIKI_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}


# ---------------------------------------------------------------------------
# 1) 티커 수집 — Wikipedia S&P 500 구성종목
# ---------------------------------------------------------------------------

def get_all_tickers() -> list[str]:
    """
    Wikipedia에서 S&P 500 구성종목 티커를 수집한다.
    실패 시 빈 리스트를 반환하며 호출부에서 적절히 처리한다.
    """
    try:
        resp = requests.get(_WIKI_SP500_URL, headers=_WIKI_HEADERS, timeout=15)
        resp.raise_for_status()
        tables = pd.read_html(StringIO(resp.text))
        df = tables[0]
        raw_tickers = df["Symbol"].tolist()
        tickers = sorted({t.strip().replace(".", "-") for t in raw_tickers if isinstance(t, str) and t.strip()})
        print(f"S&P 500 티커 {len(tickers)}개 수집 완료")
        return tickers
    except requests.RequestException as e:
        print(f"Wikipedia 네트워크 에러: {e}")
    except (ValueError, KeyError) as e:
        print(f"Wikipedia 파싱 에러: {e}")
    except Exception as e:
        print(f"티커 수집 실패: {e}")

    return []


# ---------------------------------------------------------------------------
# 2) 종목 스캔 — yf.download() 배치 다운로드
# ---------------------------------------------------------------------------

_DOWNLOAD_BATCH_SIZE = 100


def _compute_return(series: pd.Series) -> float | None:
    """Close 시리즈에서 최근 5일 수익률을 계산한다. 유효하지 않으면 None."""
    valid = series.dropna()
    if len(valid) < 2:
        return None
    first, last = float(valid.iloc[0]), float(valid.iloc[-1])
    if first == 0:
        return None
    ret = (last - first) / first
    return ret if math.isfinite(ret) else None


def scan_stocks(tickers: list[str]) -> list[dict]:
    """
    yf.download()로 5일치 종가·거래량을 배치 조회하고,
    거래량 필터를 적용한 뒤, 변동률 기준으로 정렬된 전체 유효 종목 리스트를 반환한다.
    """
    if not tickers:
        return []

    candidates: list[dict] = []

    for i in range(0, len(tickers), _DOWNLOAD_BATCH_SIZE):
        batch = tickers[i : i + _DOWNLOAD_BATCH_SIZE]
        try:
            data = yf.download(
                batch,
                period="5d",
                interval="1d",
                group_by="ticker",
                progress=False,
                threads=True,
            )
        except Exception as e:
            print(f"yf.download 에러 (batch {i}): {e}")
            continue

        if data.empty:
            continue

        is_single = len(batch) == 1
        for ticker in batch:
            try:
                if is_single:
                    ticker_data = data
                else:
                    if ticker not in data.columns.get_level_values(0):
                        continue
                    ticker_data = data[ticker]

                close_series = ticker_data["Close"].dropna()
                volume_series = ticker_data["Volume"].dropna()

                if close_series.empty or volume_series.empty:
                    continue

                last_volume = int(volume_series.iloc[-1])
                if last_volume < MIN_VOLUME:
                    continue

                ret = _compute_return(close_series)
                if ret is None:
                    continue

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

                candidates.append({
                    "ticker": ticker,
                    "return": round(ret, 6),
                    "price": round(float(close_series.iloc[-1]), 2),
                    "volume": last_volume,
                    "daily": daily_bars,
                })
            except Exception:
                continue

    candidates.sort(key=lambda x: abs(x["return"]), reverse=True)
    print(f"스캔 결과: {len(candidates)}개 유효 종목")
    return candidates


def refresh_intraday_prices(tickers: list[str]) -> dict[str, dict]:
    """
    yfinance 분봉(기본 5m)으로 당일 구간의 마지막 봉 종가·거래량·시각을 조회한다.
    스캔 루프와 별도로 호출해 체감상 더 자주 시세를 갱신할 때 사용한다.

    Returns:
        { "AAPL": {"price": float, "volume": int|None, "as_of": str}, ... }
    """
    if not tickers:
        return {}

    out: dict[str, dict] = {}
    batch_size = max(1, PRICE_DOWNLOAD_BATCH_SIZE)

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        try:
            data = yf.download(
                batch,
                period="1d",
                interval=PRICE_INTRADAY_INTERVAL,
                group_by="ticker",
                progress=False,
                threads=False,
            )
        except Exception as e:
            logger.warning("분봉 시세 yf.download 실패 (batch %s): %s", i, e)
            continue

        if data.empty:
            continue

        is_single = len(batch) == 1
        for ticker in batch:
            key = (ticker or "").upper().strip()
            if not key:
                continue
            try:
                if is_single:
                    ticker_data = data
                else:
                    if key not in data.columns.get_level_values(0):
                        continue
                    ticker_data = data[key]

                close_series = ticker_data["Close"].dropna()
                volume_series = ticker_data["Volume"].dropna()
                if close_series.empty:
                    continue

                last_px = float(close_series.iloc[-1])
                if not math.isfinite(last_px):
                    continue

                last_ts = close_series.index[-1]
                as_of = (
                    last_ts.strftime("%Y-%m-%d %H:%M:%S")
                    if hasattr(last_ts, "strftime")
                    else str(last_ts)
                )

                last_vol: int | None = None
                if not volume_series.empty:
                    try:
                        v = int(volume_series.iloc[-1])
                        last_vol = v if math.isfinite(v) else None
                    except (TypeError, ValueError):
                        last_vol = None

                out[key] = {
                    "price": round(last_px, 2),
                    "volume": last_vol,
                    "as_of": as_of,
                }
            except Exception:
                continue

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
        if u.get("volume") is not None:
            c["volume"] = u["volume"]
        c["quote_as_of"] = u["as_of"]

        # 기존 5일봉의 첫 종가를 기준으로, 최신 분봉 price에 맞춰 return 갱신
        try:
            daily = c.get("daily") or []
            if daily and isinstance(daily, list):
                base_close = daily[0].get("close")
                if base_close is not None:
                    base = float(base_close)
                    now_px = float(u["price"])
                    if math.isfinite(base) and math.isfinite(now_px) and base != 0:
                        c["return"] = round((now_px - base) / base, 6)
        except Exception:
            # return 재계산 실패는 무시하고 기존 값을 유지한다.
            pass


# ---------------------------------------------------------------------------
# 3) 매크로 지표 — yfinance fast_info
# ---------------------------------------------------------------------------

def _fetch_macro_value(ticker: str, decimals: int) -> dict:
    """yfinance fast_info로 지표 현재값·변동·변동률을 조회한다."""
    try:
        fi = yf.Ticker(ticker).fast_info
        price = fi.get("lastPrice")
        prev_close = fi.get("previousClose")

        if price is None or not math.isfinite(price):
            return {"value": None, "change": None, "pct": None}

        value = round(price, decimals)
        change = None
        pct = None

        if prev_close and math.isfinite(prev_close) and prev_close != 0:
            raw_change = price - prev_close
            change = round(raw_change, decimals)
            pct = round(raw_change / prev_close, 4)

        return {"value": value, "change": change, "pct": pct}
    except Exception as e:
        print(f"매크로 지표 조회 실패 ({ticker}): {e}")
        return {"value": None, "change": None, "pct": None}


def fetch_macro_indicators() -> dict:
    """
    yfinance로 글로벌 매크로 지표를 수집하여 marquee/sidebar 구조로 반환한다.
    """
    seen: dict[str, dict] = {}
    result: dict = {"marquee": [], "sidebar": []}

    for group_def, key in [(MACRO_MARQUEE, "marquee"), (MACRO_SIDEBAR, "sidebar")]:
        for ind in group_def:
            ticker = ind["ticker"]
            decimals = ind["decimals"]

            if ticker not in seen:
                seen[ticker] = _fetch_macro_value(ticker, decimals)

            data = seen[ticker]
            result[key].append({
                "name": ind["name"],
                "value": data["value"],
                "change": data["change"],
                "pct": data["pct"],
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
