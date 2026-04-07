from __future__ import annotations

import logging
import math
from datetime import datetime
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
    SP500_WIKI_URL,
    SP500_WIKI_HEADERS,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1) 티커 수집 — Wikipedia S&P 500 구성종목
# ---------------------------------------------------------------------------

def get_sp500_constituents() -> list[dict[str, str]]:
    """Wikipedia에서 S&P 500 구성종목(ticker, name, sector)을 가져온다.

    Returns:
        [{"ticker": "AAPL", "name": "Apple Inc.", "sector": "Information Technology"}, ...]
        실패 시 빈 리스트.
    """
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
        return rows
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
    yfinance fast_info로 종목별 최신 가격을 조회한다.
    fast_info는 장중/장후 모두 최신 가격을 반환하므로 분봉보다 신뢰성이 높다.

    Returns:
        { "AAPL": {"price": float, "volume": int|None, "as_of": str}, ... }
    """
    if not tickers:
        return {}

    out: dict[str, dict] = {}
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    from services.yf_limiter import throttled

    for ticker in tickers:
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
    from services.yf_limiter import throttled

    try:
        fi = throttled(lambda: yf.Ticker(ticker).fast_info)
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
