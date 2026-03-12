import math
import pandas as pd
import requests
import yfinance as yf
from config import (
    MIN_VOLUME, SCAN_PERIOD, SCAN_TOP_N, DOWNLOAD_BATCH_SIZE, INDIVIDUAL_RETRY_LIMIT,
    MACRO_MARQUEE, MACRO_SIDEBAR, MACRO_FALLBACK,
)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

FALLBACK_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA",
    "BRK-B", "JPM", "V", "UNH", "XOM", "MA", "JNJ", "PG", "HD",
    "AVGO", "COST", "MRK", "ABBV", "KO", "PEP", "WMT", "LLY",
    "AMD", "CRM", "NFLX", "ADBE", "ORCL", "CSCO", "INTC", "QCOM",
    "TXN", "INTU", "AMAT", "MU", "LRCX", "KLAC", "SNPS", "CDNS",
    "BA", "CAT", "GE", "DIS", "NKE", "SBUX", "MCD", "UPS",
]


def _fetch_indicator(ticker: str, decimals: int) -> dict | None:
    """단일 매크로 지표를 수집하여 {name, value, change, pct} 형태로 반환한다."""
    try:
        hist = yf.Ticker(ticker).history(period="5d")
        if hist.empty:
            return None
        closes = hist["Close"].dropna()
        if len(closes) < 2:
            return None
        current = float(closes.iloc[-1])
        prev = float(closes.iloc[-2])
        if not math.isfinite(current) or not math.isfinite(prev) or prev == 0:
            return None
        change = current - prev
        pct = change / prev
        return {
            "value": round(current, decimals),
            "change": round(change, decimals),
            "pct": round(pct, 4),
        }
    except Exception:
        return None


def fetch_macro_indicators() -> dict:
    """
    글로벌 매크로 지표를 수집하여 marquee/sidebar 구조로 반환한다.
    marquee: 상단 마키 (S&P 500, NASDAQ)
    sidebar: 좌측 사이드바 (VIX, US 10Y, DXY, BTC, GOLD, WTI)
    """
    result: dict = {"marquee": [], "sidebar": []}

    for group_def, key in [(MACRO_MARQUEE, "marquee"), (MACRO_SIDEBAR, "sidebar")]:
        for ind in group_def:
            data = _fetch_indicator(ind["ticker"], ind["decimals"])
            if data is not None:
                result[key].append({"name": ind["name"], **data})

    if not result["marquee"] and not result["sidebar"]:
        return MACRO_FALLBACK
    return result


def _scrape_wikipedia(url: str, possible_cols: list) -> list:
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=15)
        resp.raise_for_status()
        tables = pd.read_html(resp.text)
        for table in tables:
            for col in possible_cols:
                if col in table.columns:
                    return (
                        table[col]
                        .astype(str)
                        .str.replace(".", "-", regex=False)
                        .str.strip()
                        .tolist()
                    )
    except Exception:
        pass
    return []


def get_all_tickers() -> list:
    """S&P 500 + S&P 400 + NASDAQ-100 을 합쳐 1,000개 이상의 고유 티커를 확보한다."""
    sources = [
        (
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            ["Symbol", "Ticker"],
        ),
        (
            "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
            ["Symbol", "Ticker symbol", "Ticker"],
        ),
        (
            "https://en.wikipedia.org/wiki/Nasdaq-100",
            ["Ticker", "Symbol", "Company"],
        ),
    ]

    tickers: set = set()
    for url, cols in sources:
        scraped = _scrape_wikipedia(url, cols)
        tickers.update(scraped)
        print(f"  -> {url.split('/')[-1]}: {len(scraped)}개 수집")

    if len(tickers) < 100:
        tickers.update(FALLBACK_TICKERS)

    result = sorted(tickers)
    print(f"📋 총 {len(result)}개 고유 티커 확보 완료")
    return result


def _extract_return(close: pd.Series, volume: pd.Series) -> float | None:
    """종가/거래량 시리즈에서 수익률을 계산한다. 필터 미통과 시 None 반환."""
    if volume.empty or volume.mean() < MIN_VOLUME:
        return None
    if len(close) < 2:
        return None
    ret = float((close.iloc[-1] - close.iloc[0]) / close.iloc[0])
    return ret if math.isfinite(ret) else None


def _bulk_download(batch: list) -> tuple[list, set]:
    """yf.download 벌크 다운로드 후 (스캔결과, 처리완료 set) 반환."""
    scanned = []
    processed = set()

    try:
        data = yf.download(
            tickers=batch,
            period=SCAN_PERIOD,
            group_by="ticker",
            threads=True,
            progress=False,
        )
        if data.empty:
            return scanned, processed

        is_single = len(batch) == 1
        for t in batch:
            try:
                close = data["Close"].dropna() if is_single else data[t]["Close"].dropna()
                vol = data["Volume"].dropna() if is_single else data[t]["Volume"].dropna()
                ret = _extract_return(close, vol)
                processed.add(t)
                if ret is not None:
                    scanned.append({"ticker": t, "return": ret})
            except Exception:
                continue
    except Exception:
        pass

    return scanned, processed


def _individual_download(ticker: str) -> dict | None:
    """단일 종목 개별 다운로드 폴백."""
    try:
        df = yf.Ticker(ticker).history(period=SCAN_PERIOD)
        if df.empty:
            return None
        close = df["Close"].dropna()
        vol = df["Volume"].dropna()
        ret = _extract_return(close, vol)
        if ret is not None:
            return {"ticker": ticker, "return": ret}
    except Exception:
        pass
    return None


def scan_stocks(tickers: list) -> list:
    """
    전 종목 스캔 파이프라인:
    1) 배치 벌크 다운로드 (DOWNLOAD_BATCH_SIZE 단위)
    2) 실패 종목 개별 폴백 (최대 INDIVIDUAL_RETRY_LIMIT개)
    3) 거래량 < MIN_VOLUME 잡주 필터링
    4) 변동률 절대값 상위 SCAN_TOP_N개 반환
    """
    scanned = []
    processed = set()

    for i in range(0, len(tickers), DOWNLOAD_BATCH_SIZE):
        batch = tickers[i : i + DOWNLOAD_BATCH_SIZE]
        batch_scanned, batch_processed = _bulk_download(batch)
        scanned.extend(batch_scanned)
        processed.update(batch_processed)

    failed = [t for t in tickers if t not in processed]
    if failed:
        retry_list = failed[:INDIVIDUAL_RETRY_LIMIT]
        print(f"🔄 {len(retry_list)}개 종목 개별 재시도 중...")
        for t in retry_list:
            result = _individual_download(t)
            if result:
                scanned.append(result)

    scanned.sort(key=lambda x: abs(x["return"]), reverse=True)
    candidates = scanned[:SCAN_TOP_N]
    print(f"📊 스캔 결과: {len(scanned)}개 유효 종목 중 Top {len(candidates)}개 선별")
    return candidates
