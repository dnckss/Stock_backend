import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "quant_trading.db")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5")

# Scanner
MIN_VOLUME = 1_000_000
SCAN_TOP_N = 15
REPORT_TOP_N = 2
SCAN_PERIOD = "5d"
DOWNLOAD_BATCH_SIZE = 100
INDIVIDUAL_RETRY_LIMIT = 50

# Cycle
SCAN_INTERVAL_SEC = 3600
ERROR_RETRY_SEC = 60

# Signal thresholds
BUY_THRESHOLD = 0.25
SELL_THRESHOLD = -0.25

# Sentiment
POSITIVE_KEYWORDS = [
    "bull", "surge", "buy", "up", "growth", "positive",
    "beat", "strong", "rally", "gain", "upgrade", "outperform",
    "record", "soar", "boom", "breakout",
]
NEGATIVE_KEYWORDS = [
    "bear", "drop", "sell", "down", "fall", "negative",
    "miss", "weak", "crash", "loss", "downgrade", "underperform",
    "plunge", "slump", "cut", "warning",
]
SENTIMENT_WEIGHT = 0.15
SENTIMENT_MAX_HEADLINES = 10

# News Feed
NEWS_FEED_MAX_ITEMS = 30
NEWS_PER_TICKER = 6
NEWS_FEED_TTL_SEC = 300

# Macro Indicators
MACRO_INTERVAL_SEC = 300

MACRO_MARQUEE = [
    {"id": "sp500", "name": "S&P 500", "ticker": "^GSPC", "decimals": 2},
    {"id": "nasdaq", "name": "NASDAQ", "ticker": "^IXIC", "decimals": 2},
]

MACRO_SIDEBAR = [
    # US Major Indices (3대 지수)
    {"id": "dow", "name": "DOW", "ticker": "^DJI", "decimals": 2},
    {"id": "sp500", "name": "S&P 500", "ticker": "^GSPC", "decimals": 2},
    {"id": "nasdaq", "name": "NASDAQ", "ticker": "^IXIC", "decimals": 2},
    # FX
    {"id": "usd_krw", "name": "USD/KRW", "ticker": "KRW=X", "decimals": 2},
    {"id": "usd_jpy", "name": "USD/JPY", "ticker": "JPY=X", "decimals": 2},
    {"id": "eur_usd", "name": "EUR/USD", "ticker": "EURUSD=X", "decimals": 4},
    # Vol / Rates / Risk
    {"id": "vix", "name": "VIX", "ticker": "^VIX", "decimals": 2},
    {"id": "us_10y", "name": "US 10Y", "ticker": "^TNX", "decimals": 3},
    {"id": "dxy", "name": "DXY", "ticker": "DX-Y.NYB", "decimals": 2},
    {"id": "btc_usd", "name": "BTC/USD", "ticker": "BTC-USD", "decimals": 2},
    {"id": "gold", "name": "GOLD", "ticker": "GC=F", "decimals": 2},
    {"id": "wti", "name": "WTI CRUDE", "ticker": "CL=F", "decimals": 2},
]

# yfinance 연결 실패 시 프론트에 내려줄 기본 구조 (value/change/pct는 null → "—" 표시용)
MACRO_FALLBACK = {
    "marquee": [{"name": d["name"], "value": None, "change": None, "pct": None} for d in MACRO_MARQUEE],
    "sidebar": [{"name": d["name"], "value": None, "change": None, "pct": None} for d in MACRO_SIDEBAR],
}
