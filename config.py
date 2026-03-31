from dotenv import load_dotenv
import os

load_dotenv()

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5")

# Strategist (Market Strategy) OpenAI model
STRATEGIST_OPENAI_MODEL = os.getenv("STRATEGIST_OPENAI_MODEL", "gpt-5")
STRATEGIST_OPENAI_TIMEOUT_SEC = int(os.getenv("STRATEGIST_OPENAI_TIMEOUT_SEC", "20"))
# asyncio.wait_for는 클라이언트 timeout보다 약간 여유를 둔다.
STRATEGIST_OPENAI_THREAD_BUFFER_SEC = int(os.getenv("STRATEGIST_OPENAI_THREAD_BUFFER_SEC", "2"))
# OpenAI 실패 시 fallback top_picks 개수 (스캔 rows 앞에서 N개)
STRATEGIST_FALLBACK_TOP_PICKS_N = int(os.getenv("STRATEGIST_FALLBACK_TOP_PICKS_N", "2"))

# Scanner
MIN_VOLUME = 1_000_000
SCAN_TOP_N = 15
REPORT_TOP_N = 2

# Cycle
SCAN_INTERVAL_SEC = 3600
ERROR_RETRY_SEC = 60

# yfinance 분봉 시세 (스캔과 별도 — top/radar 종목만 자주 갱신)
PRICE_TICK_INTERVAL_SEC = int(os.getenv("PRICE_TICK_INTERVAL_SEC", "60"))
PRICE_TICK_MAX_SYMBOLS = int(os.getenv("PRICE_TICK_MAX_SYMBOLS", "120"))
# 1m은 호출 부담이 크므로 기본 5m (장중 마지막 봉 기준으로 체감 갱신)
PRICE_INTRADAY_INTERVAL = os.getenv("PRICE_INTRADAY_INTERVAL", "5m")
PRICE_DOWNLOAD_BATCH_SIZE = int(os.getenv("PRICE_DOWNLOAD_BATCH_SIZE", "50"))
# Stock detail: 회사명 조회 timeout (yfinance Ticker.info)
STOCK_PROFILE_TIMEOUT_SEC = float(os.getenv("STOCK_PROFILE_TIMEOUT_SEC", "6"))

# Strategist (Market Strategy)
# - 최신 스캔 사이클 데이터 추출 창(window) 크기
STRATEGIST_LATEST_SCAN_WINDOW_MINUTES = 90
# - 전략 브리핑 OpenAI 응답 캐시 TTL (1시간)
STRATEGIST_CACHE_TTL_SEC = 3600
# - yfinance Ticker.info(섹터) 호출은 타임아웃 위험이 있어
#   요청당 최대 호출 개수로 제한하고, 결과는 프로세스 내 캐싱한다.
STRATEGIST_MAX_YFINANCE_SECTOR_CALLS_PER_REQUEST = 20
# - yfinance Ticker.info 호출 대기 제한(초)
STRATEGIST_YFINANCE_SECTOR_TIMEOUT_SEC = 8

# - OpenAI temperature
STRATEGIST_TEMPERATURE = 0.3

# - 섹터 정렬 시 fallback divergence 값
STRATEGIST_DIVERGENCE_FALLBACK = 0.0

# - 뉴스 다이제스트: LLM에 전달할 주요 헤드라인 수
STRATEGIST_NEWS_TOP_N = int(os.getenv("STRATEGIST_NEWS_TOP_N", "10"))
# - 경제 캘린더 다이제스트
STRATEGIST_ECON_UPCOMING_HOURS = int(os.getenv("STRATEGIST_ECON_UPCOMING_HOURS", "48"))
STRATEGIST_ECON_LOOKBACK_HOURS = int(os.getenv("STRATEGIST_ECON_LOOKBACK_HOURS", "24"))
STRATEGIST_ECON_MIN_IMPORTANCE = int(os.getenv("STRATEGIST_ECON_MIN_IMPORTANCE", "2"))
STRATEGIST_ECON_MAX_UPCOMING = int(os.getenv("STRATEGIST_ECON_MAX_UPCOMING", "10"))
STRATEGIST_ECON_MAX_SURPRISES = int(os.getenv("STRATEGIST_ECON_MAX_SURPRISES", "5"))

# Signal thresholds (감성 괴리 fallback용)
BUY_THRESHOLD = 0.25
SELL_THRESHOLD = -0.25

# Earnings Surprise 시그널 문턱
# surprisePercent: (실제 EPS - 예상 EPS) / |예상 EPS|, 소수 (0.05 = 5%)
EARNINGS_BUY_PCT = 0.05
EARNINGS_SELL_PCT = -0.05
# Yahoo quoteSummary 연속 호출 완화(초). 0이면 대기 없음.
EARNINGS_INTER_REQUEST_DELAY_SEC = float(os.getenv("EARNINGS_INTER_REQUEST_DELAY_SEC", "0"))

# FinBERT Sentiment
SENTIMENT_MAX_HEADLINES = 10
# Finviz 스크래핑: 동시 요청·간격·429 재시도 (과도한 병렬은 429 유발)
SENTIMENT_FINVIZ_MAX_CONCURRENT = int(os.getenv("SENTIMENT_FINVIZ_MAX_CONCURRENT", "3"))
SENTIMENT_FINVIZ_DELAY_SEC = float(os.getenv("SENTIMENT_FINVIZ_DELAY_SEC", "0.25"))
SENTIMENT_FINVIZ_MAX_RETRIES = int(os.getenv("SENTIMENT_FINVIZ_MAX_RETRIES", "4"))
SENTIMENT_FINVIZ_RETRY_BASE_SEC = float(os.getenv("SENTIMENT_FINVIZ_RETRY_BASE_SEC", "1.5"))

# News Feed
NEWS_FEED_MAX_ITEMS = 30
NEWS_FEED_TTL_SEC = 300

# Economic Calendar (myfxbook 크롤링)
ECON_CALENDAR_TTL_SEC = int(os.getenv("ECON_CALENDAR_TTL_SEC", "600"))  # 10분
ECON_CALENDAR_INTERVAL_SEC = int(os.getenv("ECON_CALENDAR_INTERVAL_SEC", "600"))  # 10분 주기 크롤링
ECON_CALENDAR_TIMEOUT_SEC = float(os.getenv("ECON_CALENDAR_TIMEOUT_SEC", "12"))
ECON_CALENDAR_MAX_ITEMS = int(os.getenv("ECON_CALENDAR_MAX_ITEMS", "500"))

# News article crawling (from yfinance news url)
NEWS_ARTICLE_CACHE_TTL_SEC = int(os.getenv("NEWS_ARTICLE_CACHE_TTL_SEC", "21600"))  # 6h
NEWS_CRAWL_TIMEOUT_SEC = float(os.getenv("NEWS_CRAWL_TIMEOUT_SEC", "12"))
NEWS_CRAWL_MAX_CONCURRENT = int(os.getenv("NEWS_CRAWL_MAX_CONCURRENT", "3"))
NEWS_ARTICLE_MAX_CHARS = int(os.getenv("NEWS_ARTICLE_MAX_CHARS", "20000"))

# News analysis (Korean summary + market impact)
NEWS_ANALYSIS_OPENAI_MODEL = os.getenv("NEWS_ANALYSIS_OPENAI_MODEL", "gpt-5")
# 주 모델이 400(파라미터/엔드포인트 불일치)일 때 한 번 더 시도할 모델
NEWS_ANALYSIS_FALLBACK_OPENAI_MODEL = os.getenv(
    "NEWS_ANALYSIS_FALLBACK_OPENAI_MODEL", "gpt-5"
)
NEWS_ANALYSIS_TIMEOUT_SEC = int(os.getenv("NEWS_ANALYSIS_TIMEOUT_SEC", "18"))
NEWS_ANALYSIS_THREAD_BUFFER_SEC = int(os.getenv("NEWS_ANALYSIS_THREAD_BUFFER_SEC", "2"))
NEWS_ANALYSIS_TEMPERATURE = float(os.getenv("NEWS_ANALYSIS_TEMPERATURE", "0.2"))
# LLM 입력으로 넣는 본문 최대 길이 (토큰/요청 크기 완화)
NEWS_ANALYSIS_INPUT_MAX_CHARS = int(os.getenv("NEWS_ANALYSIS_INPUT_MAX_CHARS", "12000"))

# Macro Indicators
MACRO_INTERVAL_SEC = 300

# yfinance 심볼 컨벤션:
#   인덱스  → ^GSPC(S&P500), ^IXIC(NASDAQ), ^DJI(DOW), ^VIX
#   FX      → USDKRW=X, USDJPY=X, EURUSD=X
#   크립토  → BTC-USD
#   국채    → ^TNX (US 10Y Treasury Yield)
MACRO_MARQUEE = [
    {"id": "sp500", "name": "S&P 500", "ticker": "^GSPC", "decimals": 2},
    {"id": "nasdaq", "name": "NASDAQ", "ticker": "^IXIC", "decimals": 2},
]

MACRO_SIDEBAR = [
    {"id": "dow", "name": "DOW", "ticker": "^DJI", "decimals": 2},
    {"id": "sp500", "name": "S&P 500", "ticker": "^GSPC", "decimals": 2},
    {"id": "nasdaq", "name": "NASDAQ", "ticker": "^IXIC", "decimals": 2},
    {"id": "usd_krw", "name": "USD/KRW", "ticker": "USDKRW=X", "decimals": 2},
    {"id": "usd_jpy", "name": "USD/JPY", "ticker": "USDJPY=X", "decimals": 2},
    {"id": "eur_usd", "name": "EUR/USD", "ticker": "EURUSD=X", "decimals": 4},
    {"id": "vix", "name": "VIX", "ticker": "^VIX", "decimals": 2},
    {"id": "us_10y", "name": "US 10Y", "ticker": "^TNX", "decimals": 3},
    {"id": "btc_usd", "name": "BTC/USD", "ticker": "BTC-USD", "decimals": 2},
]

MACRO_FALLBACK = {
    "marquee": [{"name": d["name"], "value": None, "change": None, "pct": None} for d in MACRO_MARQUEE],
    "sidebar": [{"name": d["name"], "value": None, "change": None, "pct": None} for d in MACRO_SIDEBAR],
}

NEWS_FALLBACK_TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "TSLA", "META", "AMZN", "AMD",
]

# Strategist: 대표 섹터 매핑(하드코딩)
# yfinance(Ticker.info) 호출을 줄이기 위한 1차 매핑이다.
# 매핑이 없는 티커는 Unknown 또는(캐시/제한된 yfinance 호출 후) 채워진다.
STRATEGIST_TICKER_SECTOR_MAP: dict[str, str] = {
    # Technology
    "AAPL": "Technology",
    "MSFT": "Technology",
    "NVDA": "Technology",
    "AMD": "Technology",
    # Communication Services (실제 섹터 분류는 데이터 소스에 따라 다를 수 있음)
    "GOOGL": "Communication Services",
    "GOOG": "Communication Services",
    "META": "Communication Services",
    # Consumer / Discretionary
    "TSLA": "Consumer Cyclical",
    "AMZN": "Consumer Cyclical",
}

# Market gauge (VIX -> 0~100)
# VIX를 MIN_VIX~MAX_VIX로 클램프 후 로그 스케일로 0~100으로 변환
MIN_VIX = 10.0
MAX_VIX = 80.0
