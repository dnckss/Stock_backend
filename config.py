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
STRATEGIST_OPENAI_TIMEOUT_SEC = int(os.getenv("STRATEGIST_OPENAI_TIMEOUT_SEC", "90"))
# asyncio.wait_for는 클라이언트 timeout보다 약간 여유를 둔다.
STRATEGIST_OPENAI_THREAD_BUFFER_SEC = int(os.getenv("STRATEGIST_OPENAI_THREAD_BUFFER_SEC", "2"))
# OpenAI 실패 시 fallback top_picks 개수 (스캔 rows 앞에서 N개)
STRATEGIST_FALLBACK_TOP_PICKS_N = int(os.getenv("STRATEGIST_FALLBACK_TOP_PICKS_N", "2"))

# Scanner
MIN_VOLUME = 1_000_000
SCAN_TOP_N = 15
SCAN_TRADING_DAYS = 5  # 등락률 계산 기준 거래일 수
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

# ---------------------------------------------------------------------------
# Stock Fundamentals (종목 펀더멘털)
# ---------------------------------------------------------------------------
FUNDAMENTALS_MAX_QUARTERS = int(os.getenv("FUNDAMENTALS_MAX_QUARTERS", "12"))
FUNDAMENTALS_MAX_OFFICERS = int(os.getenv("FUNDAMENTALS_MAX_OFFICERS", "5"))
FUNDAMENTALS_MAX_EARNINGS_HISTORY = int(os.getenv("FUNDAMENTALS_MAX_EARNINGS_HISTORY", "8"))
FUNDAMENTALS_CACHE_TTL_SEC = int(os.getenv("FUNDAMENTALS_CACHE_TTL_SEC", "300"))
FUNDAMENTALS_VALID_SECTIONS = frozenset({
    "profile", "indicators", "profitability", "growth", "stability", "earnings",
    "price_performance",
})

# Strategist (Market Strategy)
# - 최신 스캔 사이클 데이터 추출 창(window) 크기
STRATEGIST_LATEST_SCAN_WINDOW_MINUTES = 90
# - 전략 브리핑 OpenAI 응답 캐시 TTL (5분)
STRATEGIST_CACHE_TTL_SEC = int(os.getenv("STRATEGIST_CACHE_TTL_SEC", "300"))
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

# Strategist: 신호 우선순위 임계값
STRATEGIST_VIX_ELEVATED = float(os.getenv("STRATEGIST_VIX_ELEVATED", "25"))
STRATEGIST_VIX_EXTREME = float(os.getenv("STRATEGIST_VIX_EXTREME", "35"))
STRATEGIST_GAUGE_FEAR = int(os.getenv("STRATEGIST_GAUGE_FEAR", "30"))
STRATEGIST_GAUGE_GREED = int(os.getenv("STRATEGIST_GAUGE_GREED", "70"))
# 뉴스 다이제스트: 티커당 최대 헤드라인 수 (단일 이벤트 과대 해석 방지)
STRATEGIST_NEWS_PER_TICKER_MAX = int(os.getenv("STRATEGIST_NEWS_PER_TICKER_MAX", "2"))
# 고임팩트 경제 이벤트 키워드 (리스크 플래그 트리거)
STRATEGIST_HIGH_RISK_ECON_KEYWORDS = frozenset({
    "FOMC", "Fed", "CPI", "NFP", "Non-Farm", "GDP", "PCE", "PPI",
    "ECB", "BOJ", "Employment", "Retail Sales",
})

# Composite Signal (다중 팩터 복합 스코어)
# 최종 composite_score 기준 시그널 문턱 (-1 ~ +1 스케일)
SIGNAL_BUY_THRESHOLD = float(os.getenv("SIGNAL_BUY_THRESHOLD", "0.25"))
SIGNAL_SELL_THRESHOLD = float(os.getenv("SIGNAL_SELL_THRESHOLD", "-0.25"))
# 각 팩터 가중치 (합계 = 1.0)
SIGNAL_WEIGHT_SENTIMENT = float(os.getenv("SIGNAL_WEIGHT_SENTIMENT", "0.25"))
SIGNAL_WEIGHT_EARNINGS = float(os.getenv("SIGNAL_WEIGHT_EARNINGS", "0.30"))
SIGNAL_WEIGHT_MOMENTUM = float(os.getenv("SIGNAL_WEIGHT_MOMENTUM", "0.25"))
SIGNAL_WEIGHT_VOLUME = float(os.getenv("SIGNAL_WEIGHT_VOLUME", "0.20"))

# 레거시 호환 (analyst에서 사용)
BUY_THRESHOLD = SIGNAL_BUY_THRESHOLD
SELL_THRESHOLD = SIGNAL_SELL_THRESHOLD
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
NEWS_FEED_INTERVAL_SEC = int(os.getenv("NEWS_FEED_INTERVAL_SEC", "600"))  # 10분 주기

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
NEWS_ANALYSIS_TIMEOUT_SEC = int(os.getenv("NEWS_ANALYSIS_TIMEOUT_SEC", "40"))
NEWS_ANALYSIS_THREAD_BUFFER_SEC = int(os.getenv("NEWS_ANALYSIS_THREAD_BUFFER_SEC", "2"))
NEWS_ANALYSIS_TEMPERATURE = float(os.getenv("NEWS_ANALYSIS_TEMPERATURE", "0.2"))
# LLM 입력으로 넣는 본문 최대 길이 (토큰/요청 크기 완화)
NEWS_ANALYSIS_INPUT_MAX_CHARS = int(os.getenv("NEWS_ANALYSIS_INPUT_MAX_CHARS", "12000"))

# Macro Indicators
MACRO_INTERVAL_SEC = int(os.getenv("MACRO_INTERVAL_SEC", "60"))  # 1분 주기

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

# S&P 500 Wikipedia 데이터 소스 (scanner + heatmap 공용)
SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
SP500_WIKI_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

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

# yfinance Global Rate Limiting (모든 서비스 공용)
YF_GLOBAL_CONCURRENCY = int(os.getenv("YF_GLOBAL_CONCURRENCY", "10"))
YF_MIN_INTERVAL_SEC = float(os.getenv("YF_MIN_INTERVAL_SEC", "0.05"))
YF_RATE_LIMIT_RETRIES = int(os.getenv("YF_RATE_LIMIT_RETRIES", "3"))
YF_RATE_LIMIT_BACKOFF_SEC = float(os.getenv("YF_RATE_LIMIT_BACKOFF_SEC", "2.0"))

# S&P 500 Heatmap
HEATMAP_CACHE_TTL_SEC = int(os.getenv("HEATMAP_CACHE_TTL_SEC", "120"))        # 가격 캐시 2분
HEATMAP_MCAP_CACHE_TTL_SEC = int(os.getenv("HEATMAP_MCAP_CACHE_TTL_SEC", "1800"))  # 시가총액 캐시 30분
HEATMAP_MCAP_CONCURRENCY = int(os.getenv("HEATMAP_MCAP_CONCURRENCY", "30"))   # 시가총액 동시 조회 수

# Market gauge (VIX -> 0~100)
# VIX를 MIN_VIX~MAX_VIX로 클램프 후 로그 스케일로 0~100으로 변환
MIN_VIX = 10.0
MAX_VIX = 80.0

# ---------------------------------------------------------------------------
# Multi-Agent Portfolio Builder (SSE 스트리밍 + CoT)
# ---------------------------------------------------------------------------
PORTFOLIO_AGENT_MODEL = os.getenv("PORTFOLIO_AGENT_MODEL", "gpt-5")
PORTFOLIO_AGENT_TIMEOUT_SEC = int(os.getenv("PORTFOLIO_AGENT_TIMEOUT_SEC", "600"))

# Monte Carlo 시뮬레이션
MONTE_CARLO_SIMULATIONS = int(os.getenv("MONTE_CARLO_SIMULATIONS", "10000"))
MONTE_CARLO_DAYS = int(os.getenv("MONTE_CARLO_DAYS", "252"))  # 1년 거래일

# VaR (Value at Risk)
VAR_CONFIDENCE_LEVELS = [0.95, 0.99]  # 95%, 99%

# 시나리오 시뮬레이션
SCENARIO_RATE_CHANGE_BPS = [25, 50, 100]  # 금리 변동 시나리오 (bp)
SCENARIO_MARKET_SHOCK_PCT = [-0.10, -0.20, -0.30]  # 시장 충격 시나리오

# 히스토리 데이터 기간 (상관관계 / 변동성 계산용)
RISK_HISTORY_PERIOD = os.getenv("RISK_HISTORY_PERIOD", "1y")

# XAI 에이전트 온도 (설명 품질용 — 약간 높게)
XAI_AGENT_TEMPERATURE = float(os.getenv("XAI_AGENT_TEMPERATURE", "0.4"))

# ---------------------------------------------------------------------------
# Backtesting (대시보드 시그널 + AI 전략실 추천)
# ---------------------------------------------------------------------------
BACKTEST_DEFAULT_LOOKBACK_DAYS = int(os.getenv("BACKTEST_DEFAULT_LOOKBACK_DAYS", "90"))
BACKTEST_MAX_LOOKBACK_DAYS = int(os.getenv("BACKTEST_MAX_LOOKBACK_DAYS", "365"))
# 평가 horizon (거래일 기준) — 기본: 1/5/20
BACKTEST_DEFAULT_HORIZONS = [1, 5, 20]
BACKTEST_MAX_HORIZON_DAYS = int(os.getenv("BACKTEST_MAX_HORIZON_DAYS", "60"))
# 프로세스 내 결과 캐시 TTL
BACKTEST_CACHE_TTL_SEC = int(os.getenv("BACKTEST_CACHE_TTL_SEC", "600"))  # 10분
# 버킷 통계 산출 시 최소 표본 수 (미달 시 버킷 결과 생략)
BACKTEST_MIN_SAMPLES = int(os.getenv("BACKTEST_MIN_SAMPLES", "5"))
# 연환산 계수 (Sharpe 계산) — US 거래일 기준
BACKTEST_ANNUALIZATION_FACTOR = 252
# 괴리율(|divergence|) 버킷 (분위수 기반). 0~2.5%, 2.5~6%, 6~13%, 13%+
BACKTEST_DIVERGENCE_BUCKETS: list[tuple[float, float]] = [
    (0.0, 0.025),
    (0.025, 0.06),
    (0.06, 0.13),
    (0.13, float("inf")),
]
# yfinance 일괄 다운로드 기간 여유(주말/공휴일 대비)
BACKTEST_PRICE_LOOKAHEAD_DAYS = 14

# ---------------------------------------------------------------------------
# AI Chat (종목 질의 챗봇, SSE 스트리밍)
# ---------------------------------------------------------------------------
# 챗봇은 응답 속도가 중요 — 기본값을 경량 모델로. 품질 우선이면 env로 gpt-5 지정.
CHAT_OPENAI_MODEL = os.getenv("CHAT_OPENAI_MODEL", "gpt-5-mini")
CHAT_OPENAI_TIMEOUT_SEC = int(os.getenv("CHAT_OPENAI_TIMEOUT_SEC", "90"))
CHAT_TEMPERATURE = float(os.getenv("CHAT_TEMPERATURE", "0.3"))
# 질의 1건에서 분석할 티커 최대 수 (과도한 yfinance 호출 방지)
CHAT_MAX_TICKERS_PER_QUERY = int(os.getenv("CHAT_MAX_TICKERS_PER_QUERY", "3"))
# 티커별 컨텍스트에 포함할 뉴스 개수
CHAT_NEWS_PER_TICKER = int(os.getenv("CHAT_NEWS_PER_TICKER", "5"))
# 시장 전체 질의일 때 포함할 헤드라인 수
CHAT_MARKET_NEWS_LIMIT = int(os.getenv("CHAT_MARKET_NEWS_LIMIT", "8"))
# 대화 이력 최근 N턴만 LLM에 전달 (컨텍스트 크기 제어)
CHAT_MAX_HISTORY_MESSAGES = int(os.getenv("CHAT_MAX_HISTORY_MESSAGES", "12"))
# 사용자 메시지 길이 제한 (토큰/남용 방지)
CHAT_USER_MESSAGE_MAX_CHARS = int(os.getenv("CHAT_USER_MESSAGE_MAX_CHARS", "2000"))

# 한글 종목명 → 티커 매핑 (질의 자연어 인식용)
# 대표 종목만 등록하고, 정확한 티커는 사용자가 영문 대문자로 입력해도 된다.
CHAT_KO_NAME_TO_TICKER: dict[str, str] = {
    "엔비디아": "NVDA",
    "애플": "AAPL",
    "마이크로소프트": "MSFT",
    "테슬라": "TSLA",
    "구글": "GOOGL",
    "알파벳": "GOOGL",
    "메타": "META",
    "아마존": "AMZN",
    "넷플릭스": "NFLX",
    "amd": "AMD",
    "인텔": "INTC",
    "퀄컴": "QCOM",
    "브로드컴": "AVGO",
    "팔란티어": "PLTR",
    "버크셔": "BRK-B",
    "비트코인": "BTC-USD",
    "이더리움": "ETH-USD",
}

# 챗봇이 특수 분석 없이 답해도 되는 일반 질의 감지용 키워드
# (시장 전체, 섹터, 매크로 관련)
CHAT_MARKET_KEYWORDS = frozenset({
    "시장", "market", "매크로", "macro", "전망", "outlook",
    "섹터", "sector", "지수", "index", "나스닥", "nasdaq",
    "s&p", "sp500", "다우", "dow", "vix", "금리",
})

# --- 세션/히스토리 ---
CHAT_SESSIONS_LIST_LIMIT = int(os.getenv("CHAT_SESSIONS_LIST_LIMIT", "50"))
CHAT_SESSION_TITLE_MAX_CHARS = int(os.getenv("CHAT_SESSION_TITLE_MAX_CHARS", "80"))
CHAT_SESSION_PREVIEW_MAX_CHARS = int(os.getenv("CHAT_SESSION_PREVIEW_MAX_CHARS", "120"))
# 세션 로드 시 반환할 최대 메시지 개수 (가장 최근부터)
CHAT_SESSION_MESSAGE_LIMIT = int(os.getenv("CHAT_SESSION_MESSAGE_LIMIT", "200"))

# --- 파일 첨부 (텍스트 기반만 지원) ---
CHAT_FILE_MAX_BYTES = int(os.getenv("CHAT_FILE_MAX_BYTES", str(5 * 1024 * 1024)))  # 5 MB
CHAT_FILE_TEXT_MAX_CHARS = int(os.getenv("CHAT_FILE_TEXT_MAX_CHARS", "30000"))
CHAT_FILE_ALLOWED_EXT = frozenset({
    "txt", "md", "markdown", "csv", "tsv", "json", "log",
    "py", "js", "mjs", "ts", "tsx", "jsx",
    "html", "htm", "xml", "yaml", "yml", "toml", "ini", "cfg",
    "sql",
})
# 요청 1건에서 첨부 가능한 파일 최대 개수
CHAT_FILES_PER_REQUEST_MAX = int(os.getenv("CHAT_FILES_PER_REQUEST_MAX", "5"))

# 기술적 지표(RSI/MACD/볼린저/MA 등) 수집 여부 판단 키워드.
# 없으면 기본적으로 기술적 지표 계산을 스킵해 응답 속도를 확보한다.
# 매수/매도/진입/타이밍 질의는 기술적 지표가 근거에 필수 → 포함.
CHAT_TECHNICAL_KEYWORDS = frozenset({
    # 지표·차트 용어
    "기술적", "기술지표", "차트", "rsi", "macd", "볼린저", "bollinger",
    "이동평균", "moving average", "골든크로스", "데드크로스",
    "지지", "저항", "support", "resistance", "atr", "과매수", "과매도",
    # 매매 의사결정 관련 — 기술적 근거 필요
    "사도", "팔도", "매수", "매도", "진입", "entry", "타점", "타이밍",
    "손절", "익절", "stop", "target", "목표가", "전략",
})
