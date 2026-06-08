from dotenv import load_dotenv
from datetime import timedelta, timezone
import logging
import os

load_dotenv()

_cfg_logger = logging.getLogger(__name__)


def _bool_env(name: str, default: str = "false") -> bool:
    """env 값을 truthy 문자열 집합으로 일관 해석한다 (1/true/yes/on 만 True)."""
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5")


def validate_required_env(strict: bool | None = None) -> list[str]:
    """필수 환경변수 검증.

    REQUIRED (없으면 RuntimeError):
      - SUPABASE_URL, SUPABASE_KEY  : DB 자체가 동작 불가
    OPTIONAL (없으면 WARNING + 일부 기능 비활성):
      - OPENAI_API_KEY              : LLM/감성 폴백 비활성

    strict=False 면 critical 도 RuntimeError 대신 WARNING 으로 격하 (테스트/로컬용).
    기본값은 STRICT_ENV 환경변수 (default: true).
    """
    if strict is None:
        strict = _bool_env("STRICT_ENV", default="true")

    critical_missing: list[str] = []
    optional_missing: list[str] = []

    if not SUPABASE_URL:
        critical_missing.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        critical_missing.append("SUPABASE_KEY")
    if not OPENAI_API_KEY:
        optional_missing.append("OPENAI_API_KEY")

    if optional_missing:
        _cfg_logger.warning(
            "선택 환경변수 누락 — 관련 기능 비활성: %s",
            ", ".join(optional_missing),
        )

    if critical_missing:
        msg = (
            "필수 환경변수 누락: " + ", ".join(critical_missing)
            + " — .env 또는 배포 환경에 설정 필요"
        )
        if strict:
            raise RuntimeError(msg)
        _cfg_logger.error(msg + " (STRICT_ENV=false 라 부팅 계속, 첫 DB 호출에서 실패)")

    return critical_missing


# ---------------------------------------------------------------------------
# 공용(Shared) — 여러 모듈이 import 하는 단일 출처 상수
# ---------------------------------------------------------------------------
# 한국 표준시(UTC+9). strategist / economic_calendar 등에서 공용.
KST = timezone(timedelta(hours=9))
# 외부 사이트 크롤링용 표준 브라우저 User-Agent (article_crawler / sentiment / economic_calendar 공용).
BROWSER_USER_AGENT = os.getenv(
    "BROWSER_USER_AGENT",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
)
BROWSER_HEADERS: dict[str, str] = {"User-Agent": BROWSER_USER_AGENT}


# FinBERT 대안 — transformers/torch 미설치(저메모리 환경) 시 OpenAI 로 감성 분류.
FINBERT_OPENAI_MODEL = os.getenv("FINBERT_OPENAI_MODEL", "gpt-4o-mini")
FINBERT_OPENAI_TIMEOUT_SEC = int(os.getenv("FINBERT_OPENAI_TIMEOUT_SEC", "30"))
# 한 번의 OpenAI 호출에 묶을 헤드라인 최대 개수.
# 30 에서 응답 길이 mismatch 가 잦아 20 으로 보수적 축소(호출 횟수는 약간 늘지만 신뢰도 ↑).
FINBERT_OPENAI_BATCH_SIZE = int(os.getenv("FINBERT_OPENAI_BATCH_SIZE", "20"))

# Strategist (Market Strategy) OpenAI model
STRATEGIST_OPENAI_MODEL = os.getenv("STRATEGIST_OPENAI_MODEL", "gpt-5")
# 사용자 정책: 시간이 오래 걸려도 정상 응답을 받는 것이 우선 — timeout 을 충분히 길게 잡아
# fallback 발동(에러 응답)을 피한다. 30분(1800초). httpx + asyncio.wait_for 동일 적용.
STRATEGIST_OPENAI_TIMEOUT_SEC = int(os.getenv("STRATEGIST_OPENAI_TIMEOUT_SEC", "1800"))
# asyncio.wait_for는 클라이언트 timeout보다 약간 여유를 둔다.
STRATEGIST_OPENAI_THREAD_BUFFER_SEC = int(os.getenv("STRATEGIST_OPENAI_THREAD_BUFFER_SEC", "30"))
# GPT-5 / o1 / o3 등 reasoning 모델의 reasoning_effort 단계.
# minimal/low/medium/high — medium 이 응답 안정성(JSON 형식·논리 일관성) 확보에 유리.
# low 에서 JSON 깨짐·검증 실패가 누적되어 fallback 으로 떨어지던 문제를 막는다.
STRATEGIST_REASONING_EFFORT = os.getenv("STRATEGIST_REASONING_EFFORT", "medium")
# OpenAI 실패 시 fallback top_picks 개수 (스캔 rows 앞에서 N개)
STRATEGIST_FALLBACK_TOP_PICKS_N = int(os.getenv("STRATEGIST_FALLBACK_TOP_PICKS_N", "2"))

# Scanner
MIN_VOLUME = 1_000_000
SCAN_TOP_N = 15
SCAN_TRADING_DAYS = 5  # 등락률 계산 기준 거래일 수
REPORT_TOP_N = 2
# yf.download batch 동시 실행 수 — 503 종목 / 100 batch = 6 배치를 N개 thread 로 동시 다운로드.
# yfinance 자체 connection pool + Yahoo 한 IP 동시 요청 한도 고려해 기본 3 (보수적).
# 사이클 시간이 6단계 직렬 → ceil(6/3)=2단계로 축소. 측정상 약 2~3배 빨라짐.
SCAN_DOWNLOAD_BATCH_PARALLELISM = int(os.getenv("SCAN_DOWNLOAD_BATCH_PARALLELISM", "3"))
# 분석 사이클 안정성: scan_stocks 결과가 이 개수 미만이면 yfinance 부분 차단/장시간 누적 오류로
# 간주하고 직전 스냅샷(메모리→DB)을 유지한다. 새 1~2개 결과로 top_picks/radar 를 덮어쓰지 않는다.
#
# 기준: S&P 500 503개 × MIN_VOLUME 통과율 ~84% ≈ 420개. 그 절반(~250) 이상이면 정상으로 간주.
# yfinance 가 장 오픈 직후 등 시간대에 부분 차단해 결과가 100~200으로 떨어지면 stale 유지로
# 사용자 화면이 갑자기 줄지 않게 한다.
MIN_TOP_PICKS_FRESH = int(os.getenv("MIN_TOP_PICKS_FRESH", "250"))

# 스캔 1차 누락 티커 재시도 — yfinance 부분 차단/429 로 batch 가 통째로 실패하면
# 해당 종목은 price/volume=None placeholder 로 남아 대시보드에서 VOL·거래대금이 빈칸이 된다.
# 누락분만 throttled(글로벌 rate limit 경유) 작은 batch 로 재시도해 커버리지를 끌어올린다.
#   ROUNDS=0 이면 재시도 비활성. rate limit 분산을 위해 라운드 사이 DELAY 만큼 대기.
SCAN_RETRY_MAX_ROUNDS = int(os.getenv("SCAN_RETRY_MAX_ROUNDS", "2"))
SCAN_RETRY_BATCH_SIZE = int(os.getenv("SCAN_RETRY_BATCH_SIZE", "40"))
SCAN_RETRY_DELAY_SEC = float(os.getenv("SCAN_RETRY_DELAY_SEC", "1.5"))

# Cycle
SCAN_INTERVAL_SEC = 3600
ERROR_RETRY_SEC = 60

# 백그라운드 루프 자동 복구
#   1회 실패 시 ERROR_RETRY_SEC * 2^(failures-1) 만큼 대기 (단, BACKOFF_MAX_SEC 상한).
#   FAILURE_ALERT_THRESHOLD 회 연속 실패하면 ERROR 로그로 알림.
LOOP_BACKOFF_MAX_SEC = int(os.getenv("LOOP_BACKOFF_MAX_SEC", "1800"))   # 30분 상한
LOOP_FAILURE_ALERT_THRESHOLD = int(os.getenv("LOOP_FAILURE_ALERT_THRESHOLD", "5"))

# WebSocket 운영
WS_MAX_CONNECTIONS = int(os.getenv("WS_MAX_CONNECTIONS", "200"))
WS_HEARTBEAT_INTERVAL_SEC = int(os.getenv("WS_HEARTBEAT_INTERVAL_SEC", "30"))
# 클라이언트가 이 시간 안에 어떤 메시지(텍스트/pong/ping)도 보내지 않으면 idle 로 판단해 종료
WS_IDLE_TIMEOUT_SEC = int(os.getenv("WS_IDLE_TIMEOUT_SEC", "300"))
# broadcast 시 send_bytes/send_text 동시 발사 — 직렬 송신 대비 클라이언트 N배 빨라짐.
# 단, 대량 슬로우 클라이언트가 게이트가 되지 않도록 send 자체에 5초 타임아웃 적용.
WS_BROADCAST_SEND_TIMEOUT_SEC = float(os.getenv("WS_BROADCAST_SEND_TIMEOUT_SEC", "5.0"))

# ---------------------------------------------------------------------------
# HTTP 미들웨어
# ---------------------------------------------------------------------------
# GZip 미들웨어 임계값(byte). 응답 크기가 이 값 이상일 때만 압축.
# 500 byte 미만은 압축 오버헤드(CPU + 헤더)가 더 커서 그대로 두는 게 이득.
GZIP_MIN_SIZE_BYTES = int(os.getenv("GZIP_MIN_SIZE_BYTES", "500"))

# 섹터 트래커 응답 캐시 TTL (yfinance 재호출 부담 분산)
SECTOR_TRACKER_CACHE_TTL_SEC = int(os.getenv("SECTOR_TRACKER_CACHE_TTL_SEC", "600"))
# 주간 수익률 임계 (모멘텀 라벨링)
SECTOR_TRACKER_STRONG_MOVE_PCT = float(os.getenv("SECTOR_TRACKER_STRONG_MOVE_PCT", "0.02"))
SECTOR_TRACKER_NORMAL_MOVE_PCT = float(os.getenv("SECTOR_TRACKER_NORMAL_MOVE_PCT", "0.005"))
# 로테이션 판정 시 그룹간 최소 우위 격차
SECTOR_TRACKER_ROTATION_MIN_EDGE_PCT = float(os.getenv("SECTOR_TRACKER_ROTATION_MIN_EDGE_PCT", "0.005"))

# CORS — env 미설정 시 "*"(개발 모드). 콤마 구분.
#   ex) CORS_ALLOW_ORIGINS=https://app.example.com,https://staging.example.com
_cors_raw = os.getenv("CORS_ALLOW_ORIGINS", "*").strip()
CORS_ALLOW_ORIGINS: list[str] = (
    ["*"] if _cors_raw == "*"
    else [o.strip() for o in _cors_raw.split(",") if o.strip()]
)

# yfinance 분봉 시세 (스캔과 별도 — top/radar 종목만 자주 갱신)
PRICE_TICK_INTERVAL_SEC = int(os.getenv("PRICE_TICK_INTERVAL_SEC", "30"))
PRICE_TICK_MAX_SYMBOLS = int(os.getenv("PRICE_TICK_MAX_SYMBOLS", "550"))
# 1m은 호출 부담이 크므로 기본 5m (장중 마지막 봉 기준으로 체감 갱신)
PRICE_INTRADAY_INTERVAL = os.getenv("PRICE_INTRADAY_INTERVAL", "5m")
PRICE_DOWNLOAD_BATCH_SIZE = int(os.getenv("PRICE_DOWNLOAD_BATCH_SIZE", "50"))
# 분봉 batch 조회에서 누락된 심볼만 fast_info 로 보강한다. 전체 500개를 매번
# 개별 호출하면 Yahoo rate limit 에 걸리기 쉬워 보강 개수는 별도 상한을 둔다.
PRICE_FAST_INFO_FALLBACK_MAX_SYMBOLS = int(os.getenv("PRICE_FAST_INFO_FALLBACK_MAX_SYMBOLS", "150"))
# Stock detail: 회사명 조회 timeout (yfinance Ticker.info)
STOCK_PROFILE_TIMEOUT_SEC = float(os.getenv("STOCK_PROFILE_TIMEOUT_SEC", "6"))
# Stock detail: fetch_quote/fetch_chart 인메모리 캐시
#   프런트 polling(초당 N회) 폭주 시 yfinance 호출 1/N 로 줄여 부하 차단.
STOCK_QUOTE_CACHE_TTL_SEC = int(os.getenv("STOCK_QUOTE_CACHE_TTL_SEC", "15"))
STOCK_CHART_INTRADAY_TTL_SEC = int(os.getenv("STOCK_CHART_INTRADAY_TTL_SEC", "30"))
# 일봉/주봉/월봉 차트는 상장일부터 전체(period="max") 를 받기 때문에 응답이 ~100KB 수준이다.
# 과거 데이터는 변하지 않으므로 TTL 을 길게(30분) 잡아 yfinance 호출 부담을 크게 줄인다.
# 장중 마지막 봉(오늘)의 실시간 close 는 fetch_quote(STOCK_QUOTE_CACHE_TTL_SEC=15s)가 별도 갱신.
STOCK_CHART_DAILY_TTL_SEC = int(os.getenv("STOCK_CHART_DAILY_TTL_SEC", "1800"))

# ---------------------------------------------------------------------------
# Stock Fundamentals (종목 펀더멘털)
# ---------------------------------------------------------------------------
FUNDAMENTALS_MAX_QUARTERS = int(os.getenv("FUNDAMENTALS_MAX_QUARTERS", "12"))
FUNDAMENTALS_MAX_OFFICERS = int(os.getenv("FUNDAMENTALS_MAX_OFFICERS", "5"))
FUNDAMENTALS_MAX_EARNINGS_HISTORY = int(os.getenv("FUNDAMENTALS_MAX_EARNINGS_HISTORY", "8"))
# 분기 단위 데이터라 자주 안 바뀜 — TTL 1시간으로 yfinance 호출 횟수 줄임 (rate limit 회피)
FUNDAMENTALS_CACHE_TTL_SEC = int(os.getenv("FUNDAMENTALS_CACHE_TTL_SEC", "3600"))
FUNDAMENTALS_VALID_SECTIONS = frozenset({
    "profile", "indicators", "profitability", "growth", "stability", "earnings",
    "price_performance",
})

# Strategist (Market Strategy)
# - 최신 스캔 사이클 데이터 추출 창(window) 크기
STRATEGIST_LATEST_SCAN_WINDOW_MINUTES = 90
# - 전략 브리핑 OpenAI 응답 캐시 TTL.
# 1시간 주기 자동 워밍(run_strategy_warmup_loop)이 캐시를 미리 채우므로,
# TTL 을 워밍 주기(1시간)보다 넉넉히(90분) 잡아 사용자가 직접 OpenAI 빌드를
# 트리거하지 않게 한다(항상 캐시 hit → 즉시 응답, 비용은 시간당 1회로 고정).
STRATEGIST_CACHE_TTL_SEC = int(os.getenv("STRATEGIST_CACHE_TTL_SEC", "5400"))

# - AI 전략실 자동 워밍: 1시간 주기로 전략 브리핑을 미리 산출해 캐시를 채운다.
#   사용자 진입 시 무거운 OpenAI 호출 없이 캐시 hit 으로 즉시 응답(stale-while-revalidate).
STRATEGIST_AUTO_WARMUP_ENABLED = _bool_env("STRATEGIST_AUTO_WARMUP_ENABLED", "true")
STRATEGIST_AUTO_WARMUP_INTERVAL_SEC = int(os.getenv("STRATEGIST_AUTO_WARMUP_INTERVAL_SEC", "3600"))  # 1시간
STRATEGIST_AUTO_WARMUP_INITIAL_DELAY_SEC = int(os.getenv("STRATEGIST_AUTO_WARMUP_INITIAL_DELAY_SEC", "20"))
# - yfinance Ticker.info(섹터) 호출은 타임아웃 위험이 있어
#   요청당 최대 호출 개수로 제한하고, 결과는 프로세스 내 캐싱한다.
STRATEGIST_MAX_YFINANCE_SECTOR_CALLS_PER_REQUEST = 20
# - yfinance Ticker.info 호출 대기 제한(초)
STRATEGIST_YFINANCE_SECTOR_TIMEOUT_SEC = 8

# - OpenAI temperature
STRATEGIST_TEMPERATURE = 0.3

# - 추천 가격 정규화 (LLM 환각 방지)
#   entry_zone mid 가 current_price 와 ±N 이상 벌어지면 종가 기반으로 자동 보정.
#   예: TXN 시세 289 인데 LLM 이 entry_zone 144 로 환각하는 케이스 차단.
STRATEGIST_ENTRY_DEVIATION_THRESHOLD = float(os.getenv("STRATEGIST_ENTRY_DEVIATION_THRESHOLD", "0.15"))
#   보정 시 entry_zone 폭 — current_price 의 ±0.5%
STRATEGIST_ENTRY_NORMALIZED_BAND_PCT = float(os.getenv("STRATEGIST_ENTRY_NORMALIZED_BAND_PCT", "0.005"))
#   보정 시 기본 손절폭 — current_price 의 3% (BUY 는 -3%, SELL 은 +3%)
STRATEGIST_NORMALIZED_STOP_LOSS_PCT = float(os.getenv("STRATEGIST_NORMALIZED_STOP_LOSS_PCT", "0.03"))

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
# 실적 서프라이즈 결과 메모리 캐시 TTL — 분기 실적은 자주 바뀌지 않으므로 24시간.
# 같은 ticker 재조회 시 yfinance 호출 없이 즉시 반환 → 429 회피의 핵심.
EARNINGS_CACHE_TTL_SEC = int(os.getenv("EARNINGS_CACHE_TTL_SEC", "86400"))  # 24h
# 실패(None)도 캐시할지 — True면 일시 429에 걸린 ticker 도 24h 캐시되어 사이클 부담 ↓
EARNINGS_CACHE_FAILURES = _bool_env("EARNINGS_CACHE_FAILURES", "true")
# 실패 캐시는 짧게(1시간) — 그날 안에 재시도는 가능하게
EARNINGS_FAILURE_CACHE_TTL_SEC = int(os.getenv("EARNINGS_FAILURE_CACHE_TTL_SEC", "3600"))  # 1h

# FinBERT Sentiment
SENTIMENT_MAX_HEADLINES = 10
# Finviz 스크래핑: 동시 요청·간격·429 재시도 (과도한 병렬은 429 유발)
SENTIMENT_FINVIZ_MAX_CONCURRENT = int(os.getenv("SENTIMENT_FINVIZ_MAX_CONCURRENT", "3"))
# Finviz quote URL/타임아웃 — sentiment.py 단일 출처.
FINVIZ_QUOTE_URL = os.getenv("FINVIZ_QUOTE_URL", "https://finviz.com/quote?t={}")
FINVIZ_TIMEOUT_SEC = float(os.getenv("FINVIZ_TIMEOUT_SEC", "15"))
SENTIMENT_FINVIZ_DELAY_SEC = float(os.getenv("SENTIMENT_FINVIZ_DELAY_SEC", "0.25"))
SENTIMENT_FINVIZ_MAX_RETRIES = int(os.getenv("SENTIMENT_FINVIZ_MAX_RETRIES", "4"))
SENTIMENT_FINVIZ_RETRY_BASE_SEC = float(os.getenv("SENTIMENT_FINVIZ_RETRY_BASE_SEC", "1.5"))

# News Feed
NEWS_FEED_MAX_ITEMS = 30
NEWS_FEED_TTL_SEC = 300
NEWS_FEED_INTERVAL_SEC = int(os.getenv("NEWS_FEED_INTERVAL_SEC", "600"))  # 10분 주기

# News impact 랭킹 — 시장 영향도 = clamp01(|score| × confidence × 0.5^(age_hours/half_life))
NEWS_IMPACT_HALF_LIFE_HOURS = float(os.getenv("NEWS_IMPACT_HALF_LIFE_HOURS", "24"))  # 시간 감쇠 반감기
NEWS_TOP_DEFAULT_LIMIT = int(os.getenv("NEWS_TOP_DEFAULT_LIMIT", "5"))               # /api/news/top 기본 상위 개수
NEWS_TOP_DEFAULT_WINDOW_HOURS = int(os.getenv("NEWS_TOP_DEFAULT_WINDOW_HOURS", "72"))  # 랭킹 대상 최근 구간
NEWS_TOP_MAX_WINDOW_HOURS = int(os.getenv("NEWS_TOP_MAX_WINDOW_HOURS", "720"))       # window_hours 상한 (30일)
NEWS_TOP_SCAN_MAX_ITEMS = int(os.getenv("NEWS_TOP_SCAN_MAX_ITEMS", "500"))           # 랭킹 산출용 후보 최대 조회 수

# Economic Calendar (myfxbook 크롤링)
ECON_CALENDAR_TTL_SEC = int(os.getenv("ECON_CALENDAR_TTL_SEC", "600"))  # 10분
ECON_CALENDAR_INTERVAL_SEC = int(os.getenv("ECON_CALENDAR_INTERVAL_SEC", "600"))  # 10분 주기 크롤링
ECON_CALENDAR_TIMEOUT_SEC = float(os.getenv("ECON_CALENDAR_TIMEOUT_SEC", "12"))
# 경제 캘린더 소스 URL — economic_calendar.py 단일 출처.
ECON_MYFXBOOK_URL = os.getenv("ECON_MYFXBOOK_URL", "https://www.myfxbook.com/forex-economic-calendar")
# ForexFactory 공개 JSON — myfxbook 차단(403) 시 fallback. 이번주 7일치 제공.
ECON_FOREXFACTORY_URL = os.getenv(
    "ECON_FOREXFACTORY_URL", "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
)
ECON_CALENDAR_MAX_ITEMS = int(os.getenv("ECON_CALENDAR_MAX_ITEMS", "500"))

# News article crawling (from yfinance news url)
NEWS_ARTICLE_CACHE_TTL_SEC = int(os.getenv("NEWS_ARTICLE_CACHE_TTL_SEC", "21600"))  # 6h
NEWS_CRAWL_TIMEOUT_SEC = float(os.getenv("NEWS_CRAWL_TIMEOUT_SEC", "12"))
NEWS_CRAWL_MAX_CONCURRENT = int(os.getenv("NEWS_CRAWL_MAX_CONCURRENT", "3"))
# SSRF 방어 — /api/news?url= 로 임의 URL 크롤링이 가능하므로 스킴/대상 IP 를 제한한다.
#   - 허용 스킴은 http/https 만.
#   - BLOCK_PRIVATE_IPS=true 면 호스트를 DNS 조회해 사설/루프백/링크로컬/예약 IP 를 거부(메타데이터 169.254.169.254 등).
#   - 리디렉션도 매 홉마다 재검증하며 최대 MAX_REDIRECTS 회까지만 따라간다.
NEWS_CRAWL_ALLOWED_SCHEMES: tuple[str, ...] = ("http", "https")
NEWS_CRAWL_BLOCK_PRIVATE_IPS = _bool_env("NEWS_CRAWL_BLOCK_PRIVATE_IPS", default="true")
NEWS_CRAWL_MAX_REDIRECTS = int(os.getenv("NEWS_CRAWL_MAX_REDIRECTS", "5"))
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

# ---------------------------------------------------------------------------
# Global Markets Overview (/api/markets/global)
# ---------------------------------------------------------------------------
GLOBAL_MARKETS_CACHE_TTL_SEC = int(os.getenv("GLOBAL_MARKETS_CACHE_TTL_SEC", "300"))  # 5분

# 글로벌 지수 — 응답의 symbol 은 사용자 친화 라벨, ticker 는 yfinance 호출용
GLOBAL_INDICES = [
    {"symbol": "KOSPI",   "ticker": "^KS11",     "name": "코스피",         "country": "KR", "decimals": 2},
    {"symbol": "KOSDAQ",  "ticker": "^KQ11",     "name": "코스닥",         "country": "KR", "decimals": 2},
    {"symbol": "NI225",   "ticker": "^N225",     "name": "닛케이225",      "country": "JP", "decimals": 2},
    {"symbol": "HSI",     "ticker": "^HSI",      "name": "항셍지수",       "country": "HK", "decimals": 2},
    {"symbol": "SSE",     "ticker": "000001.SS", "name": "상하이종합",     "country": "CN", "decimals": 2},
    {"symbol": "SPX",     "ticker": "^GSPC",     "name": "S&P 500",        "country": "US", "decimals": 2},
    {"symbol": "IXIC",    "ticker": "^IXIC",     "name": "NASDAQ",         "country": "US", "decimals": 2},
    {"symbol": "DJI",     "ticker": "^DJI",      "name": "다우존스",       "country": "US", "decimals": 2},
    {"symbol": "DAX",     "ticker": "^GDAXI",    "name": "DAX",            "country": "DE", "decimals": 2},
    {"symbol": "FTSE",    "ticker": "^FTSE",     "name": "FTSE 100",       "country": "GB", "decimals": 2},
    {"symbol": "CAC",     "ticker": "^FCHI",     "name": "CAC 40",         "country": "FR", "decimals": 2},
    {"symbol": "ESTX50",  "ticker": "^STOXX50E", "name": "Euro Stoxx 50",  "country": "EU", "decimals": 2},
]

# 원자재 (yfinance futures)
GLOBAL_COMMODITIES = [
    {"symbol": "GC=F",  "ticker": "GC=F",  "name": "Gold",        "decimals": 2},
    {"symbol": "SI=F",  "ticker": "SI=F",  "name": "Silver",      "decimals": 3},
    {"symbol": "CL=F",  "ticker": "CL=F",  "name": "WTI Crude",   "decimals": 2},
    {"symbol": "BZ=F",  "ticker": "BZ=F",  "name": "Brent Crude", "decimals": 2},
    {"symbol": "NG=F",  "ticker": "NG=F",  "name": "Natural Gas", "decimals": 3},
    {"symbol": "HG=F",  "ticker": "HG=F",  "name": "Copper",      "decimals": 4},
]

# 환율 (yfinance FX) + Dollar Index
GLOBAL_CURRENCIES = [
    {"symbol": "USDKRW", "ticker": "USDKRW=X", "name": "USD/KRW", "decimals": 2},
    {"symbol": "USDJPY", "ticker": "USDJPY=X", "name": "USD/JPY", "decimals": 3},
    {"symbol": "USDCNY", "ticker": "USDCNY=X", "name": "USD/CNY", "decimals": 4},
    {"symbol": "EURUSD", "ticker": "EURUSD=X", "name": "EUR/USD", "decimals": 4},
    {"symbol": "GBPUSD", "ticker": "GBPUSD=X", "name": "GBP/USD", "decimals": 4},
    {"symbol": "DXY",    "ticker": "DX-Y.NYB", "name": "Dollar Index", "decimals": 2},
]

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
SP500_CONSTITUENTS_CACHE_TTL_SEC = int(os.getenv("SP500_CONSTITUENTS_CACHE_TTL_SEC", "21600"))
# Wikipedia S&P 500 페이지 요청 타임아웃(초) — scanner.get_sp500_constituents.
SP500_WIKI_TIMEOUT_SEC = float(os.getenv("SP500_WIKI_TIMEOUT_SEC", "15"))

# ---------------------------------------------------------------------------
# Stock Universe (종목 검색용 universe 캐시)
# ---------------------------------------------------------------------------
# S&P 500 + strategy_history + analysis_results 합집합. 갱신은 잦지 않으므로 길게.
STOCK_UNIVERSE_CACHE_TTL_SEC = int(os.getenv("STOCK_UNIVERSE_CACHE_TTL_SEC", "3600"))
# DB 보조 universe (분석/추천 이력)에서 끌어올 최대 행 수.
# distinct ticker 만 추출하므로 일반적으로 5000 행이면 충분.
STOCK_UNIVERSE_DB_FETCH_LIMIT = int(os.getenv("STOCK_UNIVERSE_DB_FETCH_LIMIT", "5000"))

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
# 히트맵 새로고침 주기 — SWR 메모리 캐시가 이 시간보다 오래되면 다음 요청에서
# 직전 스냅샷을 즉시 반환하고 백그라운드로 갱신한다(= 실질 데이터 갱신 주기).
HEATMAP_CACHE_TTL_SEC = int(os.getenv("HEATMAP_CACHE_TTL_SEC", "300"))        # 가격 캐시 5분
HEATMAP_MCAP_CACHE_TTL_SEC = int(os.getenv("HEATMAP_MCAP_CACHE_TTL_SEC", "1800"))  # 시가총액 캐시 30분
# 시가총액 동시 조회 수 — 30 은 yfinance 폭주 → 차단을 유발해 8 로 보수화
HEATMAP_MCAP_CONCURRENCY = int(os.getenv("HEATMAP_MCAP_CONCURRENCY", "8"))
# 오래된 DB 스냅샷이 일부 종목만 담고 있으면 첫 요청에서도 즉시 재빌드한다.
HEATMAP_MIN_CONSTITUENTS_FOR_CACHE = int(os.getenv("HEATMAP_MIN_CONSTITUENTS_FOR_CACHE", "450"))

# Market gauge (VIX -> 0~100)
# VIX를 MIN_VIX~MAX_VIX로 클램프 후 로그 스케일로 0~100으로 변환
MIN_VIX = 10.0
MAX_VIX = 80.0

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

# ---------------------------------------------------------------------------
# Backtesting (대시보드 시그널 + AI 전략실 추천)
# ---------------------------------------------------------------------------
# 90일은 22,000+ 행 페이지네이션 → Supabase statement timeout(57014). 60일도 22,000+ 관측됨.
# 30일이면 약 11,000 행으로 안전하고 첫 호출이 frontend timeout(보통 30s) 안에 끝난다.
# 사용자가 더 긴 기간 원하면 ?lookback_days=60 같은 query param 으로 명시 호출 가능.
BACKTEST_DEFAULT_LOOKBACK_DAYS = int(os.getenv("BACKTEST_DEFAULT_LOOKBACK_DAYS", "30"))
BACKTEST_MAX_LOOKBACK_DAYS = int(os.getenv("BACKTEST_MAX_LOOKBACK_DAYS", "365"))
# 평가 horizon (거래일 기준) — 기본: 1/5/20
BACKTEST_DEFAULT_HORIZONS = [1, 5, 20]
BACKTEST_MAX_HORIZON_DAYS = int(os.getenv("BACKTEST_MAX_HORIZON_DAYS", "60"))
# 프로세스 내 결과 캐시 TTL — 자동 워밍 주기와 맞춰 30분
BACKTEST_CACHE_TTL_SEC = int(os.getenv("BACKTEST_CACHE_TTL_SEC", "1800"))  # 30분
# 백그라운드 자동 워밍: summary + trades 를 주기적으로 호출해 캐시를 유지한다.
# 워밍 주기는 캐시 TTL 보다 약간 짧아야 사용자 호출이 항상 캐시에 적중한다.
BACKTEST_AUTO_WARMUP_ENABLED = _bool_env("BACKTEST_AUTO_WARMUP_ENABLED", "true")
BACKTEST_AUTO_WARMUP_INTERVAL_SEC = int(os.getenv("BACKTEST_AUTO_WARMUP_INTERVAL_SEC", "1500"))  # 25분
# 워밍은 사용자 default lookback 과 동일해야 캐시 hit 됨.
# 다르면 캐시 키가 달라 워밍 효과가 사라지고 사용자 호출이 매번 새로 계산.
# 기본은 BACKTEST_DEFAULT_LOOKBACK_DAYS 와 일치.
BACKTEST_WARMUP_LOOKBACK_DAYS = int(
    os.getenv("BACKTEST_WARMUP_LOOKBACK_DAYS", str(BACKTEST_DEFAULT_LOOKBACK_DAYS))
)
# 서버 기동 직후 부담 회피용 초기 지연
BACKTEST_AUTO_WARMUP_INITIAL_DELAY_SEC = int(os.getenv("BACKTEST_AUTO_WARMUP_INITIAL_DELAY_SEC", "10"))
# 워밍 단계(summary → trades(strategist) → trades(signals)) 사이 분산 지연
BACKTEST_WARMUP_STEP_DELAY_SEC = int(os.getenv("BACKTEST_WARMUP_STEP_DELAY_SEC", "10"))

# yfinance batch 가격 다운로드 결과 메모리 캐시 TTL.
# (tickers_set, start, end) 가 같으면 같은 캐시 엔트리 → signals/strategist/trades 가
# 한 사이클 안에서 가격 다운로드를 중복 수행하지 않게 한다 → Yahoo 429 회피.
BACKTEST_PRICE_CACHE_TTL_SEC = int(os.getenv("BACKTEST_PRICE_CACHE_TTL_SEC", "1800"))  # 30분

# ---------------------------------------------------------------------------
# Price History (영구 저장) — 백테스트·기술지표 yfinance 호출을 DB 캐시로 대체
# ---------------------------------------------------------------------------
# 가격 backfill 백그라운드 루프 활성/주기/지연
PRICE_BACKFILL_ENABLED = _bool_env("PRICE_BACKFILL_ENABLED", "true")
PRICE_BACKFILL_INTERVAL_SEC = int(os.getenv("PRICE_BACKFILL_INTERVAL_SEC", "21600"))  # 6시간
PRICE_BACKFILL_INITIAL_DELAY_SEC = int(os.getenv("PRICE_BACKFILL_INITIAL_DELAY_SEC", "60"))
# backfill 시 매번 받을 최근 거래일 수 (5일이면 새 거래일 + 누락분 보정)
PRICE_BACKFILL_LOOKBACK_DAYS = int(os.getenv("PRICE_BACKFILL_LOOKBACK_DAYS", "7"))
# DB 에 저장된 ticker 의 마지막 거래일이 이 일수 이상 오래된 경우 yfinance 재호출 (stale)
PRICE_DB_STALE_DAYS = int(os.getenv("PRICE_DB_STALE_DAYS", "5"))
# 범위 커버리지 임계값 — 받은 거래일 수 / 예상 거래일 수 가 이 비율 미만이면 sparse 로 판정해
# yfinance 에서 범위 전체를 다시 받아 채운다 (예: 90일 요청에 5일만 있는 경우).
PRICE_DB_COVERAGE_THRESHOLD = float(os.getenv("PRICE_DB_COVERAGE_THRESHOLD", "0.5"))
# price_history 페이지네이션 안전 상한 (200페이지 × 1000 = 20만 row)
PRICE_HISTORY_MAX_PAGES = int(os.getenv("PRICE_HISTORY_MAX_PAGES", "200"))

# 풀히스토리 부트스트랩(1회) — S&P 500 전체의 period="max" 일봉 OHLCV 를 받아 DB 에 저장.
# 기동 시 커버리지가 부족할 때만 실행되며, 이후 일일 증분은 backfill_recent 가 담당한다.
PRICE_BACKFILL_FULL_HISTORY_ENABLED = _bool_env("PRICE_BACKFILL_FULL_HISTORY_ENABLED", "true")
# 부트스트랩 batch 당 ticker 수. period="max" 응답이 크니 너무 크게 잡지 말 것 (메모리).
PRICE_BACKFILL_FULL_HISTORY_BATCH_SIZE = int(os.getenv("PRICE_BACKFILL_FULL_HISTORY_BATCH_SIZE", "20"))
# 부트스트랩 batch 간 대기(초) — yfinance rate limit 분산.
PRICE_BACKFILL_FULL_HISTORY_BATCH_DELAY_SEC = float(os.getenv("PRICE_BACKFILL_FULL_HISTORY_BATCH_DELAY_SEC", "1.0"))
# 풀히스토리 충분 판정 — 최근 N일 이전 시점의 기록을 가진 ticker 가 M 개 이상이면 부트스트랩 스킵.
PRICE_HISTORY_COVERAGE_MIN_TICKERS = int(os.getenv("PRICE_HISTORY_COVERAGE_MIN_TICKERS", "400"))
PRICE_HISTORY_COVERAGE_MIN_DAYS = int(os.getenv("PRICE_HISTORY_COVERAGE_MIN_DAYS", "365"))
# 일일 증분 backfill 이 S&P 500 전체를 커버하도록 강제 — False 면 active(analysis_results 최근) 만.
PRICE_BACKFILL_RECENT_USE_SP500 = _bool_env("PRICE_BACKFILL_RECENT_USE_SP500", "true")
# 진행 중(open) 포지션 라이브 뷰 — 현재가 자주 변동 → 짧은 TTL
BACKTEST_LIVE_CACHE_TTL_SEC = int(os.getenv("BACKTEST_LIVE_CACHE_TTL_SEC", "60"))  # 1분
# 진행 중 포지션 응답에 포함할 포지션 상한 (horizon별)
BACKTEST_LIVE_POSITIONS_PER_HORIZON = int(os.getenv("BACKTEST_LIVE_POSITIONS_PER_HORIZON", "200"))

# Trade history (진입→청산 단위 거래 리스트)
BACKTEST_TRADES_DEFAULT_HORIZON = int(os.getenv("BACKTEST_TRADES_DEFAULT_HORIZON", "5"))
BACKTEST_TRADES_MAX_TRADES = int(os.getenv("BACKTEST_TRADES_MAX_TRADES", "200"))
# 일 단위 그룹핑 시 leg가 많아질 수 있어 cap 을 50 으로 확대
BACKTEST_TRADES_MAX_LEGS_PER_TRADE = int(os.getenv("BACKTEST_TRADES_MAX_LEGS_PER_TRADE", "50"))
# trade 그룹 윈도우: day | hour | minute
# - day: "그날 들어온 모든 BUY/SELL 시그널 = 한 trade" (사용자 멘탈 모델에 가장 가까움)
# - hour: 시간 단위
# - minute: batch insert 동일 분 (이전 동작)
BACKTEST_TRADES_DEFAULT_GROUP_BY = os.getenv("BACKTEST_TRADES_DEFAULT_GROUP_BY", "day")
BACKTEST_TRADES_VALID_GROUP_BYS = frozenset({"day", "hour", "minute"})
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
# 시그널 백테스트가 평가할 방향 — 보유 안 한 종목 SELL 은 일반 투자자에게
# 의미가 없어 기본 BUY 만. env 로 "BUY,SELL" 지정 시 둘 다 평가.
BACKTEST_SIGNALS_INCLUDE_DIRECTIONS = frozenset(
    s.strip().upper()
    for s in os.getenv("BACKTEST_SIGNALS_INCLUDE_DIRECTIONS", "BUY").split(",")
    if s.strip()
)
# yfinance 일괄 다운로드 기간 여유(주말/공휴일 대비)
BACKTEST_PRICE_LOOKAHEAD_DAYS = 14

# --- 거래 비용 & 슬리피지 (백테스트 현실화) ---
# 왕복(진입+청산) 거래 비용을 bps(1bp=0.01%) 로 지정. commission + slippage 합산 개념.
# 모든 백테스트 수익률(adjusted/portfolio)은 이 비용을 차감한 net 기준으로 산출되며,
# raw_return_pct(방향 미조정 총수익률)은 비용 차감 전 gross 로 유지해 투명성을 확보한다.
# 0 이면 비용 미반영. 대형주(S&P 500) 기준 왕복 10bps(0.10%) 를 보수적 기본값으로.
BACKTEST_COST_BPS = float(os.getenv("BACKTEST_COST_BPS", "10.0"))

# --- 손절/목표가 청산 모델 (전략실 추천 전용) ---
# 전략실 추천은 stop_loss / target 가격(절대가)을 함께 산출한다. price_history 의
# OHLC 고저가를 이용해 horizon 내에서 손절·목표가가 먼저 닿았는지 평가하고, 닿았다면
# 그 가격에 청산했다고 가정한다(미달이면 기존처럼 horizon 종가 시간청산).
# True 면 전략실 백테스트/trade/live 에 손절·익절이 반영돼 "계획대로 운용"한 결과가 된다.
BACKTEST_PLANNED_EXIT_ENABLED = _bool_env("BACKTEST_PLANNED_EXIT_ENABLED", "true")
# 같은 거래일 범위 안에서 손절·목표가가 동시에 닿은 경우 어느 쪽을 먼저 체결로 볼지.
# 보수적으로 손절 우선('stop'). 'target' 지정 시 목표가 우선.
BACKTEST_INTRABAR_PRIORITY = os.getenv("BACKTEST_INTRABAR_PRIORITY", "stop").strip().lower()
if BACKTEST_INTRABAR_PRIORITY not in ("stop", "target"):
    BACKTEST_INTRABAR_PRIORITY = "stop"
# 청산 사유 라벨 — 단일 소스 (services/backtest 의 _Evaluation.exit_reason / by_exit_reason)
BACKTEST_EXIT_REASON_STOP = "stop"
BACKTEST_EXIT_REASON_TARGET = "target"
BACKTEST_EXIT_REASON_TIME = "time"

# --- 벤치마크 (시장 대비 알파) ---
# 각 거래의 보유 구간(entry~exit) 동안 벤치마크(기본 SPY)가 낸 방향성 수익률 대비
# 초과수익(알파)을 horizon 별로 집계한다. 빈 문자열이면 벤치마크 비교 비활성화.
BACKTEST_BENCHMARK_TICKER = os.getenv("BACKTEST_BENCHMARK_TICKER", "SPY").upper().strip()

# --- 통계 신뢰도(표본 수) ---
# 헤드라인 지표가 통계적으로 신뢰할 만한 최소 표본 수. 미만이면 metrics.reliable=False.
BACKTEST_RELIABLE_MIN_SAMPLES = int(os.getenv("BACKTEST_RELIABLE_MIN_SAMPLES", "20"))
# CAGR(연복리) 연환산 최소 달력 구간(일). equity curve 구간이 너무 짧으면 연환산이
# 비현실적으로 폭증(예: 며칠 만의 큰 수익을 1년으로 외삽)하므로 미만이면 CAGR/Calmar=None.
BACKTEST_CAGR_MIN_SPAN_DAYS = int(os.getenv("BACKTEST_CAGR_MIN_SPAN_DAYS", "20"))

# --- 응답 지연 SLA: stale-while-revalidate (SWR) + 파일 영속화 ---
# 백테스트 페이지 진입은 항상 즉시 응답해야 한다(목표 ≤3s). 캐시가 신선하면 즉시 반환,
# soft TTL(BACKTEST_CACHE_TTL_SEC) 초과 + MAX_STALE 이내면 '직전 결과를 즉시 반환 +
# 백그라운드 재계산'(사용자는 재계산을 기다리지 않음). MAX_STALE 초과/캐시 없음이면 동기 계산.
BACKTEST_SWR_ENABLED = _bool_env("BACKTEST_SWR_ENABLED", "true")
# 완료 백테스트(과거 분석)는 하루 정도 stale 허용해도 무방 — 자동 워밍이 25분마다 갱신.
BACKTEST_SWR_MAX_STALE_SEC = int(os.getenv("BACKTEST_SWR_MAX_STALE_SEC", "86400"))  # 1일
# 라이브(진행 중 포지션)는 현재가 mark-to-market 이라 stale 허용을 짧게 — 그 이상이면 동기 계산.
BACKTEST_LIVE_SWR_MAX_STALE_SEC = int(os.getenv("BACKTEST_LIVE_SWR_MAX_STALE_SEC", "600"))  # 10분
# 결과 캐시 DB 영속화(Supabase backtest_cache 테이블) — 프로세스 재시작 후 첫 진입도
# 직전 스냅샷으로 즉답(+백그라운드 갱신). 키별 upsert + 기동 시 전체 복원.
BACKTEST_PERSIST_ENABLED = _bool_env("BACKTEST_PERSIST_ENABLED", "true")

# ---------------------------------------------------------------------------
# AI Chat (종목 질의 챗봇, SSE 스트리밍)
# ---------------------------------------------------------------------------
# 챗봇은 응답 속도가 중요 — 기본값을 경량 모델로. 품질 우선이면 env로 gpt-5 지정.
CHAT_OPENAI_MODEL = os.getenv("CHAT_OPENAI_MODEL", "gpt-5-mini")
# OpenAI client 타임아웃 — 60초 안에 답이 오지 않으면 끊는다.
CHAT_OPENAI_TIMEOUT_SEC = int(os.getenv("CHAT_OPENAI_TIMEOUT_SEC", "30"))
# 각 stage 별 cap — yfinance 차단 등으로 한 단계가 분 단위로 멈춰도 강제 종료.
# 모든 stage 가 다 timeout 으로 떨어져도 합이 60초 이하가 되도록 — "최악도 1분 안에 응답".
CHAT_STAGE_STOCKS_TIMEOUT_SEC = int(os.getenv("CHAT_STAGE_STOCKS_TIMEOUT_SEC", "12"))
CHAT_STAGE_TECHNICALS_TIMEOUT_SEC = int(os.getenv("CHAT_STAGE_TECHNICALS_TIMEOUT_SEC", "10"))
CHAT_STAGE_MARKET_TIMEOUT_SEC = int(os.getenv("CHAT_STAGE_MARKET_TIMEOUT_SEC", "6"))
# LLM 첫 토큰 도착 cap — 이후 토큰은 stream 자체가 끊기지 않으면 계속.
CHAT_LLM_FIRST_TOKEN_TIMEOUT_SEC = int(os.getenv("CHAT_LLM_FIRST_TOKEN_TIMEOUT_SEC", "28"))
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
