import asyncio
import logging
import os
import sys
import warnings
from contextlib import asynccontextmanager

# Render/PaaS 디버깅을 위해 최상단에서 logging 을 stdout 으로 강제 + line-buffered.
# 이렇게 안 하면 import 단계의 에러/로그가 buffering 으로 안 보여 "silent hang" 처럼 보인다.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
    force=True,
)
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except AttributeError:
    pass

logger = logging.getLogger(__name__)
logger.info("api.py import 시작 (Python %s)", sys.version.split()[0])
logger.info(
    "env: PORT=%s SUPABASE_URL=%s OPENAI_API_KEY=%s",
    os.environ.get("PORT", "(none)"),
    "set" if os.environ.get("SUPABASE_URL") else "MISSING",
    "set" if os.environ.get("OPENAI_API_KEY") else "MISSING",
)

warnings.filterwarnings("ignore", category=FutureWarning, module="yfinance")
warnings.filterwarnings("ignore", message=".*Timestamp.utcnow.*")
warnings.filterwarnings("ignore", message=".*Pandas4Warning.*")
warnings.filterwarnings("ignore", module="yfinance", category=DeprecationWarning)
try:
    from pandas.errors import Pandas4Warning
    warnings.filterwarnings("ignore", category=Pandas4Warning)
except ImportError:
    pass
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

# orjson: stdlib json 대비 5~10배 빠른 직렬화 + NaN/Inf 자동 None 처리(OPT_PASSTHROUGH 미사용).
# ORJSONResponse 자체는 fastapi 가 항상 export 하지만, 실제 render() 시 orjson 모듈을
# 동적으로 require 하므로 ``import orjson`` 이 실패하면 우리도 JSONResponse 로 폴백한다.
# (이 가드를 빼면 응답 렌더링 시점에 AssertionError: orjson must be installed 가 난다.)
try:
    import orjson  # noqa: F401 — 미설치 시 ImportError 발생
    from fastapi.responses import ORJSONResponse  # type: ignore
    _DEFAULT_RESPONSE_CLASS = ORJSONResponse
    logger.info("orjson 사용 — ORJSONResponse 활성")
except ImportError:  # pragma: no cover — orjson 누락 시 stdlib 폴백
    from fastapi.responses import JSONResponse as _DEFAULT_RESPONSE_CLASS  # type: ignore
    logger.warning(
        "orjson 미설치 — JSONResponse 폴백. 설치하려면: pip install orjson==3.11.9",
    )

logger.info("FastAPI/표준 import 완료, services import 시작")

from services.crud import init_db, sanitize_for_json, get_latest_scan_records
from services.engine import (
    run_analysis_loop,
    run_backtest_warmup_loop,
    run_econ_calendar_loop,
    run_macro_loop,
    run_news_feed_loop,
    run_price_backfill_loop,
    run_price_tick_loop,
    run_strategy_warmup_loop,
)
from services.websocket import manager, latest_cache
from services.utils import spawn_logged
from services.scanner import fetch_macro_indicators, get_market_gauge
from services.news_feed import build_news_feed
from routers import market, stock
from routers import strategy
from routers import news
from routers import chat
from routers import backtest
from routers import risk
from routers import sectors

logger.info("services/routers import 완료")


async def _seed_initial_caches():
    """초기 캐시 시드 — 모두 background. yield 를 막지 않도록 lifespan 외부에서 await."""
    # DB에서 마지막 스캔 데이터 로드 (페이지네이션 다중 호출, blocking → to_thread)
    try:
        from config import REPORT_TOP_N
        cached_records = await asyncio.to_thread(get_latest_scan_records)
        if cached_records:
            from services.scanner import ensure_sp500_coverage
            cached_records = await asyncio.to_thread(ensure_sp500_coverage, cached_records)
            latest_cache["top_picks"] = cached_records[:REPORT_TOP_N]
            latest_cache["radar"] = cached_records[REPORT_TOP_N:]
            latest_cache["updated_at"] = datetime.now().isoformat()
            logger.info("DB에서 마지막 스캔 데이터 %d건 로드 완료", len(cached_records))
    except Exception as e:
        logger.warning("기동 시 DB 캐시 로드 실패: %s", e, exc_info=True)

    # 매크로 (yfinance 동기 호출)
    try:
        macro = await asyncio.to_thread(fetch_macro_indicators)
        latest_cache["macro"] = macro
        gauge_data = get_market_gauge(macro)
        latest_cache["market_gauge"] = gauge_data["market_gauge"]
        latest_cache["vix"] = gauge_data["vix"]
        latest_cache["updated_at"] = datetime.now().isoformat()
    except Exception as e:
        logger.warning("기동 시 매크로 수집 실패: %s", e, exc_info=True)

    # 뉴스 + FinBERT (가장 무거움 — 모델 첫 로드 시 수백 MB 다운로드)
    # 본문 프리페치는 build_news_feed 내부에서 spawn 된다(별도 호출 불필요).
    try:
        from config import NEWS_FALLBACK_TICKERS
        feed = await build_news_feed(NEWS_FALLBACK_TICKERS)
        latest_cache["news_feed"] = feed
        latest_cache["updated_at"] = datetime.now().isoformat()
    except Exception as e:
        logger.warning("기동 시 뉴스 수집 실패: %s", e, exc_info=True)


async def _bootstrap_price_history():
    """1회성 부트스트랩 — price_history 가 부족하면 S&P 500 풀히스토리(period=max)를 백필.

    upsert 라 재실행 안전(idempotent). 백그라운드로 진행되어 서버 기동을 막지 않으며,
    완료 전엔 차트 엔드포인트가 yfinance fallback 으로 동작한다.
    """
    from config import PRICE_BACKFILL_FULL_HISTORY_ENABLED
    if not PRICE_BACKFILL_FULL_HISTORY_ENABLED:
        logger.info("price_history 부트스트랩 비활성 (PRICE_BACKFILL_FULL_HISTORY_ENABLED=false)")
        return
    try:
        from services.price_store import backfill_full_history, check_price_history_coverage
        coverage = await asyncio.to_thread(check_price_history_coverage)
    except Exception as e:
        logger.warning("price_history 커버리지 점검 실패 — 부트스트랩 스킵: %s", e)
        return
    if coverage.get("ok"):
        logger.info(
            "price_history 풀히스토리 충분 (%s rows ≥ %s) — 부트스트랩 스킵",
            coverage.get("total_rows"), coverage.get("min_rows"),
        )
        return
    logger.info(
        "price_history 부족 (%s rows < %s) — 백그라운드 부트스트랩 시작 (수십 분 소요 가능)",
        coverage.get("total_rows", 0), coverage.get("min_rows", 0),
    )
    try:
        from services.scanner import get_all_tickers
        tickers = await asyncio.to_thread(get_all_tickers)
        if not tickers:
            logger.warning("S&P 500 ticker 리스트 비어 있음 — 부트스트랩 스킵")
            return
        result = await asyncio.to_thread(backfill_full_history, tickers)
        logger.info("price_history 풀히스토리 부트스트랩 완료: %s", result)
    except Exception as e:
        logger.exception("price_history 풀히스토리 부트스트랩 실패: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # PaaS(Render 등) port scan timeout 회피: yield 까지 도달 시간을 최소화한다.
    # 무거운 시드 작업(Supabase 페이지네이션, yfinance, FinBERT 로딩)은 모두 background task.
    logger.info("lifespan 진입 — env 검증 + init_db 호출")
    from config import validate_required_env
    # 필수 env 누락 시 fail-fast (STRICT_ENV=false 면 WARNING 으로 격하)
    validate_required_env()
    try:
        init_db()
    except Exception as e:
        logger.exception("init_db 실패 (계속 진행, 백그라운드에서 재시도): %s", e)
    logger.info("init_db 완료, 백그라운드 task 등록 중")

    seed_task = asyncio.create_task(_seed_initial_caches())
    bootstrap_task = spawn_logged(_bootstrap_price_history(), name="price_history_bootstrap")
    scan_task = asyncio.create_task(run_analysis_loop())
    macro_task = asyncio.create_task(run_macro_loop())
    price_tick_task = asyncio.create_task(run_price_tick_loop())
    econ_task = asyncio.create_task(run_econ_calendar_loop())
    news_task = asyncio.create_task(run_news_feed_loop())
    backtest_warmup_task = asyncio.create_task(run_backtest_warmup_loop())
    price_backfill_task = asyncio.create_task(run_price_backfill_loop())
    strategy_warmup_task = asyncio.create_task(run_strategy_warmup_loop())
    logger.info("lifespan yield — uvicorn 이 PORT listen 시작")
    yield
    logger.info("lifespan shutdown — task 정리")
    seed_task.cancel()
    bootstrap_task.cancel()
    scan_task.cancel()
    macro_task.cancel()
    price_tick_task.cancel()
    econ_task.cancel()
    news_task.cancel()
    backtest_warmup_task.cancel()
    price_backfill_task.cancel()
    strategy_warmup_task.cancel()




app = FastAPI(
    lifespan=lifespan,
    title="Woochan AI Quant Terminal API",
    # 모든 라우터 응답을 orjson 으로 직렬화 — stdlib json 대비 5~10배 빠르고
    # NaN/Inf 는 sanitize_for_json 이 이미 None 으로 치환했으므로 orjson 의 strict 모드와 호환.
    default_response_class=_DEFAULT_RESPONSE_CLASS,
)

from config import CORS_ALLOW_ORIGINS, GZIP_MIN_SIZE_BYTES

# GZip 압축: 큰 JSON 응답(히트맵·전체기록 등)을 전송 단계에서 70~90% 축소.
# 작은 응답(< GZIP_MIN_SIZE_BYTES) 은 압축 오버헤드가 더 크므로 그대로 둔다.
app.add_middleware(GZipMiddleware, minimum_size=GZIP_MIN_SIZE_BYTES)

# API 보호: per-IP 레이트리밋 + 선택적 API 키 (Public 배포 남용/비용 방어).
# CORS 보다 먼저 add → CORS 가 더 바깥에 위치 → ① 프리플라이트(OPTIONS)는 CORS 가
# 먼저 처리하고 ② 차단 응답(401/429)에도 CORS 헤더가 실린다. 로직은 services/security.py.
from services.security import build_security_dispatch
app.add_middleware(BaseHTTPMiddleware, dispatch=build_security_dispatch(_DEFAULT_RESPONSE_CLASS))

# allow_credentials 는 화이트리스트일 때만 True (브라우저 정책상 "*" 와 동시 사용 불가)
_cors_allow_credentials = CORS_ALLOW_ORIGINS != ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=_cors_allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)
logger.info(
    "CORS 설정: origins=%s, allow_credentials=%s",
    CORS_ALLOW_ORIGINS, _cors_allow_credentials,
)

app.include_router(market.router)
app.include_router(stock.router)
app.include_router(strategy.router)
app.include_router(news.router)
app.include_router(chat.router)
app.include_router(backtest.router)
app.include_router(risk.router)
app.include_router(sectors.router)


@app.get("/health", include_in_schema=False)
async def health():
    """라이브니스 헬스체크 — 프로세스가 살아있으면 200.

    호스트(Fly 등) 헬스체크·업타임 모니터(UptimeRobot)용. /api 가 아니라 무인증·무제한이며
    DB/외부호출 없이 즉답한다(핑 부하 0). 루트 '/'(404) 대신 이걸 핑 대상으로 쓴다.
    """
    return {"status": "ok", "service": "quantix-api"}


@app.websocket("/ws/market")
async def websocket_endpoint(websocket: WebSocket):
    """마켓 브로드캐스트 채널.

    운영 안정화:
      - 연결 상한 초과 시 1013 으로 거절(manager 내부)
      - WS_IDLE_TIMEOUT_SEC 동안 클라이언트 메시지 0개 → idle 종료
      - WS_HEARTBEAT_INTERVAL_SEC 마다 ping 송신해 dead connection 조기 탐지
    """
    from config import WS_HEARTBEAT_INTERVAL_SEC, WS_IDLE_TIMEOUT_SEC

    accepted = await manager.connect(websocket)
    if not accepted:
        return
    try:
        if latest_cache.get("updated_at"):
            await websocket.send_json({"type": "MARKET_UPDATE", **sanitize_for_json(latest_cache)})

        last_seen = asyncio.get_event_loop().time()
        while True:
            now = asyncio.get_event_loop().time()
            if now - last_seen > WS_IDLE_TIMEOUT_SEC:
                logger.info("WebSocket idle 타임아웃 (>%ds) — 연결 종료", WS_IDLE_TIMEOUT_SEC)
                await websocket.close(code=1001, reason="idle timeout")
                break
            try:
                # heartbeat 주기 안에 클라 메시지 도착하면 last_seen 갱신
                await asyncio.wait_for(
                    websocket.receive_text(), timeout=WS_HEARTBEAT_INTERVAL_SEC,
                )
                last_seen = asyncio.get_event_loop().time()
            except asyncio.TimeoutError:
                # heartbeat ping 송신 (실패하면 연결 끊긴 것)
                try:
                    await websocket.send_json({"type": "PING", "ts": datetime.now().isoformat()})
                except Exception:
                    logger.debug("WebSocket ping 송신 실패 — 연결 종료 처리")
                    break
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn

    # Render/Heroku 등 PaaS 는 PORT 환경변수로 포트 주입 — 없으면 로컬 기본 8000.
    port = int(os.environ.get("PORT", "8000"))
    logger.info("uvicorn.run 호출 — host=0.0.0.0 port=%d", port)
    uvicorn.run(app, host="0.0.0.0", port=port)
