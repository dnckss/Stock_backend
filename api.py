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
)
from services.websocket import manager, latest_cache
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
    try:
        from config import NEWS_FALLBACK_TICKERS
        from services.news_feed import prefetch_news_articles
        feed = await build_news_feed(NEWS_FALLBACK_TICKERS)
        latest_cache["news_feed"] = feed
        latest_cache["updated_at"] = datetime.now().isoformat()
        asyncio.create_task(prefetch_news_articles(feed))
    except Exception as e:
        logger.warning("기동 시 뉴스 수집 실패: %s", e, exc_info=True)


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
    scan_task = asyncio.create_task(run_analysis_loop())
    macro_task = asyncio.create_task(run_macro_loop())
    price_tick_task = asyncio.create_task(run_price_tick_loop())
    econ_task = asyncio.create_task(run_econ_calendar_loop())
    news_task = asyncio.create_task(run_news_feed_loop())
    backtest_warmup_task = asyncio.create_task(run_backtest_warmup_loop())
    price_backfill_task = asyncio.create_task(run_price_backfill_loop())
    logger.info("lifespan yield — uvicorn 이 PORT listen 시작")
    yield
    logger.info("lifespan shutdown — task 정리")
    seed_task.cancel()
    scan_task.cancel()
    macro_task.cancel()
    price_tick_task.cancel()
    econ_task.cancel()
    news_task.cancel()
    backtest_warmup_task.cancel()
    price_backfill_task.cancel()




app = FastAPI(lifespan=lifespan, title="Woochan AI Quant Terminal API")

from config import CORS_ALLOW_ORIGINS

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
