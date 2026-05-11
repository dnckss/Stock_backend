import asyncio
import logging
import warnings
from contextlib import asynccontextmanager

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

logger = logging.getLogger(__name__)


async def _seed_initial_caches():
    """초기 캐시 시드 — 모두 background. yield 를 막지 않도록 lifespan 외부에서 await."""
    # DB에서 마지막 스캔 데이터 로드 (페이지네이션 다중 호출, blocking → to_thread)
    try:
        from config import REPORT_TOP_N
        cached_records = await asyncio.to_thread(get_latest_scan_records)
        if cached_records:
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
    init_db()

    seed_task = asyncio.create_task(_seed_initial_caches())
    scan_task = asyncio.create_task(run_analysis_loop())
    macro_task = asyncio.create_task(run_macro_loop())
    price_tick_task = asyncio.create_task(run_price_tick_loop())
    econ_task = asyncio.create_task(run_econ_calendar_loop())
    news_task = asyncio.create_task(run_news_feed_loop())
    backtest_warmup_task = asyncio.create_task(run_backtest_warmup_loop())
    price_backfill_task = asyncio.create_task(run_price_backfill_loop())
    yield
    seed_task.cancel()
    scan_task.cancel()
    macro_task.cancel()
    price_tick_task.cancel()
    econ_task.cancel()
    news_task.cancel()
    backtest_warmup_task.cancel()
    price_backfill_task.cancel()




app = FastAPI(lifespan=lifespan, title="Woochan AI Quant Terminal API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(market.router)
app.include_router(stock.router)
app.include_router(strategy.router)
app.include_router(news.router)
app.include_router(chat.router)
app.include_router(backtest.router)


@app.websocket("/ws/market")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        if latest_cache.get("updated_at"):
            await websocket.send_json({"type": "MARKET_UPDATE", **sanitize_for_json(latest_cache)})
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


if __name__ == "__main__":
    import os
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Render/Heroku 등 PaaS 는 PORT 환경변수로 포트 주입 — 없으면 로컬 기본 8000.
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
