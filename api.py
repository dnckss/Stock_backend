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
from services.engine import run_analysis_loop, run_macro_loop, run_price_tick_loop, run_econ_calendar_loop, run_news_feed_loop
from services.websocket import manager, latest_cache
from services.scanner import fetch_macro_indicators, get_market_gauge
from services.news_feed import build_news_feed
from routers import market, stock
from routers import strategy
from routers import news
from routers import chat

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    # DB에서 마지막 스캔 데이터를 로드하여 수집 완료 전까지 프론트 공백 방지
    try:
        from config import REPORT_TOP_N
        cached_records = get_latest_scan_records()
        if cached_records:
            latest_cache["top_picks"] = cached_records[:REPORT_TOP_N]
            latest_cache["radar"] = cached_records[REPORT_TOP_N:]
            latest_cache["updated_at"] = datetime.now().isoformat()
            logger.info("DB에서 마지막 스캔 데이터 %d건 로드 완료", len(cached_records))
    except Exception as e:
        logger.warning("기동 시 DB 캐시 로드 실패: %s", e, exc_info=True)

    try:
        macro = fetch_macro_indicators()
        latest_cache["macro"] = macro
        gauge_data = get_market_gauge(macro)
        latest_cache["market_gauge"] = gauge_data["market_gauge"]
        latest_cache["vix"] = gauge_data["vix"]
        latest_cache["updated_at"] = datetime.now().isoformat()
    except Exception as e:
        logger.warning("기동 시 매크로 수집 실패: %s", e, exc_info=True)

    try:
        from config import NEWS_FALLBACK_TICKERS
        from services.news_feed import prefetch_news_articles
        feed = await build_news_feed(NEWS_FALLBACK_TICKERS)
        latest_cache["news_feed"] = feed
        latest_cache["updated_at"] = datetime.now().isoformat()
        # 기동 시 본문 프리페치 시작
        asyncio.create_task(prefetch_news_articles(feed))
    except Exception as e:
        logger.warning("기동 시 뉴스 수집 실패: %s", e, exc_info=True)

    scan_task = asyncio.create_task(run_analysis_loop())
    macro_task = asyncio.create_task(run_macro_loop())
    price_tick_task = asyncio.create_task(run_price_tick_loop())
    econ_task = asyncio.create_task(run_econ_calendar_loop())
    news_task = asyncio.create_task(run_news_feed_loop())
    yield
    scan_task.cancel()
    macro_task.cancel()
    price_tick_task.cancel()
    econ_task.cancel()
    news_task.cancel()




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
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    uvicorn.run(app, host="0.0.0.0", port=8000)
