import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from services.crud import init_db, sanitize_for_json
from services.engine import run_analysis_loop, run_macro_loop, run_price_tick_loop
from services.websocket import manager, latest_cache
from services.scanner import fetch_macro_indicators, get_market_gauge
from services.news_feed import build_news_feed
from routers import market, stock
from routers import strategy
from routers import news

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

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
        latest_cache["news_feed"] = await build_news_feed(NEWS_FALLBACK_TICKERS)
        latest_cache["updated_at"] = datetime.now().isoformat()
    except Exception as e:
        logger.warning("기동 시 뉴스 수집 실패: %s", e, exc_info=True)

    scan_task = asyncio.create_task(run_analysis_loop())
    macro_task = asyncio.create_task(run_macro_loop())
    price_tick_task = asyncio.create_task(run_price_tick_loop())
    yield
    scan_task.cancel()
    macro_task.cancel()
    price_tick_task.cancel()


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
