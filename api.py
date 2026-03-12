import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from services.crud import init_db, sanitize_for_json
from services.engine import run_analysis_loop, run_macro_loop
from services.websocket import manager, latest_cache
from services.scanner import fetch_macro_indicators
from routers import market, stock


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # 기동 직후 매크로 1회 수집 — 프론트 첫 요청에서 바로 데이터 내려주기 위함
    try:
        macro = fetch_macro_indicators()
        latest_cache["macro"] = macro
        latest_cache["updated_at"] = datetime.now().isoformat()
    except Exception as e:
        print(f"⚠️ 기동 시 매크로 수집 실패: {e}")

    scan_task = asyncio.create_task(run_analysis_loop())
    macro_task = asyncio.create_task(run_macro_loop())
    yield
    scan_task.cancel()
    macro_task.cancel()


app = FastAPI(lifespan=lifespan, title="Woochan AI Quant Terminal API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(market.router)
app.include_router(stock.router)


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
    uvicorn.run(app, host="0.0.0.0", port=8000)
