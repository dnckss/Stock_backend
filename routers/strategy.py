from fastapi import APIRouter

from services.strategist import get_cached_market_strategy
from services.websocket import latest_cache

router = APIRouter(prefix="/api", tags=["Strategy"])


@router.get("/strategy")
async def api_strategy():
    """전체 시장 전략 브리핑(DB 집계 + OpenAI, 실패 시 fallback)."""
    macro = latest_cache.get("macro")
    market_gauge = latest_cache.get("market_gauge")
    vix = latest_cache.get("vix")
    news_feed = latest_cache.get("news_feed")
    return await get_cached_market_strategy(macro, market_gauge, vix, news_feed)
