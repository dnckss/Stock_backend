from fastapi import APIRouter, HTTPException, Query

from services.strategist import get_cached_market_strategy
from services.portfolio_builder import build_portfolio
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


@router.get("/portfolio")
async def api_portfolio(
    budget: float = Query(..., gt=0, description="투자 금액 (USD)"),
    style: str = Query("balanced", description="투자 성향: aggressive / balanced / conservative"),
    period: str = Query("medium", description="투자 기간: short(1~2주) / medium(1~3개월) / long(6개월+)"),
    exclude: str = Query("", description="제외 종목 (쉼표 구분, 예: TSLA,META)"),
):
    """
    AI 포트폴리오 빌더.
    투자 금액 + 성향 + 기간을 입력하면 최적의 종목 배분을 제안한다.
    """
    if style not in ("aggressive", "balanced", "conservative"):
        raise HTTPException(status_code=400, detail="style은 aggressive/balanced/conservative 중 하나여야 합니다.")
    if period not in ("short", "medium", "long"):
        raise HTTPException(status_code=400, detail="period는 short/medium/long 중 하나여야 합니다.")

    exclude_list = [t.strip().upper() for t in exclude.split(",") if t.strip()] if exclude else None

    # 전략 데이터 가져오기 (캐시 히트 시 즉시)
    macro = latest_cache.get("macro")
    market_gauge = latest_cache.get("market_gauge")
    vix = latest_cache.get("vix")
    news_feed = latest_cache.get("news_feed")
    strategy_data = await get_cached_market_strategy(macro, market_gauge, vix, news_feed)

    return await build_portfolio(
        budget=budget,
        style=style,
        period=period,
        strategy_data=strategy_data,
        exclude_tickers=exclude_list,
    )
