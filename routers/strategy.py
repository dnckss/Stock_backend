from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from services.strategist import get_cached_market_strategy
from services.portfolio_builder import build_portfolio
from services.portfolio_agents import stream_portfolio_build
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


@router.get("/portfolio/stream")
async def api_portfolio_stream(
    budget: float = Query(..., gt=0, description="투자 금액 (USD)"),
    style: str = Query("balanced", description="투자 성향: aggressive / balanced / conservative"),
    period: str = Query("medium", description="투자 기간: short / medium / long"),
    exclude: str = Query("", description="제외 종목 (쉼표 구분)"),
):
    """
    AI 멀티에이전트 포트폴리오 빌더 (SSE 스트리밍).

    5단계 에이전트가 순차적으로 분석하며 AI의 사고 과정(CoT)을 실시간으로 전달한다.

    SSE 이벤트 타입:
      - pipeline_start: 파이프라인 시작 정보
      - agent_start: 에이전트 시작 (step, title, description)
      - thinking: AI 사고 과정 텍스트 (실시간 스트리밍)
      - agent_result: 에이전트 분석 결과 데이터
      - agent_error: 에이전트 오류
      - complete: 최종 포트폴리오 결과 (전체 데이터 포함)
    """
    if style not in ("aggressive", "balanced", "conservative"):
        raise HTTPException(status_code=400, detail="style은 aggressive/balanced/conservative 중 하나여야 합니다.")
    if period not in ("short", "medium", "long"):
        raise HTTPException(status_code=400, detail="period는 short/medium/long 중 하나여야 합니다.")

    exclude_list = [t.strip().upper() for t in exclude.split(",") if t.strip()] if exclude else None

    # 전략 데이터 (캐시 활용)
    macro = latest_cache.get("macro")
    market_gauge = latest_cache.get("market_gauge")
    vix = latest_cache.get("vix")
    news_feed = latest_cache.get("news_feed")
    strategy_data = await get_cached_market_strategy(macro, market_gauge, vix, news_feed)

    async def _generate():
        async for event in stream_portfolio_build(
            budget=budget,
            style=style,
            period=period,
            strategy_data=strategy_data,
            exclude_tickers=exclude_list,
        ):
            yield event

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream; charset=utf-8",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
