"""
백테스팅 API 라우터.

  GET /api/backtest/signals       — analysis_results(괴리율·시그널) 기반
  GET /api/backtest/strategist    — strategy_history(AI 추천) 기반
  GET /api/backtest/summary       — 두 경로 통합 headline (AI 예측률 등)

공통 쿼리 파라미터:
  - lookback_days: 과거 며칠치 레코드를 평가할지 (기본 90, 최대 365)
  - horizons: 평가 horizon(거래일) CSV (기본 "1,5,20")
  - refresh: 1이면 캐시 무시
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query

from config import (
    BACKTEST_DEFAULT_HORIZONS,
    BACKTEST_DEFAULT_LOOKBACK_DAYS,
    BACKTEST_MAX_HORIZON_DAYS,
    BACKTEST_MAX_LOOKBACK_DAYS,
)
from services.backtest import (
    run_signals_backtest,
    run_strategist_backtest,
    run_summary,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["Backtest"])


_DEFAULT_HORIZONS_STR = ",".join(str(h) for h in BACKTEST_DEFAULT_HORIZONS)


def _parse_horizons(horizons: str | None) -> list[int]:
    """'1,5,20' → [1,5,20]. 잘못된 항목은 스킵, 결과 비면 기본값."""
    if not horizons:
        return list(BACKTEST_DEFAULT_HORIZONS)
    parsed: list[int] = []
    for token in horizons.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            value = int(token)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"horizons에 숫자가 아닌 값: {token}")
        if value <= 0 or value > BACKTEST_MAX_HORIZON_DAYS:
            raise HTTPException(
                status_code=400,
                detail=f"horizon은 1~{BACKTEST_MAX_HORIZON_DAYS} 범위여야 합니다(입력: {value})",
            )
        parsed.append(value)
    return parsed or list(BACKTEST_DEFAULT_HORIZONS)


@router.get("/backtest/signals")
async def api_backtest_signals(
    lookback_days: int = Query(
        default=BACKTEST_DEFAULT_LOOKBACK_DAYS,
        ge=1,
        le=BACKTEST_MAX_LOOKBACK_DAYS,
        description="과거 며칠치 레코드를 평가할지",
    ),
    horizons: str = Query(default=_DEFAULT_HORIZONS_STR, description="평가 horizon(거래일) CSV"),
    refresh: int = Query(default=0, description="1이면 캐시 무시"),
):
    """대시보드 시그널(괴리율 + signal) 백테스트."""
    hs = _parse_horizons(horizons)
    try:
        return await run_signals_backtest(lookback_days, hs, refresh=bool(refresh))
    except Exception as e:
        logger.exception("signals 백테스트 실패: %s", e)
        raise HTTPException(status_code=500, detail=f"signals 백테스트 실패: {e}")


@router.get("/backtest/strategist")
async def api_backtest_strategist(
    lookback_days: int = Query(
        default=BACKTEST_DEFAULT_LOOKBACK_DAYS,
        ge=1,
        le=BACKTEST_MAX_LOOKBACK_DAYS,
    ),
    horizons: str = Query(default=_DEFAULT_HORIZONS_STR),
    refresh: int = Query(default=0),
):
    """AI 전략실 추천(strategy_history) 백테스트."""
    hs = _parse_horizons(horizons)
    try:
        return await run_strategist_backtest(lookback_days, hs, refresh=bool(refresh))
    except Exception as e:
        logger.exception("strategist 백테스트 실패: %s", e)
        raise HTTPException(status_code=500, detail=f"strategist 백테스트 실패: {e}")


@router.get("/backtest/summary")
async def api_backtest_summary(
    lookback_days: int = Query(
        default=BACKTEST_DEFAULT_LOOKBACK_DAYS,
        ge=1,
        le=BACKTEST_MAX_LOOKBACK_DAYS,
    ),
    horizons: str = Query(default=_DEFAULT_HORIZONS_STR),
    refresh: int = Query(default=0),
):
    """
    두 소스의 headline 지표 통합 요약 — 백테스트 페이지 상단 카드용.

    각 horizon 별 hit_rate_pct, avg_return_pct, profit_factor, Sharpe, MDD, total_return_pct.
    """
    hs = _parse_horizons(horizons)
    try:
        return await run_summary(lookback_days, hs, refresh=bool(refresh))
    except Exception as e:
        logger.exception("summary 백테스트 실패: %s", e)
        raise HTTPException(status_code=500, detail=f"summary 백테스트 실패: {e}")
