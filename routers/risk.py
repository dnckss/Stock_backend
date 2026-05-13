"""포트폴리오 리스크 분석 라우터.

services/risk_analysis.py 의 기능을 외부 API 로 노출한다.
연산이 무거운 endpoint(MC 시뮬, 시나리오)는 그대로 동기 호출하므로
asyncio.to_thread 로 이벤트 루프 차단을 방지한다.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator

from services.crud import sanitize_for_json
from services.risk_analysis import (
    compute_correlation_matrix,
    compute_full_risk_analysis,
    compute_var,
    compute_volatility,
    detect_anomalies,
    run_monte_carlo,
    run_scenario_analysis,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/risk", tags=["Risk"])


_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
_MAX_TICKERS = 50


def _validate_tickers(tickers: list[str]) -> list[str]:
    cleaned = [(t or "").strip().upper() for t in tickers if t]
    cleaned = [t for t in cleaned if t]
    if not cleaned:
        raise HTTPException(status_code=400, detail="tickers 가 필요합니다.")
    if len(cleaned) > _MAX_TICKERS:
        raise HTTPException(status_code=400, detail=f"tickers 는 최대 {_MAX_TICKERS}개까지 허용됩니다.")
    for t in cleaned:
        if not _TICKER_RE.match(t):
            raise HTTPException(status_code=400, detail=f"잘못된 ticker 형식: {t}")
    return cleaned


def _normalize_weights(tickers: list[str], weights: list[float] | None) -> list[float]:
    if not weights:
        n = len(tickers)
        return [1.0 / n] * n
    if len(weights) != len(tickers):
        raise HTTPException(status_code=400, detail="weights 개수가 tickers 와 다릅니다.")
    total = sum(weights)
    if total <= 0:
        raise HTTPException(status_code=400, detail="weights 합계는 양수여야 합니다.")
    return [w / total for w in weights]


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class _PortfolioReq(BaseModel):
    tickers: list[str] = Field(..., min_length=1, max_length=_MAX_TICKERS)
    weights: list[float] | None = None
    portfolio_value: float = Field(default=10_000.0, gt=0)

    @field_validator("portfolio_value")
    @classmethod
    def _cap(cls, v: float) -> float:
        # 너무 큰 값(오버플로 방지) 상한
        return min(v, 1e12)


class _FullRiskReq(_PortfolioReq):
    betas: dict[str, float] | None = None


class _TickersOnlyReq(BaseModel):
    tickers: list[str] = Field(..., min_length=1, max_length=_MAX_TICKERS)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/full")
async def api_risk_full(req: _FullRiskReq) -> dict[str, Any]:
    """통합 리스크 분석 — 상관관계 + 변동성 + VaR + Monte Carlo + 시나리오 + 이상 탐지."""
    tickers = _validate_tickers(req.tickers)
    weights = _normalize_weights(tickers, req.weights)
    try:
        result = await asyncio.to_thread(
            compute_full_risk_analysis,
            tickers, weights, float(req.portfolio_value), req.betas or None,
        )
    except Exception as e:
        logger.exception("통합 리스크 분석 실패: %s", e)
        raise HTTPException(status_code=500, detail=f"리스크 분석 실패: {e}")
    return sanitize_for_json({"tickers": tickers, "weights": weights, **result})


@router.post("/correlation")
async def api_risk_correlation(req: _TickersOnlyReq) -> dict[str, Any]:
    """종목 간 상관관계 행렬."""
    tickers = _validate_tickers(req.tickers)
    try:
        result = await asyncio.to_thread(compute_correlation_matrix, tickers)
    except Exception as e:
        logger.exception("상관관계 분석 실패: %s", e)
        raise HTTPException(status_code=500, detail=f"상관관계 분석 실패: {e}")
    return sanitize_for_json({"tickers": tickers, **result})


@router.post("/volatility")
async def api_risk_volatility(req: _TickersOnlyReq) -> dict[str, Any]:
    """종목별 변동성(연환산)."""
    tickers = _validate_tickers(req.tickers)
    try:
        result = await asyncio.to_thread(compute_volatility, tickers)
    except Exception as e:
        logger.exception("변동성 계산 실패: %s", e)
        raise HTTPException(status_code=500, detail=f"변동성 계산 실패: {e}")
    return sanitize_for_json({"tickers": tickers, "volatility": result})


@router.post("/var")
async def api_risk_var(req: _PortfolioReq) -> dict[str, Any]:
    """포트폴리오 VaR (Value at Risk)."""
    tickers = _validate_tickers(req.tickers)
    weights = _normalize_weights(tickers, req.weights)
    try:
        result = await asyncio.to_thread(
            compute_var, tickers, weights, float(req.portfolio_value),
        )
    except Exception as e:
        logger.exception("VaR 계산 실패: %s", e)
        raise HTTPException(status_code=500, detail=f"VaR 계산 실패: {e}")
    return sanitize_for_json({"tickers": tickers, "weights": weights, **result})


@router.post("/monte-carlo")
async def api_risk_monte_carlo(req: _PortfolioReq) -> dict[str, Any]:
    """Monte Carlo 시뮬레이션."""
    tickers = _validate_tickers(req.tickers)
    weights = _normalize_weights(tickers, req.weights)
    try:
        result = await asyncio.to_thread(
            run_monte_carlo, tickers, weights, float(req.portfolio_value),
        )
    except Exception as e:
        logger.exception("Monte Carlo 실패: %s", e)
        raise HTTPException(status_code=500, detail=f"Monte Carlo 실패: {e}")
    return sanitize_for_json({"tickers": tickers, "weights": weights, **result})


@router.post("/scenarios")
async def api_risk_scenarios(req: _FullRiskReq) -> dict[str, Any]:
    """시나리오 분석 (시장 충격, 금리 변화 등)."""
    tickers = _validate_tickers(req.tickers)
    weights = _normalize_weights(tickers, req.weights)
    try:
        result = await asyncio.to_thread(
            run_scenario_analysis,
            tickers, weights, float(req.portfolio_value), req.betas or None,
        )
    except Exception as e:
        logger.exception("시나리오 분석 실패: %s", e)
        raise HTTPException(status_code=500, detail=f"시나리오 분석 실패: {e}")
    return sanitize_for_json({"tickers": tickers, "weights": weights, "scenarios": result})


@router.post("/anomalies")
async def api_risk_anomalies(req: _TickersOnlyReq) -> dict[str, Any]:
    """급등/급락 이상 패턴 탐지."""
    tickers = _validate_tickers(req.tickers)
    try:
        result = await asyncio.to_thread(detect_anomalies, tickers)
    except Exception as e:
        logger.exception("이상 탐지 실패: %s", e)
        raise HTTPException(status_code=500, detail=f"이상 탐지 실패: {e}")
    return sanitize_for_json({"tickers": tickers, "anomalies": result})
