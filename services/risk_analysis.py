"""
리스크 분석 모듈.
Monte Carlo 시뮬레이션, 상관관계 분석, VaR, 시나리오 시뮬레이션을 제공한다.
"""
from __future__ import annotations

import logging
import math
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf

from config import (
    MONTE_CARLO_DAYS,
    MONTE_CARLO_SIMULATIONS,
    RISK_HISTORY_PERIOD,
    SCENARIO_MARKET_SHOCK_PCT,
    SCENARIO_RATE_CHANGE_BPS,
    VAR_CONFIDENCE_LEVELS,
)

logger = logging.getLogger(__name__)


def _safe_round(v: Any, n: int = 4) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return round(f, n) if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# 히스토리 수익률 다운로드
# ---------------------------------------------------------------------------

def _download_returns(tickers: list[str], period: str = RISK_HISTORY_PERIOD) -> pd.DataFrame | None:
    """종목들의 일간 수익률 DataFrame을 반환한다. 실패 시 None."""
    if not tickers:
        return None
    try:
        data = yf.download(tickers, period=period, interval="1d", progress=False, threads=True)
    except Exception as e:
        logger.warning("리스크 분석 데이터 다운로드 실패: %s", e)
        return None

    if data.empty:
        return None

    if isinstance(data.columns, pd.MultiIndex):
        close = data["Close"]
    else:
        close = data[["Close"]]
        close.columns = tickers[:1]

    close = close.dropna(how="all")
    if len(close) < 30:
        return None

    returns = close.pct_change().dropna()
    # 단일 티커인 경우 컬럼명 보정
    if len(tickers) == 1 and isinstance(returns, pd.Series):
        returns = returns.to_frame(tickers[0])
    return returns


# ---------------------------------------------------------------------------
# 상관관계 분석
# ---------------------------------------------------------------------------

def compute_correlation_matrix(tickers: list[str]) -> dict[str, Any]:
    """
    종목 간 상관관계 행렬을 계산한다.
    위기 시 동시 하락 리스크를 사전 감지하기 위함.
    """
    returns = _download_returns(tickers)
    if returns is None or returns.shape[1] < 2:
        return {"matrix": {}, "high_correlation_pairs": [], "diversification_score": None}

    corr = returns.corr()

    # 높은 상관관계 쌍 추출 (|r| > 0.7)
    high_pairs: list[dict[str, Any]] = []
    checked: set[tuple[str, str]] = set()
    for i, t1 in enumerate(corr.columns):
        for j, t2 in enumerate(corr.columns):
            if i >= j:
                continue
            pair_key = (t1, t2)
            if pair_key in checked:
                continue
            checked.add(pair_key)
            r = corr.iloc[i, j]
            if abs(r) > 0.7:
                high_pairs.append({
                    "ticker1": t1,
                    "ticker2": t2,
                    "correlation": _safe_round(r, 3),
                    "risk": "높은 동조화" if r > 0.7 else "역상관",
                })

    # 분산 투자 효과 점수 (평균 상관이 낮을수록 좋음, 0~100)
    upper_tri = corr.where(np.triu(np.ones(corr.shape, dtype=bool), k=1))
    avg_corr = upper_tri.stack().mean()
    diversification_score = _safe_round(max(0, (1 - avg_corr) * 100), 1)

    matrix_dict = {}
    for col in corr.columns:
        matrix_dict[col] = {row: _safe_round(corr.loc[row, col], 3) for row in corr.index}

    return {
        "matrix": matrix_dict,
        "avg_correlation": _safe_round(avg_corr, 3),
        "high_correlation_pairs": high_pairs,
        "diversification_score": diversification_score,
    }


# ---------------------------------------------------------------------------
# 히스토리컬 변동성
# ---------------------------------------------------------------------------

def compute_volatility(tickers: list[str]) -> dict[str, dict[str, Any]]:
    """종목별 연간 변동성과 일간 표준편차를 계산한다."""
    returns = _download_returns(tickers)
    if returns is None:
        return {}

    result: dict[str, dict[str, Any]] = {}
    for ticker in returns.columns:
        col = returns[ticker].dropna()
        if len(col) < 20:
            continue
        daily_std = float(col.std())
        annual_vol = daily_std * math.sqrt(252)
        mean_return = float(col.mean()) * 252

        # 최대 낙폭 (Maximum Drawdown)
        cumulative = (1 + col).cumprod()
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = float(drawdown.min())

        result[ticker] = {
            "daily_std": _safe_round(daily_std, 6),
            "annual_volatility": _safe_round(annual_vol, 4),
            "annual_return": _safe_round(mean_return, 4),
            "sharpe_ratio": _safe_round(mean_return / annual_vol, 2) if annual_vol > 0 else None,
            "max_drawdown": _safe_round(max_drawdown, 4),
        }

    return result


# ---------------------------------------------------------------------------
# VaR (Value at Risk)
# ---------------------------------------------------------------------------

def compute_var(
    tickers: list[str],
    weights: list[float],
    portfolio_value: float,
) -> dict[str, Any]:
    """
    포트폴리오 VaR를 Historical Simulation 방식으로 계산한다.

    Args:
        tickers: 종목 리스트
        weights: 각 종목 비중 (합계 = 1.0)
        portfolio_value: 총 투자금
    """
    returns = _download_returns(tickers)
    if returns is None or returns.shape[1] != len(tickers):
        return {"var": {}, "cvar": {}}

    # 포트폴리오 일간 수익률
    w = np.array(weights)
    portfolio_returns = (returns.values @ w)

    var_result: dict[str, dict[str, Any]] = {}
    cvar_result: dict[str, dict[str, Any]] = {}

    for confidence in VAR_CONFIDENCE_LEVELS:
        pct_label = f"{int(confidence * 100)}%"
        cutoff = np.percentile(portfolio_returns, (1 - confidence) * 100)
        daily_var = abs(cutoff) * portfolio_value
        annual_var = daily_var * math.sqrt(252)

        # CVaR (Conditional VaR) — 꼬리 리스크 평균
        tail = portfolio_returns[portfolio_returns <= cutoff]
        cvar_daily = abs(float(tail.mean())) * portfolio_value if len(tail) > 0 else daily_var

        var_result[pct_label] = {
            "daily": _safe_round(daily_var, 2),
            "annual": _safe_round(annual_var, 2),
            "daily_pct": _safe_round(abs(cutoff) * 100, 2),
        }
        cvar_result[pct_label] = {
            "daily": _safe_round(cvar_daily, 2),
            "daily_pct": _safe_round(cvar_daily / portfolio_value * 100, 2) if portfolio_value > 0 else None,
        }

    return {"var": var_result, "cvar": cvar_result}


# ---------------------------------------------------------------------------
# Monte Carlo 시뮬레이션
# ---------------------------------------------------------------------------

def run_monte_carlo(
    tickers: list[str],
    weights: list[float],
    portfolio_value: float,
    days: int = MONTE_CARLO_DAYS,
    simulations: int = MONTE_CARLO_SIMULATIONS,
) -> dict[str, Any]:
    """
    Geometric Brownian Motion 기반 Monte Carlo 시뮬레이션.
    포트폴리오의 미래 가치 분포를 추정한다.
    """
    returns = _download_returns(tickers)
    if returns is None or returns.shape[1] != len(tickers):
        return {"error": "시뮬레이션용 데이터 부족"}

    w = np.array(weights)
    portfolio_returns = returns.values @ w

    mu = float(portfolio_returns.mean())
    sigma = float(portfolio_returns.std())

    if sigma == 0:
        return {"error": "변동성 0 — 시뮬레이션 불가"}

    # GBM 시뮬레이션
    np.random.seed(42)
    random_returns = np.random.normal(mu, sigma, (simulations, days))
    price_paths = portfolio_value * np.exp(np.cumsum(random_returns, axis=1))

    final_values = price_paths[:, -1]

    percentiles = [5, 10, 25, 50, 75, 90, 95]
    pct_values = {f"p{p}": _safe_round(float(np.percentile(final_values, p)), 2) for p in percentiles}

    # 원금 손실 확률
    loss_prob = float(np.mean(final_values < portfolio_value))

    # 대표 경로 추출 (중앙값, 5%, 95%)
    median_path_idx = np.argsort(final_values)[simulations // 2]
    worst_path_idx = np.argsort(final_values)[int(simulations * 0.05)]
    best_path_idx = np.argsort(final_values)[int(simulations * 0.95)]

    # 경로를 20개 포인트로 샘플링 (프론트 차트용)
    sample_points = np.linspace(0, days - 1, min(20, days), dtype=int)

    def _path_to_list(idx: int) -> list[dict[str, Any]]:
        return [{"day": int(d), "value": _safe_round(float(price_paths[idx, d]), 2)} for d in sample_points]

    return {
        "simulations": simulations,
        "days": days,
        "initial_value": portfolio_value,
        "final_distribution": pct_values,
        "expected_value": _safe_round(float(final_values.mean()), 2),
        "expected_return_pct": _safe_round(float((final_values.mean() - portfolio_value) / portfolio_value * 100), 2),
        "loss_probability": _safe_round(loss_prob, 4),
        "max_gain": _safe_round(float(final_values.max()), 2),
        "max_loss": _safe_round(float(final_values.min()), 2),
        "paths": {
            "median": _path_to_list(median_path_idx),
            "worst_5pct": _path_to_list(worst_path_idx),
            "best_95pct": _path_to_list(best_path_idx),
        },
    }


# ---------------------------------------------------------------------------
# 시나리오 시뮬레이션
# ---------------------------------------------------------------------------

def run_scenario_analysis(
    tickers: list[str],
    weights: list[float],
    portfolio_value: float,
    betas: dict[str, float | None] | None = None,
) -> dict[str, Any]:
    """
    금리 변동, 시장 충격 시나리오별 포트폴리오 영향을 계산한다.

    - 금리 인상: 듀레이션 효과 + 섹터별 민감도 근사치 적용
    - 시장 충격: 베타 기반 개별 종목 영향 추정
    """
    betas = betas or {}

    # 금리 시나리오
    rate_scenarios: list[dict[str, Any]] = []
    for bps in SCENARIO_RATE_CHANGE_BPS:
        impacts: list[dict[str, Any]] = []
        total_impact_pct = 0.0
        for ticker, weight in zip(tickers, weights):
            beta = betas.get(ticker) or 1.0
            # 금리 민감도 근사: 고성장(beta>1)은 금리 인상에 더 민감
            sensitivity = -0.015 * beta  # 100bp당 약 -1.5% * beta
            impact_pct = sensitivity * (bps / 100)
            impacts.append({
                "ticker": ticker,
                "weight": _safe_round(weight, 4),
                "impact_pct": _safe_round(impact_pct * 100, 2),
            })
            total_impact_pct += impact_pct * weight

        rate_scenarios.append({
            "rate_change_bps": bps,
            "label": f"금리 +{bps}bp",
            "portfolio_impact_pct": _safe_round(total_impact_pct * 100, 2),
            "portfolio_impact_usd": _safe_round(total_impact_pct * portfolio_value, 2),
            "detail": impacts,
        })

    # 시장 충격 시나리오
    shock_scenarios: list[dict[str, Any]] = []
    for shock_pct in SCENARIO_MARKET_SHOCK_PCT:
        impacts = []
        total_impact = 0.0
        for ticker, weight in zip(tickers, weights):
            beta = betas.get(ticker) or 1.0
            ticker_impact = shock_pct * beta
            impacts.append({
                "ticker": ticker,
                "weight": _safe_round(weight, 4),
                "impact_pct": _safe_round(ticker_impact * 100, 2),
            })
            total_impact += ticker_impact * weight

        shock_scenarios.append({
            "market_shock_pct": _safe_round(shock_pct * 100, 1),
            "label": f"시장 {int(shock_pct * 100)}% 충격",
            "portfolio_impact_pct": _safe_round(total_impact * 100, 2),
            "portfolio_impact_usd": _safe_round(total_impact * portfolio_value, 2),
            "detail": impacts,
        })

    return {
        "rate_scenarios": rate_scenarios,
        "shock_scenarios": shock_scenarios,
    }


# ---------------------------------------------------------------------------
# 이상 징후 감지 (Anomaly Detection)
# ---------------------------------------------------------------------------

def detect_anomalies(tickers: list[str]) -> list[dict[str, Any]]:
    """
    최근 변동성/거래량/수익률에서 이상 징후를 감지한다.
    3시그마 이상 이탈 시 경고를 발생시킨다.
    """
    returns = _download_returns(tickers, period="3mo")
    if returns is None:
        return []

    anomalies: list[dict[str, Any]] = []

    for ticker in returns.columns:
        col = returns[ticker].dropna()
        if len(col) < 20:
            continue

        mean = float(col.mean())
        std = float(col.std())
        if std == 0:
            continue

        last_return = float(col.iloc[-1])
        z_score = (last_return - mean) / std

        if abs(z_score) > 2.5:
            anomalies.append({
                "ticker": ticker,
                "type": "수익률 이상" if z_score > 0 else "급락 감지",
                "z_score": _safe_round(z_score, 2),
                "last_return_pct": _safe_round(last_return * 100, 2),
                "avg_return_pct": _safe_round(mean * 100, 4),
                "severity": "critical" if abs(z_score) > 3.5 else "warning",
            })

        # 최근 5일 변동성 급변 감지
        if len(col) >= 25:
            recent_vol = float(col.iloc[-5:].std())
            historical_vol = float(col.iloc[:-5].std())
            if historical_vol > 0:
                vol_ratio = recent_vol / historical_vol
                if vol_ratio > 2.0:
                    anomalies.append({
                        "ticker": ticker,
                        "type": "변동성 급등",
                        "vol_ratio": _safe_round(vol_ratio, 2),
                        "recent_vol": _safe_round(recent_vol * math.sqrt(252) * 100, 2),
                        "historical_vol": _safe_round(historical_vol * math.sqrt(252) * 100, 2),
                        "severity": "critical" if vol_ratio > 3.0 else "warning",
                    })

    return anomalies


# ---------------------------------------------------------------------------
# 통합 리스크 분석 (전체 파이프라인)
# ---------------------------------------------------------------------------

def compute_full_risk_analysis(
    tickers: list[str],
    weights: list[float],
    portfolio_value: float,
    betas: dict[str, float | None] | None = None,
) -> dict[str, Any]:
    """전체 리스크 분석을 실행하여 통합 결과를 반환한다."""
    correlation = compute_correlation_matrix(tickers)
    volatility = compute_volatility(tickers)
    var_data = compute_var(tickers, weights, portfolio_value)
    monte_carlo = run_monte_carlo(tickers, weights, portfolio_value)
    scenarios = run_scenario_analysis(tickers, weights, portfolio_value, betas)
    anomalies = detect_anomalies(tickers)

    # 종합 리스크 등급 산출
    risk_level = _assess_risk_level(correlation, volatility, var_data, anomalies)

    return {
        "risk_level": risk_level,
        "correlation": correlation,
        "volatility": volatility,
        "var": var_data,
        "monte_carlo": monte_carlo,
        "scenarios": scenarios,
        "anomalies": anomalies,
    }


def _assess_risk_level(
    correlation: dict[str, Any],
    volatility: dict[str, dict[str, Any]],
    var_data: dict[str, Any],
    anomalies: list[dict[str, Any]],
) -> dict[str, Any]:
    """리스크 지표들을 종합하여 등급을 산출한다."""
    score = 0  # 0(안전) ~ 100(위험)
    reasons: list[str] = []

    # 상관관계 기반
    div_score = correlation.get("diversification_score")
    if div_score is not None:
        if div_score < 30:
            score += 25
            reasons.append(f"분산 투자 효과 낮음 (점수: {div_score})")
        elif div_score < 50:
            score += 10

    # 변동성 기반
    avg_vol = 0.0
    vol_count = 0
    for v in volatility.values():
        av = v.get("annual_volatility")
        if av is not None:
            avg_vol += av
            vol_count += 1
    if vol_count > 0:
        avg_vol /= vol_count
        if avg_vol > 0.4:
            score += 25
            reasons.append(f"높은 평균 변동성 ({_safe_round(avg_vol * 100, 1)}%)")
        elif avg_vol > 0.25:
            score += 10

    # VaR 기반
    var_95 = (var_data.get("var") or {}).get("95%", {})
    daily_pct = var_95.get("daily_pct")
    if daily_pct is not None and daily_pct > 3:
        score += 20
        reasons.append(f"일간 VaR(95%) {daily_pct}% — 높은 일간 손실 가능성")

    # 이상 징후
    critical_count = sum(1 for a in anomalies if a.get("severity") == "critical")
    if critical_count > 0:
        score += 20
        reasons.append(f"{critical_count}개 종목에서 심각한 이상 징후 감지")

    level = "low"
    level_ko = "낮음"
    if score >= 60:
        level = "high"
        level_ko = "높음"
    elif score >= 30:
        level = "medium"
        level_ko = "보통"

    return {
        "level": level,
        "level_ko": level_ko,
        "score": min(100, score),
        "reasons": reasons,
    }
