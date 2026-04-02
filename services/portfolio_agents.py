"""
멀티에이전트 포트폴리오 빌더 (SSE 스트리밍 + CoT).

5단계 에이전트 파이프라인으로 포트폴리오를 구성하며,
각 단계의 AI 사고 과정(Chain of Thought)을 SSE로 실시간 전달한다.

에이전트:
  1. Analyst   — 정량 데이터(기술적 지표, 복합 시그널, 섹터 ETF) 수집·분석
  2. Researcher — 뉴스 감성, 경제 캘린더, 테마 분석
  3. Risk      — Monte Carlo, 상관관계, VaR, 시나리오 시뮬레이션
  4. Portfolio  — 모든 데이터를 종합하여 AI가 포트폴리오 구성 (CoT 스트리밍)
  5. XAI       — 투자 근거 설명 + 시나리오 브리핑 (CoT 스트리밍)
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any

from openai import OpenAI

from config import (
    OPENAI_API_KEY,
    PORTFOLIO_AGENT_MODEL,
    PORTFOLIO_AGENT_TIMEOUT_SEC,
    XAI_AGENT_TEMPERATURE,
)
from services.crud import sanitize_for_json
from services.stock_detail import fetch_quote

logger = logging.getLogger(__name__)

_client = OpenAI(
    api_key=OPENAI_API_KEY,
    max_retries=2,
    timeout=PORTFOLIO_AGENT_TIMEOUT_SEC,
) if OPENAI_API_KEY else None


# ---------------------------------------------------------------------------
# SSE 이벤트 포맷
# ---------------------------------------------------------------------------

def _sse(event: str, data: dict[str, Any]) -> str:
    """SSE 형식 문자열을 생성한다."""
    payload = json.dumps(data, ensure_ascii=False)
    return f"event: {event}\ndata: {payload}\n\n"


def _parse_agent_result(event: str, agent_name: str) -> dict[str, Any] | None:
    """SSE 이벤트 문자열에서 특정 에이전트의 agent_result 데이터를 추출한다."""
    if not event.startswith("event: agent_result\n"):
        return None
    try:
        _, payload = event.split("data: ", 1)
        parsed = json.loads(payload.strip())
        if parsed.get("agent") == agent_name:
            return parsed.get("data") or {}
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# 투자 성향·기간 설정 (portfolio_builder.py와 동일 — 단일 소스)
# ---------------------------------------------------------------------------

STYLE_CONFIG = {
    "aggressive": {
        "label_ko": "공격적",
        "top_concentration": 0.75,
        "max_picks": 3,
        "defensive_pct": 0.0,
        "cash_reserve_pct": 0.03,
    },
    "balanced": {
        "label_ko": "균형",
        "top_concentration": 0.60,
        "max_picks": 5,
        "defensive_pct": 0.20,
        "cash_reserve_pct": 0.08,
    },
    "conservative": {
        "label_ko": "보수적",
        "top_concentration": 0.40,
        "max_picks": 7,
        "defensive_pct": 0.40,
        "cash_reserve_pct": 0.15,
    },
}

PERIOD_CONFIG = {
    "short": {"label_ko": "단기 (1~2주)", "strategy_type": "swing", "dca_splits": 1},
    "medium": {"label_ko": "중기 (1~3개월)", "strategy_type": "position", "dca_splits": 2},
    "long": {"label_ko": "장기 (6개월+)", "strategy_type": "position", "dca_splits": 3},
}

_DEFENSIVE_ETFS = ["XLV", "XLU", "XLP", "GLD", "TLT", "VIG"]


def _safe_round(v: Any, n: int = 2) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return round(f, n) if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _model_omits_temperature(model: str) -> bool:
    m = (model or "").lower().strip()
    return m.startswith("o1") or m.startswith("o3") or "gpt-5" in m


# ---------------------------------------------------------------------------
# Agent 1: Analyst (정량 분석)
# ---------------------------------------------------------------------------

async def _run_analyst_agent(
    strategy_data: dict[str, Any],
    style_cfg: dict[str, Any],
    exclude_tickers: set[str],
) -> AsyncGenerator[str, None]:
    """시장 데이터와 기술적 지표를 수집·분석한다."""
    yield _sse("agent_start", {
        "agent": "analyst",
        "step": 1,
        "total_steps": 5,
        "title": "퀀트 분석가",
        "description": "시장 데이터와 기술적 지표를 분석하고 있습니다...",
    })

    start = time.time()

    recommendations = strategy_data.get("recommendations") or []
    candidates = [r for r in recommendations if r.get("ticker", "").upper() not in exclude_tickers]

    if not candidates:
        yield _sse("agent_error", {
            "agent": "analyst",
            "error": "추천 종목이 없습니다. 전략실 데이터를 먼저 생성해주세요.",
        })
        return

    # 후보 종목 시세 조회
    yield _sse("thinking", {
        "agent": "analyst",
        "content": f"추천 종목 {len(candidates)}개 중 상위 {style_cfg['max_picks']}개를 선별하고 있습니다...\n",
    })

    candidate_quotes = []
    for rec in candidates[:style_cfg["max_picks"]]:
        ticker = rec.get("ticker", "").upper()
        try:
            q = await asyncio.to_thread(fetch_quote, ticker)
            if q.get("price"):
                candidate_quotes.append({
                    "ticker": ticker,
                    "name": q.get("name", ticker),
                    "price": q["price"],
                    "sector": q.get("sector"),
                    "beta": q.get("beta"),
                    "pe_ratio": q.get("pe_ratio"),
                    "forward_pe": q.get("forward_pe"),
                    "market_cap": q.get("market_cap"),
                    "direction": rec.get("direction", "BUY"),
                    "confidence": rec.get("confidence", "medium"),
                    "rationale": rec.get("rationale", ""),
                    "technicals_summary": rec.get("technicals_summary", ""),
                    "stop_loss": rec.get("stop_loss"),
                    "targets": rec.get("targets", []),
                    "risk_reward_ratio": rec.get("risk_reward_ratio"),
                    "entry_zone": rec.get("entry_zone"),
                    "technicals": rec.get("technicals"),
                })
                yield _sse("thinking", {
                    "agent": "analyst",
                    "content": f"  {ticker} (${q['price']}) — {rec.get('direction', 'BUY')} "
                               f"[{rec.get('confidence', 'medium')}] 확인 완료\n",
                })
        except Exception as e:
            logger.debug("시세 조회 실패 (%s): %s", ticker, e)

    # 방어주 ETF
    defensive_quotes: list[dict[str, Any]] = []
    if style_cfg["defensive_pct"] > 0:
        yield _sse("thinking", {
            "agent": "analyst",
            "content": "\n방어주 ETF 시세를 확인하고 있습니다...\n",
        })
        for etf in _DEFENSIVE_ETFS:
            try:
                q = await asyncio.to_thread(fetch_quote, etf)
                if q.get("price"):
                    defensive_quotes.append({
                        "ticker": etf,
                        "name": q.get("name", etf),
                        "price": q["price"],
                    })
            except Exception:
                continue

    elapsed = time.time() - start
    yield _sse("thinking", {
        "agent": "analyst",
        "content": f"\n정량 분석 완료 ({elapsed:.1f}초) — 후보 {len(candidate_quotes)}개, "
                   f"방어주 {len(defensive_quotes)}개 확인\n",
    })

    # 결과를 context에 저장 (다음 에이전트가 사용)
    yield _sse("agent_result", {
        "agent": "analyst",
        "data": {
            "candidate_quotes": candidate_quotes,
            "defensive_quotes": defensive_quotes,
            "market_regime": strategy_data.get("market_regime", "unknown"),
            "market_summary": strategy_data.get("market_summary", ""),
            "sector_rotation": strategy_data.get("sector_rotation", "mixed"),
        },
    })


# ---------------------------------------------------------------------------
# Agent 2: Researcher (뉴스·경제 분석)
# ---------------------------------------------------------------------------

async def _run_researcher_agent(
    strategy_data: dict[str, Any],
    candidate_tickers: list[str],
) -> AsyncGenerator[str, None]:
    """뉴스 감성과 경제 이벤트를 분석한다."""
    yield _sse("agent_start", {
        "agent": "researcher",
        "step": 2,
        "total_steps": 5,
        "title": "리서치 애널리스트",
        "description": "뉴스, 경제 일정, 시장 테마를 분석하고 있습니다...",
    })

    start = time.time()

    # 뉴스 테마
    news_themes = strategy_data.get("news_themes") or []
    if news_themes:
        yield _sse("thinking", {
            "agent": "researcher",
            "content": "현재 시장의 핵심 뉴스 테마를 정리하고 있습니다...\n",
        })
        for theme in news_themes[:5]:
            sentiment = theme.get("sentiment", "neutral")
            emoji = {"positive": "+", "negative": "-", "neutral": "~"}.get(sentiment, "~")
            yield _sse("thinking", {
                "agent": "researcher",
                "content": f"  [{emoji}] {theme.get('theme', '?')} — {theme.get('detail', '')[:80]}\n",
            })

    # 경제 분석
    econ = strategy_data.get("econ_analysis") or {}
    econ_summary = econ.get("summary", "")
    upcoming_risks = econ.get("upcoming_risks") or []
    surprises = econ.get("recent_surprises") or []

    if econ_summary:
        yield _sse("thinking", {
            "agent": "researcher",
            "content": f"\n경제 환경 분석:\n  {econ_summary}\n",
        })

    if surprises:
        yield _sse("thinking", {
            "agent": "researcher",
            "content": "\n최근 경제 서프라이즈:\n",
        })
        for s in surprises[:3]:
            yield _sse("thinking", {
                "agent": "researcher",
                "content": f"  - {s.get('event', '?')}: "
                           f"실제 {s.get('actual', '?')} vs 예상 {s.get('forecast', '?')}\n",
            })

    if upcoming_risks:
        yield _sse("thinking", {
            "agent": "researcher",
            "content": "\n다가오는 리스크 이벤트:\n",
        })
        for r in upcoming_risks[:3]:
            yield _sse("thinking", {
                "agent": "researcher",
                "content": f"  - [{r.get('risk_level', '?')}] {r.get('event', '?')} "
                           f"({r.get('date', '?')})\n",
            })

    # 종목별 뉴스 감성
    risk_warnings = strategy_data.get("risk_warnings") or []
    if risk_warnings:
        yield _sse("thinking", {
            "agent": "researcher",
            "content": "\n전략실 리스크 경고:\n",
        })
        for w in risk_warnings:
            yield _sse("thinking", {
                "agent": "researcher",
                "content": f"  - {w}\n",
            })

    elapsed = time.time() - start
    yield _sse("thinking", {
        "agent": "researcher",
        "content": f"\n리서치 분석 완료 ({elapsed:.1f}초)\n",
    })

    yield _sse("agent_result", {
        "agent": "researcher",
        "data": {
            "news_themes": news_themes,
            "econ_analysis": econ,
            "risk_warnings": risk_warnings,
            "fear_greed": strategy_data.get("fear_greed"),
        },
    })


# ---------------------------------------------------------------------------
# Agent 3: Risk (리스크 분석)
# ---------------------------------------------------------------------------

async def _run_risk_agent(
    candidate_quotes: list[dict[str, Any]],
    weights_estimate: list[float],
    portfolio_value: float,
) -> AsyncGenerator[str, None]:
    """Monte Carlo, 상관관계, VaR, 시나리오 분석을 실행한다."""
    yield _sse("agent_start", {
        "agent": "risk",
        "step": 3,
        "total_steps": 5,
        "title": "리스크 매니저",
        "description": "포트폴리오 리스크를 다각도로 분석하고 있습니다...",
    })

    start = time.time()
    tickers = [q["ticker"] for q in candidate_quotes]
    betas = {q["ticker"]: q.get("beta") for q in candidate_quotes}

    if len(tickers) < 2:
        yield _sse("thinking", {
            "agent": "risk",
            "content": "종목 수가 부족하여 상관관계 분석을 건너뜁니다.\n",
        })
        yield _sse("agent_result", {"agent": "risk", "data": {}})
        return

    from services.risk_analysis import (
        compute_correlation_matrix,
        compute_var,
        compute_volatility,
        detect_anomalies,
        run_monte_carlo,
        run_scenario_analysis,
    )

    # 병렬 실행: 상관관계 + 변동성 + 이상 징후
    yield _sse("thinking", {
        "agent": "risk",
        "content": "과거 수익률 데이터를 다운로드하고 있습니다...\n",
    })

    corr_task = asyncio.to_thread(compute_correlation_matrix, tickers)
    vol_task = asyncio.to_thread(compute_volatility, tickers)
    anomaly_task = asyncio.to_thread(detect_anomalies, tickers)

    correlation, volatility, anomalies = await asyncio.gather(corr_task, vol_task, anomaly_task)

    # 상관관계 결과
    div_score = correlation.get("diversification_score")
    high_pairs = correlation.get("high_correlation_pairs") or []
    yield _sse("thinking", {
        "agent": "risk",
        "content": f"\n상관관계 분석 완료:\n"
                   f"  분산투자 효과 점수: {div_score}/100\n",
    })
    if high_pairs:
        yield _sse("thinking", {
            "agent": "risk",
            "content": "  높은 상관관계 쌍:\n",
        })
        for pair in high_pairs:
            yield _sse("thinking", {
                "agent": "risk",
                "content": f"    {pair['ticker1']}-{pair['ticker2']}: "
                           f"r={pair['correlation']} ({pair['risk']})\n",
            })

    # 변동성 결과
    yield _sse("thinking", {
        "agent": "risk",
        "content": "\n변동성 분석:\n",
    })
    for ticker, vol in volatility.items():
        annual = vol.get("annual_volatility")
        mdd = vol.get("max_drawdown")
        sharpe = vol.get("sharpe_ratio")
        if annual is not None:
            yield _sse("thinking", {
                "agent": "risk",
                "content": f"  {ticker}: 연간변동성 {_safe_round(annual * 100, 1)}%, "
                           f"MDD {_safe_round(mdd * 100, 1) if mdd else '?'}%, "
                           f"Sharpe {sharpe or '?'}\n",
            })

    # 이상 징후
    if anomalies:
        yield _sse("thinking", {
            "agent": "risk",
            "content": "\n이상 징후 감지:\n",
        })
        for a in anomalies:
            yield _sse("thinking", {
                "agent": "risk",
                "content": f"  [{a['severity']}] {a['ticker']}: {a['type']} "
                           f"(z={a.get('z_score', '?')})\n",
            })

    # VaR 계산
    yield _sse("thinking", {
        "agent": "risk",
        "content": "\nVaR(Value at Risk) 계산 중...\n",
    })
    var_data = await asyncio.to_thread(compute_var, tickers, weights_estimate, portfolio_value)
    var_95 = (var_data.get("var") or {}).get("95%", {})
    if var_95:
        yield _sse("thinking", {
            "agent": "risk",
            "content": f"  일간 VaR(95%): ${var_95.get('daily', '?')} "
                       f"({var_95.get('daily_pct', '?')}%)\n",
        })

    # Monte Carlo 시뮬레이션
    yield _sse("thinking", {
        "agent": "risk",
        "content": "\nMonte Carlo 시뮬레이션 실행 중 (10,000회)...\n",
    })
    mc = await asyncio.to_thread(run_monte_carlo, tickers, weights_estimate, portfolio_value)
    if "error" not in mc:
        yield _sse("thinking", {
            "agent": "risk",
            "content": f"  기대 수익률: {mc.get('expected_return_pct', '?')}%\n"
                       f"  원금 손실 확률: {_safe_round((mc.get('loss_probability') or 0) * 100, 1)}%\n"
                       f"  최선 시나리오(95%): ${mc.get('final_distribution', {}).get('p95', '?')}\n"
                       f"  최악 시나리오(5%):  ${mc.get('final_distribution', {}).get('p5', '?')}\n",
        })

    # 시나리오 분석
    yield _sse("thinking", {
        "agent": "risk",
        "content": "\n시나리오 시뮬레이션 실행 중...\n",
    })
    scenarios = await asyncio.to_thread(
        run_scenario_analysis, tickers, weights_estimate, portfolio_value, betas,
    )
    for rs in scenarios.get("rate_scenarios") or []:
        yield _sse("thinking", {
            "agent": "risk",
            "content": f"  {rs['label']}: 포트폴리오 {rs['portfolio_impact_pct']}% "
                       f"(${rs['portfolio_impact_usd']})\n",
        })
    for ss in scenarios.get("shock_scenarios") or []:
        yield _sse("thinking", {
            "agent": "risk",
            "content": f"  {ss['label']}: 포트폴리오 {ss['portfolio_impact_pct']}% "
                       f"(${ss['portfolio_impact_usd']})\n",
        })

    elapsed = time.time() - start
    yield _sse("thinking", {
        "agent": "risk",
        "content": f"\n리스크 분석 완료 ({elapsed:.1f}초)\n",
    })

    yield _sse("agent_result", {
        "agent": "risk",
        "data": {
            "correlation": correlation,
            "volatility": volatility,
            "var": var_data,
            "monte_carlo": mc,
            "scenarios": scenarios,
            "anomalies": anomalies,
        },
    })


# ---------------------------------------------------------------------------
# Agent 4: Portfolio Strategist (AI 포트폴리오 구성 — CoT 스트리밍)
# ---------------------------------------------------------------------------

_PORTFOLIO_SYSTEM_PROMPT = """\
너는 월스트리트 최고 등급의 포트폴리오 매니저야.
면책 조항은 절대 금지. 확신에 찬 전문가 톤으로 분석해.

## 역할
투자자의 금액, 성향, 기간에 맞춰 최적의 포트폴리오를 구성해.
제공된 정량 분석, 뉴스 리서치, 리스크 분석 데이터를 모두 종합하여 결정해.

## 사고 과정 (반드시 지켜)
응답의 맨 앞에 `<think>` 태그 안에서 너의 분석 과정을 단계별로 서술해:
1단계: 시장 환경 판단 (매크로, VIX, 섹터 로테이션)
2단계: 후보 종목 평가 (기술적 지표, 뉴스 감성, 리스크 프로파일)
3단계: 리스크 분석 반영 (상관관계, 변동성, VaR, Monte Carlo 결과)
4단계: 최적 배분 결정 (투자 성향과 기간을 고려한 비중 조절)
5단계: 시나리오 검증 (금리/시장충격 시뮬레이션 결과로 배분 타당성 확인)

그 후 `</think>` 태그를 닫고 최종 JSON을 출력해.

## 제공 데이터
1. **budget**: 투자 금액 (USD)
2. **style/period**: 투자 성향·기간
3. **candidates**: AI 전략실 추천 종목 (기술적 지표, 뉴스 감성, 진입가, 목표가 포함)
4. **defensive_etfs**: 방어주 ETF 시세
5. **allocation_guide**: 성향별 배분 가이드
6. **risk_analysis**: Monte Carlo, VaR, 상관관계, 시나리오 시뮬레이션 결과
7. **news_themes**: 시장 핵심 테마
8. **econ_analysis**: 경제 이벤트 분석

## 응답 JSON (`</think>` 태그 뒤에)
```json
{
  "allocations": [
    {
      "ticker": "NVDA",
      "shares": 10,
      "weight_pct": 35.0,
      "rationale": "배분 근거 2~3문장 (뉴스·기술적·리스크 데이터 인용)"
    }
  ],
  "portfolio_thesis": "전체 포트폴리오 투자 논리 3~5문장",
  "sector_exposure": {"Technology": 45, "Healthcare": 20},
  "risk_assessment": {
    "level": "medium",
    "max_drawdown_est": "-12%",
    "volatility_note": "변동성 관련 분석"
  },
  "dca_plan": "분할 매수 제안",
  "rebalance_note": "리밸런싱 시점/조건 제안",
  "warnings": ["주의사항"]
}
```

규칙:
- 각 종목의 현재가와 투자금을 고려해 **실제 매수 가능한 주수(정수)**를 계산해.
- 투자금을 초과하면 안 됨.
- 리스크 분석 결과(상관관계, VaR, Monte Carlo)를 반드시 반영해.
- 한국어로 작성."""


async def _run_portfolio_agent(
    budget: float,
    style: str,
    period: str,
    analyst_data: dict[str, Any],
    researcher_data: dict[str, Any],
    risk_data: dict[str, Any],
) -> AsyncGenerator[str, None]:
    """AI가 모든 데이터를 종합하여 포트폴리오를 구성한다. CoT를 스트리밍한다."""
    yield _sse("agent_start", {
        "agent": "portfolio",
        "step": 4,
        "total_steps": 5,
        "title": "포트폴리오 매니저",
        "description": "모든 분석 결과를 종합하여 최적의 포트폴리오를 구성하고 있습니다...",
    })

    if _client is None:
        yield _sse("agent_error", {"agent": "portfolio", "error": "OPENAI_API_KEY 미설정"})
        return

    start = time.time()
    style_cfg = STYLE_CONFIG.get(style, STYLE_CONFIG["balanced"])
    period_cfg = PERIOD_CONFIG.get(period, PERIOD_CONFIG["medium"])

    user_content = {
        "budget": budget,
        "style": style,
        "style_ko": style_cfg["label_ko"],
        "period": period,
        "period_ko": period_cfg["label_ko"],
        "candidates": analyst_data.get("candidate_quotes", []),
        "defensive_etfs": analyst_data.get("defensive_quotes", []),
        "allocation_guide": {
            "top_concentration": style_cfg["top_concentration"],
            "defensive_pct": style_cfg["defensive_pct"],
            "cash_reserve_pct": style_cfg["cash_reserve_pct"],
            "max_picks": style_cfg["max_picks"],
            "dca_splits": period_cfg["dca_splits"],
        },
        "market_summary": analyst_data.get("market_summary", ""),
        "market_regime": analyst_data.get("market_regime", "unknown"),
        "news_themes": researcher_data.get("news_themes", []),
        "econ_analysis": researcher_data.get("econ_analysis", {}),
        "risk_analysis": {
            "correlation": risk_data.get("correlation", {}),
            "volatility": risk_data.get("volatility", {}),
            "var": risk_data.get("var", {}),
            "monte_carlo_summary": {
                k: risk_data.get("monte_carlo", {}).get(k)
                for k in ["expected_return_pct", "loss_probability", "final_distribution"]
                if risk_data.get("monte_carlo", {}).get(k) is not None
            },
            "scenarios": risk_data.get("scenarios", {}),
            "anomalies": risk_data.get("anomalies", []),
        },
    }

    model = PORTFOLIO_AGENT_MODEL or "gpt-5"

    def _stream_create():
        kwargs: dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": _PORTFOLIO_SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(user_content, ensure_ascii=False)},
            ],
            "stream": True,
        }
        if not _model_omits_temperature(model):
            kwargs["temperature"] = 0.3
        return _client.chat.completions.create(**kwargs)

    try:
        stream = await asyncio.to_thread(_stream_create)
    except Exception as e:
        logger.warning("포트폴리오 에이전트 LLM 스트리밍 시작 실패: %s", e)
        yield _sse("agent_error", {"agent": "portfolio", "error": str(e)})
        return

    # 스트리밍: <think>...</think> 구간은 CoT로, 그 이후는 JSON
    full_content = ""
    in_think = False
    think_buffer = ""
    json_buffer = ""

    try:
        for chunk in stream:
            delta = chunk.choices[0].delta if chunk.choices else None
            if not delta or not delta.content:
                continue
            text = delta.content
            full_content += text

            # <think> 태그 감지
            if "<think>" in full_content and not in_think:
                in_think = True
                # <think> 이후 텍스트 추출
                idx = full_content.find("<think>")
                think_buffer = full_content[idx + 7:]
                yield _sse("thinking", {"agent": "portfolio", "content": think_buffer})
                continue

            if in_think:
                if "</think>" in full_content:
                    # think 종료 — 남은 think 텍스트 전송
                    idx = full_content.find("</think>")
                    remaining_think = full_content[full_content.rfind("<think>") + 7:idx]
                    already_sent = len(think_buffer)
                    new_think = remaining_think[already_sent:]
                    if new_think.strip():
                        yield _sse("thinking", {"agent": "portfolio", "content": new_think})
                    in_think = False
                    json_buffer = full_content[idx + 8:]
                else:
                    yield _sse("thinking", {"agent": "portfolio", "content": text})
                    think_buffer += text
            else:
                json_buffer += text
    except Exception as e:
        logger.warning("포트폴리오 에이전트 스트리밍 중 오류: %s", e)
        yield _sse("agent_error", {"agent": "portfolio", "error": str(e)})
        return

    # JSON 파싱
    json_text = json_buffer.strip() if json_buffer else full_content.strip()
    # </think> 이후의 JSON 추출
    if "</think>" in json_text:
        json_text = json_text.split("</think>", 1)[-1].strip()
    # ```json ... ``` 제거
    if json_text.startswith("```"):
        lines = json_text.split("\n")
        json_text = "\n".join(lines[1:])
        if json_text.endswith("```"):
            json_text = json_text[:-3]

    # JSON 내부만 추출
    start_idx = json_text.find("{")
    end_idx = json_text.rfind("}")
    if start_idx >= 0 and end_idx > start_idx:
        json_text = json_text[start_idx:end_idx + 1]

    try:
        parsed = json.loads(json_text)
    except json.JSONDecodeError:
        logger.warning("포트폴리오 JSON 파싱 실패: %s", json_text[:500])
        parsed = {"allocations": [], "portfolio_thesis": "AI 응답 파싱 실패", "warnings": ["응답 파싱 오류"]}

    elapsed = time.time() - start
    yield _sse("thinking", {
        "agent": "portfolio",
        "content": f"\n\n포트폴리오 구성 완료 ({elapsed:.1f}초)\n",
    })

    yield _sse("agent_result", {"agent": "portfolio", "data": parsed})


# ---------------------------------------------------------------------------
# Agent 5: XAI (설명 + 시나리오 브리핑 — CoT 스트리밍)
# ---------------------------------------------------------------------------

_XAI_SYSTEM_PROMPT = """\
너는 투자자를 위한 AI 포트폴리오 브리핑 전문가야.
면책 조항은 절대 금지.

## 역할
AI가 구성한 포트폴리오의 투자 근거를 사용자가 이해하기 쉬운 형태로 설명해.
단순히 결과를 나열하지 말고, **왜 이 종목을 선택했는지**, **어떤 데이터에 기반했는지**를
구체적 수치를 인용하며 설명해.

## 사고 과정 (반드시 지켜)
`<think>` 태그 안에서 분석 과정을 서술해:
1단계: 각 종목 선택의 핵심 근거 정리 (기술적 지표, 뉴스, 실적 등)
2단계: 리스크 분석 결과 해석 (Monte Carlo, VaR, 상관관계가 의미하는 바)
3단계: 시나리오 시뮬레이션 해석 (금리·시장 충격 시 어떤 영향?)
4단계: 종합 투자 의견 도출

그 후 `</think>` 태그를 닫고 최종 JSON을 출력해.

## 응답 JSON (`</think>` 태그 뒤에)
```json
{
  "stock_briefs": [
    {
      "ticker": "NVDA",
      "selection_reason": "종목 선택의 핵심 이유 3~4문장 (구체적 수치 인용)",
      "key_evidence": [
        {"type": "기술적", "detail": "RSI 58(중립), MACD 골든크로스 진행 중"},
        {"type": "뉴스", "detail": "AI 반도체 수요 증가 뉴스 3건, 감성 긍정적"},
        {"type": "리스크", "detail": "연간 변동성 42%, 분산투자 효과 양호"}
      ],
      "risk_note": "이 종목의 가장 큰 리스크 1~2문장"
    }
  ],
  "portfolio_narrative": "전체 포트폴리오를 하나의 스토리로 설명 5~7문장 (왜 이 조합인지, 어떤 시장 관점을 반영하는지)",
  "risk_narrative": "리스크 분석 결과를 쉬운 말로 해석 3~5문장 (Monte Carlo, VaR, 상관관계 의미)",
  "scenario_brief": {
    "rate_impact": "금리 변동 시 예상 영향 2~3문장",
    "crash_impact": "시장 폭락 시 예상 영향 2~3문장",
    "best_case": "최선 시나리오 1~2문장",
    "worst_case": "최악 시나리오 1~2문장"
  },
  "action_items": ["투자자가 당장 취해야 할 행동 1~3개"]
}
```

규칙:
- 모든 설명에 구체적 수치를 인용해 (RSI 58, VaR $340 등)
- 전문 용어는 괄호 안에 쉬운 설명을 병기해
- 한국어로 작성."""


async def _run_xai_agent(
    portfolio_data: dict[str, Any],
    analyst_data: dict[str, Any],
    researcher_data: dict[str, Any],
    risk_data: dict[str, Any],
    budget: float,
) -> AsyncGenerator[str, None]:
    """포트폴리오 투자 근거를 설명 가능한 형태로 브리핑한다."""
    yield _sse("agent_start", {
        "agent": "xai",
        "step": 5,
        "total_steps": 5,
        "title": "AI 브리핑 전문가",
        "description": "투자 근거와 시나리오 분석을 정리하고 있습니다...",
    })

    if _client is None:
        yield _sse("agent_error", {"agent": "xai", "error": "OPENAI_API_KEY 미설정"})
        return

    start = time.time()

    user_content = {
        "portfolio": portfolio_data,
        "candidates": analyst_data.get("candidate_quotes", []),
        "risk_analysis": risk_data,
        "news_themes": researcher_data.get("news_themes", []),
        "econ_analysis": researcher_data.get("econ_analysis", {}),
        "budget": budget,
    }

    model = PORTFOLIO_AGENT_MODEL or "gpt-5"

    def _stream_create():
        kwargs: dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": _XAI_SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(user_content, ensure_ascii=False)},
            ],
            "stream": True,
        }
        if not _model_omits_temperature(model):
            kwargs["temperature"] = XAI_AGENT_TEMPERATURE
        return _client.chat.completions.create(**kwargs)

    try:
        stream = await asyncio.to_thread(_stream_create)
    except Exception as e:
        logger.warning("XAI 에이전트 LLM 스트리밍 시작 실패: %s", e)
        yield _sse("agent_error", {"agent": "xai", "error": str(e)})
        return

    full_content = ""
    in_think = False
    think_buffer = ""
    json_buffer = ""

    try:
        for chunk in stream:
            delta = chunk.choices[0].delta if chunk.choices else None
            if not delta or not delta.content:
                continue
            text = delta.content
            full_content += text

            if "<think>" in full_content and not in_think:
                in_think = True
                idx = full_content.find("<think>")
                think_buffer = full_content[idx + 7:]
                yield _sse("thinking", {"agent": "xai", "content": think_buffer})
                continue

            if in_think:
                if "</think>" in full_content:
                    idx = full_content.find("</think>")
                    remaining_think = full_content[full_content.rfind("<think>") + 7:idx]
                    already_sent = len(think_buffer)
                    new_think = remaining_think[already_sent:]
                    if new_think.strip():
                        yield _sse("thinking", {"agent": "xai", "content": new_think})
                    in_think = False
                    json_buffer = full_content[idx + 8:]
                else:
                    yield _sse("thinking", {"agent": "xai", "content": text})
                    think_buffer += text
            else:
                json_buffer += text
    except Exception as e:
        logger.warning("XAI 에이전트 스트리밍 중 오류: %s", e)
        yield _sse("agent_error", {"agent": "xai", "error": str(e)})
        return

    # JSON 파싱
    json_text = json_buffer.strip() if json_buffer else full_content.strip()
    if "</think>" in json_text:
        json_text = json_text.split("</think>", 1)[-1].strip()
    if json_text.startswith("```"):
        lines = json_text.split("\n")
        json_text = "\n".join(lines[1:])
        if json_text.endswith("```"):
            json_text = json_text[:-3]
    start_idx = json_text.find("{")
    end_idx = json_text.rfind("}")
    if start_idx >= 0 and end_idx > start_idx:
        json_text = json_text[start_idx:end_idx + 1]

    try:
        parsed = json.loads(json_text)
    except json.JSONDecodeError:
        logger.warning("XAI JSON 파싱 실패: %s", json_text[:500])
        parsed = {"portfolio_narrative": "AI 설명 생성 실패", "action_items": []}

    elapsed = time.time() - start
    yield _sse("thinking", {
        "agent": "xai",
        "content": f"\n\n투자 브리핑 완료 ({elapsed:.1f}초)\n",
    })

    yield _sse("agent_result", {"agent": "xai", "data": parsed})


# ---------------------------------------------------------------------------
# Orchestrator: 5-Agent Pipeline (SSE 스트리밍)
# ---------------------------------------------------------------------------

async def stream_portfolio_build(
    budget: float,
    style: str = "balanced",
    period: str = "medium",
    strategy_data: dict[str, Any] | None = None,
    exclude_tickers: list[str] | None = None,
) -> AsyncGenerator[str, None]:
    """
    멀티에이전트 포트폴리오 빌더의 전체 파이프라인을 SSE로 스트리밍한다.

    5단계:
      1. Analyst   — 정량 분석 (시세, 기술적 지표)
      2. Researcher — 뉴스·경제 분석
      3. Risk      — Monte Carlo, VaR, 시나리오
      4. Portfolio  — AI 포트폴리오 구성 (CoT)
      5. XAI       — 투자 근거 설명 (CoT)
    """
    total_start = time.time()
    style_cfg = STYLE_CONFIG.get(style, STYLE_CONFIG["balanced"])
    period_cfg = PERIOD_CONFIG.get(period, PERIOD_CONFIG["medium"])
    excludes = set(t.upper() for t in (exclude_tickers or []))

    yield _sse("pipeline_start", {
        "budget": budget,
        "style": style,
        "style_ko": style_cfg["label_ko"],
        "period": period,
        "period_ko": period_cfg["label_ko"],
        "total_steps": 5,
    })

    # 전략 데이터 획득
    if not strategy_data:
        from services.strategist import get_cached_market_strategy
        strategy_data = await get_cached_market_strategy(None, None, None, None)

    # === Agent 1: Analyst ===
    analyst_data: dict[str, Any] = {}
    async for event in _run_analyst_agent(strategy_data, style_cfg, excludes):
        yield event
        result = _parse_agent_result(event, "analyst")
        if result is not None:
            analyst_data = result

    candidate_quotes = analyst_data.get("candidate_quotes") or []
    if not candidate_quotes:
        yield _sse("complete", {
            "error": "추천 종목이 없습니다.",
            "budget": budget,
            "style": style,
        })
        return

    # === Agent 2: Researcher ===
    researcher_data: dict[str, Any] = {}
    candidate_tickers = [q["ticker"] for q in candidate_quotes]
    async for event in _run_researcher_agent(strategy_data, candidate_tickers):
        yield event
        result = _parse_agent_result(event, "researcher")
        if result is not None:
            researcher_data = result

    # === Agent 3: Risk ===
    # 사전 비중 추정 (균등 배분)
    n_candidates = len(candidate_quotes)
    defensive_pct = style_cfg["defensive_pct"]
    stock_pct = 1.0 - defensive_pct - style_cfg["cash_reserve_pct"]
    weights_estimate = [stock_pct / n_candidates] * n_candidates

    risk_data: dict[str, Any] = {}
    async for event in _run_risk_agent(candidate_quotes, weights_estimate, budget):
        yield event
        result = _parse_agent_result(event, "risk")
        if result is not None:
            risk_data = result

    # === Agent 4: Portfolio Strategist (CoT 스트리밍) ===
    portfolio_data: dict[str, Any] = {}
    async for event in _run_portfolio_agent(
        budget, style, period, analyst_data, researcher_data, risk_data,
    ):
        yield event
        result = _parse_agent_result(event, "portfolio")
        if result is not None:
            portfolio_data = result

    # === Agent 5: XAI Briefer (CoT 스트리밍) ===
    xai_data: dict[str, Any] = {}
    async for event in _run_xai_agent(
        portfolio_data, analyst_data, researcher_data, risk_data, budget,
    ):
        yield event
        result = _parse_agent_result(event, "xai")
        if result is not None:
            xai_data = result

    # === 최종 결과 조립 ===
    allocations = portfolio_data.get("allocations") or []
    total_invested = 0.0
    for alloc in allocations:
        ticker = alloc.get("ticker", "").upper()
        shares = int(alloc.get("shares", 0))
        price = None
        for cq in candidate_quotes + (analyst_data.get("defensive_quotes") or []):
            if cq["ticker"] == ticker:
                price = cq["price"]
                break
        if price and shares > 0:
            amount = round(price * shares, 2)
            alloc["price"] = price
            alloc["amount"] = amount
            alloc["shares"] = shares
            total_invested += amount

    cash_remaining = round(budget - total_invested, 2)

    final_result = sanitize_for_json({
        "budget": budget,
        "currency": "USD",
        "style": style,
        "style_ko": style_cfg["label_ko"],
        "period": period,
        "period_ko": period_cfg["label_ko"],
        "allocations": allocations,
        "total_invested": round(total_invested, 2),
        "cash_remaining": max(0, cash_remaining),
        "portfolio_thesis": portfolio_data.get("portfolio_thesis", ""),
        "sector_exposure": portfolio_data.get("sector_exposure", {}),
        "risk_assessment": portfolio_data.get("risk_assessment", {}),
        "dca_plan": portfolio_data.get("dca_plan"),
        "rebalance_note": portfolio_data.get("rebalance_note"),
        "warnings": portfolio_data.get("warnings", []),
        "market_regime": analyst_data.get("market_regime", "unknown"),
        # 리스크 분석 결과
        "risk_analysis": {
            "correlation": risk_data.get("correlation", {}),
            "volatility": risk_data.get("volatility", {}),
            "var": risk_data.get("var", {}),
            "monte_carlo": risk_data.get("monte_carlo", {}),
            "scenarios": risk_data.get("scenarios", {}),
            "anomalies": risk_data.get("anomalies", []),
        },
        # XAI 설명
        "xai": xai_data,
        "generated_at": datetime.now().isoformat(),
        "total_elapsed_sec": round(time.time() - total_start, 1),
    })

    yield _sse("complete", final_result)
