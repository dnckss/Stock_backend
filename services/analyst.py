"""
다중 팩터 복합 시그널 계산 모듈.

기존 "감성 - 수익률" 단순 뺄셈 대신, 4가지 팩터를 -1~+1로 정규화한 뒤
가중 합산하여 복합 스코어(composite_score)를 산출한다.

팩터:
  1. 감성(Sentiment)     — FinBERT 뉴스 감성 (-1 ~ +1)
  2. 실적(Earnings)      — EPS 서프라이즈 비율 (정규화)
  3. 모멘텀(Momentum)    — RSI 기반 과매수/과매도 역전 신호
  4. 거래량(Volume)      — 20일 평균 대비 거래량 비율 (관심도)
"""
from __future__ import annotations

import math
from typing import Any

from openai import OpenAI
from config import (
    OPENAI_API_KEY,
    OPENAI_MODEL,
    SIGNAL_BUY_THRESHOLD,
    SIGNAL_SELL_THRESHOLD,
    SIGNAL_WEIGHT_SENTIMENT,
    SIGNAL_WEIGHT_EARNINGS,
    SIGNAL_WEIGHT_MOMENTUM,
    SIGNAL_WEIGHT_VOLUME,
    EARNINGS_BUY_PCT,
    EARNINGS_SELL_PCT,
    REPORT_TOP_N,
)

_client = OpenAI(api_key=OPENAI_API_KEY)

_SYSTEM_PROMPT = (
    "너는 월스트리트 탑 헤지펀드의 수석 퀀트 애널리스트야. "
    "반드시 아래 규칙을 지켜:\n"
    "1. '투자 조언을 제공할 수 없습니다', '전문가와 상담하세요' 같은 면책 조항을 절대 쓰지 마.\n"
    "2. '수익률이란~', '감성 점수란~' 같은 사전적 정의를 절대 쓰지 마.\n"
    "3. 주어진 수치(수익률, 감성, 복합 스코어, 실적 서프라이즈)만을 근거로 분석해.\n"
    "4. 확신에 찬 월스트리트 전문가 톤으로 마크다운 불릿 포인트 3줄 브리핑만 써.\n"
    "5. 한국어로 작성해."
)


def _clamp(v: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# 개별 팩터 정규화 (-1 ~ +1)
# ---------------------------------------------------------------------------

def _normalize_sentiment(score: float) -> float:
    """
    FinBERT score를 -1 ~ +1로 변환.
    FinBERT는 0~1 범위의 softmax 확률을 반환하며,
    label이 positive면 score 그대로, negative면 (1-score), neutral이면 0.5 근처.
    여기서는 이미 부호가 반영된 sent_score를 받는다고 가정 (-1~+1).
    """
    return _clamp(score)


def _normalize_earnings(surprise_pct: float | None) -> float:
    """
    실적 서프라이즈를 -1 ~ +1로 정규화.
    ±20% 이상이면 최대/최소값으로 클램핑.
    """
    if surprise_pct is None:
        return 0.0
    # ±20%를 ±1.0으로 매핑
    return _clamp(surprise_pct / 0.20)


def _normalize_momentum(rsi: float | None) -> float:
    """
    RSI를 모멘텀 팩터로 변환.
    RSI 50 = 중립(0), RSI 30 이하 = 과매도 역전 기회(+1), RSI 70 이상 = 과매수 경고(-1)

    역전 논리: RSI가 낮을수록 반등 가능성 → 양수(매수 기회)
    """
    if rsi is None:
        return 0.0
    # RSI 0~100 → -1~+1 (역전: 낮을수록 양수)
    normalized = (50 - rsi) / 50
    return _clamp(normalized)


def _normalize_volume(volume_ratio: float | None) -> float:
    """
    거래량 비율(20일 평균 대비)을 관심도 팩터로 변환.
    1.0 = 평균(0), 2.0+ = 높은 관심(+1), 0.5 이하 = 낮은 관심(-0.5)

    높은 거래량 자체는 방향성이 없으므로 절대값 기반 관심도로 처리.
    다른 팩터와 결합 시 방향을 결정한다.
    """
    if volume_ratio is None:
        return 0.0
    # 1.0 기준으로 초과분을 스케일링 (2.0 → +1.0, 0.5 → -0.5)
    return _clamp((volume_ratio - 1.0) / 1.0)


# ---------------------------------------------------------------------------
# 복합 스코어 계산
# ---------------------------------------------------------------------------

def _compute_composite_score(
    sentiment: float,
    earnings_surprise: float | None,
    rsi: float | None,
    volume_ratio: float | None,
) -> dict[str, Any]:
    """
    4개 팩터를 정규화 후 가중 합산하여 복합 스코어를 산출한다.
    실적 데이터가 없으면 나머지 팩터로 가중치를 재분배한다.
    """
    s_norm = _normalize_sentiment(sentiment)
    e_norm = _normalize_earnings(earnings_surprise)
    m_norm = _normalize_momentum(rsi)
    v_norm = _normalize_volume(volume_ratio)

    w_s = SIGNAL_WEIGHT_SENTIMENT
    w_e = SIGNAL_WEIGHT_EARNINGS
    w_m = SIGNAL_WEIGHT_MOMENTUM
    w_v = SIGNAL_WEIGHT_VOLUME

    # 실적 데이터 없으면 가중치 재분배
    if earnings_surprise is None:
        total_others = w_s + w_m + w_v
        if total_others > 0:
            w_s = w_s / total_others
            w_m = w_m / total_others
            w_v = w_v / total_others
        w_e = 0.0

    # 거래량은 방향 증폭기 역할: 다른 팩터의 합산 방향과 같은 방향이면 증폭
    base_score = w_s * s_norm + w_e * e_norm + w_m * m_norm
    # 거래량이 높고 기존 방향이 뚜렷하면 증폭, 아니면 축소
    if base_score != 0 and v_norm > 0:
        volume_boost = w_v * v_norm * (1 if base_score > 0 else -1)
    else:
        volume_boost = 0.0

    composite = _clamp(base_score + volume_boost)

    return {
        "composite_score": round(composite, 4),
        "factors": {
            "sentiment": round(s_norm, 4),
            "earnings": round(e_norm, 4),
            "momentum": round(m_norm, 4),
            "volume": round(v_norm, 4),
        },
        "weights": {
            "sentiment": round(w_s, 3),
            "earnings": round(w_e, 3),
            "momentum": round(w_m, 3),
            "volume": round(w_v, 3),
        },
    }


# ---------------------------------------------------------------------------
# 시그널 계산 (메인)
# ---------------------------------------------------------------------------

def compute_signals(
    candidates: list,
    sentiments: list,
    earnings: list[dict | None] | None = None,
    technicals: dict[str, dict[str, Any]] | None = None,
) -> list:
    """
    다중 팩터 복합 스코어 기반 시그널 계산.

    팩터:
      1. 감성 (FinBERT) — 뉴스 헤드라인 감성
      2. 실적 (Earnings Surprise) — EPS 괴리
      3. 모멘텀 (RSI) — 과매수/과매도 역전 신호
      4. 거래량 (Volume Ratio) — 20일 평균 대비

    composite_score > BUY_THRESHOLD  → BUY
    composite_score < SELL_THRESHOLD → SELL
    그 외                            → HOLD
    """
    for i, sent_score in enumerate(sentiments):
        earning = earnings[i] if earnings and i < len(earnings) else None
        ticker = (candidates[i].get("ticker") or "").upper()

        # 실적 서프라이즈
        surprise_pct = None
        eps_actual = None
        eps_estimate = None
        if earning and earning.get("surprise_pct") is not None:
            surprise_pct = earning["surprise_pct"]
            eps_actual = earning.get("eps_actual")
            eps_estimate = earning.get("eps_estimate")

        # 기술적 지표 (있으면)
        tech = (technicals or {}).get(ticker) or {}
        rsi = _safe_float(tech.get("rsi_14"))
        volume_ratio = _safe_float(tech.get("volume_ratio"))

        # 복합 스코어 계산
        score_data = _compute_composite_score(
            sentiment=sent_score,
            earnings_surprise=surprise_pct,
            rsi=rsi,
            volume_ratio=volume_ratio,
        )

        composite = score_data["composite_score"]
        signal = (
            "BUY" if composite > SIGNAL_BUY_THRESHOLD
            else "SELL" if composite < SIGNAL_SELL_THRESHOLD
            else "HOLD"
        )

        # 시그널 소스 결정 (가장 기여도 높은 팩터)
        factors = score_data["factors"]
        weights = score_data["weights"]
        contributions = {k: abs(factors[k]) * weights[k] for k in factors}
        signal_source = max(contributions, key=contributions.get)

        # divergence는 하위호환을 위해 유지 (composite_score와 동일)
        candidates[i].update({
            "sentiment": sent_score,
            "divergence": composite,
            "composite_score": composite,
            "signal": signal,
            "signal_source": signal_source,
            "signal_factors": score_data["factors"],
            "signal_weights": score_data["weights"],
            "eps_actual": eps_actual,
            "eps_estimate": eps_estimate,
            "earnings_surprise_pct": round(surprise_pct, 4) if surprise_pct is not None else None,
        })

    return candidates


# ---------------------------------------------------------------------------
# GPT 리포트 생성
# ---------------------------------------------------------------------------

def generate_reports(candidates: list) -> list:
    """복합 스코어 Top N 종목에 대해 GPT 분석 리포트를 생성한다."""
    candidates.sort(key=lambda x: abs(x.get("composite_score", 0)), reverse=True)
    top = candidates[:REPORT_TOP_N]

    for target in top:
        try:
            factors = target.get("signal_factors", {})
            detail_parts = [f"5일 수익률: {target['return']:.2%}"]

            if target.get("earnings_surprise_pct") is not None:
                detail_parts.append(f"실적 서프라이즈: {target['earnings_surprise_pct']:.2%}")
                detail_parts.append(f"EPS 실제/예상: {target.get('eps_actual')}/{target.get('eps_estimate')}")

            detail_parts.append(f"뉴스 감성: {target['sentiment']:.3f}")
            detail_parts.append(f"복합 스코어: {target.get('composite_score', 0):.3f}")
            detail_parts.append(
                f"팩터(감성={factors.get('sentiment', 0):.2f}, "
                f"실적={factors.get('earnings', 0):.2f}, "
                f"모멘텀={factors.get('momentum', 0):.2f}, "
                f"거래량={factors.get('volume', 0):.2f})"
            )

            detail = ", ".join(detail_parts)

            # gpt-5 temperature 미지원 분기
            model = OPENAI_MODEL or "gpt-5"
            kwargs: dict[str, Any] = {
                "model": model,
                "messages": [
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": (
                            f"종목: {target['ticker']}, "
                            f"{detail}, "
                            f"시그널: {target['signal']} (주요 팩터: {target.get('signal_source', '-')}). "
                            f"지금 즉시 분석해."
                        ),
                    },
                ],
            }
            model_lower = model.lower()
            if "gpt-5" not in model_lower and "o1" not in model_lower and "o3" not in model_lower:
                kwargs["temperature"] = 0.3

            resp = _client.chat.completions.create(**kwargs)
            target["report"] = resp.choices[0].message.content
        except Exception as e:
            print(f"리포트 생성 실패 ({target['ticker']}): {e}")
            target["report"] = None

    return candidates
