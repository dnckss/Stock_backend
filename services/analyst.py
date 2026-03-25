from __future__ import annotations

from openai import OpenAI
from config import (
    OPENAI_API_KEY,
    OPENAI_MODEL,
    BUY_THRESHOLD,
    SELL_THRESHOLD,
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
    "3. 주어진 수치(수익률, 감성, 괴리율, 실적 서프라이즈)만을 근거로 분석해.\n"
    "4. 확신에 찬 월스트리트 전문가 톤으로 마크다운 불릿 포인트 3줄 브리핑만 써.\n"
    "5. 한국어로 작성해."
)


def compute_signals(
    candidates: list,
    sentiments: list,
    earnings: list[dict | None] | None = None,
) -> list:
    """
    이익 괴리(Earnings Surprise) 우선, 실적 없으면 감성 괴리로 fallback.

    - earnings[i]가 유효하면:
        divergence = surprise_pct
        signal = BUY / SELL / HOLD (EARNINGS_BUY_PCT / EARNINGS_SELL_PCT 기준)
    - earnings[i]가 None이면:
        divergence = sentiment - return
        signal = BUY / SELL / HOLD (BUY_THRESHOLD / SELL_THRESHOLD 기준)
    """
    for i, sent_score in enumerate(sentiments):
        earning = earnings[i] if earnings and i < len(earnings) else None

        if earning and earning.get("surprise_pct") is not None:
            surprise = earning["surprise_pct"]
            signal = (
                "BUY" if surprise > EARNINGS_BUY_PCT
                else "SELL" if surprise < EARNINGS_SELL_PCT
                else "HOLD"
            )
            candidates[i].update({
                "sentiment": sent_score,
                "divergence": round(surprise, 4),
                "signal": signal,
                "signal_source": "earnings",
                "eps_actual": earning.get("eps_actual"),
                "eps_estimate": earning.get("eps_estimate"),
                "earnings_surprise_pct": round(surprise, 4),
            })
        else:
            div = sent_score - candidates[i]["return"]
            signal = (
                "BUY" if div > BUY_THRESHOLD
                else "SELL" if div < SELL_THRESHOLD
                else "HOLD"
            )
            candidates[i].update({
                "sentiment": sent_score,
                "divergence": round(div, 3),
                "signal": signal,
                "signal_source": "sentiment",
                "eps_actual": None,
                "eps_estimate": None,
                "earnings_surprise_pct": None,
            })

    return candidates


def generate_reports(candidates: list) -> list:
    """괴리율 Top N 종목에 대해 GPT 분석 리포트를 생성한다."""
    candidates.sort(key=lambda x: abs(x["divergence"]), reverse=True)
    top = candidates[:REPORT_TOP_N]

    for target in top:
        try:
            source = target.get("signal_source", "sentiment")
            if source == "earnings":
                detail = (
                    f"실적 서프라이즈: {target.get('earnings_surprise_pct', 0):.2%}, "
                    f"EPS 실제: {target.get('eps_actual')}, "
                    f"EPS 예상: {target.get('eps_estimate')}, "
                )
            else:
                detail = (
                    f"뉴스 감성: {target['sentiment']}, "
                    f"감성 괴리율: {target['divergence']}, "
                )

            resp = _client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": (
                            f"종목: {target['ticker']}, "
                            f"5일 수익률: {target['return']:.2%}, "
                            f"{detail}"
                            f"시그널: {target['signal']}. "
                            f"지금 즉시 분석해."
                        ),
                    },
                ],
                temperature=0.3,
            )
            target["report"] = resp.choices[0].message.content
        except Exception as e:
            print(f"리포트 생성 실패 ({target['ticker']}): {e}")
            target["report"] = None

    return candidates
