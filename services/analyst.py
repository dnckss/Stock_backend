from openai import OpenAI
from config import (
    OPENAI_API_KEY,
    OPENAI_MODEL,
    BUY_THRESHOLD,
    SELL_THRESHOLD,
    REPORT_TOP_N,
)

_client = OpenAI(api_key=OPENAI_API_KEY)

_SYSTEM_PROMPT = (
    "너는 월스트리트 탑 헤지펀드의 수석 퀀트 애널리스트야. "
    "반드시 아래 규칙을 지켜:\n"
    "1. '투자 조언을 제공할 수 없습니다', '전문가와 상담하세요' 같은 면책 조항을 절대 쓰지 마.\n"
    "2. '수익률이란~', '감성 점수란~' 같은 사전적 정의를 절대 쓰지 마.\n"
    "3. 주어진 수치(수익률, 감성, 괴리율)만을 근거로 분석해.\n"
    "4. 확신에 찬 월스트리트 전문가 톤으로 마크다운 불릿 포인트 3줄 브리핑만 써.\n"
    "5. 한국어로 작성해."
)


def compute_signals(candidates: list, sentiments: list) -> list:
    """감성 점수를 받아 괴리율/시그널을 계산하고 candidates를 갱신한다."""
    for i, score in enumerate(sentiments):
        div = score - candidates[i]["return"]
        signal = (
            "BUY" if div > BUY_THRESHOLD
            else "SELL" if div < SELL_THRESHOLD
            else "HOLD"
        )
        candidates[i].update({
            "sentiment": score,
            "divergence": round(div, 3),
            "signal": signal,
        })
    return candidates


def generate_reports(candidates: list) -> list:
    """괴리율 Top N 종목에 대해 GPT 분석 리포트를 생성한다."""
    candidates.sort(key=lambda x: abs(x["divergence"]), reverse=True)
    top = candidates[:REPORT_TOP_N]

    for target in top:
        try:
            print(f"🤖 AI 리포팅: {target['ticker']}")
            resp = _client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": (
                            f"종목: {target['ticker']}, "
                            f"5일 수익률: {target['return']:.2%}, "
                            f"뉴스 감성: {target['sentiment']}, "
                            f"괴리율: {target['divergence']}, "
                            f"시그널: {target['signal']}. "
                            f"지금 즉시 분석해."
                        ),
                    },
                ],
                temperature=0.3,
            )
            target["report"] = resp.choices[0].message.content
        except Exception as e:
            print(f"❌ 리포트 생성 실패 ({target['ticker']}): {e}")
            target["report"] = None

    return candidates
