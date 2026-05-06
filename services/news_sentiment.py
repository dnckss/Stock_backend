from __future__ import annotations

from typing import Literal, TypedDict

NewsPolarity = Literal["positive", "negative", "neutral"]
NewsDirection = Literal["positive", "negative", "mixed", "unclear"]


class NormalizedImpact(TypedDict, total=False):
    normalized_direction: NewsPolarity
    normalized_direction_ko: str


_KO: dict[NewsPolarity, str] = {
    "positive": "호재",
    "negative": "악재",
    "neutral": "중립",
}


def normalize_to_polarity(value: str | None) -> NewsPolarity:
    """
    서로 다른 감성/방향 라벨을 UI용 3분류(positive/negative/neutral)로 정규화한다.
    - FinBERT: positive/negative/neutral
    - LLM impact.direction: positive/negative/mixed/unclear
    """
    v = (value or "").strip().lower()
    if v == "positive":
        return "positive"
    if v == "negative":
        return "negative"
    # neutral / mixed / unclear / unknown -> neutral
    return "neutral"


def polarity_to_ko(polarity: NewsPolarity) -> str:
    return _KO.get(polarity, "중립")


def add_normalized_impact_fields(impact: dict) -> dict:
    """
    impact 객체에 normalized_direction(+ko)을 추가해 반환한다.
    원본 키(direction 등)는 유지한다.
    """
    if not isinstance(impact, dict):
        return impact
    p = normalize_to_polarity(str(impact.get("direction") or ""))
    impact["normalized_direction"] = p
    impact["normalized_direction_ko"] = polarity_to_ko(p)
    return impact


def llm_polarity_from_analysis(analysis) -> NewsPolarity | None:
    """
    news_articles.analysis_json 에서 LLM 의 impact.direction 을 polarity 3분류로 정규화.
    분석이 없거나 파싱 실패면 None — 이 경우 호출측은 FinBERT polarity 를 그대로 사용.
    """
    if not analysis:
        return None
    if isinstance(analysis, str):
        import json
        try:
            analysis = json.loads(analysis)
        except (TypeError, ValueError):
            return None
    if not isinstance(analysis, dict):
        return None
    impact = analysis.get("impact")
    if not isinstance(impact, dict):
        return None
    direction = impact.get("direction")
    if not direction:
        return None
    return normalize_to_polarity(direction)

