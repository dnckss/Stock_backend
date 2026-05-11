from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# transformers/torch 는 메모리 부담이 커서(~750MB) Render Free Plan 등 저메모리
# 환경에서는 requirements 에서 제외할 수 있다. 그 경우 모든 추론은 neutral 폴백.
try:
    from transformers import pipeline, Pipeline  # type: ignore
    _TRANSFORMERS_AVAILABLE = True
except Exception as _e:  # ImportError + transformers 내부 import 오류 모두 흡수
    _TRANSFORMERS_AVAILABLE = False
    Pipeline = None  # type: ignore[assignment]
    pipeline = None  # type: ignore[assignment]
    logger.warning(
        "transformers 미설치/로드 실패 — FinBERT 비활성, 감성은 neutral 폴백 (%s)", _e,
    )

_classifier = None  # type: ignore[var-annotated]

_LABEL_SCORE = {"positive": 1.0, "negative": -1.0, "neutral": 0.0}
_NEUTRAL = {"label": "neutral", "score": 0.0, "confidence": 0.0}


def _get_classifier():
    """ProsusAI/finbert 파이프라인을 싱글톤으로 로드한다. transformers 없으면 None."""
    global _classifier
    if not _TRANSFORMERS_AVAILABLE:
        return None
    if _classifier is None:
        logger.info("FinBERT 모델 로딩 중...")
        _classifier = pipeline(
            "sentiment-analysis",
            model="ProsusAI/finbert",
            truncation=True,
            max_length=512,
        )
        logger.info("FinBERT 모델 로딩 완료")
    return _classifier


def analyze_text(text: str) -> dict:
    """단일 텍스트 FinBERT 감성 분석. transformers 미설치 시 neutral 폴백."""
    if not text or not text.strip():
        return dict(_NEUTRAL)

    clf = _get_classifier()
    if clf is None:
        return dict(_NEUTRAL)

    try:
        result = clf(text[:512])[0]
        label = result["label"]
        confidence = round(result["score"], 4)
        score = round(_LABEL_SCORE.get(label, 0.0) * confidence, 4)
        return {"label": label, "score": score, "confidence": confidence}
    except Exception as e:
        logger.warning("FinBERT 추론 에러: %s", e)
        return dict(_NEUTRAL)


def analyze_batch(texts: list[str]) -> list[dict]:
    """배치 FinBERT 감성 분석. transformers 미설치 시 모두 neutral 폴백."""
    if not texts:
        return []

    clean = [t[:512] if t and t.strip() else "" for t in texts]
    non_empty_indices = [i for i, t in enumerate(clean) if t]

    results: list[dict] = [dict(_NEUTRAL) for _ in texts]

    if not non_empty_indices:
        return results

    clf = _get_classifier()
    if clf is None:
        return results

    try:
        batch_texts = [clean[i] for i in non_empty_indices]
        raw_results = clf(batch_texts)

        for idx, raw in zip(non_empty_indices, raw_results):
            label = raw["label"]
            confidence = round(raw["score"], 4)
            score = round(_LABEL_SCORE.get(label, 0.0) * confidence, 4)
            results[idx] = {"label": label, "score": score, "confidence": confidence}
    except Exception as e:
        logger.warning("FinBERT 배치 추론 에러: %s", e)

    return results
