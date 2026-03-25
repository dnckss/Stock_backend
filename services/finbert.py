from __future__ import annotations

from transformers import pipeline, Pipeline

_classifier: Pipeline | None = None

_LABEL_SCORE = {"positive": 1.0, "negative": -1.0, "neutral": 0.0}


def _get_classifier() -> Pipeline:
    """ProsusAI/finbert 파이프라인을 싱글톤으로 로드한다."""
    global _classifier
    if _classifier is None:
        print("FinBERT 모델 로딩 중...")
        _classifier = pipeline(
            "sentiment-analysis",
            model="ProsusAI/finbert",
            truncation=True,
            max_length=512,
        )
        print("FinBERT 모델 로딩 완료")
    return _classifier


def analyze_text(text: str) -> dict:
    """
    단일 텍스트에 대해 FinBERT 감성 분석을 수행한다.

    Returns:
        {
            "label": "positive" | "negative" | "neutral",
            "score": float (-1.0 ~ 1.0),
            "confidence": float (0.0 ~ 1.0),
        }
    """
    if not text or not text.strip():
        return {"label": "neutral", "score": 0.0, "confidence": 0.0}

    try:
        clf = _get_classifier()
        result = clf(text[:512])[0]
        label = result["label"]
        confidence = round(result["score"], 4)
        score = round(_LABEL_SCORE.get(label, 0.0) * confidence, 4)
        return {"label": label, "score": score, "confidence": confidence}
    except Exception as e:
        print(f"FinBERT 추론 에러: {e}")
        return {"label": "neutral", "score": 0.0, "confidence": 0.0}


def analyze_batch(texts: list[str]) -> list[dict]:
    """
    여러 텍스트를 배치로 FinBERT 감성 분석한다.

    Returns:
        [{"label": ..., "score": ..., "confidence": ...}, ...]
    """
    if not texts:
        return []

    clean = [t[:512] if t and t.strip() else "" for t in texts]
    non_empty_indices = [i for i, t in enumerate(clean) if t]

    results = [{"label": "neutral", "score": 0.0, "confidence": 0.0}] * len(texts)

    if not non_empty_indices:
        return results

    try:
        clf = _get_classifier()
        batch_texts = [clean[i] for i in non_empty_indices]
        raw_results = clf(batch_texts)

        for idx, raw in zip(non_empty_indices, raw_results):
            label = raw["label"]
            confidence = round(raw["score"], 4)
            score = round(_LABEL_SCORE.get(label, 0.0) * confidence, 4)
            results[idx] = {"label": label, "score": score, "confidence": confidence}
    except Exception as e:
        print(f"FinBERT 배치 추론 에러: {e}")

    return results
