"""FinBERT 감성 분석 인터페이스.

저메모리 환경(Render Free 등)에서 transformers/torch 를 제외할 수 있도록
세 단계 폴백 체인을 제공한다.

  1) 로컬 FinBERT (transformers + torch 설치 시) — 가장 정확·무료
  2) OpenAI gpt-4o-mini   (transformers 없고 OPENAI_API_KEY 있을 때) — 비용 ~0
  3) neutral              (둘 다 불가) — 최후 폴백

호출 측(`services.sentiment` 등) 인터페이스는 그대로 유지된다.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from config import (
    FINBERT_OPENAI_BATCH_SIZE,
    FINBERT_OPENAI_MODEL,
    FINBERT_OPENAI_TIMEOUT_SEC,
    OPENAI_API_KEY,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# (1) 로컬 FinBERT
# ---------------------------------------------------------------------------
try:
    from transformers import pipeline, Pipeline  # type: ignore
    _TRANSFORMERS_AVAILABLE = True
except Exception as _e:
    _TRANSFORMERS_AVAILABLE = False
    Pipeline = None  # type: ignore[assignment]
    pipeline = None  # type: ignore[assignment]
    logger.info(
        "transformers 미설치 — 로컬 FinBERT 비활성, OpenAI 폴백으로 동작 (%s)", _e,
    )

_classifier = None  # type: ignore[var-annotated]

_LABEL_SCORE = {"positive": 1.0, "negative": -1.0, "neutral": 0.0}
_NEUTRAL = {"label": "neutral", "score": 0.0, "confidence": 0.0}


def _get_classifier():
    """로컬 FinBERT 파이프라인 싱글톤. transformers 없으면 None."""
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


# ---------------------------------------------------------------------------
# (2) OpenAI 폴백
# ---------------------------------------------------------------------------
_openai_client = None  # lazy 초기화


def _get_openai_client():
    """OpenAI client 싱글톤. API key 없으면 None."""
    global _openai_client
    if _openai_client is not None:
        return _openai_client
    if not OPENAI_API_KEY:
        return None
    try:
        from openai import OpenAI
        _openai_client = OpenAI(api_key=OPENAI_API_KEY, timeout=FINBERT_OPENAI_TIMEOUT_SEC)
        return _openai_client
    except Exception as e:
        logger.warning("OpenAI 클라이언트 초기화 실패 — neutral 폴백: %s", e)
        return None


_OPENAI_SYSTEM_PROMPT = (
    "You are a financial news sentiment classifier. "
    "For each input text, classify the sentiment as one of: positive, negative, neutral. "
    "Respond ONLY with a JSON object of the form: "
    '{"results": [{"label": "positive"|"negative"|"neutral", "confidence": 0.0-1.0}, ...]} '
    "Results must be in the same order as inputs. confidence is a float in [0,1]."
)


def _normalize_result(label: str, confidence: float) -> dict:
    label = (label or "neutral").lower().strip()
    if label not in _LABEL_SCORE:
        label = "neutral"
    try:
        confidence = max(0.0, min(1.0, float(confidence)))
    except (TypeError, ValueError):
        confidence = 0.0
    score = round(_LABEL_SCORE[label] * confidence, 4)
    return {"label": label, "score": score, "confidence": round(confidence, 4)}


def _openai_classify(texts: list[str]) -> list[dict] | None:
    """OpenAI 로 텍스트 리스트의 감성을 한 번에 분류. 실패 시 None."""
    client = _get_openai_client()
    if client is None:
        return None

    payload = {"texts": [t[:512] for t in texts]}
    try:
        resp = client.chat.completions.create(
            model=FINBERT_OPENAI_MODEL,
            messages=[
                {"role": "system", "content": _OPENAI_SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )
        content = resp.choices[0].message.content or "{}"
        parsed = json.loads(content)
        results = parsed.get("results") or []
        if len(results) != len(texts):
            logger.warning(
                "OpenAI 감성 응답 길이 불일치(입력 %d, 응답 %d) — neutral 폴백",
                len(texts), len(results),
            )
            return None
        return [_normalize_result(r.get("label"), r.get("confidence", 0.0)) for r in results]
    except Exception as e:
        logger.warning("OpenAI 감성 분류 실패: %s", e)
        return None


# ---------------------------------------------------------------------------
# Public API — 호출 측은 인터페이스 변경 없이 그대로 사용
# ---------------------------------------------------------------------------

def analyze_text(text: str) -> dict:
    """단일 텍스트 감성 분석. FinBERT → OpenAI → neutral 순으로 폴백."""
    if not text or not text.strip():
        return dict(_NEUTRAL)

    clf = _get_classifier()
    if clf is not None:
        try:
            result = clf(text[:512])[0]
            return _normalize_result(result["label"], result["score"])
        except Exception as e:
            logger.warning("FinBERT 추론 에러 — OpenAI 폴백 시도: %s", e)

    fallback = _openai_classify([text])
    if fallback:
        return fallback[0]
    return dict(_NEUTRAL)


def analyze_batch(texts: list[str]) -> list[dict]:
    """배치 감성 분석. FinBERT → OpenAI(청크 분할) → neutral 순으로 폴백."""
    if not texts:
        return []

    clean = [t[:512] if t and t.strip() else "" for t in texts]
    non_empty_indices = [i for i, t in enumerate(clean) if t]
    results: list[dict] = [dict(_NEUTRAL) for _ in texts]

    if not non_empty_indices:
        return results

    clf = _get_classifier()
    if clf is not None:
        try:
            batch_texts = [clean[i] for i in non_empty_indices]
            raw_results = clf(batch_texts)
            for idx, raw in zip(non_empty_indices, raw_results):
                results[idx] = _normalize_result(raw["label"], raw["score"])
            return results
        except Exception as e:
            logger.warning("FinBERT 배치 에러 — OpenAI 폴백 시도: %s", e)

    # OpenAI 폴백 — 청크로 분할 호출 (한 호출당 토큰 한도/지연 분산)
    batch_texts = [clean[i] for i in non_empty_indices]
    chunk_size = max(1, FINBERT_OPENAI_BATCH_SIZE)
    aggregated: list[dict] = []
    for start in range(0, len(batch_texts), chunk_size):
        chunk = batch_texts[start:start + chunk_size]
        chunk_res = _openai_classify(chunk)
        if chunk_res is None:
            aggregated.extend(dict(_NEUTRAL) for _ in chunk)
        else:
            aggregated.extend(chunk_res)

    for idx, item in zip(non_empty_indices, aggregated):
        results[idx] = item
    return results
