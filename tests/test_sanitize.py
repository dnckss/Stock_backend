"""sanitize_for_json — NaN/Inf/datetime 안전 직렬화."""
import json
import math
from datetime import date, datetime

from services.crud import sanitize_for_json


def test_sanitize_nan_to_none():
    out = sanitize_for_json({"a": float("nan"), "b": 1.5})
    assert out["a"] is None
    assert out["b"] == 1.5
    json.dumps(out)  # 직렬화 가능 확인


def test_sanitize_inf_to_none():
    out = sanitize_for_json({"a": float("inf"), "b": float("-inf")})
    assert out["a"] is None
    assert out["b"] is None


def test_sanitize_nested_structures():
    nested = {
        "list": [1.0, float("nan"), {"deep": float("inf")}],
        "dict": {"x": [float("nan")]},
    }
    out = sanitize_for_json(nested)
    assert out["list"][1] is None
    assert out["list"][2]["deep"] is None
    assert out["dict"]["x"][0] is None
    json.dumps(out)


def test_sanitize_passes_datetime_through():
    # sanitize_for_json 은 datetime 을 변환하지 않는다 — NaN/Inf 만 처리.
    # datetime → isoformat 은 `_safe_value` 의 책임.
    dt = datetime(2026, 5, 13, 10, 30, 0)
    out = sanitize_for_json({"ts": dt})
    assert out["ts"] is dt


def test_sanitize_preserves_ints_and_strings():
    out = sanitize_for_json({"n": 42, "s": "hello", "b": True, "none": None})
    assert out == {"n": 42, "s": "hello", "b": True, "none": None}


def test_sanitize_finite_floats_unchanged():
    assert sanitize_for_json(0.0) == 0.0
    assert sanitize_for_json(-1.5) == -1.5
    assert math.isfinite(sanitize_for_json(3.14))
