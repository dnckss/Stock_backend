"""strategist 진입가 환각 보정 단위 테스트."""
from services.strategist import _entry_zone_mid, _normalize_recommendation_prices


def test_entry_zone_mid_with_both():
    assert _entry_zone_mid({"low": 100, "high": 110}) == 105.0


def test_entry_zone_mid_low_only():
    assert _entry_zone_mid({"low": 100}) == 100.0


def test_entry_zone_mid_invalid():
    assert _entry_zone_mid(None) is None
    assert _entry_zone_mid({}) is None
    assert _entry_zone_mid({"low": 0, "high": -5}) is None


def test_normalize_skips_when_within_threshold():
    rec = {
        "ticker": "AAPL", "direction": "BUY", "current_price": 200.0,
        "entry_zone": {"low": 198, "high": 202},
        "stop_loss": 190, "targets": [{"price": 210}],
    }
    fixed = _normalize_recommendation_prices([rec])
    assert fixed == 0
    assert rec["entry_zone"] == {"low": 198, "high": 202}
    assert rec["stop_loss"] == 190


def test_normalize_corrects_buy_hallucination():
    # TXN 5/6 실제 환각 케이스: 144 vs 289 (편차 50%)
    rec = {
        "ticker": "TXN", "direction": "BUY", "current_price": 289.44,
        "entry_zone": {"low": 142.5, "high": 146.0},
        "stop_loss": 140.7,
        "targets": [{"price": 151.5}, {"price": 158.0}],
        "risk_reward_ratio": 1.5,
    }
    fixed = _normalize_recommendation_prices([rec])
    assert fixed == 1
    # entry_zone 이 current_price 근방으로 보정
    new_lo = rec["entry_zone"]["low"]
    new_hi = rec["entry_zone"]["high"]
    assert 285 < new_lo < 290
    assert 289 < new_hi < 295
    # BUY 손절은 current_price 아래
    assert rec["stop_loss"] < 289.44
    # 환각 가격 기반의 targets 무효화
    assert rec["targets"] == []
    assert rec["risk_reward_ratio"] is None


def test_normalize_corrects_sell_hallucination():
    # SELL: 손절은 current_price 위로 가야
    rec = {
        "ticker": "XYZ", "direction": "SELL", "current_price": 100.0,
        "entry_zone": {"low": 50, "high": 52},
        "stop_loss": 55,
    }
    fixed = _normalize_recommendation_prices([rec])
    assert fixed == 1
    assert rec["stop_loss"] > 100.0


def test_normalize_skips_when_no_current_price():
    rec = {"ticker": "X", "direction": "BUY", "entry_zone": {"low": 10, "high": 11}}
    fixed = _normalize_recommendation_prices([rec])
    assert fixed == 0
    # entry_zone 그대로 유지
    assert rec["entry_zone"] == {"low": 10, "high": 11}
