"""백테스트 entry_price 보정 단위 테스트 (yfinance 호출 없음 — 가짜 close_df 사용)."""
from datetime import datetime

import pandas as pd

from services.backtest import (
    _actual_entry_close,
    _enrich_strategy_entry,
    _resolve_actual_entry_prices,
)


def _make_close_df():
    idx = pd.DatetimeIndex(["2026-05-04", "2026-05-05", "2026-05-06", "2026-05-07"])
    return pd.DataFrame({"TXN": [280.0, 281.0, 289.44, 285.0]}, index=idx)


def test_enrich_strategy_entry_picks_mid():
    row = {"entry_low": 100, "entry_high": 110}
    out = _enrich_strategy_entry(row)
    assert out["price"] == 105.0


def test_enrich_strategy_entry_handles_missing():
    assert _enrich_strategy_entry({})["price"] is None
    assert _enrich_strategy_entry({"entry_low": 100})["price"] == 100.0


def test_actual_entry_close_uses_entry_day():
    df = _make_close_df()
    dt = datetime(2026, 5, 6, 12, 0)
    assert _actual_entry_close(df, "TXN", dt) == 289.44


def test_actual_entry_close_uses_prior_trading_day_when_weekend():
    df = _make_close_df()
    # 5/9 (토) — asof 가 5/8 직전 거래일을 찾음
    dt = datetime(2026, 5, 9, 12, 0)
    val = _actual_entry_close(df, "TXN", dt)
    assert val == 285.0  # 5/7 종가가 가장 최근


def test_actual_entry_close_returns_none_for_unknown_ticker():
    df = _make_close_df()
    dt = datetime(2026, 5, 6, 12, 0)
    assert _actual_entry_close(df, "NOPE", dt) is None


def test_resolve_actual_entry_prices_overrides_hallucination():
    df = _make_close_df()
    records = [
        _enrich_strategy_entry({
            "ticker": "TXN",
            "entry_low": 142.5, "entry_high": 146.0,
            "created_at": "2026-05-06T11:05:11+00:00",
        })
    ]
    # 환각 mid = 144.25
    assert records[0]["price"] == 144.25
    fixed = _resolve_actual_entry_prices(records, df)
    # 실제 종가 289.44 로 교체
    assert abs(fixed[0]["price"] - 289.44) < 0.01


def test_resolve_keeps_fallback_when_close_missing():
    empty = pd.DataFrame()
    records = [
        _enrich_strategy_entry({
            "ticker": "ZZZ",
            "entry_low": 50, "entry_high": 52,
            "created_at": "2026-05-06T11:05:11+00:00",
        })
    ]
    fixed = _resolve_actual_entry_prices(records, empty)
    # close_df 가 비었으니 fallback(=51) 유지
    assert fixed[0]["price"] == 51.0
