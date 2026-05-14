import asyncio
import time

import pandas as pd


def test_scan_stocks_keeps_low_volume_sp500_member(monkeypatch):
    from services import scanner

    dates = pd.date_range("2026-05-01", periods=6, freq="B")
    df = pd.DataFrame(
        {
            "Open": [10, 10, 10, 10, 10, 10],
            "High": [11, 11, 11, 11, 11, 11],
            "Low": [9, 9, 9, 9, 9, 9],
            "Close": [10, 10.2, 10.4, 10.6, 10.8, 11.0],
            "Volume": [100, 100, 100, 100, 100, 100],
        },
        index=dates,
    )

    monkeypatch.setattr(scanner.yf, "download", lambda *args, **kwargs: df)
    # scan_stocks 가 마지막에 ensure_sp500_coverage 로 SP500 전체를 padding 하므로
    # 단위 테스트에서는 빈 리스트로 mock 해 입력 ticker 만 검증한다.
    monkeypatch.setattr(scanner, "get_sp500_constituents", lambda: [])

    rows = scanner.scan_stocks(["LOWV"])

    assert len(rows) == 1
    assert rows[0]["ticker"] == "LOWV"
    assert rows[0]["liquidity_ok"] is False


def test_scan_stocks_returns_placeholder_for_download_missing_ticker(monkeypatch):
    from services import scanner

    dates = pd.date_range("2026-05-01", periods=6, freq="B")
    cols = pd.MultiIndex.from_product(
        [["HAVE"], ["Open", "High", "Low", "Close", "Volume"]]
    )
    df = pd.DataFrame(
        [
            [10, 11, 9, 10.0, 1_000_000],
            [10, 11, 9, 10.2, 1_000_000],
            [10, 11, 9, 10.4, 1_000_000],
            [10, 11, 9, 10.6, 1_000_000],
            [10, 11, 9, 10.8, 1_000_000],
            [10, 11, 9, 11.0, 1_000_000],
        ],
        index=dates,
        columns=cols,
    )

    monkeypatch.setattr(scanner.yf, "download", lambda *args, **kwargs: df)
    monkeypatch.setattr(scanner, "get_sp500_constituents", lambda: [])

    rows = scanner.scan_stocks(["HAVE", "MISS"])
    by_ticker = {row["ticker"]: row for row in rows}

    assert set(by_ticker) == {"HAVE", "MISS"}
    assert by_ticker["HAVE"]["price"] == 11.0
    assert by_ticker["MISS"]["price"] is None
    assert by_ticker["MISS"]["scan_missing"] is True


def test_ensure_sp500_coverage_expands_cached_snapshot(monkeypatch):
    from services import scanner

    monkeypatch.setattr(
        scanner,
        "get_sp500_constituents",
        lambda: [
            {"ticker": "AAA", "name": "AAA Corp", "sector": "Technology"},
            {"ticker": "BBB", "name": "BBB Corp", "sector": "Industrials"},
        ],
    )

    rows = scanner.ensure_sp500_coverage([{"ticker": "AAA", "price": 10.0}])
    by_ticker = {row["ticker"]: row for row in rows}

    assert set(by_ticker) == {"AAA", "BBB"}
    assert by_ticker["AAA"]["price_available"] is True
    assert by_ticker["BBB"]["price"] is None
    assert by_ticker["BBB"]["scan_missing"] is True


def test_heatmap_keeps_constituents_when_price_is_missing(monkeypatch):
    from services import heatmap

    monkeypatch.setattr(
        heatmap,
        "_constituents",
        [
            {
                "ticker": "AAA",
                "name": "AAA Corp",
                "sector": "Technology",
                "market_cap": 1000,
            },
            {
                "ticker": "BBB",
                "name": "BBB Corp",
                "sector": "Technology",
                "market_cap": 500,
            },
        ],
    )
    monkeypatch.setattr(heatmap, "_constituents_at", time.time())
    monkeypatch.setattr(
        heatmap,
        "_fetch_prices",
        lambda tickers: {
            "AAA": {
                "price": 123.45,
                "change_pct": 1.2,
                "price_source": "daily_close",
            },
        },
    )

    result = asyncio.run(heatmap.build_sp500_heatmap())
    stocks = result["sectors"][0]["stocks"]

    assert result["meta"]["constituents_count"] == 2
    assert result["meta"]["priced_count"] == 1
    assert [s["ticker"] for s in stocks] == ["AAA", "BBB"]
    assert stocks[1]["price"] is None
    assert stocks[1]["price_available"] is False


def test_heatmap_rejects_incomplete_legacy_snapshot():
    from services import heatmap

    snapshot = {"sectors": [{"name": "Technology", "stocks": [{"ticker": "A"}]}]}

    assert heatmap._heatmap_stock_count(snapshot) == 1
    assert heatmap._heatmap_cache_is_complete(snapshot) is False
