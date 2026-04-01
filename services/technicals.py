"""
기술적 지표 계산 모듈.
yfinance 일봉 데이터 기반 RSI, MACD, 볼린저밴드, 이동평균, ATR, 지지/저항선을 계산한다.
"""
from __future__ import annotations

import logging
import math
from typing import Any

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

_DOWNLOAD_PERIOD = "6mo"  # 200일 MA 계산을 위해 충분한 기간


def _safe_round(v: Any, n: int = 2) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return round(f, n) if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def compute_technicals(ticker: str) -> dict[str, Any] | None:
    """
    종목의 기술적 지표를 계산하여 반환한다.
    실패 시 None.
    """
    try:
        df = yf.download(ticker, period=_DOWNLOAD_PERIOD, interval="1d", progress=False)
    except Exception as e:
        logger.debug("기술적 지표 다운로드 실패 (%s): %s", ticker, e)
        return None

    if df.empty or len(df) < 20:
        return None

    # MultiIndex 컬럼 처리 (단일 티커 download 결과)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    close = df["Close"].squeeze().dropna()
    high = df["High"].squeeze().dropna()
    low = df["Low"].squeeze().dropna()
    volume = df["Volume"].squeeze().dropna()

    if len(close) < 20:
        return None

    current_price = _safe_round(close.iloc[-1])

    result: dict[str, Any] = {
        "current_price": current_price,
    }

    # --- RSI (14일) ---
    result.update(_calc_rsi(close, period=14))

    # --- MACD (12, 26, 9) ---
    result.update(_calc_macd(close))

    # --- 볼린저 밴드 (20일, 2σ) ---
    result.update(_calc_bollinger(close, period=20))

    # --- 이동평균 (20, 50, 200일) ---
    result.update(_calc_moving_averages(close))

    # --- ATR (14일) ---
    result.update(_calc_atr(high, low, close, period=14))

    # --- 지지/저항선 ---
    result.update(_calc_support_resistance(high, low, close))

    # --- 거래량 비율 (20일 평균 대비) ---
    result.update(_calc_volume_ratio(volume))

    # --- 종합 MA 포지션 ---
    result["ma_position"] = _determine_ma_position(close)

    # --- 볼린저 포지션 ---
    result["bollinger_position"] = _determine_bollinger_position(close)

    # --- RSI 시그널 ---
    rsi = result.get("rsi_14")
    if rsi is not None:
        if rsi < 30:
            result["rsi_signal"] = "oversold"
        elif rsi > 70:
            result["rsi_signal"] = "overbought"
        else:
            result["rsi_signal"] = "neutral"

    return result


# ---------------------------------------------------------------------------
# RSI
# ---------------------------------------------------------------------------

def _calc_rsi(close: pd.Series, period: int = 14) -> dict[str, Any]:
    if len(close) < period + 1:
        return {"rsi_14": None, "rsi_signal": None}

    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    # Wilder's smoothing
    for i in range(period, len(avg_gain)):
        avg_gain.iloc[i] = (avg_gain.iloc[i - 1] * (period - 1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i - 1] * (period - 1) + loss.iloc[i]) / period

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    rsi_val = _safe_round(rsi.iloc[-1], 1)
    return {"rsi_14": rsi_val}


# ---------------------------------------------------------------------------
# MACD
# ---------------------------------------------------------------------------

def _calc_macd(close: pd.Series) -> dict[str, Any]:
    if len(close) < 35:
        return {"macd": None, "macd_signal_line": None, "macd_histogram": None, "macd_signal": None}

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    histogram = macd_line - signal_line

    macd_val = _safe_round(macd_line.iloc[-1], 3)
    signal_val = _safe_round(signal_line.iloc[-1], 3)
    hist_val = _safe_round(histogram.iloc[-1], 3)

    # MACD 시그널 판단
    macd_sig = "neutral"
    if len(histogram) >= 2:
        prev_hist = histogram.iloc[-2]
        curr_hist = histogram.iloc[-1]
        if prev_hist < 0 and curr_hist >= 0:
            macd_sig = "bullish_cross"
        elif prev_hist > 0 and curr_hist <= 0:
            macd_sig = "bearish_cross"
        elif curr_hist > 0:
            macd_sig = "bullish"
        elif curr_hist < 0:
            macd_sig = "bearish"

    return {
        "macd": macd_val,
        "macd_signal_line": signal_val,
        "macd_histogram": hist_val,
        "macd_signal": macd_sig,
    }


# ---------------------------------------------------------------------------
# 볼린저 밴드
# ---------------------------------------------------------------------------

def _calc_bollinger(close: pd.Series, period: int = 20, std_dev: float = 2.0) -> dict[str, Any]:
    if len(close) < period:
        return {"bb_upper": None, "bb_middle": None, "bb_lower": None, "bb_width": None}

    sma = close.rolling(window=period).mean()
    std = close.rolling(window=period).std()

    upper = sma + std_dev * std
    lower = sma - std_dev * std

    mid_val = _safe_round(sma.iloc[-1])
    upper_val = _safe_round(upper.iloc[-1])
    lower_val = _safe_round(lower.iloc[-1])

    width = None
    if mid_val and mid_val != 0:
        width = _safe_round((upper.iloc[-1] - lower.iloc[-1]) / sma.iloc[-1] * 100, 2)

    return {
        "bb_upper": upper_val,
        "bb_middle": mid_val,
        "bb_lower": lower_val,
        "bb_width": width,
    }


# ---------------------------------------------------------------------------
# 이동평균
# ---------------------------------------------------------------------------

def _calc_moving_averages(close: pd.Series) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for period in [20, 50, 200]:
        key = f"ma_{period}"
        if len(close) >= period:
            result[key] = _safe_round(close.rolling(window=period).mean().iloc[-1])
        else:
            result[key] = None
    return result


def _determine_ma_position(close: pd.Series) -> str:
    price = float(close.iloc[-1])
    ma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None
    ma50 = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else None

    if ma200 and price > ma200:
        return "above_200"
    if ma50 and price > ma50:
        return "above_50"
    if ma50 and price < ma50:
        return "below_50"
    if ma200 and price < ma200:
        return "below_200"
    return "unknown"


# ---------------------------------------------------------------------------
# ATR (Average True Range)
# ---------------------------------------------------------------------------

def _calc_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> dict[str, Any]:
    if len(close) < period + 1:
        return {"atr_14": None}

    # True Range
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.rolling(window=period).mean()
    return {"atr_14": _safe_round(atr.iloc[-1])}


# ---------------------------------------------------------------------------
# 지지/저항선
# ---------------------------------------------------------------------------

def _calc_support_resistance(high: pd.Series, low: pd.Series, close: pd.Series, lookback: int = 20) -> dict[str, Any]:
    if len(close) < lookback:
        return {"support": None, "resistance": None}

    recent_high = high.iloc[-lookback:]
    recent_low = low.iloc[-lookback:]

    resistance = _safe_round(recent_high.max())
    support = _safe_round(recent_low.min())

    return {"support": support, "resistance": resistance}


# ---------------------------------------------------------------------------
# 거래량 비율
# ---------------------------------------------------------------------------

def _calc_volume_ratio(volume: pd.Series, period: int = 20) -> dict[str, Any]:
    if len(volume) < period:
        return {"volume_ratio": None}

    avg_vol = float(volume.rolling(window=period).mean().iloc[-1])
    if avg_vol == 0 or not math.isfinite(avg_vol):
        return {"volume_ratio": None}

    current_vol = float(volume.iloc[-1])
    ratio = current_vol / avg_vol
    return {"volume_ratio": _safe_round(ratio, 2)}


# ---------------------------------------------------------------------------
# 볼린저 포지션
# ---------------------------------------------------------------------------

def _determine_bollinger_position(close: pd.Series, period: int = 20, std_dev: float = 2.0) -> str:
    if len(close) < period:
        return "unknown"

    price = float(close.iloc[-1])
    sma = float(close.rolling(window=period).mean().iloc[-1])
    std = float(close.rolling(window=period).std().iloc[-1])

    upper = sma + std_dev * std
    lower = sma - std_dev * std

    if price >= upper:
        return "upper"
    elif price <= lower:
        return "lower"
    else:
        return "middle"


# ---------------------------------------------------------------------------
# 손절라인 / 목표가 계산
# ---------------------------------------------------------------------------

def calc_stop_loss_and_targets(
    current_price: float,
    atr: float | None,
    support: float | None,
    resistance: float | None,
    direction: str = "BUY",
    atr_multiplier: float = 2.0,
) -> dict[str, Any]:
    """
    ATR 기반 동적 손절라인과 목표가를 계산한다.

    BUY: SL = 현재가 - ATR*배수, TP = 저항선 또는 SL 대칭
    SELL/SHORT: SL = 현재가 + ATR*배수, TP = 지지선 또는 SL 대칭
    """
    if not atr or atr <= 0:
        return {"stop_loss": None, "stop_loss_pct": None, "targets": [], "risk_reward_ratio": None}

    if direction in ("BUY",):
        sl = current_price - atr * atr_multiplier
        sl_pct = ((sl - current_price) / current_price) * 100

        risk = current_price - sl
        tp1 = current_price + risk * 1.5  # R:R = 1:1.5
        tp2 = current_price + risk * 2.5  # R:R = 1:2.5

        # 저항선이 있으면 TP1으로 활용
        if resistance and resistance > current_price:
            tp1 = resistance
            tp2 = current_price + (resistance - current_price) * 1.5

        targets = [
            {"label": "TP1", "price": _safe_round(tp1), "pct": _safe_round(((tp1 - current_price) / current_price) * 100, 1)},
            {"label": "TP2", "price": _safe_round(tp2), "pct": _safe_round(((tp2 - current_price) / current_price) * 100, 1)},
        ]
        rr = _safe_round((tp1 - current_price) / risk, 1) if risk > 0 else None

    else:  # SELL / SHORT
        sl = current_price + atr * atr_multiplier
        sl_pct = ((sl - current_price) / current_price) * 100

        risk = sl - current_price
        tp1 = current_price - risk * 1.5
        tp2 = current_price - risk * 2.5

        if support and support < current_price:
            tp1 = support
            tp2 = current_price - (current_price - support) * 1.5

        targets = [
            {"label": "TP1", "price": _safe_round(tp1), "pct": _safe_round(((tp1 - current_price) / current_price) * 100, 1)},
            {"label": "TP2", "price": _safe_round(tp2), "pct": _safe_round(((tp2 - current_price) / current_price) * 100, 1)},
        ]
        rr = _safe_round((current_price - tp1) / risk, 1) if risk > 0 else None

    return {
        "stop_loss": _safe_round(sl),
        "stop_loss_pct": _safe_round(sl_pct, 1),
        "targets": targets,
        "risk_reward_ratio": rr,
    }


# ---------------------------------------------------------------------------
# 배치: 여러 종목 한번에 계산
# ---------------------------------------------------------------------------

def compute_technicals_batch(tickers: list[str]) -> dict[str, dict[str, Any]]:
    """여러 종목의 기술적 지표를 계산한다. {ticker: technicals_dict}"""
    results: dict[str, dict[str, Any]] = {}
    for ticker in tickers:
        tech = compute_technicals(ticker)
        if tech:
            results[ticker] = tech
    return results
