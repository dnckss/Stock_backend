"""
백테스팅 — 대시보드 시그널(analysis_results) + AI 전략실 추천(strategy_history)의
과거 예측을 실제 주가 움직임과 대조해 예측률·평균수익률·샤프·MDD·equity curve를 계산한다.

평가 방식 (backward-looking):
  - 각 레코드의 entry_price(저장된 price / entry mid) 대비 N거래일 후 종가로 수익률 계산
  - 방향 조정: BUY → raw_return, SELL → -raw_return
  - horizon이 아직 미래인 레코드는 자동 제외

산출 지표:
  - hit_rate_pct, avg/median/best/worst, profit_factor
  - Sharpe ratio (일간 수익률 기준 연환산)
  - Max drawdown, equity curve (동등가중 누적)
  - 버킷별 분석: direction / divergence / signal_source / confidence / market_regime
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import statistics
import threading
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

import pandas as pd
import yfinance as yf

from config import (
    BACKTEST_ANNUALIZATION_FACTOR,
    BACKTEST_CACHE_TTL_SEC,
    BACKTEST_DEFAULT_HORIZONS,
    BACKTEST_DEFAULT_LOOKBACK_DAYS,
    BACKTEST_DIVERGENCE_BUCKETS,
    BACKTEST_LIVE_CACHE_TTL_SEC,
    BACKTEST_LIVE_POSITIONS_PER_HORIZON,
    BACKTEST_MAX_HORIZON_DAYS,
    BACKTEST_MAX_LOOKBACK_DAYS,
    BACKTEST_MIN_SAMPLES,
    BACKTEST_PRICE_CACHE_TTL_SEC,
    BACKTEST_PRICE_LOOKAHEAD_DAYS,
    BACKTEST_TRADES_DEFAULT_HORIZON,
    BACKTEST_TRADES_MAX_LEGS_PER_TRADE,
    BACKTEST_TRADES_MAX_TRADES,
)
from services.yf_limiter import throttled
from services.crud import (
    get_analysis_records_for_backtest,
    get_strategy_records_for_backtest,
    sanitize_for_json,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 공통 유틸
# ---------------------------------------------------------------------------

def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        # Supabase timestamptz → ISO 8601. Z/offset 둘 다 처리
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except (TypeError, ValueError):
        return None


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _round(v: float | None, n: int = 2) -> float | None:
    if v is None:
        return None
    if not math.isfinite(v):
        return None
    return round(v, n)


def _normalize_horizons(horizons: Iterable[int] | None) -> list[int]:
    """horizon 목록을 정규화: 양의 정수, 중복 제거, 최대값 제한, 오름차순."""
    if not horizons:
        return list(BACKTEST_DEFAULT_HORIZONS)
    out: set[int] = set()
    for h in horizons:
        try:
            iv = int(h)
        except (TypeError, ValueError):
            continue
        if iv <= 0 or iv > BACKTEST_MAX_HORIZON_DAYS:
            continue
        out.add(iv)
    return sorted(out) if out else list(BACKTEST_DEFAULT_HORIZONS)


# ---------------------------------------------------------------------------
# 가격 조회 (yfinance 일괄 다운로드)
# ---------------------------------------------------------------------------

# 가격 다운로드 캐시 — 같은 (ticker_set, start, end) 키로 30분 재사용.
# signals/strategist/trades 가 한 사이클에서 같은 가격 데이터를 중복 다운로드 하지 않게.
_price_cache: dict[tuple, tuple[float, pd.DataFrame]] = {}
_price_cache_lock = threading.Lock()


def _price_cache_key(tickers: list[str], start: date, end: date) -> tuple:
    return (
        tuple(sorted({(t or "").upper() for t in tickers if t})),
        start.isoformat(),
        end.isoformat(),
    )


def _price_cache_get(key: tuple) -> pd.DataFrame | None:
    with _price_cache_lock:
        entry = _price_cache.get(key)
        if not entry:
            return None
        ts, df = entry
        if time.time() - ts > BACKTEST_PRICE_CACHE_TTL_SEC:
            _price_cache.pop(key, None)
            return None
        return df


def _price_cache_put(key: tuple, df: pd.DataFrame) -> None:
    with _price_cache_lock:
        _price_cache[key] = (time.time(), df)


def _fetch_close_prices(
    tickers: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    """
    티커 목록의 종가 시계열을 한 번에 다운로드한다.
    반환: 컬럼=ticker, index=DatetimeIndex(거래일)인 DataFrame.
    실패/빈 결과면 빈 DataFrame.

    같은 (ticker_set, start, end) 호출은 BACKTEST_PRICE_CACHE_TTL_SEC 동안 메모리 재사용.
    실제 yf.download 호출은 yf_limiter throttled 로 글로벌 동시성·간격 제어.
    """
    if not tickers:
        return pd.DataFrame()

    cache_key = _price_cache_key(tickers, start, end)
    cached = _price_cache_get(cache_key)
    if cached is not None:
        logger.debug("backtest 가격 캐시 hit (%d tickers, start=%s)", len(tickers), start)
        return cached

    # yfinance 는 end 가 exclusive
    end_plus = end + timedelta(days=1)
    try:
        df = throttled(
            yf.download,
            tickers,
            start=start.isoformat(),
            end=end_plus.isoformat(),
            progress=False,
            auto_adjust=False,
            threads=True,
        )
    except Exception as e:
        logger.warning("백테스트 가격 다운로드 실패: %s", e)
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    # MultiIndex: 여러 티커 → df["Close"] 로 종가만 추출
    if isinstance(df.columns, pd.MultiIndex):
        if "Close" in df.columns.get_level_values(0):
            close = df["Close"].copy()
        else:
            return pd.DataFrame()
    else:
        # 단일 티커 → 컬럼 평면, Close 만 추출해 ticker 명으로 리네임
        if "Close" not in df.columns:
            return pd.DataFrame()
        close = df[["Close"]].rename(columns={"Close": tickers[0]})

    # 누락 티커 추가(KeyError 방지용 — None 채움)
    for t in tickers:
        if t not in close.columns:
            close[t] = float("nan")

    close = close.sort_index()
    _price_cache_put(cache_key, close)
    return close


def _exit_price_after(
    close_df: pd.DataFrame,
    ticker: str,
    entry_dt: datetime,
    horizon: int,
) -> tuple[float | None, date | None]:
    """entry_dt 이후 horizon번째 거래일의 종가. 미도달 시 (None, None)."""
    if ticker not in close_df.columns or close_df.empty:
        return None, None
    series = close_df[ticker].dropna()
    if series.empty:
        return None, None
    entry_day = entry_dt.date()
    # index 가 DatetimeIndex가 아닌 경우 안전 변환
    idx = series.index
    if not isinstance(idx, pd.DatetimeIndex):
        try:
            idx = pd.DatetimeIndex(idx)
        except Exception:
            return None, None
    # entry_day 초과 거래일만
    mask = idx.date > entry_day
    valid = series[mask]
    if len(valid) < horizon:
        return None, None
    exit_ts = valid.index[horizon - 1]
    exit_price = float(valid.iloc[horizon - 1])
    if not math.isfinite(exit_price):
        return None, None
    return exit_price, exit_ts.date()


# ---------------------------------------------------------------------------
# 메트릭 계산
# ---------------------------------------------------------------------------

def _compute_metrics(returns_pct: list[float]) -> dict[str, Any]:
    """수익률 리스트(% 단위)로 기본 통계를 계산."""
    n = len(returns_pct)
    if n == 0:
        return {
            "count": 0,
            "hit_rate_pct": None,
            "avg_return_pct": None,
            "median_return_pct": None,
            "std_pct": None,
            "best_pct": None,
            "worst_pct": None,
            "win_count": 0,
            "loss_count": 0,
            "profit_factor": None,
            "expectancy_pct": None,
        }
    wins = [r for r in returns_pct if r > 0]
    losses = [r for r in returns_pct if r < 0]
    win_sum = sum(wins)
    loss_sum_abs = abs(sum(losses))
    profit_factor: float | None = None
    if loss_sum_abs > 0:
        profit_factor = win_sum / loss_sum_abs
    elif win_sum > 0:
        profit_factor = None  # 손실 0 → 무한대라 JSON 호환 위해 None

    # expectancy: P(win)*avg(win) - P(loss)*avg(loss)
    p_win = len(wins) / n
    p_loss = len(losses) / n
    avg_win = statistics.fmean(wins) if wins else 0.0
    avg_loss = statistics.fmean(losses) if losses else 0.0
    expectancy = p_win * avg_win + p_loss * avg_loss

    return {
        "count": n,
        "hit_rate_pct": _round(p_win * 100, 2),
        "avg_return_pct": _round(statistics.fmean(returns_pct), 2),
        "median_return_pct": _round(statistics.median(returns_pct), 2),
        "std_pct": _round(statistics.pstdev(returns_pct) if n > 1 else 0.0, 2),
        "best_pct": _round(max(returns_pct), 2),
        "worst_pct": _round(min(returns_pct), 2),
        "win_count": len(wins),
        "loss_count": len(losses),
        "avg_win_pct": _round(avg_win, 2) if wins else None,
        "avg_loss_pct": _round(avg_loss, 2) if losses else None,
        "profit_factor": _round(profit_factor, 2) if profit_factor is not None else None,
        "expectancy_pct": _round(expectancy, 3),
    }


def _compute_equity_curve(
    dated_returns: list[tuple[date, float]],
) -> dict[str, Any]:
    """
    exit 날짜별로 동등가중 일간 수익률을 집계하고 복리 누적 → equity curve.
    Sharpe(연환산)와 Max Drawdown 산출.
    """
    if not dated_returns:
        return {
            "curve": [],
            "final_equity": 1.0,
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "sharpe_ratio": None,
            "days": 0,
        }

    daily_bucket: dict[date, list[float]] = defaultdict(list)
    for d, ret_pct in dated_returns:
        daily_bucket[d].append(ret_pct / 100.0)

    sorted_days = sorted(daily_bucket.keys())
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    curve: list[dict[str, Any]] = []
    daily_returns: list[float] = []

    for d in sorted_days:
        day_ret = statistics.fmean(daily_bucket[d])  # 동일 exit 일 평균
        daily_returns.append(day_ret)
        equity *= 1.0 + day_ret
        if equity > peak:
            peak = equity
        if peak > 0:
            dd = (equity - peak) / peak  # 음수
            if dd < max_dd:
                max_dd = dd
        curve.append({
            "date": d.isoformat(),
            "equity": _round(equity, 4),
            "return_pct": _round(day_ret * 100, 2),
        })

    # Sharpe (rf=0, 일간 수익률 기준 연환산)
    sharpe: float | None = None
    if len(daily_returns) > 1:
        mean_r = statistics.fmean(daily_returns)
        std_r = statistics.pstdev(daily_returns)
        if std_r > 0:
            sharpe = (mean_r / std_r) * math.sqrt(BACKTEST_ANNUALIZATION_FACTOR)

    return {
        "curve": curve,
        "final_equity": _round(equity, 4),
        "total_return_pct": _round((equity - 1) * 100, 2),
        "max_drawdown_pct": _round(max_dd * 100, 2),
        "sharpe_ratio": _round(sharpe, 2) if sharpe is not None else None,
        "days": len(sorted_days),
    }


# ---------------------------------------------------------------------------
# 레코드 → 수익률 변환 공통 로직
# ---------------------------------------------------------------------------

class _Evaluation:
    """레코드 1건 + horizon 1개의 평가 결과."""
    __slots__ = (
        "record", "direction", "entry_price", "exit_price",
        "raw_return_pct", "adjusted_return_pct",
        "entry_date", "exit_date", "horizon",
    )

    def __init__(self, record, direction, entry_price, exit_price,
                 raw_return_pct, adjusted_return_pct,
                 entry_date, exit_date, horizon):
        self.record = record
        self.direction = direction
        self.entry_price = entry_price
        self.exit_price = exit_price
        self.raw_return_pct = raw_return_pct
        self.adjusted_return_pct = adjusted_return_pct
        self.entry_date = entry_date
        self.exit_date = exit_date
        self.horizon = horizon


def _evaluate_records(
    records: list[dict],
    close_df: pd.DataFrame,
    horizon: int,
    *,
    price_key: str = "price",
    direction_key: str = "signal",
) -> list[_Evaluation]:
    """
    records 각각에 대해 horizon 후 exit price로 방향 조정 수익률 계산.
    price_key: entry price가 들어있는 필드 ('price' | 별도 entry mid).
    direction_key: BUY/SELL 방향이 들어있는 필드 ('signal' | 'direction').
    """
    out: list[_Evaluation] = []
    for r in records:
        ticker = (r.get("ticker") or "").upper()
        entry_price = _safe_float(r.get(price_key))
        entry_dt = _parse_iso(r.get("created_at"))
        direction_raw = (r.get(direction_key) or "").upper().strip()
        if not ticker or entry_price is None or entry_price <= 0 or entry_dt is None:
            continue
        if direction_raw not in ("BUY", "SELL"):
            # HOLD / WAIT / 기타는 평가 대상 아님
            continue

        exit_price, exit_date = _exit_price_after(close_df, ticker, entry_dt, horizon)
        if exit_price is None or exit_date is None:
            continue

        raw_return = (exit_price - entry_price) / entry_price * 100.0
        adjusted = raw_return if direction_raw == "BUY" else -raw_return
        out.append(_Evaluation(
            record=r,
            direction=direction_raw,
            entry_price=entry_price,
            exit_price=exit_price,
            raw_return_pct=raw_return,
            adjusted_return_pct=adjusted,
            entry_date=entry_dt.date(),
            exit_date=exit_date,
            horizon=horizon,
        ))
    return out


def _download_prices_for(records: list[dict]) -> pd.DataFrame:
    """records에서 티커와 날짜 범위 추출 후 yfinance 일괄 다운로드."""
    if not records:
        return pd.DataFrame()
    tickers: set[str] = set()
    earliest: datetime | None = None
    for r in records:
        t = (r.get("ticker") or "").upper().strip()
        if t:
            tickers.add(t)
        dt = _parse_iso(r.get("created_at"))
        if dt and (earliest is None or dt < earliest):
            earliest = dt
    if not tickers or earliest is None:
        return pd.DataFrame()

    start = earliest.date() - timedelta(days=2)  # 여유
    end = datetime.now(timezone.utc).date() + timedelta(days=BACKTEST_PRICE_LOOKAHEAD_DAYS)
    return _fetch_close_prices(sorted(tickers), start, end)


# ---------------------------------------------------------------------------
# 버킷 집계
# ---------------------------------------------------------------------------

def _bucket_metrics(
    evals: list[_Evaluation],
    key_fn,
    *,
    min_samples: int = BACKTEST_MIN_SAMPLES,
) -> dict[str, dict[str, Any]]:
    """evals 를 key_fn 그룹핑해 각 그룹의 메트릭 계산."""
    groups: dict[str, list[float]] = defaultdict(list)
    for e in evals:
        key = key_fn(e)
        if key is None:
            continue
        groups[str(key)].append(e.adjusted_return_pct)
    return {
        k: _compute_metrics(v)
        for k, v in groups.items()
        if len(v) >= min_samples
    }


def _divergence_buckets(evals: list[_Evaluation]) -> list[dict[str, Any]]:
    """|divergence| 구간별 메트릭 (BACKTEST_DIVERGENCE_BUCKETS 정의 사용)."""
    out: list[dict[str, Any]] = []
    for low, high in BACKTEST_DIVERGENCE_BUCKETS:
        returns = [
            e.adjusted_return_pct for e in evals
            if e.record.get("divergence") is not None
            and low <= abs(_safe_float(e.record.get("divergence")) or 0.0) < high
        ]
        label = (
            f"{int(low * 100)}-{int(high * 100)}%"
            if math.isfinite(high)
            else f"{int(low * 100)}%+"
        )
        out.append({
            "label": label,
            "min": low,
            "max": None if not math.isfinite(high) else high,
            "metrics": _compute_metrics(returns) if len(returns) >= BACKTEST_MIN_SAMPLES else _compute_metrics([]),
        })
    return out


def _per_ticker_top(
    evals: list[_Evaluation],
    *,
    top_n: int = 10,
    min_samples: int = 3,
) -> list[dict[str, Any]]:
    """티커별 평균 조정수익률 순위."""
    groups: dict[str, list[float]] = defaultdict(list)
    for e in evals:
        groups[(e.record.get("ticker") or "").upper()].append(e.adjusted_return_pct)
    ranked: list[dict[str, Any]] = []
    for ticker, rets in groups.items():
        if not ticker or len(rets) < min_samples:
            continue
        metrics = _compute_metrics(rets)
        ranked.append({"ticker": ticker, **metrics})
    ranked.sort(key=lambda x: x.get("avg_return_pct") or -999, reverse=True)
    return ranked[:top_n]


# ---------------------------------------------------------------------------
# 메인 로직 — Signals BT (analysis_results)
# ---------------------------------------------------------------------------

def _run_signals_backtest_sync(
    lookback_days: int,
    horizons: list[int],
) -> dict[str, Any]:
    started = time.time()
    records = get_analysis_records_for_backtest(lookback_days)
    if not records:
        return _empty_result(
            source="analysis_results",
            lookback_days=lookback_days,
            horizons=horizons,
            message="평가할 시그널 레코드가 없습니다.",
        )

    close_df = _download_prices_for(records)
    results_by_horizon: dict[str, Any] = {}

    for h in horizons:
        evals = _evaluate_records(records, close_df, h, price_key="price", direction_key="signal")
        if not evals:
            results_by_horizon[str(h)] = _empty_horizon_block(h)
            continue

        returns = [e.adjusted_return_pct for e in evals]
        results_by_horizon[str(h)] = {
            "horizon": h,
            "overall": _compute_metrics(returns),
            "by_direction": _bucket_metrics(evals, lambda e: e.direction, min_samples=1),
            "by_divergence": _divergence_buckets(evals),
            "by_source": _bucket_metrics(
                evals,
                lambda e: e.record.get("signal_source"),
            ),
            "top_tickers": _per_ticker_top(evals),
            "equity": _compute_equity_curve([(e.exit_date, e.adjusted_return_pct) for e in evals]),
        }

    total_evaluated = sum(
        (results_by_horizon[str(h)].get("overall") or {}).get("count", 0)
        for h in horizons
    )
    return {
        "source": "analysis_results",
        "lookback_days": lookback_days,
        "horizons": horizons,
        "total_records": len(records),
        "total_evaluations": total_evaluated,
        "tickers_count": len({(r.get("ticker") or "").upper() for r in records if r.get("ticker")}),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_sec": round(time.time() - started, 2),
        "results": results_by_horizon,
    }


# ---------------------------------------------------------------------------
# 메인 로직 — Strategist BT (strategy_history)
# ---------------------------------------------------------------------------

def _enrich_strategy_entry(row: dict) -> dict:
    """entry = (entry_low + entry_high) / 2 를 'price' 필드로 주입."""
    lo = _safe_float(row.get("entry_low"))
    hi = _safe_float(row.get("entry_high"))
    if lo is not None and hi is not None and lo > 0 and hi > 0:
        row["price"] = (lo + hi) / 2.0
    elif lo is not None and lo > 0:
        row["price"] = lo
    elif hi is not None and hi > 0:
        row["price"] = hi
    else:
        row["price"] = None
    return row


def _run_strategist_backtest_sync(
    lookback_days: int,
    horizons: list[int],
) -> dict[str, Any]:
    started = time.time()
    raw_records = get_strategy_records_for_backtest(lookback_days)
    if not raw_records:
        return _empty_result(
            source="strategy_history",
            lookback_days=lookback_days,
            horizons=horizons,
            message="평가할 AI 추천 레코드가 없습니다.",
        )
    records = [_enrich_strategy_entry(dict(r)) for r in raw_records]

    close_df = _download_prices_for(records)
    results_by_horizon: dict[str, Any] = {}

    for h in horizons:
        evals = _evaluate_records(records, close_df, h, price_key="price", direction_key="direction")
        if not evals:
            results_by_horizon[str(h)] = _empty_horizon_block(h)
            continue

        returns = [e.adjusted_return_pct for e in evals]
        results_by_horizon[str(h)] = {
            "horizon": h,
            "overall": _compute_metrics(returns),
            "by_direction": _bucket_metrics(evals, lambda e: e.direction, min_samples=1),
            "by_confidence": _bucket_metrics(
                evals,
                lambda e: (e.record.get("confidence") or "").lower() or None,
            ),
            "by_market_regime": _bucket_metrics(
                evals,
                lambda e: e.record.get("market_regime"),
            ),
            "by_strategy_type": _bucket_metrics(
                evals,
                lambda e: e.record.get("strategy_type"),
            ),
            "top_tickers": _per_ticker_top(evals),
            "equity": _compute_equity_curve([(e.exit_date, e.adjusted_return_pct) for e in evals]),
        }

    total_evaluated = sum(
        (results_by_horizon[str(h)].get("overall") or {}).get("count", 0)
        for h in horizons
    )
    return {
        "source": "strategy_history",
        "lookback_days": lookback_days,
        "horizons": horizons,
        "total_records": len(records),
        "total_evaluations": total_evaluated,
        "tickers_count": len({(r.get("ticker") or "").upper() for r in records if r.get("ticker")}),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_sec": round(time.time() - started, 2),
        "results": results_by_horizon,
    }


def _empty_result(*, source, lookback_days, horizons, message) -> dict[str, Any]:
    return {
        "source": source,
        "lookback_days": lookback_days,
        "horizons": horizons,
        "total_records": 0,
        "total_evaluations": 0,
        "tickers_count": 0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "message": message,
        "results": {str(h): _empty_horizon_block(h) for h in horizons},
    }


def _empty_horizon_block(h: int) -> dict[str, Any]:
    return {
        "horizon": h,
        "overall": _compute_metrics([]),
        "by_direction": {},
        "by_divergence": [],
        "by_source": {},
        "by_confidence": {},
        "by_market_regime": {},
        "by_strategy_type": {},
        "top_tickers": [],
        "equity": _compute_equity_curve([]),
    }


# ---------------------------------------------------------------------------
# 캐시 + Public API
# ---------------------------------------------------------------------------

_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def _cache_key(kind: str, lookback_days: int, horizons: list[int]) -> str:
    raw = f"{kind}|{lookback_days}|{','.join(str(h) for h in horizons)}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _cache_get(key: str) -> dict[str, Any] | None:
    entry = _cache.get(key)
    if not entry:
        return None
    ts, data = entry
    if time.time() - ts > BACKTEST_CACHE_TTL_SEC:
        _cache.pop(key, None)
        return None
    return data


def _cache_put(key: str, data: dict[str, Any]) -> None:
    _cache[key] = (time.time(), data)


def _sanitize_lookback(days: int | None) -> int:
    if not days or days <= 0:
        return BACKTEST_DEFAULT_LOOKBACK_DAYS
    return min(int(days), BACKTEST_MAX_LOOKBACK_DAYS)


async def run_signals_backtest(
    lookback_days: int | None = None,
    horizons: Iterable[int] | None = None,
    *,
    refresh: bool = False,
) -> dict[str, Any]:
    """대시보드 시그널(analysis_results) 백테스트."""
    lb = _sanitize_lookback(lookback_days)
    hs = _normalize_horizons(horizons)
    key = _cache_key("signals", lb, hs)
    if not refresh:
        cached = _cache_get(key)
        if cached is not None:
            return cached

    result = await asyncio.to_thread(_run_signals_backtest_sync, lb, hs)
    sanitized = sanitize_for_json(result)
    _cache_put(key, sanitized)
    return sanitized


async def run_strategist_backtest(
    lookback_days: int | None = None,
    horizons: Iterable[int] | None = None,
    *,
    refresh: bool = False,
) -> dict[str, Any]:
    """AI 전략실 추천(strategy_history) 백테스트."""
    lb = _sanitize_lookback(lookback_days)
    hs = _normalize_horizons(horizons)
    key = _cache_key("strategist", lb, hs)
    if not refresh:
        cached = _cache_get(key)
        if cached is not None:
            return cached

    result = await asyncio.to_thread(_run_strategist_backtest_sync, lb, hs)
    sanitized = sanitize_for_json(result)
    _cache_put(key, sanitized)
    return sanitized


def _pick_headline(result: dict[str, Any], horizon: int) -> dict[str, Any]:
    """특정 horizon의 핵심 지표만 뽑아 요약용 headline 생성."""
    block = (result.get("results") or {}).get(str(horizon)) or {}
    overall = block.get("overall") or {}
    equity = block.get("equity") or {}
    return {
        "horizon": horizon,
        "hit_rate_pct": overall.get("hit_rate_pct"),
        "avg_return_pct": overall.get("avg_return_pct"),
        "sample_count": overall.get("count"),
        "profit_factor": overall.get("profit_factor"),
        "sharpe_ratio": equity.get("sharpe_ratio"),
        "max_drawdown_pct": equity.get("max_drawdown_pct"),
        "total_return_pct": equity.get("total_return_pct"),
    }


async def run_summary(
    lookback_days: int | None = None,
    horizons: Iterable[int] | None = None,
    *,
    refresh: bool = False,
) -> dict[str, Any]:
    """대시보드 + AI 전략실 백테스트를 한 번에 실행하고 headline 요약."""
    lb = _sanitize_lookback(lookback_days)
    hs = _normalize_horizons(horizons)

    # 순차 실행: 두 작업 모두 Supabase sync client(httpx)를 공유하므로
    # 병렬(gather)로 띄우면 httpx 커넥션 풀에서 경합해 RemoteProtocolError를 유발할 수 있다.
    # 각각 결과는 10분 캐시되므로 두 번째 호출부터는 즉시 반환된다.
    signals = await run_signals_backtest(lb, hs, refresh=refresh)
    strat = await run_strategist_backtest(lb, hs, refresh=refresh)

    return sanitize_for_json({
        "lookback_days": lb,
        "horizons": hs,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "signals": {
            "total_records": signals.get("total_records", 0),
            "total_evaluations": signals.get("total_evaluations", 0),
            "tickers_count": signals.get("tickers_count", 0),
            "headlines": [_pick_headline(signals, h) for h in hs],
        },
        "strategist": {
            "total_records": strat.get("total_records", 0),
            "total_evaluations": strat.get("total_evaluations", 0),
            "tickers_count": strat.get("tickers_count", 0),
            "headlines": [_pick_headline(strat, h) for h in hs],
        },
    })


# ---------------------------------------------------------------------------
# Live (진행 중) 포지션 — horizon 미달 레코드의 현재가 mark-to-market
# ---------------------------------------------------------------------------

def _latest_close(close_df: pd.DataFrame, ticker: str) -> tuple[float | None, date | None]:
    """티커의 가장 최근 유효 종가와 날짜."""
    if ticker not in close_df.columns or close_df.empty:
        return None, None
    series = close_df[ticker].dropna()
    if series.empty:
        return None, None
    last_val = float(series.iloc[-1])
    if not math.isfinite(last_val):
        return None, None
    last_idx = series.index[-1]
    if hasattr(last_idx, "date"):
        return last_val, last_idx.date()
    return last_val, None


def _elapsed_trading_days(
    close_df: pd.DataFrame,
    ticker: str,
    entry_dt: datetime,
) -> int:
    """entry_dt 이후 지금까지 경과한 거래일 수."""
    if ticker not in close_df.columns or close_df.empty:
        return 0
    series = close_df[ticker].dropna()
    if series.empty:
        return 0
    idx = series.index
    if not isinstance(idx, pd.DatetimeIndex):
        try:
            idx = pd.DatetimeIndex(idx)
        except Exception:
            return 0
    return int((idx.date > entry_dt.date()).sum())


def _compute_live_positions(
    records: list[dict],
    close_df: pd.DataFrame,
    horizons: list[int],
    *,
    direction_key: str,
    price_key: str,
    extra_fields: list[str],
) -> dict[str, dict[str, Any]]:
    """
    horizon별로 '아직 완료되지 않은(open)' 포지션을 수집해 현재가 대비 mark-to-market.
    동일 레코드가 여러 horizon에 걸쳐 open일 수 있으므로 각 horizon 블록에 개별 추가.
    """
    per_horizon: dict[int, list[dict[str, Any]]] = {h: [] for h in horizons}

    for r in records:
        ticker = (r.get("ticker") or "").upper().strip()
        entry_price = _safe_float(r.get(price_key))
        entry_dt = _parse_iso(r.get("created_at"))
        direction = (r.get(direction_key) or "").upper().strip()
        if not ticker or entry_price is None or entry_price <= 0 or entry_dt is None:
            continue
        if direction not in ("BUY", "SELL"):
            continue

        current_price, current_date = _latest_close(close_df, ticker)
        if current_price is None:
            continue
        elapsed = _elapsed_trading_days(close_df, ticker, entry_dt)

        raw_return = (current_price - entry_price) / entry_price * 100.0
        adjusted = raw_return if direction == "BUY" else -raw_return

        base_position = {
            "ticker": ticker,
            "direction": direction,
            "entry_date": entry_dt.date().isoformat(),
            "entry_price": _round(entry_price, 4),
            "current_price": _round(current_price, 4),
            "current_date": current_date.isoformat() if current_date else None,
            "unrealized_raw_pct": _round(raw_return, 2),
            "unrealized_adjusted_pct": _round(adjusted, 2),
            "elapsed_trading_days": elapsed,
            **{k: r.get(k) for k in extra_fields if r.get(k) is not None},
        }

        for h in horizons:
            if elapsed >= h:
                # 이 horizon은 이미 완료 — open 아님
                continue
            pos = {
                **base_position,
                "horizon": h,
                "remaining_trading_days": max(0, h - elapsed),
                "progress_pct": _round(elapsed / h * 100, 1) if h > 0 else None,
            }
            per_horizon[h].append(pos)

    # 진행 최신 순(엔트리 최근 순) 정렬 + 개수 상한
    results: dict[str, dict[str, Any]] = {}
    for h in horizons:
        positions = per_horizon[h]
        positions.sort(key=lambda p: p.get("entry_date") or "", reverse=True)
        capped = positions[:BACKTEST_LIVE_POSITIONS_PER_HORIZON]
        returns = [p["unrealized_adjusted_pct"] for p in positions if p.get("unrealized_adjusted_pct") is not None]
        overall = _compute_metrics(returns)
        # hit_rate 키 이름을 문맥에 맞게 바꿔 복사
        overall_open = {
            **overall,
            "hit_rate_so_far_pct": overall.get("hit_rate_pct"),
            "avg_unrealized_pct": overall.get("avg_return_pct"),
        }
        results[str(h)] = {
            "horizon": h,
            "open_count": len(positions),
            "returned_count": len(capped),
            "overall": overall_open,
            "by_direction": {
                d: _compute_metrics([
                    p["unrealized_adjusted_pct"] for p in positions
                    if p.get("direction") == d and p.get("unrealized_adjusted_pct") is not None
                ])
                for d in ("BUY", "SELL")
            },
            "positions": capped,
        }
    return results


def _run_live_sync(lookback_days: int, horizons: list[int]) -> dict[str, Any]:
    started = time.time()

    # 두 소스 모두 최근 lookback 기간치만 조회. horizon 이상 지난 레코드는 어차피 스킵됨.
    signals_records = get_analysis_records_for_backtest(lookback_days)
    strategy_rows = get_strategy_records_for_backtest(lookback_days)
    strategy_records = [_enrich_strategy_entry(dict(r)) for r in strategy_rows]

    # 두 소스 티커 합쳐서 단일 가격 다운로드
    merged_for_prices: list[dict] = []
    merged_for_prices.extend(signals_records)
    merged_for_prices.extend(strategy_records)
    close_df = _download_prices_for(merged_for_prices)

    signals_live = _compute_live_positions(
        signals_records, close_df, horizons,
        direction_key="signal", price_key="price",
        extra_fields=["signal_source", "divergence", "sentiment"],
    )
    strategist_live = _compute_live_positions(
        strategy_records, close_df, horizons,
        direction_key="direction", price_key="price",
        extra_fields=["confidence", "strategy_type", "market_regime",
                      "stop_loss", "target1_price", "target2_price", "risk_reward_ratio"],
    )

    return {
        "lookback_days": lookback_days,
        "horizons": horizons,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_sec": round(time.time() - started, 2),
        "signals_live": {
            "source": "analysis_results",
            "total_open": sum(v["open_count"] for v in signals_live.values()),
            "results": signals_live,
        },
        "strategist_live": {
            "source": "strategy_history",
            "total_open": sum(v["open_count"] for v in strategist_live.values()),
            "results": strategist_live,
        },
    }


# 라이브 뷰는 별도 캐시(짧은 TTL) 사용
_live_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def _live_cache_get(key: str) -> dict[str, Any] | None:
    entry = _live_cache.get(key)
    if not entry:
        return None
    ts, data = entry
    if time.time() - ts > BACKTEST_LIVE_CACHE_TTL_SEC:
        _live_cache.pop(key, None)
        return None
    return data


def _live_cache_put(key: str, data: dict[str, Any]) -> None:
    _live_cache[key] = (time.time(), data)


async def run_live_positions(
    lookback_days: int | None = None,
    horizons: Iterable[int] | None = None,
    *,
    refresh: bool = False,
) -> dict[str, Any]:
    """
    현재 진행 중(open) 포지션 — horizon 미달 레코드의 현재가 mark-to-market.

    signals(대시보드) + strategist(AI 전략실) 두 소스를 한 번에 반환한다.
    캐시 TTL은 BACKTEST_LIVE_CACHE_TTL_SEC(기본 1분).
    """
    hs = _normalize_horizons(horizons)
    # 기본 lookback: 최대 horizon * 2 (오래된 건 어차피 elapsed >= h 로 자동 제외)
    default_lb = max(hs) * 2 if hs else BACKTEST_DEFAULT_LOOKBACK_DAYS
    lb = _sanitize_lookback(lookback_days or default_lb)

    key = _cache_key("live", lb, hs)
    if not refresh:
        cached = _live_cache_get(key)
        if cached is not None:
            return cached

    result = await asyncio.to_thread(_run_live_sync, lb, hs)
    sanitized = sanitize_for_json(result)
    _live_cache_put(key, sanitized)
    return sanitized


# ---------------------------------------------------------------------------
# Trade history (진입→청산 단위 거래 리스트)
# ---------------------------------------------------------------------------
# 같은 시점에 들어온 추천/시그널을 하나의 "포트폴리오 진입(trade)"으로 묶고,
# horizon 거래일 후의 종가로 청산했다고 가정해 종목별·전체 수익률을 계산한다.
# horizon 미달 trade는 status="open" + 현재가 mark-to-market 으로 보여준다.

_TRADE_SOURCES = ("strategist", "signals")


def _minute_key(dt: datetime) -> str:
    """created_at 을 분 단위로 정규화 — 같은 batch insert 를 한 그룹으로."""
    return dt.replace(second=0, microsecond=0).isoformat()


def _build_trade_legs(
    group_records: list[dict],
    close_df: pd.DataFrame,
    horizon: int,
    *,
    direction_key: str,
    price_key: str,
    leg_extra_fields: list[str],
    max_legs: int,
) -> list[dict[str, Any]]:
    """그룹 내 각 종목을 포지션 leg로 변환. 평가 불가 종목은 제외."""
    legs: list[dict[str, Any]] = []
    for r in group_records:
        if len(legs) >= max_legs:
            break
        ticker = (r.get("ticker") or "").upper().strip()
        direction = (r.get(direction_key) or "").upper().strip()
        entry_price = _safe_float(r.get(price_key))
        entry_dt = _parse_iso(r.get("created_at"))
        if not ticker or direction not in ("BUY", "SELL"):
            continue
        if entry_price is None or entry_price <= 0 or entry_dt is None:
            continue

        # 1) 우선 horizon 만족하는 exit 시도(=closed)
        exit_price, exit_date = _exit_price_after(close_df, ticker, entry_dt, horizon)
        leg_status = "closed" if exit_price is not None else "open"
        # 2) 미만족이면 최신 종가로 mark-to-market(=open)
        if exit_price is None:
            exit_price, exit_date = _latest_close(close_df, ticker)
            if exit_price is None:
                continue

        raw_return = (exit_price - entry_price) / entry_price * 100.0
        adjusted = raw_return if direction == "BUY" else -raw_return

        leg: dict[str, Any] = {
            "ticker": ticker,
            "direction": direction,
            "entry_price": _round(entry_price, 4),
            "exit_price": _round(exit_price, 4),
            "exit_date": exit_date.isoformat() if exit_date else None,
            "exit_status": leg_status,
            "raw_return_pct": _round(raw_return, 2),
            "return_pct": _round(adjusted, 2),
        }
        for k in leg_extra_fields:
            v = r.get(k)
            if v is not None:
                leg[k] = v
        legs.append(leg)
    return legs


def _build_trade_from_group(
    group_key: str,
    group_records: list[dict],
    close_df: pd.DataFrame,
    horizon: int,
    *,
    source_label: str,
    direction_key: str,
    price_key: str,
    leg_extra_fields: list[str],
    portfolio_extra_fields: list[str],
    max_legs: int,
) -> dict[str, Any] | None:
    if not group_records:
        return None

    legs = _build_trade_legs(
        group_records, close_df, horizon,
        direction_key=direction_key, price_key=price_key,
        leg_extra_fields=leg_extra_fields, max_legs=max_legs,
    )
    if not legs:
        return None

    weight = round(100.0 / len(legs), 2)
    for leg in legs:
        leg["weight_pct"] = weight

    entry_dt = _parse_iso(group_records[0].get("created_at"))
    portfolio_return = statistics.fmean(l["return_pct"] for l in legs)
    winners = sum(1 for l in legs if l["return_pct"] > 0)
    losers = sum(1 for l in legs if l["return_pct"] < 0)

    # trade-level closed 여부: 모든 leg가 closed면 trade도 closed
    all_closed = all(l["exit_status"] == "closed" for l in legs)
    status = "closed" if all_closed else "open"

    # exit_date: closed면 leg들의 마지막 exit_date(보통 동일), open이면 진행 중
    closed_legs = [l for l in legs if l["exit_status"] == "closed"]
    if closed_legs:
        exit_date_iso = max(l["exit_date"] for l in closed_legs if l.get("exit_date"))
    else:
        exit_date_iso = None

    elapsed_max = 0
    for l in legs:
        e = _elapsed_trading_days(close_df, l["ticker"], entry_dt) if entry_dt else 0
        if e > elapsed_max:
            elapsed_max = e

    trade: dict[str, Any] = {
        "trade_id": f"{source_label}-{group_key}",
        "source": source_label,
        "entry_at": entry_dt.isoformat() if entry_dt else None,
        "entry_date": entry_dt.date().isoformat() if entry_dt else None,
        "exit_date": exit_date_iso,
        "status": status,
        "horizon_trading_days": horizon,
        "elapsed_trading_days": elapsed_max,
        "remaining_trading_days": max(0, horizon - elapsed_max) if not all_closed else 0,
        "portfolio_return_pct": _round(portfolio_return, 2),
        "winners_count": winners,
        "losers_count": losers,
        "legs_count": len(legs),
        "legs": legs,
    }

    # 포트폴리오 메타 — 그룹 내 첫 레코드에서 가져오기 (같은 trade라 동일하다고 가정)
    first = group_records[0]
    for k in portfolio_extra_fields:
        v = first.get(k)
        if v is not None:
            trade[k] = v
    return trade


def _summarize_trades(trades: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [t for t in trades if t["status"] == "closed"]
    open_ = [t for t in trades if t["status"] == "open"]

    win_rate = None
    avg_ret = None
    median_ret = None
    total_return = None
    best = None
    worst = None
    if closed:
        rets = [t["portfolio_return_pct"] for t in closed if t.get("portfolio_return_pct") is not None]
        if rets:
            wins = [r for r in rets if r > 0]
            win_rate = _round(len(wins) / len(rets) * 100, 2)
            avg_ret = _round(statistics.fmean(rets), 2)
            median_ret = _round(statistics.median(rets), 2)
            # 누적: 동등가중 시계열로 단순 곱
            cum = 1.0
            for r in rets:
                cum *= 1.0 + r / 100.0
            total_return = _round((cum - 1.0) * 100, 2)
            best_idx = max(range(len(closed)), key=lambda i: closed[i].get("portfolio_return_pct") or -1e9)
            worst_idx = min(range(len(closed)), key=lambda i: closed[i].get("portfolio_return_pct") or 1e9)
            best = {
                "trade_id": closed[best_idx]["trade_id"],
                "entry_date": closed[best_idx].get("entry_date"),
                "exit_date": closed[best_idx].get("exit_date"),
                "portfolio_return_pct": closed[best_idx].get("portfolio_return_pct"),
            }
            worst = {
                "trade_id": closed[worst_idx]["trade_id"],
                "entry_date": closed[worst_idx].get("entry_date"),
                "exit_date": closed[worst_idx].get("exit_date"),
                "portfolio_return_pct": closed[worst_idx].get("portfolio_return_pct"),
            }

    open_returns = [t["portfolio_return_pct"] for t in open_ if t.get("portfolio_return_pct") is not None]
    open_avg = _round(statistics.fmean(open_returns), 2) if open_returns else None

    return {
        "total_trades": len(trades),
        "closed_trades": len(closed),
        "open_trades": len(open_),
        "win_rate_pct": win_rate,
        "avg_return_pct": avg_ret,
        "median_return_pct": median_ret,
        "total_return_pct": total_return,
        "open_avg_unrealized_pct": open_avg,
        "best_trade": best,
        "worst_trade": worst,
    }


def _run_trade_history_sync(
    source: str,
    horizon: int,
    lookback_days: int,
    *,
    include_open: bool,
) -> dict[str, Any]:
    started = time.time()

    if source == "strategist":
        raw_records = get_strategy_records_for_backtest(lookback_days)
        records = [_enrich_strategy_entry(dict(r)) for r in raw_records]
        direction_key = "direction"
        price_key = "price"
        leg_extra = ["confidence", "rationale", "stop_loss", "stop_loss_pct",
                     "target1_price", "target2_price", "risk_reward_ratio",
                     "strategy_type"]
        portfolio_extra = ["market_regime"]
    else:  # signals
        records = get_analysis_records_for_backtest(lookback_days)
        direction_key = "signal"
        price_key = "price"
        leg_extra = ["signal_source", "divergence", "sentiment"]
        portfolio_extra: list[str] = []

    if not records:
        return {
            "source": source,
            "horizon_days": horizon,
            "lookback_days": lookback_days,
            "include_open": include_open,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "elapsed_sec": round(time.time() - started, 2),
            "summary": _summarize_trades([]),
            "trades": [],
            "message": "평가할 레코드가 없습니다.",
        }

    close_df = _download_prices_for(records)

    # 분 단위로 그룹핑 (같은 batch insert 묶음 = 한 trade)
    groups: dict[str, list[dict]] = {}
    for r in records:
        dt = _parse_iso(r.get("created_at"))
        if dt is None:
            continue
        key = _minute_key(dt)
        groups.setdefault(key, []).append(r)

    trades: list[dict[str, Any]] = []
    for key, group_records in groups.items():
        trade = _build_trade_from_group(
            key, group_records, close_df, horizon,
            source_label=source,
            direction_key=direction_key, price_key=price_key,
            leg_extra_fields=leg_extra,
            portfolio_extra_fields=portfolio_extra,
            max_legs=BACKTEST_TRADES_MAX_LEGS_PER_TRADE,
        )
        if trade is None:
            continue
        if not include_open and trade["status"] == "open":
            continue
        trades.append(trade)

    # 최신 진입 순 정렬 후 상한 적용
    trades.sort(key=lambda t: t.get("entry_at") or "", reverse=True)
    trades = trades[:BACKTEST_TRADES_MAX_TRADES]

    return {
        "source": source,
        "horizon_days": horizon,
        "lookback_days": lookback_days,
        "include_open": include_open,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_sec": round(time.time() - started, 2),
        "summary": _summarize_trades(trades),
        "trades": trades,
    }


async def run_trade_history(
    source: str,
    horizon: int | None = None,
    lookback_days: int | None = None,
    *,
    include_open: bool = True,
    refresh: bool = False,
) -> dict[str, Any]:
    """
    진입(포트폴리오) → 청산 단위로 거래 내역을 반환한다.
    같은 분 안에 들어온 BUY/SELL 추천/시그널을 한 trade(포트폴리오)로 묶는다.
    """
    src = source if source in _TRADE_SOURCES else "strategist"
    h = horizon if horizon and 0 < horizon <= BACKTEST_MAX_HORIZON_DAYS else BACKTEST_TRADES_DEFAULT_HORIZON
    lb = _sanitize_lookback(lookback_days or BACKTEST_DEFAULT_LOOKBACK_DAYS)

    cache_key = hashlib.sha1(
        f"trades|{src}|{h}|{lb}|{int(include_open)}".encode("utf-8")
    ).hexdigest()
    if not refresh:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

    result = await asyncio.to_thread(
        _run_trade_history_sync, src, h, lb, include_open=include_open,
    )
    sanitized = sanitize_for_json(result)
    _cache_put(cache_key, sanitized)
    return sanitized
