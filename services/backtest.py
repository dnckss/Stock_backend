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
    BACKTEST_MAX_HORIZON_DAYS,
    BACKTEST_MAX_LOOKBACK_DAYS,
    BACKTEST_MIN_SAMPLES,
    BACKTEST_PRICE_LOOKAHEAD_DAYS,
)
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

def _fetch_close_prices(
    tickers: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    """
    티커 목록의 종가 시계열을 한 번에 다운로드한다.
    반환: 컬럼=ticker, index=DatetimeIndex(거래일)인 DataFrame.
    실패/빈 결과면 빈 DataFrame.
    """
    if not tickers:
        return pd.DataFrame()
    # yfinance 는 end 가 exclusive
    end_plus = end + timedelta(days=1)
    try:
        df = yf.download(
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

    if df.empty:
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

    return close.sort_index()


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

    signals_task = run_signals_backtest(lb, hs, refresh=refresh)
    strat_task = run_strategist_backtest(lb, hs, refresh=refresh)
    signals, strat = await asyncio.gather(signals_task, strat_task)

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
