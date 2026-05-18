"""성능 벤치마크 (외부 의존 없이 단위 비교 가능).

측정 대상 (모두 hot path):
  1. sanitize_for_json      — 모든 API 응답·WS broadcast 마다 호출
  2. JSON 직렬화             — FastAPI 응답 (stdlib json vs orjson)
  3. WebSocket broadcast     — 200 클라이언트 가정한 직렬화·전송 시뮬레이션
  4. scan_stocks 내부 변환    — yf.download 결과에서 candidate dict 생성

베이스라인은 git log f18c4bd 시점 코드를 가정한다. 최적화 후 동일 입력으로
재실행해 ms 단위 비교를 docs/PERFORMANCE_REPORT.md 에 기록한다.

실행:
  python -m benchmark.bench_perf
"""
from __future__ import annotations

import json
import math
import os
import random
import statistics
import sys
import time
from typing import Any

# 프로젝트 루트를 path 에 추가
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# 테스트와 동일하게 외부 의존 없이 import 가능하도록 env 더미값
os.environ.setdefault("STRICT_ENV", "false")
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "test-key")
os.environ.setdefault("OPENAI_API_KEY", "test-key")


# ---------------------------------------------------------------------------
# 합성 데이터 — latest_cache 1회 broadcast 페이로드를 모방
# ---------------------------------------------------------------------------

def _candidate(i: int, *, with_nan: bool = False) -> dict[str, Any]:
    """analysis_results 1건 행을 모방한 dict (daily 일봉 5개 포함)."""
    return {
        "ticker": f"T{i:04d}",
        "name": f"Sample Corp {i}",
        "sector": "Information Technology",
        "in_sp500": True,
        "price": round(100 + random.random() * 200, 2) if not with_nan else float("nan"),
        "volume": random.randint(1_000_000, 50_000_000),
        "return": round(random.uniform(-0.2, 0.2), 6),
        "liquidity_ok": True,
        "price_available": True,
        "scan_missing": False,
        "universe_source": "sp500",
        "sentiment": round(random.uniform(-1, 1), 4),
        "divergence": round(random.uniform(-0.5, 0.5), 4) if not with_nan else float("inf"),
        "signal": random.choice(["BUY", "SELL", "HOLD"]),
        "signal_source": "earnings",
        "eps_actual": round(random.uniform(0.1, 10), 4),
        "eps_estimate": round(random.uniform(0.1, 10), 4),
        "earnings_surprise_pct": round(random.uniform(-0.5, 0.5), 4),
        "daily": [
            {
                "date": f"2026-05-{(d + 1):02d}",
                "open": round(100 + random.random() * 50, 2),
                "high": round(150 + random.random() * 50, 2),
                "low": round(80 + random.random() * 50, 2),
                "close": round(120 + random.random() * 50, 2),
                "volume": random.randint(1_000_000, 50_000_000),
            }
            for d in range(5)
        ],
        "report": None,
    }


def make_payload(n_rows: int = 503, nan_ratio: float = 0.02) -> dict[str, Any]:
    """latest_cache 페이로드 (S&P 500 503 종목 + 매크로 + 뉴스피드)."""
    random.seed(42)
    rows = [_candidate(i, with_nan=(random.random() < nan_ratio)) for i in range(n_rows)]
    macro = {
        "marquee": [{"name": n, "value": random.random() * 5000, "change": random.random(), "pct": random.random()} for n in ("S&P 500", "NASDAQ")],
        "sidebar": [{"name": n, "value": random.random() * 5000, "change": random.random(), "pct": random.random()} for n in ("DOW", "VIX", "USD/KRW", "US 10Y")],
    }
    news = [
        {
            "title": f"Sample headline {i}",
            "publisher": "Yahoo Finance",
            "timestamp": int(time.time()),
            "ticker": f"T{i:04d}",
            "url": f"https://example.com/n/{i}",
            "url_hash": "0" * 64,
            "score": random.uniform(-1, 1),
            "sentiment_label": random.choice(["positive", "negative", "neutral"]),
            "sentiment_polarity": "positive",
            "sentiment_ko": "긍정",
            "confidence": random.uniform(0, 1),
        }
        for i in range(30)
    ]
    return {
        "top_picks": rows[:2],
        "radar": rows[2:],
        "macro": macro,
        "news_feed": news,
        "market_gauge": 62,
        "vix": 14.5,
        "updated_at": "2026-05-18T10:00:00",
        "quote_tick_at": "2026-05-18T10:00:30",
    }


# ---------------------------------------------------------------------------
# 측정 헬퍼
# ---------------------------------------------------------------------------

def _measure(label: str, fn, *, iterations: int = 50) -> dict[str, float]:
    """fn() 을 iterations 회 실행해 통계 산출. ms 단위."""
    times: list[float] = []
    # warmup
    for _ in range(3):
        fn()
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000.0)
    return {
        "label": label,
        "iterations": iterations,
        "min_ms": round(min(times), 3),
        "p50_ms": round(statistics.median(times), 3),
        "mean_ms": round(statistics.mean(times), 3),
        "p95_ms": round(statistics.quantiles(times, n=20)[-1] if len(times) >= 20 else max(times), 3),
        "max_ms": round(max(times), 3),
    }


def _print_row(r: dict[str, float]) -> None:
    print(
        f"  {r['label']:<48} iter={r['iterations']:>4}  "
        f"min={r['min_ms']:>7.2f}  p50={r['p50_ms']:>7.2f}  "
        f"mean={r['mean_ms']:>7.2f}  p95={r['p95_ms']:>7.2f}  max={r['max_ms']:>7.2f} ms"
    )


# ---------------------------------------------------------------------------
# 벤치: sanitize_for_json
# ---------------------------------------------------------------------------

def bench_sanitize(payload: dict[str, Any]) -> list[dict[str, float]]:
    from services.crud import sanitize_for_json

    out = [
        _measure("sanitize_for_json (S&P 500 페이로드)", lambda: sanitize_for_json(payload)),
    ]
    # NaN 없는 깨끗한 페이로드 → fast path 효과 측정 (있다면)
    clean_payload = json.loads(json.dumps(payload, default=str))  # NaN → str 제거
    out.append(
        _measure("sanitize_for_json (NaN 없는 페이로드)", lambda: sanitize_for_json(clean_payload)),
    )
    return out


# ---------------------------------------------------------------------------
# 벤치: JSON 직렬화
# ---------------------------------------------------------------------------

def bench_json_serialize(payload: dict[str, Any]) -> list[dict[str, float]]:
    from services.crud import sanitize_for_json
    sanitized = sanitize_for_json(payload)

    results = [
        _measure("json.dumps (stdlib)", lambda: json.dumps(sanitized).encode("utf-8")),
    ]
    try:
        import orjson  # type: ignore
        results.append(
            _measure("orjson.dumps", lambda: orjson.dumps(sanitized)),
        )
    except ImportError:
        print("  (orjson 미설치 — skip)")
    return results


# ---------------------------------------------------------------------------
# 벤치: WebSocket broadcast 시뮬레이션
# ---------------------------------------------------------------------------

class _FakeWS:
    """send_json/send_bytes 만 흉내 — 실제 네트워크 비용 대신 sleep(0)."""

    async def send_json(self, msg: dict[str, Any]) -> None:
        # FastAPI 내부의 json.dumps 비용을 흉내
        json.dumps(msg)

    async def send_bytes(self, b: bytes) -> None:
        # 직렬화는 caller 가 끝낸 상태 — 거의 0 비용
        pass

    async def send_text(self, s: str) -> None:
        pass


def bench_broadcast(payload: dict[str, Any]) -> list[dict[str, float]]:
    import asyncio

    n_clients = 200
    clients = [_FakeWS() for _ in range(n_clients)]

    msg = {"type": "MARKET_UPDATE", **payload}

    async def serial_send_json():
        for c in clients:
            await c.send_json(msg)

    async def parallel_send_json():
        await asyncio.gather(*[c.send_json(msg) for c in clients])

    # 사전 직렬화 + 텍스트 프레임 (실제 ConnectionManager.broadcast 와 동일 패턴)
    async def pre_serialized_send_text():
        try:
            import orjson
            s = orjson.dumps(msg).decode("utf-8")
        except ImportError:
            s = json.dumps(msg)
        await asyncio.gather(*[c.send_text(s) for c in clients])

    loop = asyncio.new_event_loop()
    try:
        results = []
        results.append(_measure(
            f"broadcast 직렬 send_json ({n_clients}연결, baseline)",
            lambda: loop.run_until_complete(serial_send_json()),
            iterations=30,
        ))
        results.append(_measure(
            f"broadcast 병렬 send_json ({n_clients}연결, baseline)",
            lambda: loop.run_until_complete(parallel_send_json()),
            iterations=30,
        ))
        results.append(_measure(
            f"broadcast 병렬 사전직렬화+send_text ({n_clients}연결, optimized)",
            lambda: loop.run_until_complete(pre_serialized_send_text()),
            iterations=30,
        ))
        return results
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# 벤치: scan_stocks DataFrame → dict 변환 (iterrows vs itertuples)
# ---------------------------------------------------------------------------

def bench_scan_conversion() -> list[dict[str, float]]:
    import pandas as pd

    # 100 ticker × 7 일의 OHLCV (yf.download batch 결과 모방)
    dates = pd.date_range(end="2026-05-17", periods=7, freq="D")
    tickers = [f"T{i:03d}" for i in range(100)]
    columns = pd.MultiIndex.from_product(
        [tickers, ["Open", "High", "Low", "Close", "Volume"]],
    )
    data = pd.DataFrame(
        [[random.uniform(50, 300) for _ in range(len(columns))] for _ in range(7)],
        index=dates, columns=columns,
    )

    def convert_iterrows():
        out = []
        for ticker in tickers[:50]:
            sub = data[ticker]
            bars = []
            for idx, row in sub.iterrows():
                bars.append({
                    "date": idx.strftime("%Y-%m-%d"),
                    "open": round(float(row["Open"]), 2),
                    "high": round(float(row["High"]), 2),
                    "low": round(float(row["Low"]), 2),
                    "close": round(float(row["Close"]), 2),
                    "volume": int(row["Volume"]),
                })
            out.append({"ticker": ticker, "daily": bars})
        return out

    def convert_itertuples():
        out = []
        for ticker in tickers[:50]:
            sub = data[ticker]
            bars = []
            # itertuples 는 namedtuple 반환 → 컬럼 접근이 attribute 로 가능
            for row in sub.itertuples(index=True):
                bars.append({
                    "date": row.Index.strftime("%Y-%m-%d"),
                    "open": round(float(row.Open), 2),
                    "high": round(float(row.High), 2),
                    "low": round(float(row.Low), 2),
                    "close": round(float(row.Close), 2),
                    "volume": int(row.Volume),
                })
            out.append({"ticker": ticker, "daily": bars})
        return out

    return [
        _measure("scan conversion: iterrows (50종목×7일)", convert_iterrows, iterations=20),
        _measure("scan conversion: itertuples (50종목×7일)", convert_itertuples, iterations=20),
    ]


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 100)
    print(f"Quantix 성능 벤치마크  (Python {sys.version.split()[0]})")
    print("=" * 100)

    payload = make_payload()
    n_rows = len(payload["top_picks"]) + len(payload["radar"])
    print(f"페이로드: {n_rows} rows × 일봉 5개 + 매크로 + 뉴스 30건\n")

    print("[1] sanitize_for_json")
    for r in bench_sanitize(payload):
        _print_row(r)

    print("\n[2] JSON 직렬화")
    for r in bench_json_serialize(payload):
        _print_row(r)

    print("\n[3] WebSocket broadcast (200 클라이언트 시뮬레이션)")
    for r in bench_broadcast(payload):
        _print_row(r)

    print("\n[4] scan_stocks 변환 (iterrows vs itertuples)")
    for r in bench_scan_conversion():
        _print_row(r)

    print("\n완료")


if __name__ == "__main__":
    main()
