"""yfinance 글로벌 속도 제한 — 429 Too Many Requests 방지.

모든 개별 yfinance 호출(fast_info, info, get_news, get_earnings_history 등)을
이 모듈의 ``throttled()``로 감싸면 프로세스 전체에서 동시 요청 수와
최소 호출 간격이 자동으로 제어된다.

Usage::

    from services.yf_limiter import throttled

    # 단일 값 조회
    mc = throttled(lambda: yf.Ticker("AAPL").fast_info["marketCap"])

    # 메서드 호출
    df = throttled(yf.Ticker("AAPL").get_earnings_history)
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, TypeVar

from config import (
    YF_GLOBAL_CONCURRENCY,
    YF_MIN_INTERVAL_SEC,
    YF_RATE_LIMIT_BACKOFF_SEC,
    YF_RATE_LIMIT_RETRIES,
)

logger = logging.getLogger(__name__)
T = TypeVar("T")

_sem = threading.Semaphore(YF_GLOBAL_CONCURRENCY)
_lock = threading.Lock()
_last_ts: float = 0.0


def _is_rate_limit(exc: BaseException) -> bool:
    """yfinance RateLimit 에러 판별."""
    cls = type(exc).__name__
    if "RateLimit" in cls:
        return True
    msg = str(exc).lower()
    return "rate limit" in msg or "too many requests" in msg


def throttled(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """yfinance 함수를 글로벌 속도 제한 + 자동 재시도로 감싼다.

    - ``YF_GLOBAL_CONCURRENCY`` 개의 동시 호출만 허용
    - 호출 사이 ``YF_MIN_INTERVAL_SEC`` 최소 간격 보장
    - 429 응답 시 지수 백오프 재시도 (최대 ``YF_RATE_LIMIT_RETRIES`` 회)
    """
    global _last_ts

    for attempt in range(1, YF_RATE_LIMIT_RETRIES + 1):
        with _sem:
            # lock은 타임스탬프 갱신만, sleep은 lock 밖에서 수행
            with _lock:
                now = time.time()
                wait_until = _last_ts + YF_MIN_INTERVAL_SEC
                gap = max(0.0, wait_until - now)
                _last_ts = max(now, wait_until)
            if gap > 0:
                time.sleep(gap)
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                if not (_is_rate_limit(e) and attempt < YF_RATE_LIMIT_RETRIES):
                    raise
        # sem 해제 후 백오프 — 슬롯을 점유하지 않음
        delay = YF_RATE_LIMIT_BACKOFF_SEC * (2 ** (attempt - 1))
        logger.warning(
            "yfinance rate limit (시도 %d/%d), %.1fs 대기 후 재시도",
            attempt, YF_RATE_LIMIT_RETRIES, delay,
        )
        time.sleep(delay)
    raise RuntimeError("yfinance throttled: max retries exceeded")
