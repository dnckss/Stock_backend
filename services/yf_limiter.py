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


def _is_transient_block(exc: BaseException) -> bool:
    """야후 일시 차단 정황을 throttle 신호로 간주한다(백오프 재시도 대상).

    명시적 429/RateLimit 외에, 야후가 빈/에러 본문을 줄 때 yfinance 내부가
    None 을 인덱싱하며 내는 ``'NoneType' object is not subscriptable`` 나
    빈 본문 JSON 디코드 실패(``Expecting value``) 도 소프트 차단으로 본다.
    즉시 실패시키면 우수수 쏟아지므로, 백오프로 한 박자 쉬고 재시도한다.
    """
    if _is_rate_limit(exc):
        return True
    msg = str(exc).lower()
    if "nonetype" in msg and "subscriptable" in msg:
        return True
    if "expecting value" in msg:
        return True
    return False


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
                if not (_is_transient_block(e) and attempt < YF_RATE_LIMIT_RETRIES):
                    raise
        # sem 해제 후 백오프 — 슬롯을 점유하지 않음
        delay = YF_RATE_LIMIT_BACKOFF_SEC * (2 ** (attempt - 1))
        logger.warning(
            "yfinance 일시 차단/속도제한 (시도 %d/%d), %.1fs 대기 후 재시도",
            attempt, YF_RATE_LIMIT_RETRIES, delay,
        )
        time.sleep(delay)
    raise RuntimeError("yfinance throttled: max retries exceeded")
