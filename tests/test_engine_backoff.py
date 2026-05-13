"""백그라운드 루프 backoff 헬퍼 단위 테스트."""
from services.engine import _backoff_delay
from config import ERROR_RETRY_SEC, LOOP_BACKOFF_MAX_SEC


def test_backoff_first_failure_uses_base():
    assert _backoff_delay(1) == ERROR_RETRY_SEC


def test_backoff_doubles_each_failure():
    # 1→base, 2→2x, 3→4x ...
    assert _backoff_delay(2) == ERROR_RETRY_SEC * 2
    assert _backoff_delay(3) == ERROR_RETRY_SEC * 4
    assert _backoff_delay(4) == ERROR_RETRY_SEC * 8


def test_backoff_caps_at_max():
    huge = _backoff_delay(100)
    assert huge == LOOP_BACKOFF_MAX_SEC


def test_backoff_zero_or_negative_returns_base():
    assert _backoff_delay(0) == ERROR_RETRY_SEC
    assert _backoff_delay(-5) == ERROR_RETRY_SEC
