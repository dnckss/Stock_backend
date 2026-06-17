"""API 보호 — per-IP 레이트리밋 + 선택적 API 키 검증 (Public 배포용).

설계 의도:
  - 비용의 진짜 방어선은 OpenAI 대시보드의 하드 스펜딩 캡이다.
  - 여기 레이트리밋은 '남용 속도/반경 제한', API 키는 '봇 차단 속도방지턱'
    (브라우저 프론트는 키가 네트워크 탭에 노출되므로 완전한 비밀은 아님).
  - 프로세스 내 슬라이딩 윈도우 — HF 단일 프로세스(uvicorn 1 worker) 기준 충분.
    다중 워커/인스턴스로 확장하면 Redis 등 공용 저장소로 옮겨야 한다.

api.py 는 build_security_dispatch(response_class) 로 미들웨어 dispatch 를 만들어
CORS 안쪽에 끼운다(차단 응답 401/429 에도 CORS 헤더가 실리도록).
"""
from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from typing import Any, Callable

from config import (
    API_ACCESS_KEY,
    API_KEY_HEADER_NAME,
    RATE_LIMIT_DEFAULT_MAX,
    RATE_LIMIT_ENABLED,
    RATE_LIMIT_LLM_MARKERS,
    RATE_LIMIT_LLM_MAX,
    RATE_LIMIT_PRUNE_EVERY,
    RATE_LIMIT_WINDOW_SEC,
    SECURITY_PROTECTED_PREFIX,
)

# per-bucket(=ip:tier) 요청 타임스탬프 슬라이딩 윈도우
_hits: dict[str, deque[float]] = defaultdict(deque)
_lock = threading.Lock()
_op_count = 0


def get_client_ip(request: Any) -> str:
    """프록시(HF) 뒤에서 실제 클라이언트 IP 추정.

    HF Spaces 는 프록시 뒤라 request.client.host 는 내부 IP 다 — X-Forwarded-For
    (첫 홉)/X-Real-IP 를 우선 사용한다. 헤더 위조 가능성은 있으나, 레이트리밋
    용도에선 '대부분의 정상/봇 트래픽을 IP 단위로 묶는' 목적에 충분하다.
    """
    headers = request.headers
    xff = headers.get("x-forwarded-for")
    if xff:
        first = xff.split(",")[0].strip()
        if first:
            return first
    xri = headers.get("x-real-ip")
    if xri and xri.strip():
        return xri.strip()
    client = getattr(request, "client", None)
    return getattr(client, "host", None) or "unknown"


def is_llm_path(path: str) -> bool:
    """경로가 LLM/비용 유발 마커를 포함하면 True (엄격 한도 적용 대상)."""
    return any(marker in path for marker in RATE_LIMIT_LLM_MARKERS)


def _prune_locked() -> None:
    """빈 버킷 제거 — 비활성 IP 누적으로 인한 메모리 누수 방지. (_lock 보유 상태 호출)"""
    empty = [k for k, dq in _hits.items() if not dq]
    for k in empty:
        del _hits[k]


def check_rate_limit(ip: str, path: str) -> tuple[bool, int]:
    """per-IP 슬라이딩 윈도우 점검. 반환 (allowed, limit).

    LLM 경로는 RATE_LIMIT_LLM_MAX, 그 외는 RATE_LIMIT_DEFAULT_MAX 를 분당 한도로 적용.
    비활성화(RATE_LIMIT_ENABLED=false) 시 항상 통과.
    """
    if not RATE_LIMIT_ENABLED:
        return True, 0

    global _op_count
    llm = is_llm_path(path)
    limit = RATE_LIMIT_LLM_MAX if llm else RATE_LIMIT_DEFAULT_MAX
    bucket = f"{ip}:{'llm' if llm else 'std'}"
    now = time.time()
    cutoff = now - RATE_LIMIT_WINDOW_SEC

    with _lock:
        dq = _hits[bucket]
        while dq and dq[0] < cutoff:
            dq.popleft()
        allowed = len(dq) < limit
        if allowed:
            dq.append(now)

        _op_count += 1
        if RATE_LIMIT_PRUNE_EVERY > 0 and _op_count % RATE_LIMIT_PRUNE_EVERY == 0:
            _prune_locked()

    return allowed, limit


def api_key_ok(request: Any) -> bool:
    """API_ACCESS_KEY 가 설정돼 있으면 헤더 일치 필요. 미설정이면 항상 통과(로컬/초기)."""
    if not API_ACCESS_KEY:
        return True
    provided = request.headers.get(API_KEY_HEADER_NAME)
    return bool(provided) and provided == API_ACCESS_KEY


def build_security_dispatch(response_class: Callable[..., Any]):
    """BaseHTTPMiddleware 용 dispatch 생성.

    SECURITY_PROTECTED_PREFIX(기본 /api) 하위 경로에만 적용. OPTIONS(프리플라이트)는
    통과시켜 CORS 가 처리하게 한다. 차단 시 response_class(orjson 등)로 401/429 반환.
    """
    protected_prefix = SECURITY_PROTECTED_PREFIX

    async def dispatch(request: Any, call_next: Callable):
        path = request.url.path
        if request.method != "OPTIONS" and path.startswith(protected_prefix):
            if not api_key_ok(request):
                return response_class(
                    {"detail": "유효한 API 키가 필요합니다."},
                    status_code=401,
                )
            allowed, limit = check_rate_limit(get_client_ip(request), path)
            if not allowed:
                return response_class(
                    {"detail": f"요청이 너무 많습니다 (분당 {limit}회 초과). 잠시 후 다시 시도하세요."},
                    status_code=429,
                    headers={"Retry-After": str(RATE_LIMIT_WINDOW_SEC)},
                )
        return await call_next(request)

    return dispatch
