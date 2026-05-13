"""섹터 ETF 기반 섹터 로테이션 / 모멘텀 라우터.

services/sector_tracker.py 의 fetch_sector_performance / determine_sector_rotation
를 외부 API 로 노출. yfinance 부담을 줄이기 위해 짧은 in-process TTL 캐시 적용.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from fastapi import APIRouter, HTTPException

from config import SECTOR_TRACKER_CACHE_TTL_SEC
from services.crud import sanitize_for_json
from services.sector_tracker import (
    SECTOR_ETFS,
    determine_sector_rotation,
    fetch_sector_performance,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sectors", tags=["Sectors"])


# 단일 응답을 공유 — 동시 호출이 들어와도 yfinance 1회만 호출되도록 lock
_cache: dict[str, Any] = {"ts": 0.0, "perf": None}
_cache_lock = asyncio.Lock()


async def _get_cached_performance(refresh: bool = False) -> list[dict[str, Any]]:
    """SECTOR_TRACKER_CACHE_TTL_SEC TTL 캐시 + 동시성 안전 회수."""
    now = time.time()
    if not refresh and _cache["perf"] is not None:
        if now - _cache["ts"] <= SECTOR_TRACKER_CACHE_TTL_SEC:
            return _cache["perf"]

    async with _cache_lock:
        # lock 대기 사이 다른 호출이 채웠을 수 있으니 재확인
        now = time.time()
        if not refresh and _cache["perf"] is not None:
            if now - _cache["ts"] <= SECTOR_TRACKER_CACHE_TTL_SEC:
                return _cache["perf"]
        perf = await asyncio.to_thread(fetch_sector_performance)
        _cache["perf"] = perf or []
        _cache["ts"] = time.time()
        return _cache["perf"]


@router.get("/performance")
async def api_sectors_performance(refresh: int = 0) -> dict[str, Any]:
    """SPDR 섹터 ETF 11종의 1주/1개월 수익률 + 모멘텀.

    refresh=1 으로 캐시 무시. 기본은 SECTOR_TRACKER_CACHE_TTL_SEC TTL 캐시.
    """
    try:
        perf = await _get_cached_performance(refresh=bool(refresh))
    except Exception as e:
        logger.exception("섹터 성과 조회 실패: %s", e)
        raise HTTPException(status_code=500, detail=f"섹터 성과 조회 실패: {e}")
    return sanitize_for_json({
        "items": perf,
        "count": len(perf),
        "cache_ttl_sec": SECTOR_TRACKER_CACHE_TTL_SEC,
    })


@router.get("/rotation")
async def api_sectors_rotation(refresh: int = 0) -> dict[str, Any]:
    """섹터 로테이션 방향: growth / defensive / cyclical / mixed / unknown."""
    try:
        perf = await _get_cached_performance(refresh=bool(refresh))
    except Exception as e:
        logger.exception("섹터 로테이션 조회 실패: %s", e)
        raise HTTPException(status_code=500, detail=f"섹터 로테이션 조회 실패: {e}")
    rotation = determine_sector_rotation(perf)
    return sanitize_for_json({
        "rotation": rotation,
        "items": perf,
        "count": len(perf),
    })


@router.get("/etfs")
async def api_sectors_etf_map() -> dict[str, Any]:
    """SPDR 섹터 ETF 매핑(메타데이터). yfinance 호출 없음 — 정적 응답."""
    return {"items": SECTOR_ETFS, "count": len(SECTOR_ETFS)}
