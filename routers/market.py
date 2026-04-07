from fastapi import APIRouter
from services.websocket import latest_cache
from services.crud import get_all_records, sanitize_for_json

router = APIRouter(prefix="/api", tags=["Market"])


@router.get("/latest")
async def api_latest():
    """최신 스캔 결과 조회 (메모리 캐시). NaN/Inf는 null로 내려감."""
    return sanitize_for_json(latest_cache)


@router.get("/all-records")
async def api_all_records(limit: int = 100):
    """DB에 저장된 전체 분석 기록 조회"""
    return get_all_records(limit)


@router.get("/heatmap/sp500")
async def api_heatmap_sp500():
    """S&P 500 섹터별 히트맵 데이터 (시가총액·등락률·현재가)"""
    from services.heatmap import get_cached_sp500_heatmap
    return sanitize_for_json(await get_cached_sp500_heatmap())
