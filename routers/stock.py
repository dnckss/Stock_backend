from fastapi import APIRouter, HTTPException
from services.crud import get_latest_report, get_history

router = APIRouter(prefix="/api", tags=["Stock"])


@router.get("/stock/{ticker}")
async def api_stock_detail(ticker: str):
    """
    종목 상세 분석 페이지용 엔드포인트.
    - latest_report: 가장 최근 AI 리포트 전문 1건
    - history: 최근 30일치 (수익률, 감성, 괴리율) 시계열 데이터
    """
    upper = ticker.upper()
    latest = get_latest_report(upper)
    history = get_history(upper, days=30)

    if not latest and not history:
        raise HTTPException(status_code=404, detail=f"{upper} 데이터 없음")

    return {
        "ticker": upper,
        "latest_report": latest,
        "history": history,
    }
