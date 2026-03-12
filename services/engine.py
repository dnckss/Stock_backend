import asyncio
from datetime import datetime

from config import SCAN_INTERVAL_SEC, ERROR_RETRY_SEC, REPORT_TOP_N, MACRO_INTERVAL_SEC
from services.scanner import get_all_tickers, scan_stocks, fetch_macro_indicators
from services.sentiment import analyze_sentiments
from services.analyst import compute_signals
from services.crud import save_candidates, sanitize_for_json
from services.websocket import manager, latest_cache
from services.news_feed import build_news_feed


async def run_analysis_loop():
    """
    1시간 주기 자동 분석 파이프라인:
      Step 1: 전 종목 스캔 + 잡주 필터링 + Top 15
      Step 2: 비동기 감성 분석
      Step 3: 괴리율 계산
      Step 4: DB 저장 + WebSocket 브로드캐스트
    """
    tickers = get_all_tickers()

    while True:
        try:
            start = datetime.now()
            print(f"\n⚡ 스캔 엔진 가동: {start.strftime('%H:%M:%S')}")

            # Step 1
            candidates = scan_stocks(tickers)
            if not candidates:
                print("⚠️ 유효 종목 0개 — 다음 사이클 대기")
                await asyncio.sleep(ERROR_RETRY_SEC)
                continue

            # Step 2
            ticker_list = [c["ticker"] for c in candidates]
            sentiments = await analyze_sentiments(ticker_list)

            # Step 3
            candidates = compute_signals(candidates, sentiments)

            # Step 4 (리포트는 사용자 요청 시 온디맨드 생성)
            save_candidates(candidates)

            # News Feed (AI SENTIMENT FEED)
            latest_cache["news_feed"] = await build_news_feed(ticker_list)

            top = candidates[:REPORT_TOP_N]
            latest_cache["top_picks"] = top
            latest_cache["radar"] = candidates[REPORT_TOP_N:]
            latest_cache["updated_at"] = datetime.now().isoformat()
            await manager.broadcast({"type": "MARKET_UPDATE", **sanitize_for_json(latest_cache)})

            elapsed = datetime.now() - start
            print(f"✅ 스캔 완료! (소요: {elapsed})")
            await asyncio.sleep(SCAN_INTERVAL_SEC)

        except Exception as e:
            print(f"❌ 엔진 에러: {e}")
            await asyncio.sleep(ERROR_RETRY_SEC)


async def run_macro_loop():
    """
    5분 주기 매크로 지표 업데이트 루프.
    marquee/sidebar 데이터를 수집하여 latest_cache에 통합하고 WebSocket으로 브로드캐스트한다.
    """
    while True:
        try:
            macro = fetch_macro_indicators()
            count = len(macro["marquee"]) + len(macro["sidebar"])

            latest_cache["macro"] = macro
            latest_cache["updated_at"] = datetime.now().isoformat()
            await manager.broadcast({"type": "MARKET_UPDATE", **sanitize_for_json(latest_cache)})

            print(f"📈 매크로 지표 {count}개 업데이트 완료")
        except Exception as e:
            print(f"❌ 매크로 에러: {e}")

        await asyncio.sleep(MACRO_INTERVAL_SEC)
