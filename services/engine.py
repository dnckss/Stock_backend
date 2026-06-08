import asyncio
import logging
from datetime import datetime

from config import (
    ECON_CALENDAR_INTERVAL_SEC,
    ERROR_RETRY_SEC,
    LOOP_BACKOFF_MAX_SEC,
    LOOP_FAILURE_ALERT_THRESHOLD,
    MACRO_INTERVAL_SEC,
    MIN_TOP_PICKS_FRESH,
    NEWS_FALLBACK_TICKERS,
    NEWS_FEED_INTERVAL_SEC,
    PRICE_BACKFILL_ENABLED,
    PRICE_BACKFILL_INITIAL_DELAY_SEC,
    PRICE_BACKFILL_INTERVAL_SEC,
    PRICE_TICK_INTERVAL_SEC,
    PRICE_TICK_MAX_SYMBOLS,
    REPORT_TOP_N,
    SCAN_INTERVAL_SEC,
)
from services.scanner import (
    get_all_tickers,
    scan_stocks,
    count_priced,
    fetch_macro_indicators,
    get_market_gauge,
    refresh_intraday_prices,
    merge_intraday_into_candidates,
    backfill_missing_returns,
    backfill_missing_volume,
    ensure_sp500_coverage,
)
from services.sentiment import analyze_sentiments
from services.earnings import get_earnings_surprises
from services.analyst import compute_signals
from services.crud import get_latest_scan_records, save_candidates, sanitize_for_json
from services.news_feed import build_news_feed
from services.price_store import backfill_recent
from services.websocket import manager, latest_cache
from services.utils import spawn_logged

logger = logging.getLogger(__name__)


def _current_market_rows() -> list[dict]:
    """latest_cache 의 top_picks + radar 를 하나의 rows 리스트로 반환한다."""
    return list(latest_cache.get("top_picks") or []) + list(latest_cache.get("radar") or [])


def _write_market_rows(rows: list[dict]) -> None:
    """rows 를 top/radar 구조로 다시 기록한다."""
    latest_cache["top_picks"] = rows[:REPORT_TOP_N]
    latest_cache["radar"] = rows[REPORT_TOP_N:]


def _ensure_latest_cache_sp500_coverage() -> int:
    """메모리 캐시를 S&P 500 전체 종목으로 확장하고 총 row 수를 반환한다."""
    rows = _current_market_rows()
    if not rows:
        return 0
    expanded = ensure_sp500_coverage(rows)
    if len(expanded) != len(rows):
        _write_market_rows(expanded)
        logger.info("메모리 마켓 캐시 S&P 500 보강: %d → %d rows", len(rows), len(expanded))
    return len(expanded)


def _backoff_delay(failures: int) -> int:
    """연속 실패 회수에 따른 대기 시간 — 지수 백오프 + 상한.

    failures=1 → ERROR_RETRY_SEC, =2 → 2x, =3 → 4x ... 상한은 LOOP_BACKOFF_MAX_SEC.
    """
    if failures <= 0:
        return ERROR_RETRY_SEC
    delay = ERROR_RETRY_SEC * (2 ** (failures - 1))
    return min(delay, LOOP_BACKOFF_MAX_SEC)


def _log_loop_failure(loop_name: str, failures: int, exc: BaseException) -> None:
    """루프 실패를 일관 형식으로 로깅. 누적 임계 초과 시 ERROR 로 격상."""
    if failures >= LOOP_FAILURE_ALERT_THRESHOLD:
        logger.error(
            "%s 루프 누적 실패 %d회 (>=%d) — 점검 필요: %s",
            loop_name, failures, LOOP_FAILURE_ALERT_THRESHOLD, exc,
            exc_info=True,
        )
    else:
        logger.exception("%s 루프 에러 (연속 %d회): %s", loop_name, failures, exc)


async def _preserve_or_restore_snapshot(reason: str) -> None:
    """
    스캔 결과가 부실할 때 직전 스냅샷을 유지한다.
    메모리 캐시에도 top_picks 가 없으면 DB(analysis_results) 마지막 스냅샷으로 복원한다.
    어느 쪽이든 'scan_stale=True' 마커를 부여해 프런트가 stale 상태를 식별할 수 있게 한다.
    """
    if latest_cache.get("top_picks"):
        _ensure_latest_cache_sp500_coverage()
        latest_cache["scan_stale"] = True
        latest_cache["scan_stale_reason"] = reason
        latest_cache["updated_at"] = datetime.now().isoformat()
        await manager.broadcast({"type": "MARKET_UPDATE", **sanitize_for_json(latest_cache)})
        logger.info("직전 스냅샷 유지 (메모리 캐시): %s", reason)
        return

    try:
        cached_records = await asyncio.to_thread(get_latest_scan_records)
    except Exception as e:
        logger.warning("DB 스냅샷 복원 실패: %s", e)
        return

    if not cached_records:
        logger.warning("DB 스냅샷도 비어있음 — 캐시 복원 불가 (%s)", reason)
        return

    cached_records = ensure_sp500_coverage(cached_records)
    _write_market_rows(cached_records)
    latest_cache["scan_stale"] = True
    latest_cache["scan_stale_reason"] = reason
    latest_cache["updated_at"] = datetime.now().isoformat()
    await manager.broadcast({"type": "MARKET_UPDATE", **sanitize_for_json(latest_cache)})
    logger.info("DB 스냅샷 %d건으로 복원 (%s)", len(cached_records), reason)


async def run_analysis_loop():
    """
    1시간 주기 자동 분석 파이프라인:
      Step 1: yfinance로 S&P 500 전 종목 스캔 + 거래량 필터링
      Step 2: 비동기 감성 분석 (FinBERT)
      Step 3: 실적 서프라이즈(이익 괴리) 조회
      Step 4: 시그널 계산 (이익 괴리 우선, 감성 fallback)
      Step 5: DB 저장 + WebSocket 브로드캐스트
    """
    # get_all_tickers/scan_stocks 등은 동기 + 네트워크/CPU 작업이어서 이벤트 루프를 막을 수 있다.
    tickers = await asyncio.to_thread(get_all_tickers)
    failures = 0

    while True:
        try:
            start = datetime.now()
            logger.info("스캔 엔진 가동: %s", start.strftime("%H:%M:%S"))

            candidates = await asyncio.to_thread(scan_stocks, tickers)
            # 장시간 운용 중 yfinance 일시 차단/배치 실패가 누적되면 실제 가격 확보 종목이
            # 급감할 수 있다. scan_stocks 는 S&P 500 전체를 placeholder 로 패딩해 반환하므로
            # len(candidates)(≈503)이 아니라 가격이 실제로 채워진 종목 수(count_priced)로
            # 판정해야 한다. 부실 스캔이면 직전 스냅샷을 유지해 VOL·거래대금이 대량으로
            # 빈칸이 되는 현상을 방지한다.
            priced = count_priced(candidates)
            if priced < MIN_TOP_PICKS_FRESH:
                reason = (
                    f"유효 가격 종목 {priced}/{len(candidates)}개 "
                    f"(< MIN_TOP_PICKS_FRESH={MIN_TOP_PICKS_FRESH})"
                )
                logger.warning("%s — 직전 스냅샷 유지/복원, 다음 사이클 대기", reason)
                await _preserve_or_restore_snapshot(reason)
                await asyncio.sleep(ERROR_RETRY_SEC)
                continue

            ticker_list = [c["ticker"] for c in candidates]

            sentiments = await analyze_sentiments(ticker_list)

            logger.info("실적 서프라이즈 조회 시작 (%s개)...", len(ticker_list))
            earnings = await asyncio.to_thread(get_earnings_surprises, ticker_list)
            earned_count = sum(1 for e in earnings if e is not None)
            logger.info("실적 서프라이즈: %s/%s개 확보", earned_count, len(ticker_list))

            # 기술적 지표 계산 (상위 종목)
            from services.technicals import compute_technicals_batch
            tech_tickers = ticker_list[:30]  # 상위 30종목만
            logger.info("기술적 지표 계산 시작 (%s개)...", len(tech_tickers))
            technicals = await asyncio.to_thread(compute_technicals_batch, tech_tickers)
            logger.info("기술적 지표: %s/%s개 확보", len(technicals), len(tech_tickers))

            candidates = compute_signals(candidates, sentiments, earnings, technicals)
            await asyncio.to_thread(save_candidates, candidates)

            latest_cache["news_feed"] = await build_news_feed(ticker_list)

            top = candidates[:REPORT_TOP_N]
            latest_cache["top_picks"] = top
            latest_cache["radar"] = candidates[REPORT_TOP_N:]
            latest_cache["scan_stale"] = False
            latest_cache["scan_stale_reason"] = None
            latest_cache["updated_at"] = datetime.now().isoformat()
            await manager.broadcast({"type": "MARKET_UPDATE", **sanitize_for_json(latest_cache)})

            elapsed = datetime.now() - start
            logger.info("스캔 완료 (소요: %s)", elapsed)
            failures = 0  # 성공 시 카운터 리셋
            await asyncio.sleep(SCAN_INTERVAL_SEC)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            failures += 1
            _log_loop_failure("스캔", failures, e)
            await asyncio.sleep(_backoff_delay(failures))


def _tag_macro_flash(prev: dict | None, cur: dict) -> dict:
    """이전 매크로 스냅샷과 비교하여 value가 변경된 항목에 flash=True를 부여한다."""
    if not prev:
        return cur

    # name → value 매핑 (marquee + sidebar 통합)
    prev_map: dict[str, float | None] = {}
    for key in ("marquee", "sidebar"):
        for item in prev.get(key) or []:
            prev_map[item["name"]] = item.get("value")

    for key in ("marquee", "sidebar"):
        for item in cur.get(key) or []:
            old_val = prev_map.get(item["name"])
            item["flash"] = (
                old_val is not None
                and item.get("value") is not None
                and old_val != item["value"]
            )

    return cur


async def run_macro_loop():
    """
    1분 주기 매크로 지표 업데이트 루프.
    yfinance fast_info로 글로벌 지표를 수집하여 latest_cache에 반영하고 WebSocket으로 브로드캐스트한다.
    값이 변경된 지표에는 flash=True를 부여하여 프런트에서 강조 효과를 적용할 수 있도록 한다.
    """
    failures = 0
    while True:
        try:
            prev_macro = latest_cache.get("macro")
            macro = await asyncio.to_thread(fetch_macro_indicators)
            macro = _tag_macro_flash(prev_macro, macro)
            count = len(macro["marquee"]) + len(macro["sidebar"])

            latest_cache["macro"] = macro
            gauge_data = get_market_gauge(macro)
            latest_cache["market_gauge"] = gauge_data["market_gauge"]
            latest_cache["vix"] = gauge_data["vix"]
            latest_cache["updated_at"] = datetime.now().isoformat()
            await manager.broadcast({"type": "MARKET_UPDATE", **sanitize_for_json(latest_cache)})

            logger.info("매크로 지표 %s개 업데이트 완료", count)
            failures = 0
            await asyncio.sleep(MACRO_INTERVAL_SEC)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            failures += 1
            _log_loop_failure("매크로", failures, e)
            await asyncio.sleep(_backoff_delay(failures))


def _tickers_for_price_refresh() -> list[str]:
    """top_picks + radar를 S&P 500 전체로 보강한 뒤 최대 PRICE_TICK_MAX_SYMBOLS개."""
    _ensure_latest_cache_sp500_coverage()
    seen: set[str] = set()
    out: list[str] = []
    for c in _current_market_rows():
        t = (c.get("ticker") or "").upper().strip()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
        if len(out) >= PRICE_TICK_MAX_SYMBOLS:
            break
    return out


async def run_price_tick_loop():
    """
    yfinance 분봉으로 top/radar 종목의 price·volume만 짧은 주기로 갱신한다.
    (전체 스캔·감성·실적은 run_analysis_loop 주기 유지)
    """
    if PRICE_TICK_INTERVAL_SEC <= 0:
        logger.info("분봉 시세 틱 비활성화 (PRICE_TICK_INTERVAL_SEC<=0)")
        return

    failures = 0
    while True:
        try:
            tickers = await asyncio.to_thread(_tickers_for_price_refresh)
            if not tickers:
                await asyncio.sleep(PRICE_TICK_INTERVAL_SEC)
                continue

            live = await asyncio.to_thread(refresh_intraday_prices, tickers)
            if live:
                merge_intraday_into_candidates(latest_cache.get("top_picks") or [], live)
                merge_intraday_into_candidates(latest_cache.get("radar") or [], live)

                # 라이브 3경로가 모두 놓친 종목의 VOL·거래대금을 price_history DB last
                # 일봉으로 보강(placeholder 면 price·일봉도). 등락률 backfill 보다 먼저
                # 실행해 채워진 price 를 등락률 계산이 활용하게 한다.
                vol_top = await asyncio.to_thread(
                    backfill_missing_volume, latest_cache.get("top_picks") or [],
                )
                vol_radar = await asyncio.to_thread(
                    backfill_missing_volume, latest_cache.get("radar") or [],
                )
                if vol_top + vol_radar:
                    logger.info(
                        "VOL·거래대금 DB backfill: %d개 종목 보강 (top=%d radar=%d)",
                        vol_top + vol_radar, vol_top, vol_radar,
                    )

                # _preserve_or_restore_snapshot 으로 placeholder(daily=[])가 남은 종목들의
                # 5일 등락률을 price_history DB 에서 batch 보강한다.
                # DB 캐시 hit 이면 yfinance 추가 호출 없이 즉시 채워짐.
                filled_top = await asyncio.to_thread(
                    backfill_missing_returns, latest_cache.get("top_picks") or [],
                )
                filled_radar = await asyncio.to_thread(
                    backfill_missing_returns, latest_cache.get("radar") or [],
                )
                if filled_top + filled_radar:
                    logger.info(
                        "5일 등락률 backfill: %d개 종목 보강 (top=%d radar=%d)",
                        filled_top + filled_radar, filled_top, filled_radar,
                    )

                latest_cache["quote_tick_at"] = datetime.now().isoformat()
                latest_cache["updated_at"] = datetime.now().isoformat()
                await manager.broadcast({"type": "MARKET_UPDATE", **sanitize_for_json(latest_cache)})
                logger.info("분봉 시세 갱신 완료 (%s/%s 심볼)", len(live), len(tickers))
            failures = 0
            await asyncio.sleep(PRICE_TICK_INTERVAL_SEC)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            failures += 1
            _log_loop_failure("가격 틱", failures, e)
            await asyncio.sleep(_backoff_delay(failures))


async def run_econ_calendar_loop():
    """30분 주기 경제 캘린더 크롤링 루프."""
    from services.economic_calendar import fetch_economic_calendar

    failures = 0
    while True:
        try:
            result = await fetch_economic_calendar(refresh=True)
            count = len(result.get("items") or [])
            logger.info("경제 캘린더 갱신 완료: %d건", count)
            failures = 0
            await asyncio.sleep(ECON_CALENDAR_INTERVAL_SEC)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            failures += 1
            _log_loop_failure("경제 캘린더", failures, e)
            await asyncio.sleep(_backoff_delay(failures))


async def run_news_feed_loop():
    """10분 주기 뉴스 피드 갱신 루프."""
    failures = 0
    while True:
        try:
            # top_picks/radar에서 티커 추출, 없으면 fallback 티커 사용
            tickers: list[str] = []
            for c in list(latest_cache.get("top_picks") or []) + list(latest_cache.get("radar") or []):
                t = (c.get("ticker") or "").upper().strip()
                if t and t not in tickers:
                    tickers.append(t)
            if not tickers:
                tickers = list(NEWS_FALLBACK_TICKERS)

            feed = await build_news_feed(tickers)
            latest_cache["news_feed"] = feed
            latest_cache["updated_at"] = datetime.now().isoformat()
            await manager.broadcast({"type": "MARKET_UPDATE", **sanitize_for_json(latest_cache)})
            logger.info("뉴스 피드 갱신 완료: %d건", len(feed))
            # 본문 프리페치는 build_news_feed 내부에서 spawn 됨(중복 호출 불필요).
            failures = 0
            await asyncio.sleep(NEWS_FEED_INTERVAL_SEC)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            failures += 1
            _log_loop_failure("뉴스 피드", failures, e)
            await asyncio.sleep(_backoff_delay(failures))


async def run_strategy_warmup_loop():
    """
    1시간 주기로 AI 전략 브리핑을 미리 산출해 캐시를 워밍한다.
    사용자가 전략 페이지에 들어오면 무거운 OpenAI 빌드 없이 캐시 hit 으로 즉시 응답된다
    (stale-while-revalidate). latest_cache 의 macro/gauge/vix/news_feed 를 입력으로 사용.
    """
    from config import (
        STRATEGIST_AUTO_WARMUP_ENABLED,
        STRATEGIST_AUTO_WARMUP_INITIAL_DELAY_SEC,
        STRATEGIST_AUTO_WARMUP_INTERVAL_SEC,
    )

    if not STRATEGIST_AUTO_WARMUP_ENABLED:
        logger.info("AI 전략 자동 워밍 비활성화 — 건너뜀")
        return

    # 서버 기동 직후 매크로/스캔 시드와 충돌 회피 — 약간의 초기 지연 후 시작.
    await asyncio.sleep(max(0, STRATEGIST_AUTO_WARMUP_INITIAL_DELAY_SEC))

    from services.strategist import refresh_market_strategy_cache

    failures = 0
    while True:
        try:
            start = datetime.now()
            macro = latest_cache.get("macro")
            market_gauge = latest_cache.get("market_gauge")
            vix = latest_cache.get("vix")
            news_feed = latest_cache.get("news_feed")
            await refresh_market_strategy_cache(macro, market_gauge, vix, news_feed)
            logger.info(
                "AI 전략 자동 워밍 완료 (%.1fs)",
                (datetime.now() - start).total_seconds(),
            )
            failures = 0
            await asyncio.sleep(STRATEGIST_AUTO_WARMUP_INTERVAL_SEC)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            failures += 1
            _log_loop_failure("전략 워밍", failures, e)
            await asyncio.sleep(_backoff_delay(failures))


async def run_price_backfill_loop():
    """
    매일 1회(기본 6시간 주기) active ticker 의 최근 OHLCV 를 yfinance 로 받아
    price_history 테이블에 upsert. 평상시 백테스트·기술지표는 DB 조회만으로 끝나
    yfinance 부담을 거의 0 으로 줄인다.
    """
    if not PRICE_BACKFILL_ENABLED:
        logger.info("가격 backfill 비활성화 — 건너뜀")
        return

    await asyncio.sleep(max(0, PRICE_BACKFILL_INITIAL_DELAY_SEC))

    failures = 0
    while True:
        try:
            result = await asyncio.to_thread(backfill_recent)
            logger.info(
                "가격 backfill 완료: tickers=%s rows=%s elapsed=%.1fs",
                result.get("tickers"), result.get("rows_written"), result.get("elapsed_sec", 0),
            )
            failures = 0
            await asyncio.sleep(PRICE_BACKFILL_INTERVAL_SEC)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            failures += 1
            _log_loop_failure("가격 backfill", failures, e)
            await asyncio.sleep(_backoff_delay(failures))


async def run_backtest_warmup_loop():
    """
    백테스트 결과를 주기적으로 자동 산출해 캐시를 워밍한다.
    사용자가 백테스트 페이지에 들어왔을 때 첫 호출도 캐시 hit으로 즉시 응답되게 한다.

    워밍 대상:
      - run_summary  : signals/strategist 백테스트 헤드라인
      - run_trade_history(strategist|signals) : 진입→청산 거래 내역
      - run_live_positions : 진행 중 포지션 — SWR 가 stale 도 즉시 주므로 워밍해두면
        진입 시 1분 TTL 만료 후에도 직전 결과 즉답(+백그라운드 갱신)된다.
    """
    from config import (
        BACKTEST_AUTO_WARMUP_ENABLED,
        BACKTEST_AUTO_WARMUP_INITIAL_DELAY_SEC,
        BACKTEST_AUTO_WARMUP_INTERVAL_SEC,
        BACKTEST_WARMUP_LOOKBACK_DAYS,
        BACKTEST_WARMUP_STEP_DELAY_SEC,
    )

    if not BACKTEST_AUTO_WARMUP_ENABLED:
        logger.info("백테스트 자동 워밍 비활성화 — 건너뜀")
        return

    # 서버 기동 직후 다른 초기 작업과 충돌 회피
    await asyncio.sleep(max(0, BACKTEST_AUTO_WARMUP_INITIAL_DELAY_SEC))

    from services.backtest import run_summary, run_trade_history, run_live_positions

    step_delay = max(0, BACKTEST_WARMUP_STEP_DELAY_SEC)
    failures = 0

    while True:
        start = datetime.now()
        try:
            logger.info("백테스트 자동 워밍 시작")

            # 1) summary — 내부적으로 signals/strategist 백테스트 모두 캐시됨.
            #    워밍은 BACKTEST_WARMUP_LOOKBACK_DAYS(=30) 만 — 사용자 직접 호출은 90일 그대로.
            try:
                await run_summary(lookback_days=BACKTEST_WARMUP_LOOKBACK_DAYS, refresh=True)
                logger.info("  · summary 워밍 완료 (lookback=%dd)", BACKTEST_WARMUP_LOOKBACK_DAYS)
            except Exception as e:
                logger.warning("  · summary 워밍 실패: %s", e)

            # 2) trades — 두 source 모두 별도 캐시. 단계 사이 sleep 으로 yfinance 부담 분산.
            for source in ("strategist", "signals"):
                if step_delay:
                    await asyncio.sleep(step_delay)
                try:
                    await run_trade_history(
                        source=source,
                        lookback_days=BACKTEST_WARMUP_LOOKBACK_DAYS,
                        refresh=True,
                    )
                    logger.info("  · trades(%s) 워밍 완료", source)
                except Exception as e:
                    logger.warning("  · trades(%s) 워밍 실패: %s", source, e)

            # 3) live — 진행 중 포지션. SWR 가 stale 도 즉시 주므로 캐시를 채워두면
            #    진입 시 1분 TTL 만료 뒤에도 직전 결과로 즉답된다.
            if step_delay:
                await asyncio.sleep(step_delay)
            try:
                await run_live_positions(refresh=True)
                logger.info("  · live 워밍 완료")
            except Exception as e:
                logger.warning("  · live 워밍 실패: %s", e)

            elapsed = (datetime.now() - start).total_seconds()
            logger.info("백테스트 자동 워밍 종료 (%.1fs)", elapsed)
            failures = 0
            await asyncio.sleep(BACKTEST_AUTO_WARMUP_INTERVAL_SEC)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            failures += 1
            _log_loop_failure("백테스트 워밍", failures, e)
            await asyncio.sleep(_backoff_delay(failures))
