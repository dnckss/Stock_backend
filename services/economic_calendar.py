from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from bs4 import BeautifulSoup

from config import (
    ECON_CALENDAR_MAX_ITEMS,
    ECON_CALENDAR_TIMEOUT_SEC,
    ECON_CALENDAR_TTL_SEC,
)
from services.crud import upsert_economic_events, get_economic_events

logger = logging.getLogger(__name__)

_cache: list[dict[str, Any]] = []
_cache_at: datetime | None = None

_KST = timezone(timedelta(hours=9))

_MYFXBOOK_URL = "https://www.myfxbook.com/forex-economic-calendar"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
}

_IMPACT_MAP = {
    "high": 3,
    "medium": 2,
    "low": 1,
}

# 통화 코드 → 국가 코드 / 국가명 매핑
_CURRENCY_COUNTRY: dict[str, tuple[str, str]] = {
    "USD": ("US", "미국"),
    "EUR": ("EU", "유럽"),
    "GBP": ("GB", "영국"),
    "JPY": ("JP", "일본"),
    "AUD": ("AU", "호주"),
    "NZD": ("NZ", "뉴질랜드"),
    "CAD": ("CA", "캐나다"),
    "CHF": ("CH", "스위스"),
    "CNY": ("CN", "중국"),
    "KRW": ("KR", "한국"),
}


# 경제 이벤트 한국어 번역 — 키워드 기반 매칭 (정확 매칭 우선, 부분 매칭 fallback)
_EVENT_KO_EXACT: dict[str, str] = {}

_EVENT_KO_KEYWORD: list[tuple[str, str]] = [
    # 고용
    ("Nonfarm Payrolls", "비농업부문 고용자수"),
    ("Unemployment Rate", "실업률"),
    ("Unemployment Change", "실업자수 변동"),
    ("Unemployed Persons", "실업자수"),
    ("Initial Jobless Claims", "신규 실업수당 청구건수"),
    ("Continuing Jobless Claims", "계속 실업수당 청구건수"),
    ("ADP Employment Change", "ADP 민간고용 변동"),
    ("Job Openings", "구인건수 (JOLTS)"),
    ("Employment Change", "고용 변동"),
    ("Labor Cost", "노동비용"),
    # 물가/인플레이션
    ("Core CPI", "근원 소비자물가"),
    ("CPI MoM", "소비자물가 전월비"),
    ("CPI YoY", "소비자물가 전년비"),
    ("CPI", "소비자물가지수"),
    ("Core Inflation Rate YoY", "근원 인플레이션율 전년비"),
    ("Core Inflation Rate MoM", "근원 인플레이션율 전월비"),
    ("Inflation Rate YoY", "인플레이션율 전년비"),
    ("Inflation Rate MoM", "인플레이션율 전월비"),
    ("Harmonised Inflation Rate MoM", "조화 인플레이션율 전월비"),
    ("Harmonised Inflation Rate YoY", "조화 인플레이션율 전년비"),
    ("Core PCE Price Index", "근원 PCE 물가지수"),
    ("PCE Price Index", "PCE 물가지수"),
    ("PPI MoM", "생산자물가 전월비"),
    ("PPI YoY", "생산자물가 전년비"),
    ("PPI", "생산자물가지수"),
    ("Import Prices MoM", "수입물가 전월비"),
    ("Import Prices YoY", "수입물가 전년비"),
    # GDP/성장
    ("GDP Growth Rate QoQ", "GDP 성장률 전분기비"),
    ("GDP Growth Rate YoY", "GDP 성장률 전년비"),
    ("GDP Price Index", "GDP 물가지수"),
    ("GDP", "국내총생산"),
    # 소비/소매
    ("Retail Sales MoM", "소매판매 전월비"),
    ("Retail Sales YoY", "소매판매 전년비"),
    ("Retail Sales", "소매판매"),
    ("Consumer Confidence", "소비자신뢰지수"),
    ("Consumer Sentiment", "소비자심리지수"),
    ("Household Consumption", "가계소비"),
    ("Personal Spending", "개인소비지출"),
    ("Personal Income", "개인소득"),
    # 제조업/산업
    ("Non Manufacturing PMI", "비제조업 PMI"),
    ("Manufacturing PMI", "제조업 PMI"),
    ("Services PMI", "서비스업 PMI"),
    ("NBS Non Manufacturing PMI", "NBS 비제조업 PMI"),
    ("NBS General PMI", "NBS 종합 PMI"),
    ("NBS Manufacturing PMI", "NBS 제조업 PMI"),
    ("ISM Non-Manufacturing PMI", "ISM 비제조업 PMI"),
    ("ISM Manufacturing PMI", "ISM 제조업 PMI"),
    ("Industrial Production", "산업생산"),
    ("Factory Orders", "공장주문"),
    ("Durable Goods Orders", "내구재주문"),
    ("Construction Orders", "건설수주"),
    # 주택
    ("Housing Starts", "주택착공건수"),
    ("Building Permits", "건축허가건수"),
    ("Existing Home Sales", "기존주택 판매"),
    ("New Home Sales", "신규주택 판매"),
    ("Nationwide Housing Prices MoM", "전국 주택가격 전월비"),
    ("Nationwide Housing Prices YoY", "전국 주택가격 전년비"),
    ("Housing Credit", "주택신용"),
    # 무역/경상
    ("Trade Balance", "무역수지"),
    ("Current Account", "경상수지"),
    ("Exports", "수출"),
    ("Imports", "수입"),
    # 중앙은행/금리
    ("Interest Rate Decision", "금리 결정"),
    ("Fed Interest Rate Decision", "연준 금리 결정"),
    ("FOMC", "FOMC"),
    ("ECB", "ECB"),
    ("BOJ", "일본은행"),
    ("BOE", "영란은행"),
    ("RBA Meeting Minutes", "호주중앙은행 의사록"),
    ("RBA", "호주중앙은행"),
    ("RBNZ", "뉴질랜드중앙은행"),
    ("Meeting Minutes", "의사록"),
    ("Monetary Policy", "통화정책"),
    # 기업/투자
    ("Business Confidence", "기업신뢰지수"),
    ("Business Investment QoQ", "기업투자 전분기비"),
    ("Business Investment YoY", "기업투자 전년비"),
    ("Private Sector Credit MoM", "민간부문 신용 전월비"),
    ("Private Sector Credit YoY", "민간부문 신용 전년비"),
    # 채권 입찰
    ("Bill Auction", "단기채 입찰"),
    ("Bond Auction", "국채 입찰"),
    ("JGB Auction", "일본 국채 입찰"),
    ("Treasury", "국채"),
    # 기타
    ("ANZ Business Confidence", "ANZ 기업신뢰지수"),
    ("Tokyo Core CPI", "도쿄 근원 소비자물가"),
    ("Tankan", "단칸 지수"),
    ("ZEW Economic Sentiment", "ZEW 경기기대지수"),
    ("Ifo Business Climate", "Ifo 기업환경지수"),
    ("BRC Shop Price Index", "BRC 소매물가지수"),
    ("KOF Economic Barometer", "KOF 경기선행지수"),
    ("Freedom Day", "자유의 날 (공휴일)"),
    ("Speaks", "연설"),
    ("Speech", "연설"),
    ("Holiday", "공휴일"),
]


def _translate_event(event_name: str) -> str:
    """영문 이벤트명을 한국어로 번역한다. 매칭 실패 시 원문 반환."""
    if not event_name:
        return ""

    # 괄호 안 기간 정보 분리: "Retail Sales MoM(Feb)" → base="Retail Sales MoM", period="(Feb)"
    period = ""
    base = event_name
    paren_idx = event_name.find("(")
    if paren_idx > 0:
        base = event_name[:paren_idx].strip()
        period = event_name[paren_idx:]

    # 정확 매칭
    if base in _EVENT_KO_EXACT:
        return _EVENT_KO_EXACT[base] + (f" {period}" if period else "")

    # 키워드 매칭 (긴 키워드 우선 — 리스트 순서대로)
    for keyword, ko in _EVENT_KO_KEYWORD:
        if keyword in base:
            return ko + (f" {period}" if period else "")

    return event_name


def _is_fresh() -> bool:
    if _cache_at is None:
        return False
    return (datetime.now() - _cache_at).total_seconds() < ECON_CALENDAR_TTL_SEC


def _parse_impact(raw: str) -> int:
    return _IMPACT_MAP.get(raw.strip().lower(), 0) if raw else 0


def _format_date_label(dt: datetime) -> str:
    weekdays = ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"]
    return f"{dt.year}년 {dt.month}월 {dt.day}일 {weekdays[dt.weekday()]}"


def _parse_myfxbook_datetime(date_text: str) -> datetime | None:
    """
    myfxbook 날짜 문자열(예: 'Mar 31, 00:30')을 datetime으로 파싱.
    myfxbook은 GMT 기준이므로 UTC로 처리 후 KST 변환.
    """
    date_text = date_text.strip()
    if not date_text:
        return None
    try:
        now = datetime.now(timezone.utc)
        # "Mar 31, 00:30" 형식
        parsed = datetime.strptime(date_text, "%b %d, %H:%M")
        # 연도 추정: 현재 연도 사용, 12월→1월 경계 처리
        year = now.year
        dt = parsed.replace(year=year, tzinfo=timezone.utc)
        if dt.month == 1 and now.month == 12:
            dt = dt.replace(year=year + 1)
        return dt.astimezone(_KST)
    except (ValueError, TypeError):
        return None


def _scrape_myfxbook(html: str) -> list[dict[str, Any]]:
    """myfxbook HTML에서 경제 일정을 파싱한다."""
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        return []

    table = tables[0]
    rows = table.find_all("tr")
    events: list[dict[str, Any]] = []

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 9:
            continue

        date_text = tds[0].get_text(strip=True)
        currency = tds[3].get_text(strip=True).upper()
        event_name = tds[4].get_text(strip=True)
        impact_text = tds[5].get_text(strip=True)
        previous = tds[6].get_text(strip=True)
        forecast = tds[7].get_text(strip=True)
        actual = tds[8].get_text(strip=True)

        if not event_name or not date_text:
            continue

        dt_kst = _parse_myfxbook_datetime(date_text)
        country_code, country_name = _CURRENCY_COUNTRY.get(currency, (currency, ""))

        events.append({
            "event_date": dt_kst.strftime("%Y-%m-%d") if dt_kst else None,
            "date_label": _format_date_label(dt_kst) if dt_kst else "",
            "time_label": dt_kst.strftime("%H:%M") if dt_kst else "",
            "event_at": dt_kst.isoformat() if dt_kst else None,
            "country_code": country_code or None,
            "country_name": country_name or None,
            "currency": currency or None,
            "importance": _parse_impact(impact_text),
            "event": event_name,
            "actual": actual or None,
            "forecast": forecast or None,
            "previous": previous or None,
        })

    return events


def _to_response_item(row: dict[str, Any]) -> dict[str, Any]:
    """DB 레코드를 API 응답 형식으로 변환한다."""
    # DB의 event_time은 이미 KST 기준으로 저장됨 → 그대로 사용
    time_label = row.get("event_time") or row.get("time_label") or ""
    date_label = row.get("date_label") or ""

    # date_label이 없으면 event_at에서 KST 변환 후 생성
    if not date_label and row.get("event_at"):
        try:
            dt = datetime.fromisoformat(row["event_at"]).astimezone(_KST)
            date_label = _format_date_label(dt)
            if not time_label:
                time_label = dt.strftime("%H:%M")
        except (ValueError, TypeError):
            pass

    event_name = row.get("event") or ""
    return {
        "event_id": row.get("id"),
        "date_label": date_label,
        "time_label": time_label,
        "country_code": row.get("country_code"),
        "country_name": row.get("country_name"),
        "currency": row.get("currency"),
        "importance": row.get("importance", 0),
        "event": event_name,
        "event_ko": _translate_event(event_name),
        "actual": row.get("actual"),
        "forecast": row.get("forecast"),
        "previous": row.get("previous"),
    }


async def _fetch_and_save() -> list[dict[str, Any]]:
    """myfxbook에서 경제 일정을 크롤링하여 DB에 저장하고 반환한다."""
    timeout = httpx.Timeout(ECON_CALENDAR_TIMEOUT_SEC)
    async with httpx.AsyncClient(
        timeout=timeout, follow_redirects=True, headers=_HEADERS
    ) as client:
        resp = await client.get(_MYFXBOOK_URL)
        if resp.status_code != 200:
            logger.warning("myfxbook HTTP %d", resp.status_code)
            return []

        events = _scrape_myfxbook(resp.text)
        if not events:
            return []

        # DB 저장 (event_date가 있는 것만)
        db_events = [e for e in events if e.get("event_date")]
        if db_events:
            try:
                upsert_economic_events(db_events)
            except Exception as e:
                logger.warning("경제 일정 DB 저장 실패: %s", e)

        return events


async def fetch_economic_calendar(
    refresh: bool = False,
) -> dict[str, Any]:
    global _cache, _cache_at

    if not refresh and _is_fresh():
        return {
            "source": "myfxbook",
            "items": _cache,
            "fetched_at": _cache_at.isoformat() if _cache_at else None,
            "cache_hit": True,
            "cache_ttl_sec": ECON_CALENDAR_TTL_SEC,
            "error": None,
        }

    try:
        # 1) 크롤링 → DB 저장
        await _fetch_and_save()

        # 2) DB에서 전체 조회 (과거 포함, 제한 없음)
        db_rows = get_economic_events()
        items = [_to_response_item(r) for r in db_rows]

        _cache = items
        _cache_at = datetime.now()

        return {
            "source": "myfxbook",
            "items": _cache,
            "fetched_at": _cache_at.isoformat(),
            "cache_hit": False,
            "cache_ttl_sec": ECON_CALENDAR_TTL_SEC,
            "error": None,
        }
    except Exception as e:
        logger.warning("경제 캘린더 수집 실패: %s", e, exc_info=True)

        # 크롤링 실패 시 DB fallback
        try:
            db_rows = get_economic_events()
            if db_rows:
                items = [_to_response_item(r) for r in db_rows]
                return {
                    "source": "myfxbook",
                    "items": items,
                    "fetched_at": datetime.now().isoformat(),
                    "cache_hit": False,
                    "cache_ttl_sec": ECON_CALENDAR_TTL_SEC,
                    "error": None,
                }
        except Exception:
            pass

        return {
            "source": "myfxbook",
            "items": [],
            "fetched_at": datetime.now().isoformat(),
            "cache_hit": False,
            "cache_ttl_sec": ECON_CALENDAR_TTL_SEC,
            "error": {"code": "fetch_error", "message": str(e)},
        }
