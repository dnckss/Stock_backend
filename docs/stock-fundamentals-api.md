# Stock Fundamentals API 명세서

종목 펀더멘털 데이터를 제공하는 API. 토스증권 종목 상세 페이지와 유사한 6개 섹션 데이터를 반환한다.

---

## 엔드포인트 목록

| Method | Path | 설명 |
|--------|------|------|
| GET | `/api/stock/{ticker}/fundamentals` | 전체 펀더멘털 (6섹션 한 번에) |
| GET | `/api/stock/{ticker}/fundamentals/{section}` | 개별 섹션 조회 |

---

## 1. 전체 펀더멘털 조회

```
GET /api/stock/{ticker}/fundamentals
```

### Path Parameters

| 파라미터 | 타입 | 설명 | 예시 |
|----------|------|------|------|
| `ticker` | string | 종목 티커 심볼 (대소문자 무관) | `AAPL`, `msft` |

### Response (200 OK)

```json
{
  "ticker": "AAPL",
  "profile": { ... },
  "indicators": { ... },
  "profitability": { ... },
  "growth": { ... },
  "stability": { ... },
  "earnings": { ... }
}
```

### 캐싱

- 서버 측 5분(300초) TTL 캐시 적용
- 재무제표 데이터는 분기별 변경이므로 빈번한 폴링 불필요

---

## 2. 개별 섹션 조회

```
GET /api/stock/{ticker}/fundamentals/{section}
```

탭 전환이나 lazy-loading 시 사용. 캐시를 거치지 않고 항상 최신 데이터를 가져온다.

### Path Parameters

| 파라미터 | 타입 | 설명 | 유효값 |
|----------|------|------|--------|
| `ticker` | string | 종목 티커 심볼 | `AAPL` |
| `section` | string | 섹션 이름 | `profile`, `indicators`, `profitability`, `growth`, `stability`, `earnings` |

### Response (200 OK)

해당 섹션만 반환:

```json
{
  "ticker": "AAPL",
  "profitability": { "quarters": [...] }
}
```

### Error Response (400 Bad Request)

```json
{
  "detail": "유효한 섹션: earnings, growth, indicators, profitability, profile, stability"
}
```

---

## 섹션별 응답 스키마

### profile (기업 개요)

```json
{
  "name": "Apple Inc.",
  "sector": "Technology",
  "industry": "Consumer Electronics",
  "description": "Apple Inc. designs, manufactures, and markets...",
  "website": "https://www.apple.com",
  "employees": 150000,
  "officers": [
    { "name": "Mr. Timothy D. Cook", "title": "CEO & Director" },
    { "name": "Mr. Kevan Parekh", "title": "Senior VP & CFO" }
  ],
  "market_cap": 3828515864576,
  "market_cap_display": "$3.83T",
  "shares_outstanding": 14681140000,
  "country": "United States",
  "headquarters": "Cupertino, CA"
}
```

| 필드 | 타입 | 설명 | nullable |
|------|------|------|----------|
| `name` | string | 회사명 | Y |
| `sector` | string | 섹터 | Y |
| `industry` | string | 산업 | Y |
| `description` | string | 사업 설명 (영문) | Y |
| `website` | string | 홈페이지 URL | Y |
| `employees` | integer | 직원 수 | Y |
| `officers` | array | 임원 목록 (최대 5명) | N (빈 배열 가능) |
| `officers[].name` | string | 임원 이름 | N |
| `officers[].title` | string | 직책 | Y |
| `market_cap` | number | 시가총액 (USD, 원시값) | Y |
| `market_cap_display` | string | 시가총액 표시용 문자열 (`$3.83T`) | Y |
| `shares_outstanding` | number | 발행주식수 | Y |
| `country` | string | 국가 | Y |
| `headquarters` | string | 본사 소재지 (`City, State`) | Y |

---

### indicators (투자 지표)

```json
{
  "valuation": {
    "per": 33.01,
    "forward_per": 27.97,
    "psr": 8.79,
    "pbr": 43.43
  },
  "per_share": {
    "eps": 7.89,
    "bps": 6.0,
    "roe": 152.02
  },
  "dividends": {
    "dividend_yield": 0.4,
    "dividend_rate": 1.04,
    "payout_ratio": 13.04,
    "ex_dividend_date": "2026-02-09"
  },
  "financial_health": {
    "debt_ratio": 102.63,
    "current_ratio": 0.97,
    "interest_coverage_ratio": null
  }
}
```

#### valuation (가치평가)

| 필드 | 타입 | 설명 | 단위 |
|------|------|------|------|
| `per` | number | PER (주가수익비율, trailing) | 배 |
| `forward_per` | number | Forward PER (예상) | 배 |
| `psr` | number | PSR (주가매출비율) | 배 |
| `pbr` | number | PBR (주가순자산비율) | 배 |

#### per_share (수익)

| 필드 | 타입 | 설명 | 단위 |
|------|------|------|------|
| `eps` | number | EPS (주당순이익, trailing) | USD |
| `bps` | number | BPS (주당순자산) | USD |
| `roe` | number | ROE (자기자본이익률) | % |

#### dividends (배당)

| 필드 | 타입 | 설명 | 단위 |
|------|------|------|------|
| `dividend_yield` | number | 배당수익률 | % |
| `dividend_rate` | number | 주당 배당금 | USD |
| `payout_ratio` | number | 배당성향 | % |
| `ex_dividend_date` | string | 배당 기준일 | `YYYY-MM-DD` |

#### financial_health (재무건전성)

| 필드 | 타입 | 설명 | 단위 |
|------|------|------|------|
| `debt_ratio` | number | 부채비율 (총부채/자기자본) | % |
| `current_ratio` | number | 유동비율 (유동자산/유동부채) | 배 |
| `interest_coverage_ratio` | number | 이자보상비율 (영업이익/이자비용) | 배 |

> **참고**: 일부 회사(예: AAPL)는 이자비용 데이터가 없어 `interest_coverage_ratio`가 `null`일 수 있음

---

### profitability (수익성)

분기별 매출, 순이익, 순이익률 트렌드. 차트 렌더링용.

```json
{
  "quarters": [
    {
      "date": "2024-12-31",
      "revenue": 124300000000,
      "net_income": 36330000000,
      "net_margin": 29.23,
      "net_income_yoy": 15.87
    }
  ]
}
```

| 필드 | 타입 | 설명 | 단위 |
|------|------|------|------|
| `date` | string | 분기 종료일 | `YYYY-MM-DD` |
| `revenue` | number | 매출 | USD |
| `net_income` | number | 순이익 | USD |
| `net_margin` | number | 순이익률 (순이익/매출) | % |
| `net_income_yoy` | number | 순이익 YoY 성장률 | % |

> `quarters` 배열은 **오래된 순** 정렬 (차트 X축: 왼쪽=과거, 오른쪽=최근).
> YoY는 4분기 전 데이터가 있을 때만 계산. 없으면 `null`.
> ETF 등 재무제표가 없는 종목은 빈 배열 반환.

---

### growth (성장성)

분기별 영업이익, 영업이익률, YoY 성장률 트렌드.

```json
{
  "quarters": [
    {
      "date": "2024-12-31",
      "operating_income": 42832000000,
      "operating_margin": 34.46,
      "operating_income_yoy": 18.72
    }
  ]
}
```

| 필드 | 타입 | 설명 | 단위 |
|------|------|------|------|
| `date` | string | 분기 종료일 | `YYYY-MM-DD` |
| `operating_income` | number | 영업이익 | USD |
| `operating_margin` | number | 영업이익률 (영업이익/매출) | % |
| `operating_income_yoy` | number | 영업이익 YoY 성장률 | % |

---

### stability (안정성)

분기별 자본, 부채, 부채비율 트렌드.

```json
{
  "quarters": [
    {
      "date": "2025-12-31",
      "total_equity": 88190000000,
      "total_debt": 90509000000,
      "debt_ratio": 102.63
    }
  ]
}
```

| 필드 | 타입 | 설명 | 단위 |
|------|------|------|------|
| `date` | string | 분기 종료일 | `YYYY-MM-DD` |
| `total_equity` | number | 자기자본 (Stockholders Equity) | USD |
| `total_debt` | number | 총부채 | USD |
| `debt_ratio` | number | 부채비율 (총부채/자기자본) | % |

> 일부 분기에 데이터가 없으면 해당 필드가 `null`

---

### earnings (실적)

실적 발표일, EPS 히스토리, 애널리스트 컨센서스.

```json
{
  "next_earnings_date": "2026-04-30",
  "history": [
    {
      "date": "2026-01-29",
      "eps_actual": 2.84,
      "eps_estimate": 2.67,
      "surprise_pct": 6.34
    }
  ],
  "analyst_count": 40,
  "target_mean_price": 296.33,
  "target_high_price": null,
  "target_low_price": null,
  "recommendation": "buy"
}
```

| 필드 | 타입 | 설명 | 단위 |
|------|------|------|------|
| `next_earnings_date` | string | 다음 실적 발표 예정일 | `YYYY-MM-DD` |
| `history` | array | EPS 히스토리 (최신순, 최대 8건) | - |
| `history[].date` | string | 실적 발표일 | `YYYY-MM-DD` |
| `history[].eps_actual` | number | 실제 EPS | USD |
| `history[].eps_estimate` | number | 예상 EPS | USD |
| `history[].surprise_pct` | number | 서프라이즈 | % |
| `analyst_count` | integer | 분석 애널리스트 수 | 명 |
| `target_mean_price` | number | 평균 목표가 | USD |
| `target_high_price` | number | 최고 목표가 | USD |
| `target_low_price` | number | 최저 목표가 | USD |
| `recommendation` | string | 투자 의견 | `buy`/`hold`/`sell` 등 |

---

## 에러 응답

| 코드 | 상황 | 응답 |
|------|------|------|
| 400 | 잘못된 섹션 이름 | `{"detail": "유효한 섹션: ..."}` |
| 500 | yfinance 조회 실패 | `{"detail": "펀더멘털 조회 실패: ..."}` |

> ETF, 잘못된 티커 등 재무 데이터가 없는 경우 → **에러 없이** 빈 배열/null 반환

---

## 프론트엔드 사용 가이드

### 추천 호출 패턴

1. **초기 로드**: `/api/stock/{ticker}/fundamentals` 로 전체 데이터 한 번에 가져오기
2. **탭 전환 시 갱신**: `/api/stock/{ticker}/fundamentals/{section}` 으로 해당 섹션만 갱신

### null 처리

모든 필드가 `null`일 수 있으므로 프론트에서 fallback UI 필요:
- 숫자 null → `-` 또는 `N/A`
- 문자열 null → 미표시
- 빈 quarters 배열 → "데이터 없음" 메시지

### 단위 참고

- `revenue`, `net_income`, `operating_income`, `total_equity`, `total_debt`: **USD 원시값** → 프론트에서 포맷 필요 (예: `$124.3B`)
- `market_cap`: USD 원시값. `market_cap_display`는 포맷 완료 문자열
- `*_margin`, `*_yoy`, `debt_ratio`, `roe`, `dividend_yield`, `payout_ratio`: **퍼센트(%)** → 숫자 그대로 표시 + `%` 붙이기
- `per`, `psr`, `pbr`: **배수** → 숫자 + `배` 표시
- `current_ratio`, `interest_coverage_ratio`: **배수** → 숫자 + `배` 표시
