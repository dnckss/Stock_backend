# Economic Calendar API Spec

대시보드 좌측 하단 "경제 일정" 위젯에서 사용할 API 명세입니다.

## Endpoint

- `GET /api/economic-calendar`

## Query Parameters

- `refresh` (optional, default: `0`)
  - `0`: TTL 캐시 사용
  - `1`: 캐시 무시하고 즉시 재크롤링
- `limit` (optional, default: `20`)
  - 반환할 이벤트 개수
  - 최소 `1`
  - 최대값은 서버 설정 `ECON_CALENDAR_MAX_ITEMS`(기본 `50`)

### Example Request

- `/api/economic-calendar?limit=15`
- `/api/economic-calendar?refresh=1&limit=30`

## Response Shape

```json
{
  "source": "investing",
  "items": [
    {
      "event_id": "1234567",
      "date_label": "2026년 3월 27일 금요일",
      "time_label": "15:00",
      "country_code": "US",
      "country_name": "미국",
      "currency": "USD",
      "importance": 3,
      "event": "비농업부문 고용자수",
      "actual": "220K",
      "forecast": "205K",
      "previous": "198K"
    }
  ],
  "fetched_at": "2026-03-27T10:00:00.000000",
  "cache_hit": false,
  "cache_ttl_sec": 600,
  "error": null
}
```

## Field Details

- `source`: 데이터 소스 식별자 (`investing`)
- `items`: 경제 일정 목록
  - `event_id`: 이벤트 식별자(없을 수 있음)
  - `date_label`: 원본 날짜 라벨 문자열
  - `time_label`: 이벤트 시간 라벨 문자열
  - `country_code`: 국가코드(예: `US`, `KR`)
  - `country_name`: 국가명(없을 수 있음)
  - `currency`: 통화 코드(예: `USD`, `EUR`)
  - `importance`: 중요도 정수 (`0~3`)
  - `event`: 이벤트명
  - `actual`, `forecast`, `previous`: 발표/예측/이전 값 (미발표 시 `null`)
- `fetched_at`: 서버 수집 시각(ISO 8601)
- `cache_hit`: 캐시 사용 여부
- `cache_ttl_sec`: 서버 캐시 TTL(초)
- `error`: 에러 정보 또는 `null`

## Error Contract

요청 자체는 200으로 내려가며, 크롤링 실패 시 `error`에 상세 정보를 담습니다.

```json
{
  "source": "investing",
  "items": [],
  "fetched_at": "2026-03-27T10:01:12.123456",
  "cache_hit": false,
  "cache_ttl_sec": 600,
  "error": {
    "code": "http_error",
    "message": "HTTP 403",
    "status": 403
  }
}
```

서비스는 내부적으로 아래 순서로 수집합니다.

1. Investing 캘린더 서비스 엔드포인트(XHR JSON) 호출
2. 실패 시 경제 캘린더 HTML 파싱으로 폴백

따라서 에러 코드는 다음과 같은 접두를 가질 수 있습니다.

- `service_*`: 내부 서비스 엔드포인트 처리 실패
- `html_*`: HTML 폴백 처리 실패
- `empty_parse`: 두 경로 모두 파싱 결과가 빈 경우

또는:

```json
{
  "error": {
    "code": "crawl_error",
    "message": "..."
  }
}
```

## Frontend Rendering Guide

- 기본 정렬: 응답 순서 그대로 사용(서버 파싱 순)
- 배지 규칙:
  - `importance=3` -> High
  - `importance=2` -> Medium
  - `importance<=1` -> Low
- 새로고침 UX:
  - 일반 조회: `refresh=0`
  - 사용자 수동 새로고침 버튼 클릭 시: `refresh=1`
- 예외 처리:
  - `items.length === 0` 이고 `error != null`이면 "데이터 수집 지연" 안내 문구 노출
  - `items.length === 0` 이고 `error == null`이면 "표시할 일정 없음" 처리
