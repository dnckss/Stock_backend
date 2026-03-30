## 프로젝트 한 줄 요약

**Woochan AI Quant Terminal API** — FastAPI 기반 백엔드. 시장·종목·전략(스캐너/스트래티지스트)과 WebSocket으로 실시간 마켓 요약을 제공하고, **뉴스 피드·기사 크롤링·LLM 한국어 분석·경제 캘린더** 등 뉴스/매크로 기능을 확장 중이다.

## 기술 스택

- Python, **FastAPI**, `uvicorn`
- DB: Supabase (PostgreSQL) — `config.SUPABASE_URL`, `config.SUPABASE_KEY`
- 데이터: **yfinance** (시세·뉴스), 선택적 **OpenAI** (뉴스 분석·스트래티지스트 등)
- 감성: **FinBERT** (`services/finbert.py` 등) + 뉴스용 정규화 레이어 `services/news_sentiment.py`

## 진입점·라우팅

- `api.py`: `FastAPI` 앱, `lifespan`에서 `init_db()`, 기동 시 매크로·뉴스 피드 시드, 백그라운드 태스크(`run_analysis_loop`, `run_macro_loop`, `run_price_tick_loop`)
- 라우터: `routers/market.py`, `stock.py`, `strategy.py`, **`news.py`**
- WebSocket: `GET /ws/market` — `latest_cache` 스냅샷 브로드캐스트

## 설정의 단일 출처

- **`config.py`**: Supabase 연결, OpenAI 모델/타임아웃, 스캐너/주기, Finviz/뉴스/경제캘린더/기사 크롤링 관련 상수·환경변수 기본값
- 새 의미 있는 숫자·URL·TTL은 여기에 두고 다른 모듈은 import로 사용할 것 (하드코딩 분산 금지)

## 뉴스·캘린더 관련 (최근 작업 중심)

### 뉴스 피드 (`services/news_feed.py`)

- yfinance `Ticker.get_news()`로 티커별 뉴스 수집 → **FinBERT 배치**로 감성 → 최신순 정렬, `NEWS_FEED_MAX_ITEMS` / `NEWS_FEED_TTL_SEC` 캐시
- 기동 시 `api.py`에서 `build_news_feed(NEWS_FALLBACK_TICKERS)`로 캐시 시드

### 뉴스 상세 API (`routers/news.py` → `services/news_article.py`)

- `GET /api/news?url=...&refresh=0|1&analyze=0|1`
- URL SHA256으로 Supabase 캐시 (`NEWS_ARTICLE_CACHE_TTL_SEC`)
- 크롤링: `services/article_crawler.py` (`fetch_and_extract`)
- 캐시 히트 시에도 제목 기준 **FinBERT** + 선택 시 **LLM 분석** 보강
- `services/news_sentiment.py`: FinBERT 라벨과 LLM `impact.direction`을 UI용 3분류(`positive`/`negative`/`neutral`)로 정규화, `add_normalized_impact_fields`로 impact에 한글 방향 필드 추가

### 뉴스 LLM 분석 (`services/news_analysis.py`)

- OpenAI로 **한국어 요약 + 시장 영향 JSON** (`ko_summary`, `impact`: sectors, themes, direction, confidence, reason_ko 등)
- `NEWS_ANALYSIS_*` 설정, 타임아웃·폴백 모델, 본문 `_truncate_article` (`NEWS_ANALYSIS_INPUT_MAX_CHARS`)
- GPT-5 등 temperature 미지원 모델 분기 (`_model_omits_temperature`)

### 경제 캘린더 (`services/economic_calendar.py`)

- **Investing.com** (기본 `kr.investing.com`) XHR + HTML 파싱, 프로세스 내 TTL 캐시 (`ECON_CALENDAR_*`)
- `GET /api/economic-calendar?refresh=0|1&limit=N`
- 프런트/계약 참고: `docs/economic-calendar-api.md`

## 기타 핵심 모듈 (요약)

- `services/engine.py`: 주기 스캔·매크로·분봉 틱 루프
- `services/scanner.py`, `strategist.py`, `websocket.py`, `crud.py`
- `services/sentiment.py` / `finbert.py`: Finviz 등과 연계된 감성 (뉴스 피드는 `news_feed` + FinBERT 경로)

## 에이전트 작업 시 권장 사항

1. **패턴 일치**: 기존 `async`/`try`/`logger` 스타일, `sanitize_for_json`, HTTP 예외 처리 방식 유지
2. **에러 경로**: 외부 HTTP·크롤링·OpenAI는 실패 시 로그 + 안전한 기본값·캐시 폴백
3. **문서**: 사용자가 요청하지 않은 한 새 `.md` 파일을 임의로 추가하지 말 것 (기존 `docs/`는 예외적으로 유지)
4. **DB**: Supabase(PostgreSQL) 사용 — `.env`에 `SUPABASE_URL`, `SUPABASE_KEY` 설정 필요

### 1. 기존 프로젝트 패턴 존중
Claude는 프로젝트의 기존 관례(폴더 구조, 디자인 패턴, 네이밍 규칙, 리팩토링 표준, CRUD 관례 등)를 무시하면 안 됩니다.  
생성되는 코드는 프로젝트가 이미 정의한 아키텍처/코딩 스타일에 맞아야 합니다.

새 코드를 작성하기 전, Claude는 현재 프로젝트 구조와 구현 패턴을 먼저 분석하여 **새 코드가 기존 패턴에 자연스럽게 녹아들도록** 해야 하며, 임의로 새로운 규칙/구조를 도입하면 안 됩니다.

AI가 생성한 코드는 최종적으로 **개발자가 검토**하여, 프로젝트 규칙을 준수하는지 확인해야 합니다.

### 2. 단일 소스 오브 트루스(Single Source of Truth) 원칙 준수
Claude는 핵심 모델/타입/상수/설정값을 여러 파일에 중복 정의하면 안 됩니다.  
공유 의미를 갖는 값/타입(예: Product 모델)은 전용 “단일 소스” 위치(예: 모델 파일, 설정 모듈, 공용 타입)에 **정확히 한 번만** 정의되어야 합니다.

그 외 모든 모듈은 이를 **import 해서 재사용**해야 하며, 의미 있는 정의를 중복하는 행위는 금지됩니다.

### 3. 상수/상태 값 하드코딩 금지
Claude는 숫자 리터럴, 문자열 상태값, 반복되는 값들을 코드 곳곳에 흩뿌리면 안 됩니다.  
값이 의미를 갖거나 여러 곳에서 반복된다면, 반드시 전역 config/constants 모듈에 정의하고 import 해서 재사용해야 합니다.

하드코딩은 불일치/누락/정책 충돌을 유발하므로 금지됩니다.

### 4. 견고한 에러/예외 처리 구현
Claude는 “정상 동작(해피 패스)”만 가정한 코드를 작성하면 안 됩니다.  
모든 함수/컴포넌트/네트워크 상호작용은 아래 상황을 반드시 고려해야 합니다.

- 잘못된 사용자 입력
- 빠른 연속 실행(중복 클릭/중복 요청)
- 네트워크 요청 실패
- 예기치 않은 서버 응답
- 리소스/권한/가용성 문제

에러 처리는 try/catch, fallback 로직, safeguard, 에러 바운더리, 사용자 피드백(토스트/알럿/disabled 상태 등)을 포함해야 합니다.  
`console.log`에 의존하는 방식은 허용되지 않으며, `any`는 임시 회피 수단으로 남용하면 안 됩니다.

**성공이 아니라 실패를 기본으로 설계**해야 합니다.

### 5. 책임 분리(Separation of Responsibilities) 유지
하나의 함수는 하나의 책임만 수행해야 합니다.  
Claude는 UI, 데이터 패칭, 비즈니스 로직이 한 파일에 섞인 “갓 컴포넌트”를 만들면 안 됩니다.

아래 분리를 엄격히 따릅니다.
- UI 컴포넌트 → 렌더링 & 인터랙션
- Hooks/Services → 데이터 패칭, 비즈니스 규칙, 사이드이펙트
- Utility 모듈 → 재사용 가능한 공용 로직

Claude는 코드를 역할별로 작고 명확한 단위로 분해해, 경계가 분명하도록 구성해야 합니다.

### 6. 공용 코드 중앙화 및 재사용
재사용 가능한 유틸/공용 컴포넌트/헬퍼/어댑터/훅은 파일마다 복사해서 만들면 안 됩니다.  
반드시 프로젝트의 shared(또는 동등한) 위치에 중앙화하고 import로 재사용해야 합니다.

유사 로직/중복 구현은 금지되며, 공용 모듈은 “재사용 가능한 빌딩 블록”으로 유지되어야 합니다.

### 요약(반드시 준수)
Claude가 코드를 생성/수정할 때 반드시:
- 기존 프로젝트 규칙/아키텍처 패턴을 따른다.
- 공용 값/타입은 단일 소스로 유지한다.
- 하드코딩/중복을 피하고 상수화를 한다.
- 방어적인 에러/예외 처리를 구현한다.
- UI / 데이터 / 비즈니스 로직 책임을 분리한다.
- 공용 코드를 중앙화하여 재사용한다.

이 규칙을 위반한 코드는 이 프로젝트에서 **허용되지 않습니다**.

## Coding Rules (프로젝트 적용 규칙)
- 기존 프로젝트의 폴더 구조, 네이밍, 디자인 패턴을 반드시 따른다.
- 새로운 코드를 작성하기 전에 현재 구조와 구현 패턴을 먼저 파악한다.
- 공통 모델, 타입, 상수, 설정값은 한 곳에서만 정의하고 import 해서 사용한다.
  - 타입 단일 소스: `types/dashboard.ts`
  - 상수 단일 소스: `lib/constants.ts`
  - API/변환 단일 소스: `lib/api.ts`
- 의미 있는 값이나 반복되는 값은 하드코딩하지 않고 `lib/constants.ts`로 분리한다.
- 성공 케이스만 가정하지 말고, 입력 오류, 중복 실행, 요청 실패, 예외 응답, 권한 문제를 항상 처리한다.
- `console.log`에 의존한 에러 처리나 `any` 남용을 지양한다. (`unknown` 후 narrowing 우선)
- UI, 데이터 처리, 비즈니스 로직을 한 파일에 섞지 않고 역할별로 분리한다.
  - UI: `components/`
  - 데이터/사이드이펙트: `hooks/` (예: `hooks/useMarketData.ts`)
  - API/변환/포맷/에러: `lib/api.ts`
- 재사용 가능한 유틸, 훅, 헬퍼, 공통 컴포넌트는 중앙화하여 중복 없이 관리한다.
- AI가 생성한 코드도 최종적으로 개발자가 검토하고 프로젝트 규칙 준수 여부를 확인한다.

이 원칙을 지키지 않은 코드는 허용되지 않는다.

## 커밋 규칙
항상 작업을 끝내고 난 뒤에 본인이 한 작업에 대해서 커밋을 한후 푸쉬를 진행한다.
커밋 메세지는 한눈에 알아볼수있으면서 짧게 작성한다.

## 로컬 실행

```bash
python api.py
# 또는 uvicorn api:app --host 0.0.0.0 --port 8000
