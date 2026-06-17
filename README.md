---
title: Woochan AI Quant Terminal API
emoji: 📊
colorFrom: blue
colorTo: indigo
sdk: docker
app_port: 7860
pinned: false
---

# Woochan AI Quant Terminal API

FastAPI 백엔드 — 시장·종목·전략(스캐너/스트래티지스트)을 WebSocket 으로 실시간 마켓
요약 + 뉴스 피드·기사 크롤링·LLM 한국어 분석·경제 캘린더 등을 제공한다.

자세한 구조와 운영 규칙은 [`CLAUDE.md`](./CLAUDE.md) 참조.

## 환경변수 (Settings → Repository secrets)

| Key | 설명 |
|---|---|
| `SUPABASE_URL` | Supabase 프로젝트 URL |
| `SUPABASE_KEY` | Supabase API key |
| `OPENAI_API_KEY` | OpenAI API key |

선택 (모델/타임아웃 등 튜닝): `STRATEGIST_OPENAI_MODEL`,
`STRATEGIST_REASONING_EFFORT`, `MIN_TOP_PICKS_FRESH`,
`ECON_CALENDAR_INTERVAL_SEC` 등 — `config.py` 참고.

### API 보호 (Public 배포 시 권장)

| Key | 설명 |
|---|---|
| `CORS_ALLOW_ORIGINS` | 허용 origin 화이트리스트(콤마). 미설정 시 `*` |
| `API_ACCESS_KEY` | 설정 시 `/api/*` 요청에 헤더 일치 필수. 비우면 비활성 |
| `API_KEY_HEADER_NAME` | API 키 헤더명 (기본 `X-API-Key`) |
| `RATE_LIMIT_DEFAULT_MAX` / `RATE_LIMIT_LLM_MAX` | per-IP 분당 한도 (기본 120 / 20) |

> 레이트리밋은 기본 활성(per-IP). **비용의 진짜 방어선은 OpenAI 대시보드의 하드 스펜딩 캡**이다 — Public 배포 시 반드시 설정할 것.

## 로컬 실행

```bash
python api.py
# 또는 uvicorn api:app --host 0.0.0.0 --port 8000
```
