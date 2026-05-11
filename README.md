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

## 로컬 실행

```bash
python api.py
# 또는 uvicorn api:app --host 0.0.0.0 --port 8000
```
