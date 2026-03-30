-- Supabase SQL Editor에서 실행하세요.
-- 기존 SQLite 스키마를 PostgreSQL로 변환한 테이블 생성 SQL입니다.

-- 1. analysis_results 테이블
CREATE TABLE IF NOT EXISTS analysis_results (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ticker TEXT NOT NULL,
    price_return DOUBLE PRECISION,
    sentiment DOUBLE PRECISION,
    divergence DOUBLE PRECISION,
    signal TEXT,
    signal_source TEXT,
    eps_actual DOUBLE PRECISION,
    eps_estimate DOUBLE PRECISION,
    earnings_surprise_pct DOUBLE PRECISION,
    report TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ticker ON analysis_results (ticker);
CREATE INDEX IF NOT EXISTS idx_created_at ON analysis_results (created_at);

-- 2. news_articles 테이블
CREATE TABLE IF NOT EXISTS news_articles (
    url_hash TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    title TEXT,
    publisher TEXT,
    author TEXT,
    ticker TEXT,
    "timestamp" BIGINT,
    article_text TEXT,
    article_markdown TEXT,
    media_json TEXT,
    domains_json TEXT,
    extraction_status TEXT,
    error_reason TEXT,
    http_status INTEGER,
    final_url TEXT,
    canonical_url TEXT,
    analysis_json TEXT,
    analysis_at TIMESTAMPTZ,
    fetched_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_ticker ON news_articles (ticker);
CREATE INDEX IF NOT EXISTS idx_news_fetched_at ON news_articles (fetched_at);

-- 3. Row Level Security (RLS) 비활성화 (서버 사이드에서만 접근하므로)
-- 필요 시 Supabase 대시보드에서 RLS를 활성화하고 정책을 추가하세요.
ALTER TABLE analysis_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE news_articles ENABLE ROW LEVEL SECURITY;

-- service_role 키 사용 시 RLS 바이패스됨.
-- anon 키 사용 시 아래 정책 필요:
CREATE POLICY "Allow all for anon" ON analysis_results FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all for anon" ON news_articles FOR ALL USING (true) WITH CHECK (true);
