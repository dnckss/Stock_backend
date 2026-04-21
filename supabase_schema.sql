-- Supabase SQL Editor에서 실행하세요.
-- 기존 SQLite 스키마를 PostgreSQL로 변환한 테이블 생성 SQL입니다.

-- 1. analysis_results 테이블
CREATE TABLE IF NOT EXISTS analysis_results (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ticker TEXT NOT NULL,
    price DOUBLE PRECISION,
    volume BIGINT,
    daily_json TEXT,
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

-- 3. economic_events 테이블
CREATE TABLE IF NOT EXISTS economic_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_date DATE NOT NULL,
    event_time TEXT,
    event_at TIMESTAMPTZ,
    country_code TEXT,
    country_name TEXT,
    currency TEXT,
    importance INTEGER DEFAULT 0,
    event TEXT NOT NULL,
    actual TEXT,
    forecast TEXT,
    previous TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (event_date, event_time, event, currency)
);

CREATE INDEX IF NOT EXISTS idx_econ_event_date ON economic_events (event_date);
CREATE INDEX IF NOT EXISTS idx_econ_event_at ON economic_events (event_at);

ALTER TABLE economic_events ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all for anon" ON economic_events FOR ALL USING (true) WITH CHECK (true);

-- 4. news_items 테이블 (뉴스 피드 항목)
CREATE TABLE IF NOT EXISTS news_items (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url_hash TEXT NOT NULL UNIQUE,
    url TEXT NOT NULL,
    title TEXT NOT NULL,
    publisher TEXT,
    ticker TEXT,
    "timestamp" BIGINT,
    sentiment_score DOUBLE PRECISION,
    sentiment_label TEXT,
    sentiment_polarity TEXT,
    sentiment_ko TEXT,
    confidence DOUBLE PRECISION,
    has_article BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_items_ticker ON news_items (ticker);
CREATE INDEX IF NOT EXISTS idx_news_items_timestamp ON news_items ("timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_news_items_created ON news_items (created_at DESC);

ALTER TABLE news_items ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all for anon" ON news_items FOR ALL USING (true) WITH CHECK (true);

-- 5. strategy_history 테이블 (AI 전략 추천 이력)
CREATE TABLE IF NOT EXISTS strategy_history (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ticker TEXT NOT NULL,
    direction TEXT,
    confidence TEXT,
    strategy_type TEXT,
    entry_low DOUBLE PRECISION,
    entry_high DOUBLE PRECISION,
    stop_loss DOUBLE PRECISION,
    stop_loss_pct DOUBLE PRECISION,
    target1_price DOUBLE PRECISION,
    target2_price DOUBLE PRECISION,
    risk_reward_ratio DOUBLE PRECISION,
    rationale TEXT,
    market_regime TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strategy_ticker ON strategy_history (ticker);
CREATE INDEX IF NOT EXISTS idx_strategy_created ON strategy_history (created_at DESC);

ALTER TABLE strategy_history ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all for anon" ON strategy_history FOR ALL USING (true) WITH CHECK (true);

-- 6. econ_event_details 테이블 (경제 지표 상세 정보 캐시)
CREATE TABLE IF NOT EXISTS econ_event_details (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_name TEXT NOT NULL UNIQUE,
    detail_json TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE econ_event_details ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all for anon" ON econ_event_details FOR ALL USING (true) WITH CHECK (true);

-- 7. Row Level Security (RLS) 비활성화 (서버 사이드에서만 접근하므로)
-- 필요 시 Supabase 대시보드에서 RLS를 활성화하고 정책을 추가하세요.
ALTER TABLE analysis_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE news_articles ENABLE ROW LEVEL SECURITY;

-- service_role 키 사용 시 RLS 바이패스됨.
-- anon 키 사용 시 아래 정책 필요:
CREATE POLICY "Allow all for anon" ON analysis_results FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all for anon" ON news_articles FOR ALL USING (true) WITH CHECK (true);

-- ---------------------------------------------------------------------------
-- 8. chat_sessions / chat_messages / chat_files (AI 챗봇 영구 저장)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS chat_sessions (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '새 채팅',
    last_message_preview TEXT,
    message_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_sessions_updated ON chat_sessions (updated_at DESC);

ALTER TABLE chat_sessions ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all for anon" ON chat_sessions FOR ALL USING (true) WITH CHECK (true);

CREATE TABLE IF NOT EXISTS chat_messages (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES chat_sessions(id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    content TEXT NOT NULL,
    attachments_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_messages_session ON chat_messages (session_id, created_at);

ALTER TABLE chat_messages ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all for anon" ON chat_messages FOR ALL USING (true) WITH CHECK (true);

CREATE TABLE IF NOT EXISTS chat_files (
    id TEXT PRIMARY KEY,
    session_id TEXT REFERENCES chat_sessions(id) ON DELETE SET NULL,
    filename TEXT NOT NULL,
    content_type TEXT,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    extracted_text TEXT NOT NULL DEFAULT '',
    char_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_files_session ON chat_files (session_id);
CREATE INDEX IF NOT EXISTS idx_chat_files_created ON chat_files (created_at DESC);

ALTER TABLE chat_files ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all for anon" ON chat_files FOR ALL USING (true) WITH CHECK (true);
