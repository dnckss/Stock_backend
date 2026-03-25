import math
import sqlite3
import pandas as pd
from datetime import timedelta
from datetime import datetime

from config import DB_PATH, STRATEGIST_LATEST_SCAN_WINDOW_MINUTES, NEWS_ARTICLE_CACHE_TTL_SEC


def sanitize_for_json(obj):
    """dict/list 내 float NaN·Inf를 None으로 치환해 JSON 직렬화 시 500 방지."""
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS analysis_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            price_return REAL,
            sentiment REAL,
            divergence REAL,
            signal TEXT,
            signal_source TEXT,
            eps_actual REAL,
            eps_estimate REAL,
            earnings_surprise_pct REAL,
            report TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ticker ON analysis_results (ticker)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON analysis_results (created_at)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS news_articles (
            url_hash TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            title TEXT,
            publisher TEXT,
            ticker TEXT,
            timestamp INTEGER,
            article_text TEXT,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_news_ticker ON news_articles (ticker)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_news_fetched_at ON news_articles (fetched_at)")

    _migrate_columns(cur)

    conn.commit()
    conn.close()


def _migrate_columns(cur: sqlite3.Cursor):
    """기존 DB에 새 컬럼이 없으면 추가한다."""
    cur.execute("PRAGMA table_info(analysis_results)")
    existing = {row[1] for row in cur.fetchall()}

    new_columns = [
        ("signal_source", "TEXT"),
        ("eps_actual", "REAL"),
        ("eps_estimate", "REAL"),
        ("earnings_surprise_pct", "REAL"),
    ]
    for col_name, col_type in new_columns:
        if col_name not in existing:
            cur.execute(f"ALTER TABLE analysis_results ADD COLUMN {col_name} {col_type}")


def save_candidates(candidates: list):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for item in candidates:
        cur.execute(
            """
            INSERT INTO analysis_results
                (ticker, price_return, sentiment, divergence, signal,
                 signal_source, eps_actual, eps_estimate, earnings_surprise_pct, report)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item["ticker"],
                item["return"],
                item["sentiment"],
                item["divergence"],
                item["signal"],
                item.get("signal_source"),
                item.get("eps_actual"),
                item.get("eps_estimate"),
                item.get("earnings_surprise_pct"),
                item.get("report"),
            ),
        )
    conn.commit()
    conn.close()


def _safe_value(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _sanitize(df: pd.DataFrame) -> list:
    records = df.to_dict(orient="records")
    return [{k: _safe_value(v) for k, v in row.items()} for row in records]


def get_latest_report(ticker: str) -> dict | None:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        """
        SELECT * FROM analysis_results
        WHERE ticker = ? AND report IS NOT NULL
        ORDER BY created_at DESC LIMIT 1
        """,
        conn,
        params=(ticker.upper(),),
    )
    conn.close()
    rows = _sanitize(df)
    return rows[0] if rows else None


def get_history(ticker: str, days: int = 30) -> list:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        """
        SELECT price_return, sentiment, divergence, signal,
               signal_source, eps_actual, eps_estimate, earnings_surprise_pct,
               created_at
        FROM analysis_results
        WHERE ticker = ? AND created_at >= datetime('now', ?)
        ORDER BY created_at DESC
        """,
        conn,
        params=(ticker.upper(), f"-{days} days"),
    )
    conn.close()
    return _sanitize(df)


def get_all_records(limit: int = 100) -> list:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT * FROM analysis_results ORDER BY created_at DESC LIMIT ?",
        conn,
        params=(limit,),
    )
    conn.close()
    return _sanitize(df)


def get_cached_news_article(url_hash: str) -> dict | None:
    """
    url_hash로 뉴스 본문 캐시를 조회한다.
    캐시 TTL(NEWS_ARTICLE_CACHE_TTL_SEC)이 지나면 None 처리한다.
    """
    if not url_hash:
        return None
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        """
        SELECT url_hash, url, title, publisher, ticker, timestamp, article_text, fetched_at
        FROM news_articles
        WHERE url_hash = ?
        LIMIT 1
        """,
        conn,
        params=(url_hash,),
    )
    conn.close()
    rows = _sanitize(df)
    if not rows:
        return None

    row = rows[0]
    fetched_at = row.get("fetched_at")
    try:
        fetched_dt = pd.to_datetime(fetched_at, errors="coerce")
    except Exception:
        fetched_dt = pd.NaT

    if fetched_dt is pd.NaT:
        return row

    age_sec = (datetime.now() - fetched_dt.to_pydatetime()).total_seconds()
    if age_sec > NEWS_ARTICLE_CACHE_TTL_SEC:
        return None
    return row


def upsert_news_article(item: dict) -> None:
    """
    news_articles에 url_hash 기준 upsert.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO news_articles (url_hash, url, title, publisher, ticker, timestamp, article_text, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(url_hash) DO UPDATE SET
            url=excluded.url,
            title=excluded.title,
            publisher=excluded.publisher,
            ticker=excluded.ticker,
            timestamp=excluded.timestamp,
            article_text=excluded.article_text,
            fetched_at=CURRENT_TIMESTAMP
        """,
        (
            item.get("url_hash"),
            item.get("url"),
            item.get("title"),
            item.get("publisher"),
            item.get("ticker"),
            item.get("timestamp"),
            item.get("article_text"),
        ),
    )
    conn.commit()
    conn.close()


def get_latest_scan_records(
    window_minutes: int = STRATEGIST_LATEST_SCAN_WINDOW_MINUTES,
) -> list[dict]:
    """
    analysis_results에서 가장 최근에 기록된 스캔 시점(max(created_at))을 기준으로
    window_minutes 범위 내 기록을 가져온 뒤,
    티커별로 created_at이 가장 최신 1건만 남긴다.
    """
    conn = sqlite3.connect(DB_PATH)
    max_row = conn.execute("SELECT MAX(created_at) FROM analysis_results").fetchone()
    conn.close()

    if not max_row or not max_row[0]:
        return []

    max_ts = pd.to_datetime(max_row[0], errors="coerce")
    if max_ts is pd.NaT:
        return []

    # SQLite의 created_at 기본 포맷은 보통 "YYYY-MM-DD HH:MM:SS" (space 구분)이다.
    # cutoff를 ISO8601("...T...")로 넘기면 문자열 비교가 어긋나 빈 결과가 나올 수 있어,
    # DB 포맷에 맞춰 통일한다.
    cutoff_dt = max_ts - timedelta(minutes=window_minutes)
    cutoff = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        """
        SELECT *
        FROM analysis_results
        WHERE created_at >= ?
        ORDER BY created_at DESC
        """,
        conn,
        params=(cutoff,),
    )
    conn.close()

    if df.empty:
        return []

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df = df.sort_values("created_at", ascending=False).drop_duplicates(
        subset=["ticker"], keep="first"
    )
    return _sanitize(df)
