import math
import sqlite3
import pandas as pd
from config import DB_PATH


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
            report TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ticker ON analysis_results (ticker)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON analysis_results (created_at)")
    conn.commit()
    conn.close()


def save_candidates(candidates: list):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for item in candidates:
        cur.execute(
            """
            INSERT INTO analysis_results
                (ticker, price_return, sentiment, divergence, signal, report)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                item["ticker"],
                item["return"],
                item["sentiment"],
                item["divergence"],
                item["signal"],
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
        SELECT price_return, sentiment, divergence, signal, created_at
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
