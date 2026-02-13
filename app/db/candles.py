from datetime import datetime
import pandas as pd
from app.db.neon import exec_sql, query_one

def get_last_ts(pair: str, tf: str):
    row = query_one("SELECT last_ts FROM candle_state WHERE pair=%s AND tf=%s", (pair, tf))
    return row["last_ts"] if row else None

def set_last_ts(pair: str, tf: str, ts: datetime):
    exec_sql("""
    INSERT INTO candle_state(pair, tf, last_ts)
    VALUES (%s,%s,%s)
    ON CONFLICT (pair, tf) DO UPDATE SET last_ts=EXCLUDED.last_ts
    """, (pair, tf, ts))

def upsert_candles(pair: str, tf: str, df: pd.DataFrame):
    """
    df index must be UTC timestamps, columns: open/high/low/close
    """
    if df is None or df.empty:
        return

    for ts, r in df.iterrows():
        exec_sql("""
        INSERT INTO candles(pair, tf, ts, open, high, low, close)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (pair, tf, ts) DO UPDATE
        SET open=EXCLUDED.open,
            high=EXCLUDED.high,
            low=EXCLUDED.low,
            close=EXCLUDED.close
        """, (
            pair, tf, ts.to_pydatetime(),
            float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])
        ))

def load_candles(pair: str, tf: str, limit: int):
    rows = exec_sql("""
    SELECT ts, open, high, low, close
    FROM candles
    WHERE pair=%s AND tf=%s
    ORDER BY ts ASC
    LIMIT %s
    """, (pair, tf, int(limit)), fetch=True)

    return [{
        "time": r["ts"].isoformat(),
        "open": float(r["open"]),
        "high": float(r["high"]),
        "low": float(r["low"]),
        "close": float(r["close"]),
    } for r in (rows or [])]
