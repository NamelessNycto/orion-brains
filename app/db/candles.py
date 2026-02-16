from __future__ import annotations

# app/db/candles.py

from datetime import datetime
from typing import Optional, Union

import pandas as pd

from app.db.neon import exec_sql, query_one, query_all


# ============================================================
# TS NORMALIZATION (fixes 21:02 / 21:17 / ... drift)
# ============================================================

TsLike = Union[datetime, pd.Timestamp]


def _to_utc_timestamp(ts: TsLike) -> pd.Timestamp:
    """
    Convert datetime/pandas timestamp to UTC pandas Timestamp (tz-aware).
    """
    t = pd.Timestamp(ts)

    # if naive -> assume UTC
    if t.tzinfo is None:
        return t.tz_localize("UTC")

    # if aware -> convert to UTC
    return t.tz_convert("UTC")


def _normalize_ts(ts: datetime | pd.Timestamp, tf: str) -> pd.Timestamp:
    """
    Normalize candle timestamps so they land on the expected grid:
      - 15m => :00/:15/:30/:45
      - 1h  => :00
    Always returns UTC Timestamp.
    """
    t = pd.Timestamp(ts)

    # ensure UTC
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    else:
        t = t.tz_convert("UTC")

    if tf == "15m":
        return t.floor("15min")
    if tf == "1h":
        return t.floor("1H")

    return t


# ============================================================
# META
# ============================================================

def get_count(pair: str, tf: str) -> int:
    row = query_one(
        "SELECT COUNT(*) AS n FROM candles WHERE pair=%s AND tf=%s",
        (pair, tf),
    )
    return int(row["n"]) if row else 0


def get_oldest_ts(pair: str, tf: str) -> Optional[datetime]:
    row = query_one(
        "SELECT MIN(ts) AS ts FROM candles WHERE pair=%s AND tf=%s",
        (pair, tf),
    )
    return row["ts"] if row and row.get("ts") else None


def get_last_ts(pair: str, tf: str) -> Optional[datetime]:
    row = query_one(
        "SELECT last_ts FROM candle_state WHERE pair=%s AND tf=%s",
        (pair, tf),
    )
    return row["last_ts"] if row and row.get("last_ts") else None


def set_last_ts(pair: str, tf: str, ts: datetime) -> None:
    nts = _normalize_ts(ts, tf).to_pydatetime()
    exec_sql(
        """
        INSERT INTO candle_state(pair, tf, last_ts)
        VALUES (%s,%s,%s)
        ON CONFLICT (pair, tf) DO UPDATE SET last_ts=EXCLUDED.last_ts
        """,
        (pair, tf, nts),
    )


# ============================================================
# UPSERT / LOAD
# ============================================================

def upsert_candles(pair: str, tf: str, df: pd.DataFrame) -> None:
    """
    df index must be timestamps, columns: open/high/low/close
    We normalize timestamps to the TF grid before writing.
    """
    if df is None or df.empty:
        return

    df = df.copy()

    # Ensure index is UTC tz-aware
    df.index = pd.to_datetime(df.index, utc=True)

    for ts, r in df.iterrows():
        nts = _normalize_ts(ts, tf)

        exec_sql(
            """
            INSERT INTO candles(pair, tf, ts, open, high, low, close)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (pair, tf, ts) DO UPDATE
            SET open=EXCLUDED.open,
                high=EXCLUDED.high,
                low=EXCLUDED.low,
                close=EXCLUDED.close
            """,
            (
                pair,
                tf,
                nts.to_pydatetime(),
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
            ),
        )


def load_candles(pair: str, tf: str, limit: int):
    """
    Returns candles ASC (oldest -> newest).
    """
    rows = query_all(
        """
        SELECT ts, open, high, low, close
        FROM candles
        WHERE pair=%s AND tf=%s
        ORDER BY ts DESC
        LIMIT %s
        """,
        (pair, tf, int(limit)),
    ) or []

    rows = list(reversed(rows))  # ASC

    return [
        {
            "time": r["ts"].isoformat(),
            "open": float(r["open"]),
            "high": float(r["high"]),
            "low": float(r["low"]),
            "close": float(r["close"]),
        }
        for r in rows
    ]


# ============================================================
# TRIM (keep last N)
# ============================================================

def trim_candles(pair: str, tf: str, keep: int) -> None:
    """
    Keep only the most recent `keep` candles for (pair, tf).
    Deletes older rows.
    """
    exec_sql(
        """
        DELETE FROM candles c
        USING (
            SELECT ts
            FROM candles
            WHERE pair=%s AND tf=%s
            ORDER BY ts DESC
            OFFSET %s
        ) old
        WHERE c.pair=%s
          AND c.tf=%s
          AND c.ts = old.ts
        """,
        (pair, tf, int(keep), pair, tf),
    )

def get_newest_ts(pair: str, tf: str):
    row = query_one("SELECT MAX(ts) AS ts FROM candles WHERE pair=%s AND tf=%s", (pair, tf))
    return row["ts"] if row and row.get("ts") else None
