# app/services/polygon.py

import time
from datetime import datetime, timezone, timedelta
import requests
import pandas as pd

from app.core.config import settings

BASE = "https://api.polygon.io"
sess = requests.Session()

PAGE_SLEEP_SEC = 0.25
SLEEP_429_SEC = 8


# ============================================================
# HELPERS
# ============================================================

def _to_range_arg(x) -> str:
    if isinstance(x, datetime):
        if x.tzinfo is None:
            x = x.replace(tzinfo=timezone.utc)
        else:
            x = x.astimezone(timezone.utc)
        return str(int(x.timestamp() * 1000))
    return str(x)


def _apply_close_timestamp(df: pd.DataFrame, mult: int, span: str) -> pd.DataFrame:
    """
    Polygon returns OPEN timestamp.
    We convert to CLOSE timestamp.
    """
    if df.empty:
        return df

    if span == "minute":
        delta = timedelta(minutes=mult)
    elif span == "hour":
        delta = timedelta(hours=mult)
    else:
        delta = timedelta(0)

    df = df.copy()
    df.index = df.index + delta
    return df


# ============================================================
# CORE FETCH
# ============================================================

def _fetch_agg(ticker: str, mult: int, span: str, start, end) -> pd.DataFrame:
    start_arg = _to_range_arg(start)
    end_arg = _to_range_arg(end)

    url = f"{BASE}/v2/aggs/ticker/{ticker}/range/{mult}/{span}/{start_arg}/{end_arg}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50,   # 🚀 important: let pagination work naturally
        "apiKey": settings.POLYGON_API_KEY,
    }

    rows: list[dict] = []

    while True:
        r = sess.get(url, params=params, timeout=30)

        if r.status_code == 429:
            time.sleep(SLEEP_429_SEC)
            continue

        if r.status_code in (401, 403):
            try:
                print("Polygon error:", r.status_code, r.json())
            except Exception:
                print("Polygon error:", r.status_code, r.text)
        
        r.raise_for_status()
        j = r.json()

        for c in (j.get("results") or []):
            rows.append(
                {
                    "time": pd.to_datetime(c["t"], unit="ms", utc=True),
                    "open": float(c["o"]),
                    "high": float(c["h"]),
                    "low": float(c["l"]),
                    "close": float(c["c"]),
                }
            )

        nxt = j.get("next_url")
        if not nxt:
            break

        url = nxt
        if "apiKey=" not in url:
            url += ("&" if "?" in url else "?") + f"apiKey={settings.POLYGON_API_KEY}"

        params = None
        time.sleep(PAGE_SLEEP_SEC)

    if not rows:
        return pd.DataFrame(columns=["open", "high", "low", "close"])

    df = (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["time"])
        .sort_values("time")
        .set_index("time")
    )

    # 🔥 convert OPEN timestamp → CLOSE timestamp
    df = _apply_close_timestamp(df, mult, span)

    return df


# ============================================================
# PUBLIC CALLS
# ============================================================

def fetch_15m_fx(pair: str, start, end) -> pd.DataFrame:
    return _fetch_agg(pair, 15, "minute", start, end)


def fetch_1h_fx(pair: str, start, end) -> pd.DataFrame:
    return _fetch_agg(pair, 1, "hour", start, end)
