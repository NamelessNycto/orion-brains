import time
import requests
import pandas as pd
from datetime import datetime
from app.core.config import settings

BASE = "https://api.polygon.io"
sess = requests.Session()

PAGE_SLEEP_SEC = 0.25
SLEEP_429_SEC = 8


# ============================================================
# HELPERS
# ============================================================

def _to_range_arg(x):
    """
    Polygon range endpoint accepts:
      - YYYY-MM-DD
      - unix timestamp in ms

    We convert datetime -> ms timestamp
    """
    if isinstance(x, datetime):
        return str(int(x.timestamp() * 1000))
    return str(x)


# ============================================================
# CORE FETCH
# ============================================================

def _fetch_agg(ticker: str, mult: int, span: str, start, end, limit: int = 500):
    start_arg = _to_range_arg(start)
    end_arg   = _to_range_arg(end)

    url = f"{BASE}/v2/aggs/ticker/{ticker}/range/{mult}/{span}/{start_arg}/{end_arg}"

    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": int(limit),
        "apiKey": settings.POLYGON_API_KEY,
    }

    rows = []

    while True:

        r = sess.get(url, params=params, timeout=30)

        if r.status_code == 429:
            time.sleep(SLEEP_429_SEC)
            continue

        r.raise_for_status()
        j = r.json()

        for c in (j.get("results") or []):
            rows.append({
                "time": pd.to_datetime(c["t"], unit="ms", utc=True),
                "open": float(c["o"]),
                "high": float(c["h"]),
                "low":  float(c["l"]),
                "close":float(c["c"]),
            })

        nxt = j.get("next_url")
        if not nxt:
            break

        # Pagination safe handling
        url = nxt
        if "apiKey=" not in url:
            url += ("&" if "?" in url else "?") + f"apiKey={settings.POLYGON_API_KEY}"

        params = None
        time.sleep(PAGE_SLEEP_SEC)

    if not rows:
        return pd.DataFrame(columns=["time","open","high","low","close"]).set_index("time")

    df = (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["time"])
        .sort_values("time")
        .set_index("time")
    )

    return df


# ============================================================
# PUBLIC CALLS
# ============================================================

def fetch_15m_fx(pair, start, end, limit=450):
    return _fetch_agg(pair, 15, "minute", start, end, limit=limit)


def fetch_1h_fx(pair, start, end, limit=180):
    return _fetch_agg(pair, 1, "hour", start, end, limit=limit)


def fetch_5m_fx(pair, start, end, limit=60):
    return _fetch_agg(pair, 5, "minute", start, end, limit=limit)
