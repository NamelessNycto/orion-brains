import time
import requests
import pandas as pd
from app.core.config import settings

BASE = "https://api.polygon.io"
sess = requests.Session()

PAGE_SLEEP_SEC = 0.25
SLEEP_429_SEC = 8

def _fetch_agg(ticker: str, mult: int, span: str, start: str, end: str, limit: int = 50000):
    url = f"{BASE}/v2/aggs/ticker/{ticker}/range/{mult}/{span}/{start}/{end}"
    params = {"adjusted":"true","sort":"asc","limit":limit,"apiKey":settings.POLYGON_API_KEY}
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
                "low": float(c["l"]),
                "close": float(c["c"]),
            })
        nxt = j.get("next_url")
        if not nxt:
            break
        url = nxt + ("&" if "?" in nxt else "?") + f"apiKey={settings.POLYGON_API_KEY}" if "apiKey=" not in nxt else nxt
        params = None
        time.sleep(PAGE_SLEEP_SEC)

    if not rows:
        return pd.DataFrame(columns=["time","open","high","low","close"]).set_index("time")

    df = pd.DataFrame(rows).drop_duplicates(subset=["time"]).sort_values("time").set_index("time")
    return df

def fetch_15m_fx(pair: str, start: str, end: str):
    # pair expected: "C:EURUSD"
    return _fetch_agg(pair, 15, "minute", start, end, limit=500)

def fetch_1h_fx(pair: str, start: str, end: str):
    return _fetch_agg(pair, 1, "hour", start, end, limit=500)

def fetch_5m_fx(pair: str, start: str, end: str):
    return _fetch_agg(pair, 5, "minute", start, end, limit=200)
