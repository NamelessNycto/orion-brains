import time
import pandas as pd
from app.core.config import settings
from app.core.http import get

def fetch_aggs(pair: str, mult: int, span: str, start: str, end: str, limit: int = 50000) -> pd.DataFrame:
    base = settings.POLYGON_BASE
    url = f"{base}/v2/aggs/ticker/{pair}/range/{mult}/{span}/{start}/{end}"
    params = {"adjusted":"true","sort":"asc","limit":limit,"apiKey":settings.POLYGON_API_KEY}
    rows = []

    while True:
        j = get(url, params=params, timeout=30)
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
        if "apiKey=" not in nxt:
            nxt = nxt + ("&" if "?" in nxt else "?") + f"apiKey={settings.POLYGON_API_KEY}"
        url = nxt
        params = None
        time.sleep(0.25)

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["time"]).sort_values("time").set_index("time")
    return df
