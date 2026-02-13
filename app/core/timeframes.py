import pandas as pd

def to_utc_ts(t: int) -> pd.Timestamp:
    return pd.to_datetime(int(t), unit="s", utc=True)

def floor_time(ts: pd.Timestamp, minutes: int) -> pd.Timestamp:
    return ts.floor(f"{minutes}min")
