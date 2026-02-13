import pandas as pd

def resample_15m(df: pd.DataFrame) -> pd.DataFrame:
    # expects UTC datetime index
    o = df["open"].resample("15min").first()
    h = df["high"].resample("15min").max()
    l = df["low"].resample("15min").min()
    c = df["close"].resample("15min").last()
    out = pd.concat([o,h,l,c], axis=1).dropna()
    out.columns = ["open","high","low","close"]
    return out

def resample_1h(df: pd.DataFrame) -> pd.DataFrame:
    o = df["open"].resample("1h").first()
    h = df["high"].resample("1h").max()
    l = df["low"].resample("1h").min()
    c = df["close"].resample("1h").last()
    out = pd.concat([o,h,l,c], axis=1).dropna()
    out.columns = ["open","high","low","close"]
    return out
