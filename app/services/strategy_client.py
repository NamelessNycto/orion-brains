import requests
from app.core.config import settings

def call_trend_engine(pair: str, candles_15m: list, candles_1h: list) -> dict:
    """
    Strategy service must return either:
      {"signal": null}
    or:
      {
        "signal": {
          "side":"BUY/SELL",
          "mode":"EARLY/CONFIRMED",
          "entry": 1.2345,
          "sl": 1.2300,
          "meta": {...}   # optional
        }
      }
    """
    url = settings.STRATEGIES_BASE_URL.rstrip("/") + "/v1/trend/signal"
    payload = {
        "pair": pair,                 # "C:EURUSD"
        "candles_15m": candles_15m,   # list of dicts: time/open/high/low/close
        "candles_1h": candles_1h
    }
    r = requests.post(url, json=payload, timeout=settings.STRATEGY_TIMEOUT_SEC)
    r.raise_for_status()
    return r.json()
