import logging
import requests
from app.core.config import settings

log = logging.getLogger("strategy_client")


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

    url = settings.STRATEGIES_BASE_URL.rstrip("/") + "/v1/trend_engine"

    payload = {
        "pair": pair,                 # ex: "C:EURUSD"
        "candles_15m": candles_15m,
        "candles_1h": candles_1h
    }

    log.info(
        f"[TREND_ENGINE] pair={pair} "
        f"15m={len(candles_15m)} "
        f"1h={len(candles_1h)}"
    )

    try:
        r = requests.post(
            url,
            json=payload,
            timeout=settings.STRATEGY_TIMEOUT_SEC
        )

        r.raise_for_status()
        data = r.json()

    except requests.exceptions.Timeout:
        log.error("Trend engine timeout")
        return {"signal": None}

    except requests.exceptions.HTTPError as e:
        log.error(f"Trend engine HTTP error: {e}")
        return {"signal": None}

    except Exception as e:
        log.error(f"Trend engine unexpected error: {e}")
        return {"signal": None}

    # --- Defensive validation ---
    if not isinstance(data, dict):
        log.error("Trend engine returned non-dict response")
        return {"signal": None}

    if "signal" not in data:
        log.error("Trend engine response missing 'signal' key")
        return {"signal": None}

    # Optional: validate structure of signal
    sig = data.get("signal")
    if sig:
        required = {"side", "mode", "entry", "sl"}
        if not isinstance(sig, dict) or not required.issubset(sig.keys()):
            log.error("Trend engine signal malformed")
            return {"signal": None}

    return data
