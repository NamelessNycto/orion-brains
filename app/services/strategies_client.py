from app.core.config import settings
from app.core.http import post

def send_bars(candles: list[dict]) -> dict:
    url = f"{settings.STRATEGIES_BASE_URL}/v1/bars"
    payload = {"source":"brain", "candles": candles}
    return post(url, json_body=payload, timeout=settings.STRATEGIES_TIMEOUT_SEC)
