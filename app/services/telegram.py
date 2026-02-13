import requests
import logging
from app.core.config import settings

log = logging.getLogger("telegram")

def send_telegram(text: str):
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
        log.info("telegram disabled (missing token/chat_id)")
        return

    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": settings.TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code >= 300:
            log.warning(f"telegram error {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log.warning(f"telegram send failed: {e}")
