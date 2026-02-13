from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    APP_NAME: str = "orion-brain"
    ENV: str = "dev"
    LOG_LEVEL: str = "INFO"

    POLYGON_API_KEY: str
    POLYGON_BASE: str = "https://api.polygon.io"

    STRATEGIES_BASE_URL: str
    STRATEGIES_TIMEOUT_SEC: int = 12

    NEON_DSN: str

    TELEGRAM_BOT_TOKEN: str
    TELEGRAM_CHAT_ID: str

    PAIRS: str = "C:EURUSD,C:GBPUSD,C:USDJPY"

    FETCH_LOOKBACK_DAYS: int = 8
    TF_MIN: int = 15

    DEDUPE_TTL_SEC: int = 60*60*24*30
    MAX_SIGNALS_PER_RUN: int = 6

    class Config:
        env_file = ".env"

settings = Settings()
