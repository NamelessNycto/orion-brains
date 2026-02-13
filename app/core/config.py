from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "orion-brain"
    ENV: str = "prod"

    # Comma-separated: EURUSD,GBPUSD,USDJPY
    PAIRS: str = "EURUSD,GBPUSD,USDJPY"

    # Services
    STRATEGIES_BASE_URL: str = "http://orion-strategies:8000"  # change on Railway
    STRATEGY_TIMEOUT_SEC: int = 20

    # Polygon
    POLYGON_API_KEY: str = ""

    # Neon
    DATABASE_URL: str = ""

    # Telegram
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""

    # Runtime
    RUN_TF_MIN: int = 5          # cron frequency (minutes)
    TRAIL_TF_MIN: int = 15       # trail updates only on 15m close
    SL_CHECK_TF_MIN: int = 1     # use 1m candles to detect SL/trail hits

    class Config:
        env_file = ".env"

settings = Settings()
