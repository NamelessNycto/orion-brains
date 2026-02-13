import psycopg2
from app.core.config import settings

DDL = """
CREATE TABLE IF NOT EXISTS signals (
  signal_id TEXT PRIMARY KEY,
  ts BIGINT NOT NULL,
  strategy TEXT NOT NULL,
  pair TEXT NOT NULL,
  side TEXT NOT NULL,
  mode TEXT NOT NULL,
  entry DOUBLE PRECISION NOT NULL,
  sl DOUBLE PRECISION NOT NULL,
  trail_activate_r DOUBLE PRECISION NOT NULL,
  p_trend DOUBLE PRECISION NOT NULL,
  payload_json TEXT NOT NULL
);
"""

def get_conn():
    return psycopg2.connect(settings.NEON_DSN)

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

def insert_signal(signal: dict) -> bool:
    """
    Returns True if inserted, False if already exists.
    """
    import json
    q = """
    INSERT INTO signals (signal_id, ts, strategy, pair, side, mode, entry, sl, trail_activate_r, p_trend, payload_json)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (signal_id) DO NOTHING
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (
                signal["signal_id"],
                signal["ts"],
                signal["strategy"],
                signal["pair"],
                signal["side"],
                signal["mode"],
                signal["entry"],
                signal["sl"],
                signal["trail_activate_r"],
                signal["p_trend"],
                json.dumps(signal),
            ))
            inserted = (cur.rowcount == 1)
        conn.commit()
    return inserted
