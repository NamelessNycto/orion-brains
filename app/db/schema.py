# app/db/schema.py

from app.db.neon import exec_sql


def init_db():
    # ==========================================================
    # 1) Pair counters (TR-{PAIR}-000001-OR)
    # ==========================================================
    exec_sql("""
    CREATE TABLE IF NOT EXISTS pair_counters (
      pair TEXT PRIMARY KEY,
      next_seq BIGINT NOT NULL DEFAULT 1
    );
    """)

    # ==========================================================
    # 2) Positions (live trading state)
    # ==========================================================
    exec_sql("""
    CREATE TABLE IF NOT EXISTS positions (
      id TEXT PRIMARY KEY,

      pair TEXT NOT NULL,
      side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
      mode TEXT NOT NULL CHECK (mode IN ('EARLY','CONFIRMED')),

      entry_price DOUBLE PRECISION NOT NULL,
      sl_price DOUBLE PRECISION NOT NULL,

      -- Trailing
      trail_price DOUBLE PRECISION,
      trail_on BOOLEAN NOT NULL DEFAULT FALSE,

      -- Lifecycle
      opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      closed_at TIMESTAMPTZ,
      close_reason TEXT,

      -- Engine tracking
      last_15m_ts TIMESTAMPTZ,
      last_swing_price DOUBLE PRECISION,
      last_swing_ts TIMESTAMPTZ
    );
    """)

    # Index for fast lookup of open positions
    exec_sql("""
    CREATE INDEX IF NOT EXISTS idx_positions_pair_open
    ON positions(pair)
    WHERE closed_at IS NULL;
    """)

    # ==========================================================
    # SAFE MIGRATIONS (idempotent)
    # ==========================================================
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS trail_price DOUBLE PRECISION;")
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS trail_on BOOLEAN NOT NULL DEFAULT FALSE;")
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW();")
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ;")
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS close_reason TEXT;")
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS last_15m_ts TIMESTAMPTZ;")
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS last_swing_price DOUBLE PRECISION;")
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS last_swing_ts TIMESTAMPTZ;")
