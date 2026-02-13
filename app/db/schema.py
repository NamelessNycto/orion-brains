from app.db.neon import exec_sql

def init_db():
    exec_sql("""
    CREATE TABLE IF NOT EXISTS pair_counters (
      pair TEXT PRIMARY KEY,
      next_seq BIGINT NOT NULL DEFAULT 1
    );
    """)

    exec_sql("""
    CREATE TABLE IF NOT EXISTS positions (
      id TEXT PRIMARY KEY,
      pair TEXT NOT NULL,
      side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
      mode TEXT NOT NULL CHECK (mode IN ('EARLY','CONFIRMED')),
      entry_price DOUBLE PRECISION NOT NULL,
      sl_price DOUBLE PRECISION NOT NULL,
      trail_price DOUBLE PRECISION,
      trail_on BOOLEAN NOT NULL DEFAULT FALSE,

      opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      closed_at TIMESTAMPTZ,
      close_reason TEXT,

      last_15m_ts TIMESTAMPTZ,         -- last 15m bar processed for trailing
      last_check_1m_ts TIMESTAMPTZ     -- last 1m ts checked for SL/trail hits
    );
    """)

    exec_sql("""
    CREATE INDEX IF NOT EXISTS idx_positions_pair_open
    ON positions(pair)
    WHERE closed_at IS NULL;
    """)
