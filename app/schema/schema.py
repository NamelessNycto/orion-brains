from app.db.neon import exec_sql


def init_db():
    # ==========================================================
    # Pair counters (pour générer les IDs TR-{PAIR}-XXXXXX-OR)
    # ==========================================================
    exec_sql("""
    CREATE TABLE IF NOT EXISTS pair_counters (
      pair TEXT PRIMARY KEY,
      next_seq BIGINT NOT NULL DEFAULT 1
    );
    """)

    # ==========================================================
    # Positions (stateful execution tracking)
    # ==========================================================
    exec_sql("""
    CREATE TABLE IF NOT EXISTS positions (
      id TEXT PRIMARY KEY,

      pair TEXT NOT NULL,
      side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
      mode TEXT NOT NULL CHECK (mode IN ('EARLY','CONFIRMED')),

      entry_price DOUBLE PRECISION NOT NULL,
      sl_price DOUBLE PRECISION NOT NULL,

      -- Dynamic trailing stop
      trail_price DOUBLE PRECISION,
      trail_on BOOLEAN NOT NULL DEFAULT FALSE,

      -- Lifecycle
      opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      closed_at TIMESTAMPTZ,
      close_reason TEXT,

      -- ===============================
      -- Engine tracking timestamps
      -- ===============================
      last_15m_ts TIMESTAMPTZ,     -- last 15m bar processed for trailing logic
      last_check_1m_ts TIMESTAMPTZ, -- last micro check for SL/trail hit

      -- ===============================
      -- NEW: Structure trailing state
      -- ===============================
      last_swing_price DOUBLE PRECISION,  -- last confirmed pivot price
      last_swing_ts TIMESTAMPTZ           -- pivot timestamp
    );
    """)

    # ==========================================================
    # Index (fast lookup open positions)
    # ==========================================================
    exec_sql("""
    CREATE INDEX IF NOT EXISTS idx_positions_pair_open
    ON positions(pair)
    WHERE closed_at IS NULL;
    """)
