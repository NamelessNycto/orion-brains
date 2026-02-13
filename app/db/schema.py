from app.db.neon import exec_sql

def init_db():
    # ==========================================================
    # 1) Pair counters (pour générer TR-{PAIR}-000001-OR)
    # ==========================================================
    exec_sql("""
    CREATE TABLE IF NOT EXISTS pair_counters (
      pair TEXT PRIMARY KEY,
      next_seq BIGINT NOT NULL DEFAULT 1
    );
    """)

    # ==========================================================
    # 2) Positions (stateful execution tracking)
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
      last_check_1m_ts TIMESTAMPTZ,

      -- Structure trail state (backtest-like)
      last_swing_price DOUBLE PRECISION,
      last_swing_ts TIMESTAMPTZ
    );
    """)

    exec_sql("""
    CREATE INDEX IF NOT EXISTS idx_positions_pair_open
    ON positions(pair)
    WHERE closed_at IS NULL;
    """)

    # ==========================================================
    # 3) Candles cache (15m + 1h + extensible)
    #    ts = timestamp de clôture de la bougie (UTC)
    # ==========================================================
    exec_sql("""
    CREATE TABLE IF NOT EXISTS candles (
      pair TEXT NOT NULL,        -- "EURUSD"
      tf   TEXT NOT NULL,        -- "15m" | "1h" | "5m" (si tu veux plus tard)
      ts   TIMESTAMPTZ NOT NULL, -- candle close timestamp (UTC)

      open  DOUBLE PRECISION NOT NULL,
      high  DOUBLE PRECISION NOT NULL,
      low   DOUBLE PRECISION NOT NULL,
      close DOUBLE PRECISION NOT NULL,

      PRIMARY KEY(pair, tf, ts)
    );
    """)

    exec_sql("""
    CREATE INDEX IF NOT EXISTS idx_candles_pair_tf_ts
    ON candles(pair, tf, ts DESC);
    """)

    # ==========================================================
    # 4) Candle state (dernier ts sync par pair/tf)
    # ==========================================================
    exec_sql("""
    CREATE TABLE IF NOT EXISTS candle_state (
      pair TEXT NOT NULL,
      tf   TEXT NOT NULL,
      last_ts TIMESTAMPTZ,
      PRIMARY KEY(pair, tf)
    );
    """)

    # ==========================================================
    # 5) MIGRATIONS SAFE (si tables déjà existantes)
    #    => évite "column does not exist"
    # ==========================================================
    # positions
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW();")
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ;")
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS close_reason TEXT;")
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS last_15m_ts TIMESTAMPTZ;")
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS last_check_1m_ts TIMESTAMPTZ;")
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS last_swing_price DOUBLE PRECISION;")
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS last_swing_ts TIMESTAMPTZ;")
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS trail_price DOUBLE PRECISION;")
    exec_sql("ALTER TABLE positions ADD COLUMN IF NOT EXISTS trail_on BOOLEAN NOT NULL DEFAULT FALSE;")

    # candles (au cas où tu avais une vieille version sans PK / colonnes)
    exec_sql("ALTER TABLE candles ADD COLUMN IF NOT EXISTS open DOUBLE PRECISION;")
    exec_sql("ALTER TABLE candles ADD COLUMN IF NOT EXISTS high DOUBLE PRECISION;")
    exec_sql("ALTER TABLE candles ADD COLUMN IF NOT EXISTS low DOUBLE PRECISION;")
    exec_sql("ALTER TABLE candles ADD COLUMN IF NOT EXISTS close DOUBLE PRECISION;")

    # indexes (idempotent déjà)
    exec_sql("""
    CREATE INDEX IF NOT EXISTS idx_positions_pair_open
    ON positions(pair)
    WHERE closed_at IS NULL;
    """)

    exec_sql("""
    CREATE INDEX IF NOT EXISTS idx_candles_pair_tf_ts
    ON candles(pair, tf, ts DESC);
    """)
