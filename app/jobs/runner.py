import logging
from datetime import datetime, timedelta, timezone
import pandas as pd

from app.core.config import settings
from app.db.neon import query_one, exec_sql
from app.db.candles import get_last_ts, set_last_ts, upsert_candles
from app.services.polygon import fetch_15m_fx, fetch_1h_fx
from app.services.strategy_client import call_trend_engine
from app.services.telegram import send_telegram

log = logging.getLogger("runner")

# ============================================================
# BACKTEST MATCHING PARAMS
# ============================================================

FRACTAL_K = 2
SWING_ATR_MIN_K = 0.35
STRUCT_PAD_ATR = 0.05

FLOOR_ATR_15_CONFIRMED = 1.20
FLOOR_ATR_15_EARLY = 1.35

ATR_LEN_15 = 14


# ============================================================
# HELPERS
# ============================================================

def _utc_now():
    return datetime.now(timezone.utc)

def _fmt_pair(pair: str) -> str:
    return pair.replace("C:", "").strip()

def _iso_date(dt: datetime) -> str:
    return dt.date().isoformat()

def _load_df_from_neon(pair_short: str, tf: str, limit: int) -> pd.DataFrame:
    rows = exec_sql("""
      SELECT ts, open, high, low, close
      FROM candles
      WHERE pair=%s AND tf=%s
      ORDER BY ts DESC
      LIMIT %s
    """, (pair_short, tf, int(limit)), fetch=True) or []

    if not rows:
        return pd.DataFrame(columns=["open","high","low","close"])

    # rows are DESC, we want ASC
    rows = list(reversed(rows))
    df = pd.DataFrame([{
        "time": r["ts"],
        "open": float(r["open"]),
        "high": float(r["high"]),
        "low":  float(r["low"]),
        "close":float(r["close"]),
    } for r in rows])

    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.set_index("time").sort_index()
    return df

def _df_to_candles(df: pd.DataFrame) -> list[dict]:
    out = []
    if df is None or df.empty:
        return out
    for ts, r in df.iterrows():
        out.append({
            "time": ts.isoformat(),
            "open": float(r["open"]),
            "high": float(r["high"]),
            "low":  float(r["low"]),
            "close":float(r["close"]),
        })
    return out


# ================= ATR =================

def compute_atr(df: pd.DataFrame, n: int = 14):
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)

    prev_close = close.shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)

    return tr.rolling(n).mean()


# ================= FRACTALS =================

def is_pivot_low(df, i, k):
    if i - k < 0 or i + k >= len(df):
        return False
    v = df["low"].iloc[i]
    return v < df["low"].iloc[i-k:i].min() and v < df["low"].iloc[i+1:i+k+1].min()

def is_pivot_high(df, i, k):
    if i - k < 0 or i + k >= len(df):
        return False
    v = df["high"].iloc[i]
    return v > df["high"].iloc[i-k:i].max() and v > df["high"].iloc[i+1:i+k+1].max()


# ============================================================
# CANDLES SYNC (Polygon -> Neon)
# ============================================================

def sync_tf(pair_ticker: str, pair_short: str, tf: str, now: datetime, fetch_fn, bootstrap_days: int):
    """
    tf: "15m" or "1h"
    fetch_fn: fetch_15m_fx / fetch_1h_fx
    returns: (has_new, last_ts_after)
    """
    last_ts = get_last_ts(pair_short, tf)

    if last_ts is None:
        start_dt = now - timedelta(days=bootstrap_days)
    else:
        # buffer 1 day to be safe with Polygon ranges
        start_dt = pd.Timestamp(last_ts).to_pydatetime() - timedelta(days=1)

    df = fetch_fn(pair_ticker, _iso_date(start_dt), _iso_date(now))
    if df is None or df.empty:
        return False, last_ts

    # save in Neon
    upsert_candles(pair_short, tf, df)

    new_last = df.index[-1].to_pydatetime()
    if last_ts is None or pd.Timestamp(new_last) > pd.Timestamp(last_ts):
        set_last_ts(pair_short, tf, new_last)
        return True, new_last

    return False, last_ts


# ============================================================
# DB HELPERS (positions)
# ============================================================

def get_open_position(pair_short):
    return query_one("""
    SELECT * FROM positions
    WHERE pair=%s AND closed_at IS NULL
    LIMIT 1
    """, (pair_short,))

def open_position(pos_id, pair_short, side, mode, entry, sl):
    exec_sql("""
    INSERT INTO positions(
        id,pair,side,mode,
        entry_price,sl_price,
        trail_price,trail_on,
        last_15m_ts,last_check_1m_ts,
        last_swing_price,last_swing_ts
    )
    VALUES(%s,%s,%s,%s,%s,%s,NULL,FALSE,NULL,NULL,NULL,NULL)
    """, (pos_id, pair_short, side, mode, float(entry), float(sl)))

def close_position(pos_id, reason):
    exec_sql("""
    UPDATE positions
    SET closed_at=NOW(), close_reason=%s
    WHERE id=%s
    """, (reason, pos_id))

def update_trail(pos_id, trail_price, trail_on, last_15m_ts, swing_price, swing_ts):
    exec_sql("""
    UPDATE positions
    SET trail_price=%s,
        trail_on=%s,
        last_15m_ts=%s,
        last_swing_price=%s,
        last_swing_ts=%s
    WHERE id=%s
    """, (
        float(trail_price),
        bool(trail_on),
        last_15m_ts,
        (float(swing_price) if swing_price is not None else None),
        swing_ts,
        pos_id
    ))


# ============================================================
# TRAIL ENGINE (MATCH BACKTEST)
# ============================================================

def compute_trail(pos, df15):
    df15 = df15.copy()
    df15["ATR15"] = compute_atr(df15, ATR_LEN_15)

    side = pos["side"]
    mode = pos["mode"]
    entry = float(pos["entry_price"])
    sl = float(pos["sl_price"])
    trail_on = bool(pos["trail_on"])
    trail = float(pos["trail_price"] or sl)

    last_swing = pos["last_swing_price"]
    last_swing_ts = pos["last_swing_ts"]

    if len(df15) < (ATR_LEN_15 + FRACTAL_K + 5):
        return trail_on, trail, last_swing, last_swing_ts

    atr15 = float(df15["ATR15"].iloc[-1])
    if not (pd.notna(atr15) and atr15 > 0):
        return trail_on, trail, last_swing, last_swing_ts

    floor_mult = FLOOR_ATR_15_EARLY if mode == "EARLY" else FLOOR_ATR_15_CONFIRMED

    pivot_i = len(df15) - 1 - FRACTAL_K
    if pivot_i > ATR_LEN_15:
        atr_piv = float(df15["ATR15"].iloc[pivot_i])
        ts = df15.index[pivot_i].to_pydatetime()

        if side == "BUY" and is_pivot_low(df15, pivot_i, FRACTAL_K):
            piv = float(df15["low"].iloc[pivot_i])
            if last_swing is None or abs(piv - float(last_swing)) >= SWING_ATR_MIN_K * atr_piv:
                last_swing = piv
                last_swing_ts = ts

        if side == "SELL" and is_pivot_high(df15, pivot_i, FRACTAL_K):
            piv = float(df15["high"].iloc[pivot_i])
            if last_swing is None or abs(piv - float(last_swing)) >= SWING_ATR_MIN_K * atr_piv:
                last_swing = piv
                last_swing_ts = ts

    if not trail_on:
        return trail_on, trail, last_swing, last_swing_ts

    if side == "BUY":
        struct = None if last_swing is None else float(last_swing) - STRUCT_PAD_ATR * atr15
        floor = entry - floor_mult * atr15
        cand = floor if struct is None else max(struct, floor)
        trail = max(trail, cand, sl)
    else:
        struct = None if last_swing is None else float(last_swing) + STRUCT_PAD_ATR * atr15
        floor = entry + floor_mult * atr15
        cand = floor if struct is None else min(struct, floor)
        trail = min(trail, cand, sl)

    return trail_on, float(trail), last_swing, last_swing_ts


# ============================================================
# MAIN ENGINE
# ============================================================

def run_once(universe):
    now = _utc_now()
    out = {"pairs": {}}

    for pair in universe:
        pair_short = _fmt_pair(pair)
        out["pairs"][pair_short] = {"actions": []}

        # 1) Sync candles to Neon (small calls)
        new_15m, last15 = sync_tf(pair, pair_short, "15m", now, fetch_15m_fx, bootstrap_days=4)
        new_1h,  last1h  = sync_tf(pair, pair_short, "1h",  now, fetch_1h_fx,  bootstrap_days=7)

        # If no new 15m candle -> nothing to do (fast cron)
        if not new_15m:
            out["pairs"][pair_short]["actions"].append("no_new_15m")
            continue

        # 2) Load candles from Neon (cache)
        df15 = _load_df_from_neon(pair_short, "15m", limit=450)
        df1h = _load_df_from_neon(pair_short, "1h",  limit=250)

        if df15.empty or df1h.empty:
            out["pairs"][pair_short]["actions"].append("no_cached_data")
            continue

        last15_ts = df15.index[-1].to_pydatetime()

        pos = get_open_position(pair_short)

        # ====================================================
        # MANAGE OPEN POSITION (only on new 15m close)
        # ====================================================
        if pos:
            # skip if already processed this 15m candle
            if pos["last_15m_ts"] is not None:
                if pd.Timestamp(pos["last_15m_ts"]).to_pydatetime() >= last15_ts:
                    out["pairs"][pair_short]["actions"].append("already_processed_15m")
                    continue

            # SL / TRAIL hit check using last closed 15m candle (backtest-like)
            last = df15.iloc[-1]
            hi = float(last["high"])
            lo = float(last["low"])

            sl = float(pos["sl_price"])
            trail_on = bool(pos["trail_on"])
            trail = float(pos["trail_price"]) if pos["trail_price"] is not None else None

            if pos["side"] == "BUY":
                if lo <= sl:
                    close_position(pos["id"], "SL")
                    send_telegram(f"❌ SL HIT\nid={pos['id']}\npair={pair_short}\nside=BUY\nsl={sl}")
                    out["pairs"][pair_short]["actions"].append("closed_SL")
                    continue
                if trail_on and trail is not None and lo <= trail:
                    close_position(pos["id"], "TRAIL")
                    send_telegram(f"✅ EXIT TRAIL\nid={pos['id']}\npair={pair_short}\nside=BUY\ntrail={trail}")
                    out["pairs"][pair_short]["actions"].append("closed_TRAIL")
                    continue
            else:
                if hi >= sl:
                    close_position(pos["id"], "SL")
                    send_telegram(f"❌ SL HIT\nid={pos['id']}\npair={pair_short}\nside=SELL\nsl={sl}")
                    out["pairs"][pair_short]["actions"].append("closed_SL")
                    continue
                if trail_on and trail is not None and hi >= trail:
                    close_position(pos["id"], "TRAIL")
                    send_telegram(f"✅ EXIT TRAIL\nid={pos['id']}\npair={pair_short}\nside=SELL\ntrail={trail}")
                    out["pairs"][pair_short]["actions"].append("closed_TRAIL")
                    continue

            # activation (0.9 early / 0.7 confirmed)
            risk = abs(float(pos["entry_price"]) - float(pos["sl_price"]))
            if risk > 0:
                fav = (hi - float(pos["entry_price"])) / risk if pos["side"] == "BUY" else (float(pos["entry_price"]) - lo) / risk
            else:
                fav = 0.0

            act = 0.9 if pos["mode"] == "EARLY" else 0.7
            if (not trail_on) and (fav >= act):
                trail_on = True
                send_telegram(f"🟣 TRAIL ON\nid={pos['id']}\npair={pair_short}\nmode={pos['mode']}")

            old_trail = float(pos["trail_price"]) if pos["trail_price"] is not None else float(pos["sl_price"])
            trail_on, new_trail, swing, swing_ts = compute_trail({**pos, "trail_on": trail_on}, df15)

            update_trail(pos["id"], new_trail, trail_on, last15_ts, swing, swing_ts)

            if trail_on and abs(new_trail - old_trail) > 0.00005:
                send_telegram(f"🔁 TRAIL UPDATE\nid={pos['id']}\npair={pair_short}\ntrail={new_trail}")

            out["pairs"][pair_short]["actions"].append("managed_position")
            continue

        # ====================================================
        # ENTRY (only when new 15m closes)
        # ====================================================

        payload = call_trend_engine(pair, _df_to_candles(df15.tail(400)), _df_to_candles(df1h.tail(200)))
        sig = payload.get("signal")

        if not sig:
            out["pairs"][pair_short]["actions"].append("no_signal")
            continue

        sid = f"TR-{pair_short}-{int(now.timestamp())}-OR"
        open_position(sid, pair_short, sig["side"], sig["mode"], sig["entry"], sig["sl"])

        send_telegram(
            f"🚀 ENTRY {sig['side']} ({sig['mode']})\n"
            f"id={sid}\n"
            f"pair={pair_short}\n"
            f"entry={sig['entry']}\n"
            f"sl={sig['sl']}"
        )

        out["pairs"][pair_short]["actions"].append("opened")

    return out
