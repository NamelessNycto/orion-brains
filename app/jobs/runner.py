import logging
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np

from app.core.config import settings
from app.db.neon import query_one, exec_sql
from app.services.polygon import fetch_15m_fx, fetch_1h_fx, fetch_5m_fx
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
# DB HELPERS
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
    """, (pos_id, pair_short, side, mode, entry, sl))

def close_position(pos_id, reason):
    exec_sql("""
    UPDATE positions
    SET closed_at=NOW(), close_reason=%s
    WHERE id=%s
    """, (reason, pos_id))

def update_trail(pos_id, trail_price, trail_on,
                 last_15m_ts,
                 swing_price,
                 swing_ts):

    exec_sql("""
    UPDATE positions
    SET trail_price=%s,
        trail_on=%s,
        last_15m_ts=%s,
        last_swing_price=%s,
        last_swing_ts=%s
    WHERE id=%s
    """, (trail_price, trail_on, last_15m_ts,
          swing_price, swing_ts, pos_id))

def update_check_1m_ts(pos_id, ts):
    exec_sql("""
    UPDATE positions
    SET last_check_1m_ts=%s
    WHERE id=%s
    """, (ts, pos_id))


# ============================================================
# TRAIL ENGINE (MATCH BACKTEST)
# ============================================================

def compute_trail(pos, df15):

    df15["ATR15"] = compute_atr(df15, ATR_LEN_15)

    side = pos["side"]
    mode = pos["mode"]
    entry = float(pos["entry_price"])
    sl = float(pos["sl_price"])
    trail_on = bool(pos["trail_on"])
    trail = float(pos["trail_price"] or sl)

    last_swing = pos["last_swing_price"]
    last_swing_ts = pos["last_swing_ts"]

    atr15 = df15["ATR15"].iloc[-1]
    floor_mult = FLOOR_ATR_15_EARLY if mode == "EARLY" else FLOOR_ATR_15_CONFIRMED

    pivot_i = len(df15) - 1 - FRACTAL_K

    if pivot_i > ATR_LEN_15:

        atr_piv = df15["ATR15"].iloc[pivot_i]
        ts = df15.index[pivot_i].to_pydatetime()

        if side == "BUY" and is_pivot_low(df15, pivot_i, FRACTAL_K):
            piv = df15["low"].iloc[pivot_i]
            if last_swing is None or abs(piv - last_swing) >= SWING_ATR_MIN_K * atr_piv:
                last_swing = piv
                last_swing_ts = ts

        if side == "SELL" and is_pivot_high(df15, pivot_i, FRACTAL_K):
            piv = df15["high"].iloc[pivot_i]
            if last_swing is None or abs(piv - last_swing) >= SWING_ATR_MIN_K * atr_piv:
                last_swing = piv
                last_swing_ts = ts

    if not trail_on:
        return trail_on, trail, last_swing, last_swing_ts

    if side == "BUY":
        struct = None if last_swing is None else last_swing - STRUCT_PAD_ATR * atr15
        floor = entry - floor_mult * atr15
        cand = floor if struct is None else max(struct, floor)
        trail = max(trail, cand, sl)
    else:
        struct = None if last_swing is None else last_swing + STRUCT_PAD_ATR * atr15
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

        pos = get_open_position(pair_short)

        # ====================================================
        # MANAGE OPEN POSITION
        # ====================================================

        if pos:

            # --- M1: fetch only last 5 minutes (avoid timeout) ---
            WINDOW_MIN = 10
            start_5m_dt = now - timedelta(minutes=WINDOW_MIN)

            df5m = fetch_5m_fx(
                pair,
                start_5m_dt.date().isoformat(),
                now.date().isoformat()
            )

            if df5m is not None and not df5m.empty:
                df5m = df5m[df5m.index >= start_5m_dt]  # trim exact window
                highs = df5m["high"].astype(float)
                lows = df5m["low"].astype(float)

                if pos["side"] == "BUY":
                    if lows.min() <= float(pos["sl_price"]):
                        close_position(pos["id"], "SL")
                        send_telegram(f"❌ SL HIT {pos['id']}")
                        continue
                else:
                    if highs.max() >= float(pos["sl_price"]):
                        close_position(pos["id"], "SL")
                        send_telegram(f"❌ SL HIT {pos['id']}")
                        continue

            df15 = fetch_15m_fx(pair, (now - timedelta(days=2)).date().isoformat(), now.date().isoformat())

            last15 = df15.index[-1].to_pydatetime()

            if pos["last_15m_ts"] and pd.Timestamp(pos["last_15m_ts"]).to_pydatetime() >= last15:
                continue

            # activation
            risk = abs(pos["entry_price"] - pos["sl_price"])
            last = df15.iloc[-1]

            fav = (last["high"] - pos["entry_price"]) / risk if pos["side"] == "BUY" else (pos["entry_price"] - last["low"]) / risk

            act = 0.9 if pos["mode"] == "EARLY" else 0.7
            trail_on = pos["trail_on"]

            if not trail_on and fav >= act:
                trail_on = True
                send_telegram(f"🟣 TRAIL ON {pos['id']}")

            old = pos["trail_price"] or pos["sl_price"]

            trail_on, new, swing, ts = compute_trail({**pos, "trail_on": trail_on}, df15)

            update_trail(pos["id"], new, trail_on, last15, swing, ts)

            if trail_on and abs(new - old) > 0.00005:
                send_telegram(f"🔁 TRAIL {pos['id']} → {new}")

            continue


        # ====================================================
        # ENTRY
        # ====================================================

        df15 = fetch_15m_fx(pair, (now - timedelta(days=4)).date().isoformat(), now.date().isoformat())
        df1h = fetch_1h_fx(pair, (now - timedelta(days=7)).date().isoformat(), now.date().isoformat())

        sig = call_trend_engine(pair, _df_to(df15), _df_to(df1h)).get("signal")

        if not sig:
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

    return out


def _df_to(df):
    return [
        {
            "time": ts.isoformat(),
            "open": float(r["open"]),
            "high": float(r["high"]),
            "low": float(r["low"]),
            "close": float(r["close"])
        }
        for ts, r in df.iterrows()
    ]
