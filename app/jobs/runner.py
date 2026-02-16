# app/jobs/runner.py

import os
import logging
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests

from app.db.neon import query_one, exec_sql
from app.services.polygon import fetch_15m_fx, fetch_1h_fx
from app.services.strategy_client import call_trend_engine
from app.services.telegram import send_telegram

log = logging.getLogger("runner")

# ============================================================
# FLAGS
# ============================================================

# DATA_ONLY=1 => fetch only, no strategy, no positions, no telegram
DATA_ONLY = os.getenv("DATA_ONLY", "0") == "1"

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
# WINDOWS / PAYLOAD
# ============================================================

# we fetch a bit more than payload to compute ATR/fractals safely
FETCH_15M_DAYS = 8      # 8 days * 24 * 4 = 768 bars max
FETCH_1H_DAYS = 14      # 14 days * 24 = 336 bars max

PAYLOAD_15M = 400
PAYLOAD_1H = 200

MIN_15M = PAYLOAD_15M   # minimal required to run strategy
MIN_1H = PAYLOAD_1H

# ============================================================
# HELPERS
# ============================================================

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

def _fmt_pair(pair: str) -> str:
    return pair.replace("C:", "").strip()

def _df_to_candles(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    out: list[dict] = []
    for ts, r in df.iterrows():
        out.append({
            "time": ts.isoformat(),
            "open": float(r["open"]),
            "high": float(r["high"]),
            "low":  float(r["low"]),
            "close": float(r["close"]),
        })
    return out

def _is_http_429(err: Exception) -> bool:
    if isinstance(err, requests.exceptions.HTTPError):
        resp = getattr(err, "response", None)
        return bool(resp is not None and getattr(resp, "status_code", None) == 429)
    return False

def _is_http_403_plan(err: Exception) -> bool:
    if isinstance(err, requests.exceptions.HTTPError):
        resp = getattr(err, "response", None)
        return bool(resp is not None and getattr(resp, "status_code", None) == 403)
    return False

def _ensure_utc_index(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    df.index = pd.to_datetime(df.index, utc=True)
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df

def _fetch_last_n(pair_ticker: str, tf: str, n: int, now: datetime) -> pd.DataFrame:
    """
    Fetch from Polygon on a rolling window then take last n rows.
    Your polygon.py already converts OPEN -> CLOSE timestamp.
    """
    if tf == "15m":
        start = now - timedelta(days=FETCH_15M_DAYS)
        df = fetch_15m_fx(pair_ticker, start, now)
    elif tf == "1h":
        start = now - timedelta(days=FETCH_1H_DAYS)
        df = fetch_1h_fx(pair_ticker, start, now)
    else:
        raise ValueError(f"Unsupported tf: {tf}")

    df = _ensure_utc_index(df)
    if df is None or df.empty:
        return df

    # take last n
    return df.tail(int(n))

# ============================================================
# INDICATORS
# ============================================================

def compute_atr(df: pd.DataFrame, n: int = 14):
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)

    prev_close = close.shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return tr.rolling(n).mean()

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
# DB HELPERS (positions)
# ============================================================

def get_open_position(pair_short):
    return query_one(
        """
        SELECT * FROM positions
        WHERE pair=%s AND closed_at IS NULL
        LIMIT 1
        """,
        (pair_short,),
    )

def open_position(pos_id, pair_short, side, mode, entry, sl):
    exec_sql(
        """
        INSERT INTO positions(
            id,pair,side,mode,
            entry_price,sl_price,
            trail_price,trail_on,
            last_15m_ts,last_check_1m_ts,
            last_swing_price,last_swing_ts
        )
        VALUES(%s,%s,%s,%s,%s,%s,NULL,FALSE,NULL,NULL,NULL,NULL)
        """,
        (pos_id, pair_short, side, mode, float(entry), float(sl)),
    )

def close_position(pos_id, reason):
    exec_sql(
        """
        UPDATE positions
        SET closed_at=NOW(), close_reason=%s
        WHERE id=%s
        """,
        (reason, pos_id),
    )

def update_trail(pos_id, trail_price, trail_on, last_15m_ts, swing_price, swing_ts):
    exec_sql(
        """
        UPDATE positions
        SET trail_price=%s,
            trail_on=%s,
            last_15m_ts=%s,
            last_swing_price=%s,
            last_swing_ts=%s
        WHERE id=%s
        """,
        (
            float(trail_price),
            bool(trail_on),
            last_15m_ts,
            (float(swing_price) if swing_price is not None else None),
            swing_ts,
            pos_id,
        ),
    )

# ============================================================
# TRAIL ENGINE (MATCH BACKTEST)
# ============================================================

def compute_trail(pos, df15: pd.DataFrame):
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

    # Only run strategy on real 15m close
    if now.minute % 15 != 0:
        return {"skipped": "not_15m_close"}
    
    out = {"pairs": {}}

    for pair in universe:
        pair_short = _fmt_pair(pair)
        out["pairs"][pair_short] = {"actions": []}

        try:
            # 1) Fetch last N candles directly from Polygon
            df15 = _fetch_last_n(pair, "15m", n=max(PAYLOAD_15M, 450), now=now)
            log.info(f"NOW UTC: {now.isoformat()}")
            log.info(f"LAST 15m CLOSE FROM POLYGON: {df15.index[-1].isoformat()}")
            df1h = _fetch_last_n(pair, "1h",  n=max(PAYLOAD_1H, 250), now=now)

        except Exception as e:
            if _is_http_429(e):
                log.warning("Polygon 429 hit — stopping run until next cron.")
                return {"error": "polygon_rate_limited"}
            if _is_http_403_plan(e):
                log.error("Polygon 403 plan/timeframe issue.")
                return {"error": "polygon_forbidden"}
            raise

        if df15 is None or df15.empty or df1h is None or df1h.empty:
            out["pairs"][pair_short]["actions"].append("no_polygon_data")
            continue

        # 2) If DATA_ONLY => just report latest timestamps
        if DATA_ONLY:
            out["pairs"][pair_short]["actions"].append(
                f"data_only df15={len(df15)} last15={df15.index[-1].isoformat()}"
            )
            out["pairs"][pair_short]["actions"].append(
                f"data_only df1h={len(df1h)} last1h={df1h.index[-1].isoformat()}"
            )
            continue

        # 3) Make sure we have enough bars for strategy payload
        if len(df15) < MIN_15M or len(df1h) < MIN_1H:
            out["pairs"][pair_short]["actions"].append(
                f"not_enough_data df15={len(df15)} df1h={len(df1h)}"
            )
            continue

        # last closed 15m candle (CLOSE ts)
        last15_ts = df15.index[-1].to_pydatetime()

        pos = get_open_position(pair_short)

        # ====================================================
        # MANAGE OPEN POSITION (only once per 15m close)
        # ====================================================
        if pos:
            if pos["last_15m_ts"] is not None:
                if pd.Timestamp(pos["last_15m_ts"]).to_pydatetime() >= last15_ts:
                    out["pairs"][pair_short]["actions"].append("already_processed_15m")
                    continue

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

        payload = call_trend_engine(
            pair,
            _df_to_candles(df15.tail(PAYLOAD_15M)),
            _df_to_candles(df1h.tail(PAYLOAD_1H)),
        )
        sig = {
        "side": "BUY",
        "mode": "EARLY",
        "entry": float(df15.iloc[-1]["close"]),
        "sl": float(df15.iloc[-1]["close"]) - 0.0020
        }

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
