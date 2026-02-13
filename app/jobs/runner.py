import logging
from datetime import datetime, timedelta, timezone
import pandas as pd

from app.core.config import settings
from app.db.neon import query_one, exec_sql
from app.services.polygon import fetch_15m_fx, fetch_1h_fx, fetch_1m_fx
from app.services.strategy_client import call_trend_engine
from app.services.telegram import send_telegram

log = logging.getLogger("runner")

def _utc_now():
    return datetime.now(timezone.utc)

def _fmt_pair(pair: str) -> str:
    # "C:EURUSD" -> "EURUSD"
    return pair.replace("C:", "").strip()

def init_counter_if_missing(pair_short: str):
    exec_sql("""
    INSERT INTO pair_counters(pair, next_seq)
    VALUES (%s, 1)
    ON CONFLICT (pair) DO NOTHING
    """, (pair_short,))

def next_signal_id(pair_short: str) -> str:
    """
    Atomic increment in DB.
    """
    init_counter_if_missing(pair_short)
    row = query_one("""
    UPDATE pair_counters
    SET next_seq = next_seq + 1
    WHERE pair = %s
    RETURNING next_seq
    """, (pair_short,))
    # next_seq returned is the incremented value, so used = next_seq-1
    used = int(row["next_seq"]) - 1
    return f"TR-{pair_short}-{used:06d}-OR"

def get_open_position(pair_short: str):
    return query_one("""
      SELECT * FROM positions
      WHERE pair=%s AND closed_at IS NULL
      LIMIT 1
    """, (pair_short,))

def open_position(pos_id: str, pair_short: str, side: str, mode: str, entry: float, sl: float):
    exec_sql("""
    INSERT INTO positions(
      id, pair, side, mode, entry_price, sl_price,
      trail_price, trail_on, last_15m_ts, last_check_1m_ts
    ) VALUES (%s,%s,%s,%s,%s,%s,NULL,FALSE,NULL,NULL)
    """, (pos_id, pair_short, side, mode, float(entry), float(sl)))

def close_position(pos_id: str, reason: str):
    exec_sql("""
    UPDATE positions
    SET closed_at = NOW(), close_reason=%s
    WHERE id=%s
    """, (reason, pos_id))

def update_trail(pos_id: str, trail_price: float, trail_on: bool, last_15m_ts: datetime):
    exec_sql("""
    UPDATE positions
    SET trail_price=%s, trail_on=%s, last_15m_ts=%s
    WHERE id=%s
    """, (float(trail_price), bool(trail_on), last_15m_ts, pos_id))

def update_check_1m_ts(pos_id: str, last_1m_ts: datetime):
    exec_sql("""
    UPDATE positions
    SET last_check_1m_ts=%s
    WHERE id=%s
    """, (last_1m_ts, pos_id))

def _df_to_candles(df: pd.DataFrame) -> list:
    # to list of dict for strategy
    out = []
    if df is None or df.empty:
        return out
    for ts, row in df.iterrows():
        out.append({
            "time": ts.isoformat(),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
        })
    return out

def _last_closed_15m_ts(df15: pd.DataFrame) -> datetime | None:
    if df15 is None or df15.empty:
        return None
    # last index is a closed bar in Polygon aggs context
    return df15.index[-1].to_pydatetime()

def _compute_trail_from_15m(pos: dict, df15: pd.DataFrame) -> tuple[bool, float]:
    """
    ⚠️ Simple & clean:
    - Trail is updated only on closed 15m bars.
    - We delegate the exact trail logic to strategy? (possible)
      BUT to stay simple and match your backtest behavior,
      we implement a minimal "structure-ish" trail here:

    Rule (simple):
    - Once trail_on, trail_price moves to:
        BUY: max(prev_trail, lowest_low_last_6_bars - small_pad)
        SELL: min(prev_trail, highest_high_last_6_bars + small_pad)

    This is robust, cheap, and matches “under HL / above LH” spirit.
    """
    LOOKBACK = 6
    PAD = 0.0001  # for FX majors; you can tune per pair later

    side = pos["side"]
    entry = float(pos["entry_price"])
    sl = float(pos["sl_price"])
    trail_on = bool(pos["trail_on"])
    trail = pos["trail_price"]
    trail = float(trail) if trail is not None else sl

    if df15 is None or len(df15) < LOOKBACK:
        return trail_on, trail

    look = df15.iloc[-LOOKBACK:]
    if side == "BUY":
        struct = float(look["low"].min()) - PAD
        new_trail = max(trail, struct, sl)
    else:
        struct = float(look["high"].max()) + PAD
        new_trail = min(trail, struct, sl)

    return trail_on, new_trail

def _trail_activation_check(pos: dict, df15: pd.DataFrame) -> bool:
    """
    Activate trail after favorable move (R-based).
    We keep it simple: compute R using SL distance.
    """
    side = pos["side"]
    entry = float(pos["entry_price"])
    sl = float(pos["sl_price"])
    risk = abs(entry - sl)
    if risk <= 0 or df15 is None or df15.empty:
        return False

    last = df15.iloc[-1]
    hi = float(last["high"])
    lo = float(last["low"])

    # thresholds from your engine (keep same names)
    if pos["mode"] == "EARLY":
        activate_r = 0.90
    else:
        activate_r = 0.70

    if side == "BUY":
        fav = (hi - entry) / risk
    else:
        fav = (entry - lo) / risk

    return fav >= activate_r

def _check_hits_1m(pos: dict, df1m: pd.DataFrame) -> str | None:
    """
    Return "SL" or "TRAIL" if hit since last check, else None.
    """
    if df1m is None or df1m.empty:
        return None
    side = pos["side"]
    sl = float(pos["sl_price"])
    trail = pos["trail_price"]
    trail = float(trail) if trail is not None else None
    trail_on = bool(pos["trail_on"])

    highs = df1m["high"].astype(float)
    lows = df1m["low"].astype(float)

    if side == "BUY":
        if (lows.min() <= sl):
            return "SL"
        if trail_on and trail is not None and (lows.min() <= trail):
            return "TRAIL"
    else:
        if (highs.max() >= sl):
            return "SL"
        if trail_on and trail is not None and (highs.max() >= trail):
            return "TRAIL"

    return None

def run_once(universe_pairs: list[str]) -> dict:
    """
    Called by /v1/run
    """
    now = _utc_now()
    start_lookback = (now - timedelta(days=7)).date().isoformat()
    end_today = now.date().isoformat()

    out = {"ok": True, "ts": now.isoformat(), "pairs": {}}

    for pair in universe_pairs:
        pair = pair if pair.startswith("C:") else f"C:{pair}"
        pair_short = _fmt_pair(pair)
        try:
            out["pairs"][pair_short] = {"actions": []}

            # 1) if position open -> manage it
            pos = get_open_position(pair_short)
            if pos:
                # 1a) check SL/trail hits via 1m since last check
                last_check = pos["last_check_1m_ts"]
                if last_check is None:
                    # start from 10 minutes ago
                    start_1m = (now - timedelta(minutes=10)).isoformat()[:10]
                else:
                    # Polygon endpoint takes date strings; keep simple: take today range
                    start_1m = now.date().isoformat()

                df1m = fetch_1m_fx(pair, start_1m, end_today)
                hit = _check_hits_1m(pos, df1m)
                if hit:
                    close_position(pos["id"], hit)
                    send_telegram(
                        f"✅ EXIT {hit}\n"
                        f"id={pos['id']}\npair={pair_short}\nside={pos['side']}\n"
                        f"entry={pos['entry_price']}\nsl={pos['sl_price']}\ntrail={pos['trail_price']}"
                    )
                    out["pairs"][pair_short]["actions"].append(f"closed_{hit}")
                    continue

                # update last_check_1m_ts to last 1m bar time if available
                if df1m is not None and not df1m.empty:
                    update_check_1m_ts(pos["id"], df1m.index[-1].to_pydatetime())

                # 1b) update trail only when a NEW 15m candle is available
                df15 = fetch_15m_fx(pair, start_lookback, end_today)
                if df15 is None or df15.empty:
                    out["pairs"][pair_short]["actions"].append("no_15m_data")
                    continue

                last_15m = _last_closed_15m_ts(df15)
                if last_15m is None:
                    continue

                # if already processed this 15m bar, skip
                if pos["last_15m_ts"] is not None:
                    if pd.Timestamp(pos["last_15m_ts"]).to_pydatetime() >= last_15m:
                        out["pairs"][pair_short]["actions"].append("trail_skip_same_15m")
                        continue

                # activate trail if conditions met
                trail_on = bool(pos["trail_on"])
                if not trail_on:
                    if _trail_activation_check(pos, df15):
                        trail_on = True
                        send_telegram(
                            f"🟣 TRAIL_ON\nid={pos['id']}\npair={pair_short}\nside={pos['side']}\n"
                            f"entry={pos['entry_price']}\nsl={pos['sl_price']}"
                        )
                        out["pairs"][pair_short]["actions"].append("trail_on")

                # compute new trail
                old_trail = float(pos["trail_price"]) if pos["trail_price"] is not None else float(pos["sl_price"])
                _, new_trail = _compute_trail_from_15m({**pos, "trail_on": trail_on}, df15)

                # update in db + telegram only if changed meaningfully
                # For EURUSD-like: 0.00005 = 0.5 pip
                changed = abs(new_trail - old_trail) >= 0.00005
                update_trail(pos["id"], new_trail, trail_on, last_15m)

                if trail_on and changed:
                    send_telegram(
                        f"🔁 TRAIL_UPDATE\nid={pos['id']}\npair={pair_short}\nside={pos['side']}\n"
                        f"trail={new_trail:.5f}"
                    )
                    out["pairs"][pair_short]["actions"].append("trail_update")
                else:
                    out["pairs"][pair_short]["actions"].append("trail_no_change")

                continue

            # 2) no open position -> ask strategy for a new entry
            df15 = fetch_15m_fx(pair, start_lookback, end_today)
            df1h = fetch_1h_fx(pair, start_lookback, end_today)

            if df15 is None or df15.empty or df1h is None or df1h.empty:
                out["pairs"][pair_short]["actions"].append("no_data_for_entry")
                continue

            payload = call_trend_engine(
                pair=pair,
                candles_15m=_df_to_candles(df15.tail(400)),
                candles_1h=_df_to_candles(df1h.tail(400))
            )

            sig = payload.get("signal")
            if not sig:
                out["pairs"][pair_short]["actions"].append("no_signal")
                continue

            side = sig["side"]
            mode = sig["mode"]
            entry = float(sig["entry"])
            sl = float(sig["sl"])

            # open
            sid = next_signal_id(pair_short)
            open_position(sid, pair_short, side, mode, entry, sl)

            send_telegram(
                f"🚀 ENTRY {side} ({mode})\n"
                f"id={sid}\n"
                f"pair={pair_short}\n"
                f"entry={entry}\n"
                f"sl={sl}"
            )
            out["pairs"][pair_short]["actions"].append(f"opened_{side}_{mode}")

        except Exception as e:
            log.exception(f"run_once error pair={pair_short}: {e}")
            out["pairs"][pair_short] = {"error": str(e)}

    return out
