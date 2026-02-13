import pandas as pd
from datetime import datetime, timedelta, timezone
from app.core.config import settings
from app.marketdata.polygon import fetch_aggs
from app.marketdata.resample import resample_15m, resample_1h
from app.services.strategies_client import send_bars
from app.services.telegram import send_message
from app.services.neon import insert_signal

def _date_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def build_candles_payload(pair: str, df15: pd.DataFrame, df1h: pd.DataFrame) -> list[dict]:
    candles = []
    # send only recent bars (last ~50) to strategies (it stores history)
    df15s = df15.iloc[-80:]
    df1hs = df1h.iloc[-200:]

    for t, r in df1hs.iterrows():
        candles.append({
            "pair": pair, "tf": "1h",
            "t": int(t.timestamp()),
            "open": float(r.open), "high": float(r.high), "low": float(r.low), "close": float(r.close)
        })
    for t, r in df15s.iterrows():
        candles.append({
            "pair": pair, "tf": "15m",
            "t": int(t.timestamp()),
            "open": float(r.open), "high": float(r.high), "low": float(r.low), "close": float(r.close)
        })
    return candles

def run_once(pairs: list[str]) -> dict:
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=settings.FETCH_LOOKBACK_DAYS)

    start_s = _date_str(start)
    end_s   = _date_str(now + timedelta(days=1))

    all_signals = []
    reports = []

    for pair in pairs:
        # Fetch native 15m from Polygon directly (faster). If you prefer 1m->resample, change here.
        df15 = fetch_aggs(pair, 15, "minute", start_s, end_s)
        df1h = fetch_aggs(pair, 1, "hour", start_s, end_s)

        if df15.empty or df1h.empty:
            reports.append({"pair": pair, "ok": False, "reason": "no_data"})
            continue

        # Ensure clean resample if needed (optional)
        # df15 = resample_15m(df15)
        # df1h = resample_1h(df1h)

        candles = build_candles_payload(pair, df15, df1h)
        resp = send_bars(candles)

        sigs = resp.get("signals", [])
        reports.append({"pair": pair, "ok": True, "signals": len(sigs)})

        # safety max
        if len(sigs) > settings.MAX_SIGNALS_PER_RUN:
            sigs = sigs[:settings.MAX_SIGNALS_PER_RUN]

        for s in sigs:
            # Neon unique constraint is the true dedupe
            inserted = insert_signal(s)
            if inserted:
                all_signals.append(s)
                # Telegram format
                txt = (
                    f"<b>ORION TREND SIGNAL</b>\n"
                    f"<b>{s['pair']}</b> | {s['side']} | mode={s['mode']}\n"
                    f"entry: <code>{s['entry']}</code>\n"
                    f"sl: <code>{s['sl']}</code>\n"
                    f"p_trend: <code>{round(s['p_trend'], 3)}</code>\n"
                    f"id: <code>{s['signal_id']}</code>"
                )
                send_message(txt)

    return {"signals_sent": len(all_signals), "signals": all_signals, "reports": reports}
