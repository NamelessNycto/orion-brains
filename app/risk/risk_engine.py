from datetime import datetime

class RiskEngine:
    def __init__(self, positions_repo, notifier):
        self.positions_repo = positions_repo
        self.notifier = notifier

    def check_position(self, pos: dict, quote: dict, now: datetime):
        """
        pos fields expected:
          id, pair, side ("BUY"/"SELL"), sl, trail, trail_on (bool)
        quote fields expected (at least one set):
          bid/ask OR mid, plus ideally high/low of last minute (or last 5m)
        """
        side = pos["side"]
        sl = float(pos["sl"])
        trail_on = bool(pos.get("trail_on", False))
        trail = float(pos.get("trail", sl))

        # Prefer bid/ask; fallback to mid
        bid = float(quote.get("bid", quote.get("mid")))
        ask = float(quote.get("ask", quote.get("mid")))
        low = float(quote.get("low", quote.get("mid")))
        high = float(quote.get("high", quote.get("mid")))

        if bid is None or ask is None:
            return  # no price => skip

        # Determine active stop level
        if side == "BUY":
            stop_level = max(sl, trail) if trail_on else sl
            hit = (low <= stop_level) or (bid <= stop_level)
        else:  # SELL
            stop_level = min(sl, trail) if trail_on else sl
            hit = (high >= stop_level) or (ask >= stop_level)

        if hit:
            self.positions_repo.close_position(
                pos_id=pos["id"],
                reason="STOP_HIT",
                price=stop_level,
                closed_at=now,
            )
            self.notifier.exit(pos, reason="STOP_HIT", price=stop_level)
