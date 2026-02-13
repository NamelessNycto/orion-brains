import time
from datetime import datetime, timezone

def risk_loop(positions_repo, market_feed, risk_engine, interval_sec: int = 300):
    """
    interval_sec = 300 => 5 minutes
    """
    while True:
        start = time.time()
        now = datetime.now(timezone.utc)

        positions = positions_repo.list_open_positions()
        if positions:
            pairs = sorted(set(p["pair"] for p in positions))
            quotes = market_feed.get_quotes(pairs)  # <= IMPORTANT: tu as déjà ton feed dans brain

            for pos in positions:
                q = quotes.get(pos["pair"])
                if not q:
                    continue
                risk_engine.check_position(pos, q, now)

        elapsed = time.time() - start
        time.sleep(max(1, interval_sec - elapsed))
