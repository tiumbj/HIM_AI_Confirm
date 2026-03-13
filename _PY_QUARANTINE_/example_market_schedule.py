from __future__ import annotations

import logging
from datetime import date, time, timezone

from market_schedule import DailySession, TimeManager, market_open_required


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


time_manager = TimeManager(
    tz=timezone.utc,
    holidays={date(2026, 1, 1), date(2026, 12, 25)},
    sessions_by_weekday=[
        (0, DailySession(start_local=time(0, 5), end_local=time(23, 55))),
        (1, DailySession(start_local=time(0, 5), end_local=time(23, 55))),
        (2, DailySession(start_local=time(0, 5), end_local=time(23, 55))),
        (3, DailySession(start_local=time(0, 5), end_local=time(23, 55))),
        (4, DailySession(start_local=time(0, 5), end_local=time(23, 55))),
    ],
)


@market_open_required(time_manager, blocked_return=False)
def execute_trade(symbol: str, side: str, qty: float) -> bool:
    return True


if __name__ == "__main__":
    ok = execute_trade("XAUUSD", "BUY", 0.10)
    logging.getLogger(__name__).info("executed=%s", ok)
