from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Callable, Iterable, Optional, ParamSpec, Protocol, Sequence, Set, Tuple, TypeVar
from urllib.request import Request, urlopen

P = ParamSpec("P")
R = TypeVar("R")

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MarketDecision:
    allowed: bool
    reason: str
    now_utc: datetime


@dataclass(frozen=True, slots=True)
class DailySession:
    start_local: time
    end_local: time


class UtcNowProvider(Protocol):
    def __call__(self) -> datetime: ...


def system_utc_now() -> datetime:
    return datetime.now(timezone.utc)


def http_utc_now(
    *,
    url: str,
    timeout_sec: float = 2.5,
    bearer_token: Optional[str] = None,
    header_name: str = "X-Server-Time-UTC",
) -> datetime:
    headers = {"Accept": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    req = Request(url, method="GET", headers=headers)
    with urlopen(req, timeout=timeout_sec) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    try:
        js = json.loads(body)
        if isinstance(js, dict):
            v = js.get("utc") or js.get("now_utc") or js.get("timestamp_utc")
            if isinstance(v, str) and v.strip():
                return datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(timezone.utc)
            v2 = js.get("epoch_ms") or js.get("ts_ms")
            if isinstance(v2, (int, float)):
                return datetime.fromtimestamp(float(v2) / 1000.0, tz=timezone.utc)
    except Exception:
        pass

    return datetime.fromisoformat(body.strip().replace("Z", "+00:00")).astimezone(timezone.utc)


class TimeManager:
    def __init__(
        self,
        *,
        tz: timezone = timezone.utc,
        sessions_by_weekday: Optional[Sequence[Tuple[int, DailySession]]] = None,
        holidays: Optional[Iterable[date]] = None,
        utc_now: Optional[UtcNowProvider] = None,
        fail_closed: bool = True,
    ) -> None:
        self._tz = tz
        self._holidays: Set[date] = set(holidays or [])
        self._utc_now: UtcNowProvider = utc_now or system_utc_now
        self._fail_closed = bool(fail_closed)

        if sessions_by_weekday is None:
            sessions_by_weekday = [
                (0, DailySession(time(0, 5), time(23, 55))),
                (1, DailySession(time(0, 5), time(23, 55))),
                (2, DailySession(time(0, 5), time(23, 55))),
                (3, DailySession(time(0, 5), time(23, 55))),
                (4, DailySession(time(0, 5), time(23, 55))),
            ]

        tmp: dict[int, DailySession] = {}
        for wd, sess in sessions_by_weekday:
            tmp[int(wd)] = sess
        self._sessions_by_weekday = tmp

    def decide(self, now_utc: Optional[datetime] = None) -> MarketDecision:
        try:
            now = now_utc or self._utc_now()
            if now.tzinfo is None:
                now = now.replace(tzinfo=timezone.utc)
            now = now.astimezone(timezone.utc)
        except Exception as e:
            logger.exception("time_source_failed: %s", e)
            if self._fail_closed:
                return MarketDecision(False, "time_source_failed", system_utc_now())
            now = system_utc_now()

        now_local = now.astimezone(self._tz)
        today_local = now_local.date()
        weekday = int(now_local.weekday())

        if weekday >= 5:
            return MarketDecision(False, "weekend", now)

        if today_local in self._holidays:
            return MarketDecision(False, "holiday", now)

        sess = self._sessions_by_weekday.get(weekday)
        if not sess:
            return MarketDecision(False, "no_session_configured", now)

        t = now_local.time()
        if sess.start_local <= sess.end_local:
            inside = (t >= sess.start_local) and (t <= sess.end_local)
        else:
            inside = (t >= sess.start_local) or (t <= sess.end_local)

        if not inside:
            return MarketDecision(False, "outside_trading_hours", now)

        return MarketDecision(True, "ok", now)


def market_open_required(
    manager: TimeManager,
    *,
    blocked_return: Optional[R] = None,
    log_level: int = logging.INFO,
) -> Callable[[Callable[P, R]], Callable[P, Optional[R]]]:
    def decorator(fn: Callable[P, R]) -> Callable[P, Optional[R]]:
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> Optional[R]:
            try:
                decision = manager.decide()
            except Exception as e:
                logger.exception("market_time_check_crashed: %s", e)
                logger.log(log_level, "Market schedule check failed; aborting trade.")
                return blocked_return

            if not decision.allowed:
                logger.log(log_level, "Market Closed (%s). Aborting trade.", decision.reason)
                return blocked_return

            try:
                return fn(*args, **kwargs)
            except Exception as e:
                logger.exception("wrapped_execution_failed: %s", e)
                raise

        return wrapped

    return decorator

