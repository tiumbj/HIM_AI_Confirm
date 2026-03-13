from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Final, Literal, Union, overload
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


MT5_SERVER_TZ_NAME: Final[str] = "Europe/Helsinki"  # EET/EEST (DST-aware)
UTC_TZ_NAME: Final[str] = "UTC"
BANGKOK_TZ_NAME: Final[str] = "Asia/Bangkok"


def _zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise RuntimeError(
            "IANA time zone database not found for zoneinfo. "
            "On Windows, install it via: pip install tzdata"
        ) from exc


@dataclass(frozen=True, slots=True)
class Mt5TimeConversionResult:
    server_time: datetime
    utc_time: datetime
    bangkok_time: datetime


def _parse_iso_datetime(value: str) -> datetime:
    s = value.strip()
    if not s:
        raise ValueError("Empty datetime string is not allowed.")

    if s.endswith(("Z", "z")):
        s = s[:-1] + "+00:00"

    try:
        return datetime.fromisoformat(s)
    except ValueError as exc:
        raise ValueError(f"Invalid ISO datetime string: {value!r}") from exc


@overload
def mt5_server_time_to_bangkok(
    value: Union[datetime, str],
    *,
    fold: Literal[0, 1] = 0,
    return_steps: Literal[False] = False,
) -> datetime: ...


@overload
def mt5_server_time_to_bangkok(
    value: Union[datetime, str],
    *,
    fold: Literal[0, 1] = 0,
    return_steps: Literal[True],
) -> Mt5TimeConversionResult: ...


def mt5_server_time_to_bangkok(
    value: Union[datetime, str],
    *,
    fold: Literal[0, 1] = 0,
    return_steps: bool = False,
) -> Union[datetime, Mt5TimeConversionResult]:
    """
    Normalize and convert MetaTrader 5 (MT5) server time to Thailand local time (Asia/Bangkok).

    Conversion sequence is strictly:
        MT5 Server Time (EET/EEST) -> UTC -> Asia/Bangkok

    Input:
        - datetime: naive is treated as MT5 server local time (EET/EEST); aware is accepted
        - ISO 8601 string: naive or with offset is accepted

    Output:
        - timezone-aware datetime in Asia/Bangkok
        - optionally includes intermediate server/UTC timestamps via return_steps=True

    DST ambiguity:
        - For naive datetimes during a fall-back transition, select fold=0 or fold=1 explicitly.
    """
    try:
        dt = _parse_iso_datetime(value) if isinstance(value, str) else value
    except Exception as exc:
        raise ValueError("Failed to parse input as datetime or ISO string.") from exc

    if not isinstance(dt, datetime):
        raise TypeError("Input must be a datetime or ISO datetime string.")

    server_tz = _zone(MT5_SERVER_TZ_NAME)
    utc_tz = _zone(UTC_TZ_NAME)
    bangkok_tz = _zone(BANGKOK_TZ_NAME)

    if dt.tzinfo is None:
        server_dt = dt.replace(tzinfo=server_tz, fold=fold)
    else:
        server_dt = dt.astimezone(server_tz)

    utc_dt = server_dt.astimezone(utc_tz)
    bangkok_dt = utc_dt.astimezone(bangkok_tz)

    if return_steps:
        return Mt5TimeConversionResult(server_time=server_dt, utc_time=utc_dt, bangkok_time=bangkok_dt)
    return bangkok_dt


if __name__ == "__main__":
    winter_mt5_naive = "2025-01-15T12:00:00"  # EET (UTC+02)
    summer_mt5_naive = "2025-07-15T12:00:00"  # EEST (UTC+03)

    try:
        winter_steps = mt5_server_time_to_bangkok(winter_mt5_naive, return_steps=True)
        summer_steps = mt5_server_time_to_bangkok(summer_mt5_naive, return_steps=True)
    except RuntimeError as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    assert isinstance(winter_steps, Mt5TimeConversionResult)
    assert isinstance(summer_steps, Mt5TimeConversionResult)

    winter_offset = winter_steps.server_time.utcoffset()
    summer_offset = summer_steps.server_time.utcoffset()
    assert winter_offset is not None
    assert summer_offset is not None
    assert winter_offset.total_seconds() == 2 * 3600
    assert summer_offset.total_seconds() == 3 * 3600

    assert winter_steps.utc_time.hour == 10 and winter_steps.bangkok_time.hour == 17
    assert summer_steps.utc_time.hour == 9 and summer_steps.bangkok_time.hour == 16

    print("Winter:", winter_steps)
    print("Summer:", summer_steps)
    print("OK: DST-safe conversion MT5(EET/EEST) -> UTC -> Asia/Bangkok")
