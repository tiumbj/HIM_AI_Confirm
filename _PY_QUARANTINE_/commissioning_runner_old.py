"""
Hybrid Intelligence Mentor (HIM)
File: commissioning_runner.py
Version: v2.12.8
Stable build:
- Event-based (NEW_BAR)
- MT5 time compatibility
- Structured JSON executor
- No missing functions
"""

from __future__ import annotations

import json
import os
import sys
import time
import traceback
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import MetaTrader5 as mt5


# ---------------------------------------------------------
# Utilities
# ---------------------------------------------------------

def utc_iso_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def append_jsonl(path: str, obj: Dict[str, Any]):
    ensure_dir(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"config not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def cfg_get(cfg: Dict[str, Any], keys: List[str], default=None):
    cur = cfg
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


# ---------------------------------------------------------
# MT5 Time Compatibility
# ---------------------------------------------------------

def server_time_epoch() -> int:
    fn = getattr(mt5, "time_current", None)
    if callable(fn):
        try:
            return int(fn())
        except Exception:
            pass
    return int(time.time())


@dataclass
class TickSnapshot:
    bid: float
    ask: float
    tick_time: int
    tick_age_sec: float


def get_tick_snapshot(symbol: str) -> Optional[TickSnapshot]:
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return None

    tick_time = int(getattr(tick, "time", 0))
    server_time = server_time_epoch()
    age = max(0, server_time - tick_time)

    return TickSnapshot(
        bid=float(getattr(tick, "bid", 0.0) or 0.0),
        ask=float(getattr(tick, "ask", 0.0) or 0.0),
        tick_time=tick_time,
        tick_age_sec=float(age),
    )


# ---------------------------------------------------------
# Bar detection
# ---------------------------------------------------------

TF_MAP = {
    "M1": mt5.TIMEFRAME_M1,
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "H1": mt5.TIMEFRAME_H1,
}


def get_latest_bar_time(symbol: str, tf: str):
    tf_code = TF_MAP.get(tf)
    if tf_code is None:
        return None
    rates = mt5.copy_rates_from_pos(symbol, tf_code, 0, 1)
    if not rates:
        return None
    return int(rates[0]["time"])


# ---------------------------------------------------------
# Executor JSON Call
# ---------------------------------------------------------

def run_executor_json(py_exe: str):
    cmd = [py_exe, "mentor_executor_json.py"]
    p = subprocess.run(cmd, capture_output=True, text=True)
    out = (p.stdout or "").strip()
    try:
        obj = json.loads(out) if out else {}
    except Exception:
        obj = {"parse_error": True, "raw": out}
    return p.returncode, obj


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():

    config_path = "config.json"
    cfg = load_config(config_path)

    symbol = cfg.get("symbol", "GOLD")
    poll_sec = cfg_get(cfg, ["commissioning", "poll_sec"], 1.0)
    tick_max_age = cfg_get(cfg, ["execution", "tick_max_age_sec"], 15)
    tf = cfg_get(cfg, ["commissioning", "event_timeframes"], ["M5"])[0]

    events_log = "logs/commissioning_events.jsonl"
    state_file = ".commission_event_state.json"

    last_bar = None

    print("commissioning_runner v2.12.8 start")

    if not mt5.initialize():
        print("MT5 init failed")
        return

    if not mt5.symbol_info(symbol):
        print("Symbol not found")
        return

    while True:
        try:
            tick = get_tick_snapshot(symbol)
            if tick is None:
                time.sleep(poll_sec)
                continue

            if tick.tick_age_sec > tick_max_age:
                time.sleep(poll_sec)
                continue

            bar_time = get_latest_bar_time(symbol, tf)

            if last_bar is None:
                last_bar = bar_time

            if bar_time != last_bar:
                last_bar = bar_time

                rc, ex_json = run_executor_json(sys.executable)

                append_jsonl(events_log, {
                    "ts": utc_iso_now(),
                    "type": "EVENT",
                    "symbol": symbol,
                    "reason": "NEW_BAR",
                    "tick_age_sec": tick.tick_age_sec,
                    "executor_obs": ex_json.get("metrics"),
                    "executor_ok": ex_json.get("ok"),
                })

            time.sleep(poll_sec)

        except KeyboardInterrupt:
            break
        except Exception as e:
            append_jsonl(events_log, {
                "ts": utc_iso_now(),
                "type": "ERROR",
                "error": str(e),
                "trace": traceback.format_exc(limit=2)
            })
            time.sleep(1)


if __name__ == "__main__":
    raise SystemExit(main())