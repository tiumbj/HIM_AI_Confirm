# ============================================================
#  risk_guard_hardstop.py — HIM Equity Risk Guard (Hard Stop)
#  Version: v1.0.0
#  Created: 2026-03-05
#
#  Strategy Header (Project Rule):
#   - Purpose: Hard-stop trading on drawdown breach using sidecar guard.
#   - Production Model: Does NOT modify trading core. Writes KILL_SWITCH.txt only.
#   - Data Source: logs/trades.csv or logs/trade_log.json (best-effort).
#
#  Defaults (override via config.json -> risk_guard):
#   - initial_equity: 10000
#   - max_daily_dd_pct: 2.0   (2% from equity at start of day)
#   - max_total_dd_pct: 8.0   (8% from initial equity)
#   - poll_interval_sec: 5
#
#  Changelog:
#   - v1.0.0:
#       * Log-driven equity curve + daily/total DD checks
#       * Writes KILL_SWITCH.txt on breach (fail-safe)
#       * Writes logs/risk_guard.jsonl
# ============================================================

from __future__ import annotations

import csv
import json
import os
import time
from typing import Any, Dict, List, Tuple, Optional

APP_VERSION = "v1.0.0"

PROJECT_ROOT = os.getcwd()
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
TRADE_LOG_JSON = os.path.join(LOG_DIR, "trade_log.json")

RISK_LOG = os.path.join(LOG_DIR, "risk_guard.jsonl")
KILL_SWITCH_PATH = os.path.join(PROJECT_ROOT, "KILL_SWITCH.txt")
CONFIG_PATH = os.environ.get("HIM_CONFIG_PATH", "config.json").strip() or "config.json"


def _ts() -> int:
    return int(time.time())


def _ensure_logs_dir() -> None:
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
    except Exception:
        pass


def _append_jsonl(path: str, obj: Dict[str, Any]) -> None:
    _ensure_logs_dir()
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _load_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _today_ymd_local(ts: int) -> str:
    return time.strftime("%Y-%m-%d", time.localtime(ts))


def _write_kill_switch(reason: str) -> None:
    # Do not overwrite if already exists (keep first breach reason)
    if os.path.exists(KILL_SWITCH_PATH):
        return
    try:
        with open(KILL_SWITCH_PATH, "w", encoding="utf-8") as f:
            f.write(reason.strip()[:1000])
    except Exception:
        pass


def _kill_switch_exists() -> bool:
    return os.path.exists(KILL_SWITCH_PATH)


def _read_trades_from_csv(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    out: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not isinstance(row, dict):
                    continue
                out.append(row)
    except Exception:
        return []
    return out


def _read_trades_from_trade_log_json(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            data = json.load(f)
        # best-effort: may be list or dict containing list
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            for k in ("trades", "history", "orders", "items"):
                v = data.get(k)
                if isinstance(v, list):
                    return [x for x in v if isinstance(x, dict)]
    except Exception:
        return []
    return []


def _extract_profit(trade: Dict[str, Any]) -> float:
    # best-effort field mapping
    for k in ("profit", "pnl", "pl", "net_profit", "net_pnl"):
        v = trade.get(k)
        f = _safe_float(v)
        if f is not None:
            return f
    # some schemas use "result" or "commission" separate; ignore for now
    return 0.0


def _extract_ts(trade: Dict[str, Any]) -> int:
    # best-effort: ts / time / close_time / exit_time
    for k in ("ts", "time", "close_ts", "close_time", "exit_ts", "exit_time"):
        v = trade.get(k)
        if isinstance(v, (int, float)):
            return int(v)
        if isinstance(v, str):
            # if string like "2026-03-05 ..." -> fallback: use file time? here just now
            # keep simple: can't parse robustly without dependencies
            continue
    return _ts()


def _build_equity_curve(trades: List[Dict[str, Any]], initial_equity: float) -> List[Tuple[int, float, float]]:
    """
    Returns list of (ts, equity, profit) sorted by ts
    """
    items = []
    for t in trades:
        ts = _extract_ts(t)
        profit = _extract_profit(t)
        items.append((ts, profit))
    items.sort(key=lambda x: x[0])

    curve: List[Tuple[int, float, float]] = []
    eq = float(initial_equity)
    for ts, profit in items:
        eq += float(profit)
        curve.append((ts, eq, float(profit)))
    return curve


def main() -> int:
    cfg = _load_config()
    rg = cfg.get("risk_guard", {}) if isinstance(cfg.get("risk_guard", {}), dict) else {}

    initial_equity = float(rg.get("initial_equity", 10000.0))
    max_daily_dd_pct = float(rg.get("max_daily_dd_pct", 2.0))
    max_total_dd_pct = float(rg.get("max_total_dd_pct", 8.0))
    poll_interval_sec = int(rg.get("poll_interval_sec", 5))

    _append_jsonl(RISK_LOG, {
        "ts": _ts(),
        "event": "risk_guard_start",
        "version": APP_VERSION,
        "initial_equity": initial_equity,
        "max_daily_dd_pct": max_daily_dd_pct,
        "max_total_dd_pct": max_total_dd_pct,
        "poll_interval_sec": poll_interval_sec,
        "data_sources": {"trades_csv": TRADES_CSV, "trade_log_json": TRADE_LOG_JSON},
        "kill_switch_path": KILL_SWITCH_PATH,
    })

    last_check = 0
    while True:
        try:
            time.sleep(0.5)
            if _ts() - last_check < poll_interval_sec:
                continue
            last_check = _ts()

            # Load trades
            trades = _read_trades_from_csv(TRADES_CSV)
            if not trades:
                trades = _read_trades_from_trade_log_json(TRADE_LOG_JSON)

            curve = _build_equity_curve(trades, initial_equity)
            if not curve:
                _append_jsonl(RISK_LOG, {
                    "ts": _ts(),
                    "event": "risk_guard_tick",
                    "version": APP_VERSION,
                    "status": "no_trades_data",
                })
                continue

            last_ts, last_eq, _ = curve[-1]
            total_dd_pct = max(0.0, (initial_equity - last_eq) / initial_equity * 100.0)

            # Daily DD: compare to equity at first trade of day (or initial_equity if none)
            day = _today_ymd_local(last_ts)
            day_start_eq = None
            for ts, eq, _p in curve:
                if _today_ymd_local(ts) == day:
                    # equity after this trade is eq; equity at start-of-day is eq - profit_of_this_trade
                    # easier: approximate day start as equity before first trade of day:
                    # find first trade of day and reconstruct:
                    idx = curve.index((ts, eq, _p))
                    if idx == 0:
                        day_start_eq = initial_equity
                    else:
                        day_start_eq = curve[idx - 1][1]
                    break
            if day_start_eq is None:
                day_start_eq = initial_equity

            daily_dd_pct = max(0.0, (day_start_eq - last_eq) / day_start_eq * 100.0)

            breach = []
            if daily_dd_pct >= max_daily_dd_pct:
                breach.append(f"daily_dd_pct={daily_dd_pct:.2f}>=max_daily_dd_pct={max_daily_dd_pct:.2f}")
            if total_dd_pct >= max_total_dd_pct:
                breach.append(f"total_dd_pct={total_dd_pct:.2f}>=max_total_dd_pct={max_total_dd_pct:.2f}")

            _append_jsonl(RISK_LOG, {
                "ts": _ts(),
                "event": "risk_guard_tick",
                "version": APP_VERSION,
                "equity": round(last_eq, 2),
                "day": day,
                "daily_dd_pct": round(daily_dd_pct, 4),
                "total_dd_pct": round(total_dd_pct, 4),
                "kill_switch": _kill_switch_exists(),
            })

            if breach and not _kill_switch_exists():
                reason = (
                    f"[HIM HARD STOP]\n"
                    f"ts={_ts()}\n"
                    f"equity={last_eq:.2f}\n"
                    f"initial_equity={initial_equity:.2f}\n"
                    f"day={day}\n"
                    f"daily_dd_pct={daily_dd_pct:.4f}\n"
                    f"total_dd_pct={total_dd_pct:.4f}\n"
                    f"breach={' | '.join(breach)}\n"
                )
                _write_kill_switch(reason)
                _append_jsonl(RISK_LOG, {
                    "ts": _ts(),
                    "event": "kill_switch_written",
                    "version": APP_VERSION,
                    "reason": " | ".join(breach)[:400],
                })

        except KeyboardInterrupt:
            _append_jsonl(RISK_LOG, {"ts": _ts(), "event": "risk_guard_stop", "version": APP_VERSION})
            return 0
        except Exception as e:
            _append_jsonl(RISK_LOG, {"ts": _ts(), "event": "risk_guard_error", "version": APP_VERSION, "error": str(e)[:300]})
            time.sleep(1.0)


if __name__ == "__main__":
    raise SystemExit(main())