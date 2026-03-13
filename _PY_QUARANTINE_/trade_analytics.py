# ============================================================
#  trade_analytics.py — HIM Trade Analytics Engine
#  Version: v1.0.0
#  Created: 2026-03-05
#
#  Strategy Header (Project Rule):
#   - Purpose: Compute live performance KPIs (Winrate / Expectancy / Sharpe / MaxDD)
#   - Production Model: Read-only; consumes logs and writes summary file.
#   - Data sources: logs/trades.csv or logs/trade_log.json
#
#  Output:
#   - logs/performance_summary_live.json
#
#  Changelog:
#   - v1.0.0:
#       * Best-effort parsing for common trade logs
#       * Equity curve + KPIs + JSON output
#       * --update (one-shot), --daemon (loop)
# ============================================================

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

APP_VERSION = "v1.0.0"

PROJECT_ROOT = os.getcwd()
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
TRADE_LOG_JSON = os.path.join(LOG_DIR, "trade_log.json")
OUT_JSON = os.path.join(LOG_DIR, "performance_summary_live.json")

CONFIG_PATH = os.environ.get("HIM_CONFIG_PATH", "config.json").strip() or "config.json"


def _ts() -> int:
    return int(time.time())


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


def _read_trades_from_csv(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    out: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if isinstance(row, dict):
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
    for k in ("profit", "pnl", "pl", "net_profit", "net_pnl"):
        f = _safe_float(trade.get(k))
        if f is not None:
            return f
    return 0.0


def _extract_ts(trade: Dict[str, Any]) -> int:
    for k in ("ts", "time", "close_ts", "close_time", "exit_ts", "exit_time"):
        v = trade.get(k)
        if isinstance(v, (int, float)):
            return int(v)
    return _ts()


def _equity_curve(trades: List[Dict[str, Any]], initial_equity: float) -> List[Tuple[int, float, float]]:
    items = []
    for t in trades:
        items.append((_extract_ts(t), _extract_profit(t)))
    items.sort(key=lambda x: x[0])

    eq = float(initial_equity)
    curve: List[Tuple[int, float, float]] = []
    for ts, profit in items:
        eq += float(profit)
        curve.append((ts, eq, float(profit)))
    return curve


def _max_drawdown_pct(curve: List[Tuple[int, float, float]], initial_equity: float) -> float:
    peak = float(initial_equity)
    max_dd = 0.0
    for _ts_, eq, _p in curve:
        peak = max(peak, eq)
        dd = (peak - eq) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)
    return max_dd * 100.0


def _sharpe_from_returns(returns: List[float]) -> float:
    # Simple Sharpe (risk-free=0). Not annualized (trade-level).
    n = len(returns)
    if n < 2:
        return 0.0
    mean = sum(returns) / n
    var = sum((r - mean) ** 2 for r in returns) / (n - 1)
    std = math.sqrt(var)
    if std <= 1e-12:
        return 0.0
    return mean / std


def compute_and_write() -> Dict[str, Any]:
    cfg = _load_config()
    rg = cfg.get("risk_guard", {}) if isinstance(cfg.get("risk_guard", {}), dict) else {}

    initial_equity = float(rg.get("initial_equity", 10000.0))

    trades = _read_trades_from_csv(TRADES_CSV)
    source = "trades.csv"
    if not trades:
        trades = _read_trades_from_trade_log_json(TRADE_LOG_JSON)
        source = "trade_log.json"

    curve = _equity_curve(trades, initial_equity)

    profits = [p for _t, _eq, p in curve]
    n = len(profits)
    wins = sum(1 for p in profits if p > 0)
    losses = sum(1 for p in profits if p < 0)
    winrate = (wins / n * 100.0) if n > 0 else 0.0

    avg_profit = (sum(profits) / n) if n > 0 else 0.0
    gross_profit = sum(p for p in profits if p > 0)
    gross_loss = sum(p for p in profits if p < 0)  # negative
    profit_factor = (gross_profit / abs(gross_loss)) if gross_loss < 0 else (float("inf") if gross_profit > 0 else 0.0)

    last_eq = curve[-1][1] if curve else initial_equity
    total_return_pct = ((last_eq - initial_equity) / initial_equity * 100.0) if initial_equity > 0 else 0.0

    max_dd_pct = _max_drawdown_pct(curve, initial_equity) if curve else 0.0

    # Trade returns (profit / equity_before_trade)
    returns: List[float] = []
    eq_prev = initial_equity
    for _t, eq, p in curve:
        if eq_prev > 0:
            returns.append(p / eq_prev)
        eq_prev = eq
    sharpe = _sharpe_from_returns(returns)

    out = {
        "ok": True,
        "ts": _ts(),
        "version": APP_VERSION,
        "source": source,
        "initial_equity": round(initial_equity, 2),
        "equity_last": round(last_eq, 2),
        "total_return_pct": round(total_return_pct, 6),
        "max_drawdown_pct": round(max_dd_pct, 6),
        "trades": {
            "count": n,
            "wins": wins,
            "losses": losses,
            "winrate_pct": round(winrate, 6),
            "avg_profit_per_trade": round(avg_profit, 6),
            "gross_profit": round(gross_profit, 6),
            "gross_loss": round(gross_loss, 6),
            "profit_factor": (round(profit_factor, 6) if math.isfinite(profit_factor) else "inf"),
        },
        "sharpe_trade_level": round(sharpe, 6),
    }

    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        with open(OUT_JSON, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
    except Exception:
        out["ok"] = False
        out["error"] = "write_failed"

    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--update", action="store_true", help="one-shot update summary")
    ap.add_argument("--daemon", action="store_true", help="run continuously")
    ap.add_argument("--interval", type=int, default=10, help="daemon interval seconds")
    args = ap.parse_args()

    if args.update:
        compute_and_write()
        return 0

    if args.daemon:
        while True:
            try:
                compute_and_write()
                time.sleep(max(2, int(args.interval)))
            except KeyboardInterrupt:
                return 0
            except Exception:
                time.sleep(2)
        return 0

    # default: one-shot
    compute_and_write()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())