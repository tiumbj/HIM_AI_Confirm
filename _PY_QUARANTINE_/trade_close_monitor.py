"""
HIM Trade Close Monitor
Version: 2.0.0
Changelog:
- 2.0.0: Poll MT5 history_deals_get and append `trade_close` events to trade_history.json.
         Filters by magic number used by HIM executor.
         Computes real winrate from profit; computes R multiple when possible.

Design:
- State file: trade_close_state.json keeps last_deal_time (epoch seconds).
- Uses MT5 deals (DEAL_ENTRY_OUT / DEAL_ENTRY_OUT_BY) as close events.
- Attempts to compute R:
  R = (close_price - open_price)/abs(open_price - sl) for BUY
  R = (open_price - close_price)/abs(open_price - sl) for SELL
  - open_price taken from the first IN deal for the same position_id
  - sl taken by matching the nearest logged order event in trade_history.json
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import MetaTrader5 as mt5

from trade_logger import TradeLogger


MAGIC_DEFAULT = 20260225


def _load_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _atomic_write_json(path: str, data: Dict[str, Any]) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _ensure_mt5() -> None:
    if mt5.terminal_info() is not None:
        return
    if not mt5.initialize():
        raise RuntimeError(f"MT5 initialize failed: {mt5.last_error()}")


def _mt5_epoch_to_ts(epoch: int) -> int:
    return int(epoch)


def _to_dt(ts: int) -> datetime:
    return datetime.fromtimestamp(int(ts))


def _deal_entry_type_name(entry: int) -> str:
    # best-effort human label
    try:
        if entry == mt5.DEAL_ENTRY_IN:
            return "IN"
        if entry == mt5.DEAL_ENTRY_OUT:
            return "OUT"
        if entry == mt5.DEAL_ENTRY_INOUT:
            return "INOUT"
        if entry == mt5.DEAL_ENTRY_OUT_BY:
            return "OUT_BY"
    except Exception:
        pass
    return str(entry)


def _is_close_deal(entry: int) -> bool:
    try:
        return entry in (mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_OUT_BY)
    except Exception:
        return False


def _is_open_deal(entry: int) -> bool:
    try:
        return entry == mt5.DEAL_ENTRY_IN
    except Exception:
        return False


def _load_trade_history_events(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _find_order_sl_for_position(
    trade_events: List[Dict[str, Any]],
    symbol: str,
    direction: str,
    open_ts: int,
    open_price: float,
    time_window_sec: int = 6 * 3600,
    price_tol: float = 5.0,  # GOLD price tolerance (points / dollars) best-effort
) -> Optional[float]:
    """
    Match the closest successful order event to infer initial SL.
    (Because MT5 close deal doesn't always carry the SL used at entry.)
    """
    best = None
    best_key = 1e18

    for ev in trade_events:
        if ev.get("type") != "order":
            continue
        if str(ev.get("symbol", "")) != symbol:
            continue
        if str(ev.get("direction", "")) != direction:
            continue

        order_result = ev.get("order_result", {}) or {}
        if not bool(order_result.get("ok", False)):
            continue

        ts = int(ev.get("ts", 0))
        if abs(ts - open_ts) > time_window_sec:
            continue

        req = order_result.get("request", {}) or {}
        sl = req.get("sl", None)
        if sl is None:
            continue

        entry_price_logged = float(order_result.get("price", 0.0))
        # Combine time distance + price distance
        k = abs(ts - open_ts) + (abs(entry_price_logged - open_price) * 100.0) + (abs(entry_price_logged - open_price) > price_tol) * 1e9
        if k < best_key:
            best_key = k
            best = float(sl)

    return best


def _compute_r_multiple(direction: str, open_price: float, close_price: float, sl: Optional[float]) -> Optional[float]:
    if sl is None:
        return None
    risk = abs(open_price - float(sl))
    if risk <= 1e-12:
        return None
    if direction == "BUY":
        return float((close_price - open_price) / risk)
    if direction == "SELL":
        return float((open_price - close_price) / risk)
    return None


class TradeCloseMonitor:
    def __init__(self, magic: int = MAGIC_DEFAULT):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.state_path = os.path.join(base_dir, "trade_close_state.json")
        self.history_path = os.path.join(base_dir, "trade_history.json")
        self.tlog = TradeLogger(self.history_path)
        self.magic = int(magic)

    def _load_state(self) -> Dict[str, Any]:
        s = _load_json(self.state_path)
        if "last_deal_ts" not in s:
            # default: scan last 7 days
            s["last_deal_ts"] = int(time.time()) - 7 * 24 * 3600
        return s

    def _save_state(self, state: Dict[str, Any]) -> None:
        _atomic_write_json(self.state_path, state)

    def poll_once(self) -> int:
        _ensure_mt5()

        state = self._load_state()
        last_ts = int(state.get("last_deal_ts", int(time.time()) - 7 * 24 * 3600))

        # MT5 history query window
        dt_from = _to_dt(last_ts) - timedelta(minutes=5)
        dt_to = datetime.now() + timedelta(minutes=5)

        deals = mt5.history_deals_get(dt_from, dt_to)
        if deals is None:
            raise RuntimeError(f"history_deals_get failed: {mt5.last_error()}")

        trade_events = _load_trade_history_events(self.history_path)

        # Dedup: load already logged close deal ids
        logged_close_ids = set()
        for ev in trade_events:
            if ev.get("type") == "trade_close":
                did = ev.get("deal_id", None)
                if did is not None:
                    logged_close_ids.add(int(did))

        new_count = 0
        max_seen_ts = last_ts

        # Build index of open deals by position_id (first IN deal)
        open_by_pos: Dict[int, Dict[str, Any]] = {}
        for d in deals:
            if int(getattr(d, "magic", 0)) != self.magic:
                continue
            entry = int(getattr(d, "entry", -1))
            if not _is_open_deal(entry):
                continue
            pos_id = int(getattr(d, "position_id", 0))
            ts = _mt5_epoch_to_ts(int(getattr(d, "time", 0)))
            price = float(getattr(d, "price", 0.0))
            symbol = str(getattr(d, "symbol", ""))
            dtype = int(getattr(d, "type", -1))
            direction = "BUY" if dtype == mt5.DEAL_TYPE_BUY else "SELL" if dtype == mt5.DEAL_TYPE_SELL else "UNKNOWN"

            # keep earliest open deal for the position
            if pos_id not in open_by_pos or ts < int(open_by_pos[pos_id]["open_ts"]):
                open_by_pos[pos_id] = {
                    "open_ts": ts,
                    "open_price": price,
                    "symbol": symbol,
                    "direction": direction,
                }

        for d in deals:
            magic = int(getattr(d, "magic", 0))
            if magic != self.magic:
                continue

            deal_id = int(getattr(d, "ticket", 0))
            if deal_id in logged_close_ids:
                continue

            ts = _mt5_epoch_to_ts(int(getattr(d, "time", 0)))
            if ts > max_seen_ts:
                max_seen_ts = ts

            entry = int(getattr(d, "entry", -1))
            if not _is_close_deal(entry):
                continue

            symbol = str(getattr(d, "symbol", ""))
            pos_id = int(getattr(d, "position_id", 0))
            dtype = int(getattr(d, "type", -1))
            direction = "BUY" if dtype == mt5.DEAL_TYPE_SELL else "SELL" if dtype == mt5.DEAL_TYPE_BUY else "UNKNOWN"
            # NOTE: Close deal direction is opposite of open. For R we need open direction.
            open_info = open_by_pos.get(pos_id)

            close_price = float(getattr(d, "price", 0.0))
            profit = float(getattr(d, "profit", 0.0))
            commission = float(getattr(d, "commission", 0.0))
            swap = float(getattr(d, "swap", 0.0))
            volume = float(getattr(d, "volume", 0.0))
            comment = str(getattr(d, "comment", ""))

            open_price = None
            open_ts = None
            open_dir = None

            if open_info:
                open_price = float(open_info["open_price"])
                open_ts = int(open_info["open_ts"])
                open_dir = str(open_info["direction"])
            else:
                # fallback: cannot compute R reliably
                open_price = None
                open_ts = None
                open_dir = None

            sl = None
            r_mult = None
            if open_price is not None and open_ts is not None and open_dir in ("BUY", "SELL"):
                sl = _find_order_sl_for_position(
                    trade_events=trade_events,
                    symbol=symbol,
                    direction=open_dir,
                    open_ts=open_ts,
                    open_price=open_price,
                )
                r_mult = _compute_r_multiple(open_dir, open_price, close_price, sl)

            ev = {
                "type": "trade_close",
                "deal_id": deal_id,
                "position_id": pos_id,
                "magic": magic,
                "symbol": symbol,
                "close_ts": ts,
                "close_ts_iso": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)),
                "close_price": close_price,
                "profit": profit,
                "commission": commission,
                "swap": swap,
                "volume": volume,
                "comment": comment,
                "entry_type": _deal_entry_type_name(entry),
                "open": {
                    "open_ts": open_ts,
                    "open_ts_iso": None if open_ts is None else time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(open_ts)),
                    "open_price": open_price,
                    "direction": open_dir,  # direction of the opened position
                    "sl": sl,
                },
                "r_multiple": r_mult,
            }

            self.tlog.append(ev)
            new_count += 1

        # update state
        state["last_deal_ts"] = int(max_seen_ts)
        self._save_state(state)
        return new_count


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--magic", type=int, default=MAGIC_DEFAULT)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--interval", type=int, default=15)
    args = ap.parse_args()

    mon = TradeCloseMonitor(magic=args.magic)

    if args.once or not args.loop:
        n = mon.poll_once()
        print(f"new_close_events={n}")
        return

    while True:
        try:
            n = mon.poll_once()
            if n > 0:
                print(f"new_close_events={n}")
        except Exception as e:
            print(f"CRITICAL: {e}")
        time.sleep(int(args.interval))


if __name__ == "__main__":
    main()