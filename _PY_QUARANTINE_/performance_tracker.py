"""
HIM Performance Tracker
Version: 1.0.0

Purpose:
- Statistical validation for HIM sideway_scalp signals (M5 execution).
- Collect signals -> resolve outcomes (TP/SL hit-first) -> compute winrate/avgR/drawdown.

Data contract:
- signals saved to logs/signals.csv
- resolved trades saved to logs/trades.csv
- summary saved to logs/performance_summary.json

Important:
- This module evaluates signal outcomes using MT5 price history after the signal timestamp.
- It does NOT place trades. It is an evaluator/logger for statistical validation.

Backtest evidence (placeholder):
- Not available yet (requires 30–50 signals collection and resolution).

Parameter rationale (practical):
- max_horizon_minutes default 360: limit evaluation window to avoid "never resolved" signals.
- timeframe default M5: matches execution TF requirement.

"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import MetaTrader5 as mt5


LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
SIGNALS_CSV = os.path.join(LOG_DIR, "signals.csv")
TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
SUMMARY_JSON = os.path.join(LOG_DIR, "performance_summary.json")


def _ensure_dirs() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)


def _utc_now_ts() -> int:
    return int(time.time())


def _dt_utc_from_ts(ts: int) -> datetime:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return int(default)
        return int(v)
    except Exception:
        return int(default)


def _tf_from_str(tf: str) -> int:
    t = (tf or "").strip().upper()
    mapping = {
        "M1": mt5.TIMEFRAME_M1,
        "M2": mt5.TIMEFRAME_M2,
        "M3": mt5.TIMEFRAME_M3,
        "M4": mt5.TIMEFRAME_M4,
        "M5": mt5.TIMEFRAME_M5,
        "M6": mt5.TIMEFRAME_M6,
        "M10": mt5.TIMEFRAME_M10,
        "M12": mt5.TIMEFRAME_M12,
        "M15": mt5.TIMEFRAME_M15,
        "M20": mt5.TIMEFRAME_M20,
        "M30": mt5.TIMEFRAME_M30,
        "H1": mt5.TIMEFRAME_H1,
        "H2": mt5.TIMEFRAME_H2,
        "H3": mt5.TIMEFRAME_H3,
        "H4": mt5.TIMEFRAME_H4,
        "H6": mt5.TIMEFRAME_H6,
        "H8": mt5.TIMEFRAME_H8,
        "H12": mt5.TIMEFRAME_H12,
        "D1": mt5.TIMEFRAME_D1,
        "W1": mt5.TIMEFRAME_W1,
        "MN1": mt5.TIMEFRAME_MN1,
    }
    if t not in mapping:
        raise ValueError(f"unknown timeframe: {tf}")
    return mapping[t]


def _mt5_init_or_raise() -> None:
    if mt5.initialize():
        return
    time.sleep(0.2)
    if not mt5.initialize():
        raise RuntimeError("MT5 initialize failed")


@dataclass
class SignalRow:
    signal_id: str
    ts: int
    symbol: str
    mode: str
    tf_exec: str
    direction: str
    entry: float
    sl: float
    tp: float
    rr: float
    proximity_score: float
    blocked_by: str

    # context metrics (optional but recommended)
    atr: float
    adx: float
    bb_width_atr: float
    rsi: float
    watch_state: str
    engine_version: str


class PerformanceTracker:
    def __init__(self) -> None:
        _ensure_dirs()
        self._init_signals_csv()
        self._init_trades_csv()

    def _init_signals_csv(self) -> None:
        if os.path.exists(SIGNALS_CSV):
            return
        with open(SIGNALS_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "signal_id",
                    "ts",
                    "symbol",
                    "mode",
                    "tf_exec",
                    "direction",
                    "entry",
                    "sl",
                    "tp",
                    "rr",
                    "proximity_score",
                    "blocked_by",
                    "atr",
                    "adx",
                    "bb_width_atr",
                    "rsi",
                    "watch_state",
                    "engine_version",
                ]
            )

    def _init_trades_csv(self) -> None:
        if os.path.exists(TRADES_CSV):
            return
        with open(TRADES_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "signal_id",
                    "ts_open",
                    "ts_close",
                    "symbol",
                    "tf_exec",
                    "direction",
                    "entry",
                    "sl",
                    "tp",
                    "rr_plan",
                    "outcome",
                    "r_result",
                    "exit_price",
                    "bars_to_close",
                    "max_adverse",
                    "max_favorable",
                ]
            )

    def _read_csv_rows(self, path: str) -> List[Dict[str, str]]:
        if not os.path.exists(path):
            return []
        with open(path, "r", newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            return [dict(row) for row in r]

    def _append_signal_row(self, row: SignalRow) -> None:
        with open(SIGNALS_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    row.signal_id,
                    row.ts,
                    row.symbol,
                    row.mode,
                    row.tf_exec,
                    row.direction,
                    row.entry,
                    row.sl,
                    row.tp,
                    row.rr,
                    row.proximity_score,
                    row.blocked_by,
                    row.atr,
                    row.adx,
                    row.bb_width_atr,
                    row.rsi,
                    row.watch_state,
                    row.engine_version,
                ]
            )

    def append_signal_from_engine_package(self, pkg: Dict[str, Any], tf_exec: str = "M5") -> Dict[str, Any]:
        """
        Accepts the payload from engine.generate_signal_package() (the "signal" object in /api/signal_preview).
        Logs BOTH passed signals and blocked attempts (important for filter tuning).
        """
        ctx = (pkg.get("context") or {}) if isinstance(pkg, dict) else {}
        symbol = str(pkg.get("symbol", "GOLD"))
        mode = str(ctx.get("mode", "unknown"))
        engine_version = str(ctx.get("engine_version", "unknown"))
        direction = str(pkg.get("direction", "NONE"))
        blocked_by = str(ctx.get("blocked_by") or "")

        # create deterministic-ish id
        ts = _utc_now_ts()
        signal_id = f"{symbol}-{ts}-{direction}"

        entry = _safe_float(pkg.get("entry_candidate"), 0.0)
        sl = _safe_float(pkg.get("stop_candidate"), 0.0)
        tp = _safe_float(pkg.get("tp_candidate"), 0.0)
        rr = _safe_float(pkg.get("rr"), 0.0)
        proximity_score = _safe_float(pkg.get("score"), 0.0)

        atr = _safe_float(ctx.get("atr"), 0.0)
        adx = _safe_float(ctx.get("adx"), float("nan"))
        bb_width_atr = _safe_float(ctx.get("bb_width_atr"), float("nan"))
        rsi = _safe_float(ctx.get("rsi"), float("nan"))
        watch_state = str(ctx.get("watch_state", ""))

        row = SignalRow(
            signal_id=signal_id,
            ts=ts,
            symbol=symbol,
            mode=mode,
            tf_exec=str(tf_exec),
            direction=direction,
            entry=entry,
            sl=sl,
            tp=tp,
            rr=rr,
            proximity_score=proximity_score,
            blocked_by=blocked_by,
            atr=atr,
            adx=adx,
            bb_width_atr=bb_width_atr,
            rsi=rsi,
            watch_state=watch_state,
            engine_version=engine_version,
        )
        self._append_signal_row(row)
        return {"ok": True, "signal_id": signal_id}

    def _resolved_signal_ids(self) -> set:
        rows = self._read_csv_rows(TRADES_CSV)
        return {r.get("signal_id", "") for r in rows if r.get("signal_id")}

    def _load_unresolved_trade_candidates(self) -> List[Dict[str, str]]:
        signals = self._read_csv_rows(SIGNALS_CSV)
        resolved = self._resolved_signal_ids()

        # resolve only actionable signals
        out = []
        for s in signals:
            sid = s.get("signal_id", "")
            if not sid or sid in resolved:
                continue
            if (s.get("direction") or "").upper() not in ("BUY", "SELL"):
                continue
            if (s.get("blocked_by") or ""):
                # blocked attempts are logged for tuning, but not evaluated as trades
                continue
            out.append(s)
        return out

    def _fetch_rates_range(self, symbol: str, tf_exec: str, ts_from: int, ts_to: int) -> Optional[List[Dict[str, Any]]]:
        _mt5_init_or_raise()
        timeframe = _tf_from_str(tf_exec)

        dt_from = _dt_utc_from_ts(ts_from)
        dt_to = _dt_utc_from_ts(ts_to)

        rates = mt5.copy_rates_range(symbol, timeframe, dt_from, dt_to)
        if rates is None or len(rates) == 0:
            return None

        out: List[Dict[str, Any]] = []
        for r in rates:
            out.append(
                {
                    "time": int(r["time"]),
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                }
            )
        return out

    def _evaluate_hit_first(
        self,
        direction: str,
        entry: float,
        sl: float,
        tp: float,
        bars: List[Dict[str, Any]],
    ) -> Tuple[str, float, float, int, float, float]:
        """
        Determine which is hit first: TP or SL.
        Conservative rule for ambiguous same-bar hits:
        - BUY: if low <= SL and high >= TP on same bar => assume SL first (pessimistic)
        - SELL: if high >= SL and low <= TP on same bar => assume SL first
        Returns: outcome, r_result, exit_price, bars_to_close, max_adverse, max_favorable
        """
        d = direction.upper()
        risk = abs(entry - sl)
        if risk <= 1e-9:
            return "invalid_risk", 0.0, entry, 0, 0.0, 0.0

        max_fav = 0.0
        max_adv = 0.0

        for idx, b in enumerate(bars):
            hi = float(b["high"])
            lo = float(b["low"])

            if d == "BUY":
                # track MAE/MFE
                fav = hi - entry
                adv = entry - lo
                max_fav = max(max_fav, fav)
                max_adv = max(max_adv, adv)

                sl_hit = lo <= sl
                tp_hit = hi >= tp

                if sl_hit and tp_hit:
                    # pessimistic
                    return "SL", -1.0, sl, idx + 1, max_adv, max_fav
                if sl_hit:
                    return "SL", -1.0, sl, idx + 1, max_adv, max_fav
                if tp_hit:
                    rr = (tp - entry) / risk
                    return "TP", float(rr), tp, idx + 1, max_adv, max_fav

            else:  # SELL
                fav = entry - lo
                adv = hi - entry
                max_fav = max(max_fav, fav)
                max_adv = max(max_adv, adv)

                sl_hit = hi >= sl
                tp_hit = lo <= tp

                if sl_hit and tp_hit:
                    return "SL", -1.0, sl, idx + 1, max_adv, max_fav
                if sl_hit:
                    return "SL", -1.0, sl, idx + 1, max_adv, max_fav
                if tp_hit:
                    rr = (entry - tp) / risk
                    return "TP", float(rr), tp, idx + 1, max_adv, max_fav

        return "OPEN", 0.0, entry, len(bars), max_adv, max_fav

    def resolve_signals_to_trades(self, max_horizon_minutes: int = 360) -> Dict[str, Any]:
        """
        Convert unresolved actionable signals to resolved trades (if TP/SL hit).
        Writes resolved trades to logs/trades.csv.
        """
        _ensure_dirs()
        candidates = self._load_unresolved_trade_candidates()
        if not candidates:
            return {"ok": True, "resolved": 0, "note": "no_unresolved_candidates"}

        now_ts = _utc_now_ts()
        resolved_n = 0
        with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)

            for s in candidates:
                sid = s["signal_id"]
                symbol = s["symbol"]
                tf_exec = s.get("tf_exec", "M5")
                direction = (s.get("direction") or "").upper()

                ts_open = _safe_int(s.get("ts"), 0)
                if ts_open <= 0:
                    continue

                entry = _safe_float(s.get("entry"), 0.0)
                sl = _safe_float(s.get("sl"), 0.0)
                tp = _safe_float(s.get("tp"), 0.0)
                rr_plan = _safe_float(s.get("rr"), 0.0)

                # time window: from signal open to min(now, open + horizon)
                ts_to = min(now_ts, ts_open + int(max_horizon_minutes) * 60)
                if ts_to <= ts_open:
                    continue

                try:
                    bars = self._fetch_rates_range(symbol, tf_exec, ts_open, ts_to)
                except Exception:
                    bars = None

                if not bars or len(bars) < 2:
                    continue

                outcome, r_result, exit_price, bars_to_close, max_adv, max_fav = self._evaluate_hit_first(
                    direction=direction,
                    entry=entry,
                    sl=sl,
                    tp=tp,
                    bars=bars,
                )

                if outcome == "OPEN":
                    continue  # not resolved yet within horizon

                ts_close = int(bars[min(bars_to_close - 1, len(bars) - 1)]["time"])
                w.writerow(
                    [
                        sid,
                        ts_open,
                        ts_close,
                        symbol,
                        tf_exec,
                        direction,
                        entry,
                        sl,
                        tp,
                        rr_plan,
                        outcome,
                        float(r_result),
                        float(exit_price),
                        int(bars_to_close),
                        float(max_adv),
                        float(max_fav),
                    ]
                )
                resolved_n += 1

        return {"ok": True, "resolved": resolved_n}

    @staticmethod
    def _equity_and_drawdown(r_list: List[float]) -> Tuple[List[float], float]:
        eq = []
        peak = 0.0
        dd_max = 0.0
        cur = 0.0
        for r in r_list:
            cur += float(r)
            eq.append(cur)
            peak = max(peak, cur)
            dd = peak - cur
            dd_max = max(dd_max, dd)
        return eq, float(dd_max)

    def compute_summary(self) -> Dict[str, Any]:
        trades = self._read_csv_rows(TRADES_CSV)
        if not trades:
            summary = {
                "ok": True,
                "ready": False,
                "reason": "no_trades_resolved_yet",
                "trades": 0,
                "winrate": None,
                "avg_r": None,
                "max_drawdown_r": None,
                "ts": _utc_now_ts(),
            }
            with open(SUMMARY_JSON, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2)
            return summary

        r_results: List[float] = []
        wins = 0
        for t in trades:
            outcome = (t.get("outcome") or "").upper()
            r = _safe_float(t.get("r_result"), 0.0)
            r_results.append(r)
            if outcome == "TP" and r > 0:
                wins += 1

        n = len(r_results)
        winrate = float(wins / n) if n > 0 else 0.0
        avg_r = float(sum(r_results) / n) if n > 0 else 0.0
        _, max_dd = self._equity_and_drawdown(r_results)

        summary = {
            "ok": True,
            "ready": True,
            "trades": n,
            "wins": wins,
            "winrate": winrate,
            "avg_r": avg_r,
            "max_drawdown_r": max_dd,
            "ts": _utc_now_ts(),
        }
        with open(SUMMARY_JSON, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        return summary

    def update_and_summarize(self, max_horizon_minutes: int = 360) -> Dict[str, Any]:
        r1 = self.resolve_signals_to_trades(max_horizon_minutes=max_horizon_minutes)
        s = self.compute_summary()
        return {"ok": True, "resolved": r1.get("resolved", 0), "summary": s}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--update", action="store_true", help="resolve signals to trades (TP/SL hit-first)")
    p.add_argument("--summary", action="store_true", help="print summary")
    p.add_argument("--horizon", type=int, default=360, help="max horizon minutes for resolution window")
    args = p.parse_args()

    tracker = PerformanceTracker()

    if args.update:
        res = tracker.update_and_summarize(max_horizon_minutes=int(args.horizon))
        if args.summary:
            print(json.dumps(res, indent=2))
        else:
            print(json.dumps(res, indent=2))
        return

    if args.summary:
        s = tracker.compute_summary()
        print(json.dumps(s, indent=2))
        return

    # default behavior
    s = tracker.compute_summary()
    print(json.dumps(s, indent=2))


if __name__ == "__main__":
    main()