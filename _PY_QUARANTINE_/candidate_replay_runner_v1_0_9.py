# ==========================================================
# Candidate Replay Runner - Engine Package Dumper + MT5Shim Trace
# File: C:\Hybrid_Intelligence_Mentor\candidate_replay_runner_v1_0_9.py
# Version: 1.0.9
# Changelog:
# - v1.0.9:
#   * Dump engine.generate_signal_package() outputs to .state/engine_pkg_dump.jsonl
#     (dump N samples after warmup, regardless of candidate)
#   * Trace MT5Shim calls and record missing timeframes/data to .state/mt5_shim_trace.log
#   * Keep: replay via MT5Shim (no-lookahead), timeframe constants
#
# Backtest Evidence:
# - Debug/dump only (no performance metrics yet).
# ==========================================================

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import traceback
from typing import Any, Dict, Optional, Set

try:
    import pandas as pd
except Exception:
    pd = None  # type: ignore

try:
    import MetaTrader5 as mt5
except Exception:
    mt5 = None  # type: ignore


def die(msg: str, code: int = 1) -> None:
    print(f"[FATAL] {msg}", file=sys.stderr)
    sys.exit(code)


def ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def parse_yyyy_mm_dd(s: str) -> dt.datetime:
    return dt.datetime.strptime(s, "%Y-%m-%d")


def iso(ts: dt.datetime) -> str:
    return ts.replace(tzinfo=None).isoformat(sep=" ")


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def as_py_datetime(x: Any) -> dt.datetime:
    if isinstance(x, dt.datetime):
        return x
    if pd is not None:
        try:
            if isinstance(x, pd.Timestamp):
                return x.to_pydatetime()
        except Exception:
            pass
    try:
        return dt.datetime.fromisoformat(str(x))
    except Exception:
        return dt.datetime.utcnow()


def to_json_safe(obj: Any) -> Any:
    if pd is not None:
        try:
            if isinstance(obj, pd.Timestamp):
                return obj.to_pydatetime().isoformat(sep=" ")
        except Exception:
            pass

    if isinstance(obj, dt.datetime):
        return obj.isoformat(sep=" ")
    if isinstance(obj, dt.date):
        return obj.isoformat()

    try:
        import numpy as np
        if isinstance(obj, np.datetime64):
            if pd is not None:
                return pd.Timestamp(obj).to_pydatetime().isoformat(sep=" ")
            return str(obj)
    except Exception:
        pass

    if isinstance(obj, dict):
        return {str(k): to_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return [to_json_safe(v) for v in obj]

    try:
        import numpy as np
        if isinstance(obj, (np.integer, np.floating, np.bool_)):
            return obj.item()
    except Exception:
        pass

    return obj


def df_from_mt5_rates(rates) -> "pd.DataFrame":
    if pd is None:
        die("pandas not available.")
    if rates is None or len(rates) == 0:
        return pd.DataFrame()
    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s")
    return df


def to_mt5_tf(tf_str: str) -> int:
    if mt5 is None:
        die("MetaTrader5 module not available.")
    m = {
        "M1": mt5.TIMEFRAME_M1,
        "M5": mt5.TIMEFRAME_M5,
        "M15": mt5.TIMEFRAME_M15,
        "M30": mt5.TIMEFRAME_M30,
        "H1": mt5.TIMEFRAME_H1,
        "H4": mt5.TIMEFRAME_H4,
        "D1": mt5.TIMEFRAME_D1,
    }
    key = tf_str.strip().upper()
    if key not in m:
        die(f"Unsupported timeframe '{tf_str}'. Supported: {', '.join(sorted(m.keys()))}")
    return m[key]


class MT5Shim:
    """
    Replay MT5 shim:
    - Provides TIMEFRAME_* constants (engine expects them)
    - copy_rates_* returns historical bars up to current_time (no-lookahead)
    - Optional tracing: logs calls and missing TF/data
    """

    def __init__(self, real_mt5, symbol: str, tf_map: Dict[int, "pd.DataFrame"], state_dir: str, trace: bool):
        if pd is None:
            die("pandas not available.")

        self._real = real_mt5
        self._symbol = symbol
        self._tf_map = tf_map
        self.current_time: Optional[dt.datetime] = None
        self._state_dir = state_dir
        self._trace = trace
        self.missing_timeframes: Set[int] = set()
        ensure_dir(state_dir)

        # TIMEFRAME constants (critical)
        self.TIMEFRAME_M1 = real_mt5.TIMEFRAME_M1
        self.TIMEFRAME_M5 = real_mt5.TIMEFRAME_M5
        self.TIMEFRAME_M15 = real_mt5.TIMEFRAME_M15
        self.TIMEFRAME_M30 = real_mt5.TIMEFRAME_M30
        self.TIMEFRAME_H1 = real_mt5.TIMEFRAME_H1
        self.TIMEFRAME_H4 = real_mt5.TIMEFRAME_H4
        self.TIMEFRAME_D1 = real_mt5.TIMEFRAME_D1

    def _log(self, msg: str) -> None:
        if not self._trace:
            return
        path = os.path.join(self._state_dir, "mt5_shim_trace.log")
        with open(path, "a", encoding="utf-8") as f:
            f.write(msg + "\n")

    def initialize(self) -> bool:
        return True

    def shutdown(self) -> None:
        return None

    def last_error(self):
        return (0, "OK")

    def symbol_select(self, symbol: str, enable: bool = True) -> bool:
        return symbol == self._symbol

    def symbol_info(self, symbol: str):
        try:
            return self._real.symbol_info(symbol)
        except Exception:
            return None

    def symbol_info_tick(self, symbol: str):
        try:
            return self._real.symbol_info_tick(symbol)
        except Exception:
            return None

    def _clip_to_current(self, df: "pd.DataFrame") -> "pd.DataFrame":
        if self.current_time is None:
            return df
        t = pd.Timestamp(self.current_time)
        return df[df["time"] <= t]

    def _df_to_rates(self, df: "pd.DataFrame"):
        if df is None or len(df) == 0:
            return []
        if "tick_volume" not in df.columns:
            df = df.assign(tick_volume=0)

        times = (df["time"].astype("int64") // 10**9).astype(int)
        out = []
        for idx, row in df.iterrows():
            out.append(
                {
                    "time": int(times.loc[idx]),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "tick_volume": int(row.get("tick_volume", 0)),
                    "spread": 0,
                    "real_volume": 0,
                }
            )
        return out

    def _get_df(self, timeframe: int) -> Optional["pd.DataFrame"]:
        df = self._tf_map.get(timeframe)
        if df is None:
            self.missing_timeframes.add(timeframe)
        return df

    def copy_rates_range(self, symbol: str, timeframe: int, date_from: dt.datetime, date_to: dt.datetime):
        if symbol != self._symbol:
            self._log(f"[copy_rates_range] symbol_mismatch symbol={symbol}")
            return None

        df = self._get_df(timeframe)
        if df is None or len(df) == 0:
            self._log(f"[copy_rates_range] MISSING_TF tf={timeframe} t={self.current_time}")
            return None

        df2 = self._clip_to_current(df)
        f = pd.Timestamp(date_from)
        t = pd.Timestamp(date_to)
        out = df2[(df2["time"] >= f) & (df2["time"] <= t)]
        self._log(f"[copy_rates_range] tf={timeframe} rows={len(out)} t={self.current_time}")
        return self._df_to_rates(out)

    def copy_rates_from(self, symbol: str, timeframe: int, date_from: dt.datetime, count: int):
        if symbol != self._symbol:
            self._log(f"[copy_rates_from] symbol_mismatch symbol={symbol}")
            return None

        df = self._get_df(timeframe)
        if df is None or len(df) == 0:
            self._log(f"[copy_rates_from] MISSING_TF tf={timeframe} t={self.current_time}")
            return None

        df2 = self._clip_to_current(df)
        f = pd.Timestamp(date_from)
        out = df2[df2["time"] >= f].head(int(count))
        self._log(f"[copy_rates_from] tf={timeframe} count={count} rows={len(out)} t={self.current_time}")
        return self._df_to_rates(out)

    def copy_rates_from_pos(self, symbol: str, timeframe: int, start_pos: int, count: int):
        if symbol != self._symbol:
            self._log(f"[copy_rates_from_pos] symbol_mismatch symbol={symbol}")
            return None

        df = self._get_df(timeframe)
        if df is None or len(df) == 0:
            self._log(f"[copy_rates_from_pos] MISSING_TF tf={timeframe} t={self.current_time}")
            return None

        df2 = self._clip_to_current(df)
        start = int(start_pos)
        end = start + int(count)
        out = df2.iloc[start:end]
        self._log(f"[copy_rates_from_pos] tf={timeframe} start={start} count={count} rows={len(out)} t={self.current_time}")
        return self._df_to_rates(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--warmup", type=int, default=200)
    ap.add_argument("--dump-pkg", type=int, default=15, help="Dump N packages after warmup")
    ap.add_argument("--state-dir", default=".state")
    ap.add_argument("--trace-mt5", action="store_true")
    args = ap.parse_args()

    if pd is None:
        die("pandas not available.")
    if mt5 is None:
        die("MetaTrader5 module not available.")

    cfg_obj = load_json(args.config)

    symbol = str(cfg_obj.get("symbol", cfg_obj.get("Symbol", "GOLD")))
    tfs = cfg_obj.get("timeframes", {}) if isinstance(cfg_obj.get("timeframes", {}), dict) else {}
    ltf = str(tfs.get("LTF", cfg_obj.get("ltf", "M5"))).upper()
    mtf_s = tfs.get("MTF", cfg_obj.get("mtf", "M15"))
    htf_s = tfs.get("HTF", cfg_obj.get("htf", "H1"))
    mtf_str = str(mtf_s).upper() if mtf_s else None
    htf_str = str(htf_s).upper() if htf_s else None

    start_dt = parse_yyyy_mm_dd(args.start)
    end_dt = parse_yyyy_mm_dd(args.end) + dt.timedelta(days=1)

    if not mt5.initialize():
        die(f"MT5 initialize failed: {mt5.last_error()}")
    if not mt5.symbol_select(symbol, True):
        die(f"MT5 cannot select symbol '{symbol}'")

    tf_ltf = to_mt5_tf(ltf)
    tf_mtf = to_mt5_tf(mtf_str) if mtf_str else None
    tf_htf = to_mt5_tf(htf_str) if htf_str else None

    df_ltf = df_from_mt5_rates(mt5.copy_rates_range(symbol, tf_ltf, start_dt, end_dt))
    if df_ltf is None or len(df_ltf) == 0:
        die("No LTF rates returned.")

    tf_map: Dict[int, "pd.DataFrame"] = {tf_ltf: df_ltf}

    if tf_mtf is not None:
        df_mtf = df_from_mt5_rates(mt5.copy_rates_range(symbol, tf_mtf, start_dt, end_dt))
        if df_mtf is not None and len(df_mtf) > 0:
            tf_map[tf_mtf] = df_mtf

    if tf_htf is not None:
        df_htf = df_from_mt5_rates(mt5.copy_rates_range(symbol, tf_htf, start_dt, end_dt))
        if df_htf is not None and len(df_htf) > 0:
            tf_map[tf_htf] = df_htf

    mt5.shutdown()

    ensure_dir(args.state_dir)
    dump_path = os.path.join(args.state_dir, "engine_pkg_dump.jsonl")
    err_path = os.path.join(args.state_dir, "engine_eval_errors.log")
    trace_path = os.path.join(args.state_dir, "mt5_shim_trace.log")

    # fresh
    for p in (dump_path, err_path, trace_path):
        if os.path.exists(p):
            os.remove(p)

    # inject shim
    try:
        import engine  # type: ignore
        from engine import TradingEngine  # type: ignore
    except Exception as e:
        die(f"Cannot import engine/TradingEngine: {type(e).__name__}: {str(e)}")

    shim = MT5Shim(real_mt5=mt5, symbol=symbol, tf_map=tf_map, state_dir=args.state_dir, trace=args.trace_mt5)
    engine.mt5 = shim  # type: ignore

    try:
        eng = TradingEngine(args.config)
    except Exception as e:
        die(f"TradingEngine init failed under shim: {type(e).__name__}: {str(e)}")

    total = len(df_ltf)
    warmup = int(args.warmup)
    dump_n = int(args.dump_pkg)

    if total <= warmup:
        die(f"Not enough bars. Have={total}, warmup={warmup}")

    dumped = 0
    errors = 0

    for i in range(warmup, total):
        t_ltf = as_py_datetime(df_ltf.iloc[i]["time"])
        shim.current_time = t_ltf

        try:
            pkg = eng.generate_signal_package()
            if not isinstance(pkg, dict):
                pkg = {"raw": str(pkg)}

            row = {
                "i": i,
                "t": iso(t_ltf),
                "pkg_keys": list(pkg.keys()),
                "pkg": pkg,
            }
            with open(dump_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(to_json_safe(row), ensure_ascii=False) + "\n")

        except Exception as e:
            errors += 1
            with open(err_path, "a", encoding="utf-8") as f:
                f.write(f"\n--- i={i} t={iso(t_ltf)} ---\n")
                f.write(f"{type(e).__name__}: {str(e)}\n")
                f.write(traceback.format_exc(limit=10))

        dumped += 1
        if dumped >= dump_n:
            break

    print("============================================================")
    print("ENGINE PKG DUMPER v1.0.9")
    print(f"Symbol: {symbol} | LTF: {ltf} | MTF: {mtf_str} | HTF: {htf_str}")
    print(f"Range: {args.start} to {args.end}")
    print(f"Warmup: {warmup} | Dumped: {dumped} | Errors: {errors}")
    print(f"Dump: {dump_path}")
    print(f"Err:  {err_path}")
    if args.trace_mt5:
        print(f"Trace: {trace_path}")
        if len(shim.missing_timeframes) > 0:
            print(f"Missing TF ids: {sorted(list(shim.missing_timeframes))}")
    print("============================================================")


if __name__ == "__main__":
    main()