# ==========================================================
# Candidate Replay Runner - Engine Output Sampler
# File: C:\Hybrid_Intelligence_Mentor\candidate_replay_runner_v1_0_7.py
# Version: 1.0.7
# Changelog:
# - v1.0.7:
#   * Use MT5Shim (same as v1.0.6) to run engine in true replay mode
#   * Dump raw engine.generate_signal_package() outputs to .state/engine_pkg_samples.jsonl
#   * Sample K outputs after warmup for analysis
#
# Backtest Evidence:
# - Sampling only (no trading decisions).
# ==========================================================

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import traceback
from typing import Any, Dict, Optional

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


def df_from_mt5_rates(rates) -> "pd.DataFrame":
    if pd is None:
        die("pandas not available.")
    if rates is None or len(rates) == 0:
        return pd.DataFrame()
    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s")
    return df


class MT5Shim:
    def __init__(self, real_mt5, symbol: str, tf_map, state_dir: str):
        self._real = real_mt5
        self._symbol = symbol
        self._tf_map = tf_map
        self.current_time = None
        self._state_dir = state_dir
        ensure_dir(state_dir)

        # ==== ADD THIS BLOCK ====
        # Copy timeframe constants from real mt5
        try:
            self.TIMEFRAME_M1  = real_mt5.TIMEFRAME_M1
            self.TIMEFRAME_M5  = real_mt5.TIMEFRAME_M5
            self.TIMEFRAME_M15 = real_mt5.TIMEFRAME_M15
            self.TIMEFRAME_M30 = real_mt5.TIMEFRAME_M30
            self.TIMEFRAME_H1  = real_mt5.TIMEFRAME_H1
            self.TIMEFRAME_H4  = real_mt5.TIMEFRAME_H4
            self.TIMEFRAME_D1  = real_mt5.TIMEFRAME_D1
        except Exception:
            pass
        # ========================

    def initialize(self) -> bool:
        return True

    def shutdown(self) -> None:
        return None

    def last_error(self):
        return (0, "OK")

    def symbol_select(self, symbol: str, enable: bool = True) -> bool:
        return symbol == self._symbol

    def symbol_info(self, symbol: str):
        return None

    def _clip_to_current(self, df: "pd.DataFrame") -> "pd.DataFrame":
        if self.current_time is None:
            return df
        t = pd.Timestamp(self.current_time)
        return df[df["time"] <= t]

    def copy_rates_range(self, symbol: str, timeframe: int, date_from: dt.datetime, date_to: dt.datetime):
        if symbol != self._symbol:
            return None
        df = self._tf_map.get(timeframe)
        if df is None or len(df) == 0:
            return None

        df2 = self._clip_to_current(df)
        f = pd.Timestamp(date_from)
        t = pd.Timestamp(date_to)
        out = df2[(df2["time"] >= f) & (df2["time"] <= t)]
        return self._df_to_rates(out)

    def copy_rates_from(self, symbol: str, timeframe: int, date_from: dt.datetime, count: int):
        if symbol != self._symbol:
            return None
        df = self._tf_map.get(timeframe)
        if df is None or len(df) == 0:
            return None

        df2 = self._clip_to_current(df)
        f = pd.Timestamp(date_from)
        out = df2[df2["time"] >= f].head(int(count))
        return self._df_to_rates(out)

    def copy_rates_from_pos(self, symbol: str, timeframe: int, start_pos: int, count: int):
        if symbol != self._symbol:
            return None
        df = self._tf_map.get(timeframe)
        if df is None or len(df) == 0:
            return None

        df2 = self._clip_to_current(df)
        start = int(start_pos)
        end = start + int(count)
        out = df2.iloc[start:end]
        return self._df_to_rates(out)

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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--warmup", type=int, default=120)
    ap.add_argument("--sample", type=int, default=20, help="Number of engine outputs to dump after warmup")
    ap.add_argument("--state-dir", default=".state")
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

    # Load historical data once from real MT5
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
    out_path = os.path.join(args.state_dir, "engine_pkg_samples.jsonl")
    err_path = os.path.join(args.state_dir, "engine_eval_errors.log")

    if os.path.exists(out_path):
        os.remove(out_path)
    if os.path.exists(err_path):
        os.remove(err_path)

    # Inject shim into engine module
    try:
        import engine  # type: ignore
        from engine import TradingEngine  # type: ignore
    except Exception as e:
        die(f"Cannot import engine/TradingEngine: {type(e).__name__}: {str(e)}")

    shim = MT5Shim(symbol=symbol, tf_map=tf_map, state_dir=args.state_dir)
    engine.mt5 = shim  # type: ignore

    try:
        eng = TradingEngine(args.config)
    except Exception as e:
        die(f"TradingEngine init failed under shim: {type(e).__name__}: {str(e)}")

    total = len(df_ltf)
    warmup = int(args.warmup)
    if total < warmup + args.sample + 1:
        die(f"Not enough bars for sampling. Have={total}, need>={warmup + args.sample + 1}")

    dumped = 0
    for i in range(warmup, total):
        t_ltf = as_py_datetime(df_ltf.iloc[i]["time"])
        shim.current_time = t_ltf

        try:
            pkg = eng.generate_signal_package()
            if not isinstance(pkg, dict):
                pkg = {"raw": str(pkg)}
            row = {"i": i, "t": iso(t_ltf), "pkg": pkg, "pkg_keys": list(pkg.keys())}
            with open(out_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(to_json_safe(row), ensure_ascii=False) + "\n")
        except Exception as e:
            with open(err_path, "a", encoding="utf-8") as f:
                f.write(f"\n--- i={i} t={iso(t_ltf)} ---\n")
                f.write(f"{type(e).__name__}: {str(e)}\n")
                f.write(traceback.format_exc(limit=10))
            row = {"i": i, "t": iso(t_ltf), "error": f"{type(e).__name__}: {str(e)}"}
            with open(out_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(to_json_safe(row), ensure_ascii=False) + "\n")

        dumped += 1
        if dumped >= int(args.sample):
            break

    print("============================================================")
    print("ENGINE OUTPUT SAMPLER v1.0.7")
    print(f"Symbol: {symbol} | LTF: {ltf} | Range: {args.start} to {args.end}")
    print(f"Warmup: {warmup} | Sample dumped: {dumped}")
    print(f"Output: {out_path}")
    print(f"Errors: {err_path}")
    print("============================================================")


if __name__ == "__main__":
    main()