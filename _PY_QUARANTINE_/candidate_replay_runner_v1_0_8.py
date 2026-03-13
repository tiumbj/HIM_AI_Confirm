# ==========================================================
# Candidate Replay Runner - Engine Replay Binding (Runner Full)
# File: C:\Hybrid_Intelligence_Mentor\candidate_replay_runner_v1_0_8.py
# Version: 1.0.8
# Changelog:
# - v1.0.8:
#   * Runner full (not sampler)
#   * Fix: MT5Shim now includes TIMEFRAME_* constants (engine expects mt5.TIMEFRAME_M1, etc.)
#   * Keep: replay by MT5 shim (no-lookahead), JSON-safe export
#   * Ensure: output directory is created, and empty dataset file exists even if exported=0
#
# Backtest Evidence:
# - Dataset builder only (no performance metrics yet).
# ==========================================================

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
import sys
import time
import traceback
from typing import Any, Dict, Optional, Tuple

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


def save_json(path: str, obj: Dict[str, Any]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


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


def jsonl_append(path: str, row: Dict[str, Any]) -> None:
    ensure_dir(os.path.dirname(path))
    safe = to_json_safe(row)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(safe, ensure_ascii=False) + "\n")


def touch_file(path: str) -> None:
    ensure_dir(os.path.dirname(path))
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write("")


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


def slice_window(df, end_idx: int, window: int) -> "pd.DataFrame":
    start = max(0, end_idx - window + 1)
    return df.iloc[start : end_idx + 1].copy()


@dataclasses.dataclass
class EffectiveConfig:
    symbol: str
    mode: str
    ltf: str
    mtf: Optional[str]
    htf: Optional[str]
    warmup_bars: int
    ltf_window: int
    mtf_window: int
    htf_window: int


def resolve_effective_config(config_path: str) -> EffectiveConfig:
    raw = load_json(config_path)
    symbol = str(raw.get("symbol", raw.get("Symbol", "GOLD")))
    mode = str(raw.get("mode", raw.get("Mode", "sideway_scalp")))

    tfs = raw.get("timeframes", {}) if isinstance(raw.get("timeframes", {}), dict) else {}
    ltf = str(tfs.get("LTF", raw.get("ltf", "M5"))).upper()
    mtf_val = tfs.get("MTF", raw.get("mtf", "M15"))
    htf_val = tfs.get("HTF", raw.get("htf", "H1"))
    mtf_ = str(mtf_val).upper() if mtf_val else None
    htf_ = str(htf_val).upper() if htf_val else None

    replay = raw.get("replay", {}) if isinstance(raw.get("replay", {}), dict) else {}
    warmup_bars = int(replay.get("warmup_bars", 500))
    ltf_window = int(replay.get("ltf_window", 200))
    mtf_window = int(replay.get("mtf_window", 120))
    htf_window = int(replay.get("htf_window", 80))

    return EffectiveConfig(
        symbol=symbol,
        mode=mode,
        ltf=ltf,
        mtf=mtf_,
        htf=htf_,
        warmup_bars=warmup_bars,
        ltf_window=ltf_window,
        mtf_window=mtf_window,
        htf_window=htf_window,
    )


class MT5Shim:
    """
    Emulates minimal MetaTrader5 API used by engine.py
    Returns bars from cached DataFrames up to current_time (no-lookahead).
    IMPORTANT: Must provide TIMEFRAME_* constants, engine expects mt5.TIMEFRAME_M1 etc.
    """

    def __init__(self, real_mt5, symbol: str, tf_map: Dict[int, "pd.DataFrame"], state_dir: str):
        self._real = real_mt5
        self._symbol = symbol
        self._tf_map = tf_map
        self.current_time: Optional[dt.datetime] = None
        self._state_dir = state_dir
        ensure_dir(state_dir)

        # ===== FIX (v1.0.8): provide TIMEFRAME constants =====
        try:
            self.TIMEFRAME_M1 = real_mt5.TIMEFRAME_M1
            self.TIMEFRAME_M5 = real_mt5.TIMEFRAME_M5
            self.TIMEFRAME_M15 = real_mt5.TIMEFRAME_M15
            self.TIMEFRAME_M30 = real_mt5.TIMEFRAME_M30
            self.TIMEFRAME_H1 = real_mt5.TIMEFRAME_H1
            self.TIMEFRAME_H4 = real_mt5.TIMEFRAME_H4
            self.TIMEFRAME_D1 = real_mt5.TIMEFRAME_D1
        except Exception:
            # If real_mt5 isn't available for constants, keep attributes absent (engine may fail)
            pass
        # =====================================================

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

    def copy_rates_range(self, symbol: str, timeframe: int, date_from: dt.datetime, date_to: dt.datetime):
        if pd is None:
            return None
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
        if pd is None:
            return None
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
        if pd is None:
            return None
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


class EngineReplayAdapter:
    def __init__(self, engine_obj, state_dir: str):
        self.engine_obj = engine_obj
        self.state_dir = state_dir
        ensure_dir(state_dir)

    def _log_error(self, tag: str, e: Exception) -> None:
        path = os.path.join(self.state_dir, "engine_eval_errors.log")
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"\n--- {tag} ---\n")
            f.write(f"{type(e).__name__}: {str(e)}\n")
            f.write(traceback.format_exc(limit=10))

    def evaluate(self) -> Tuple[bool, Dict[str, Any], bool]:
        """
        Returns (is_candidate, pkg, has_error)
        Candidate rule (temporary): detect direction/signal keys in pkg.
        """
        try:
            pkg = self.engine_obj.generate_signal_package()
            if not isinstance(pkg, dict):
                pkg = {"raw": str(pkg)}
        except Exception as e:
            self._log_error("ENGINE_CALL", e)
            return False, {"error": f"{type(e).__name__}: {str(e)}"}, True

        def as_dir(v: Any) -> Optional[str]:
            if v is None:
                return None
            if isinstance(v, str):
                s = v.strip().lower()
                if s in ("buy", "sell", "long", "short"):
                    return s
            try:
                f = float(v)
                if f > 0:
                    return "buy"
                if f < 0:
                    return "sell"
            except Exception:
                return None
            return None

        # direct keys
        for k in ("Signal", "signal", "direction", "dir"):
            if k in pkg:
                d = as_dir(pkg.get(k))
                if d is not None:
                    return True, pkg, False

        # nested containers
        for container_key in ("dry_run_order", "order", "result", "payload", "signal", "context"):
            sub = pkg.get(container_key)
            if isinstance(sub, dict):
                for k in ("Signal", "signal", "direction", "dir"):
                    if k in sub:
                        d = as_dir(sub.get(k))
                        if d is not None:
                            return True, pkg, False

        # if engine provides explicit "direction" at top-level but as "NONE"/etc, it's not candidate
        return False, pkg, False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--max-events", type=int, default=5000)
    ap.add_argument("--outdir", default="backtests")
    ap.add_argument("--state-dir", default=".state")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--sleep-ms", type=int, default=0)
    ap.add_argument("--progress-every", type=int, default=200)
    ap.add_argument("--warmup", type=int, default=None)
    args = ap.parse_args()

    if pd is None:
        die("pandas not available.")
    if mt5 is None:
        die("MetaTrader5 module not available.")

    cfg = resolve_effective_config(args.config)

    # Load historical data once from real MT5
    if not mt5.initialize():
        die(f"MT5 initialize failed: {mt5.last_error()}")
    if not mt5.symbol_select(cfg.symbol, True):
        die(f"MT5 cannot select symbol '{cfg.symbol}'")

    tf_ltf = to_mt5_tf(cfg.ltf)
    tf_mtf = to_mt5_tf(cfg.mtf) if cfg.mtf else None
    tf_htf = to_mt5_tf(cfg.htf) if cfg.htf else None

    start_dt = parse_yyyy_mm_dd(args.start)
    end_dt = parse_yyyy_mm_dd(args.end) + dt.timedelta(days=1)

    df_ltf = df_from_mt5_rates(mt5.copy_rates_range(cfg.symbol, tf_ltf, start_dt, end_dt))
    if df_ltf is None or len(df_ltf) == 0:
        die("No LTF rates returned.")

    df_mtf = None
    df_htf = None
    if tf_mtf is not None:
        df_mtf = df_from_mt5_rates(mt5.copy_rates_range(cfg.symbol, tf_mtf, start_dt, end_dt))
    if tf_htf is not None:
        df_htf = df_from_mt5_rates(mt5.copy_rates_range(cfg.symbol, tf_htf, start_dt, end_dt))

    # Shutdown real mt5 after data load (prevent engine reading live)
    mt5.shutdown()

    ensure_dir(args.outdir)
    ensure_dir(args.state_dir)

    dataset_path = os.path.join(args.outdir, f"candidates_{cfg.symbol}_{cfg.ltf}_{args.start}_to_{args.end}.jsonl")
    resume_path = os.path.join(args.state_dir, f"replay_resume_{cfg.symbol}_{cfg.ltf}.json")
    errlog_path = os.path.join(args.state_dir, "engine_eval_errors.log")

    # Fresh run always clearer (ignore resume for now unless you need it later)
    if os.path.exists(dataset_path):
        os.remove(dataset_path)
    if os.path.exists(errlog_path):
        os.remove(errlog_path)

    # Ensure file exists even when exported=0 (so Get-Content won't fail)
    touch_file(dataset_path)

    warmup = int(args.warmup) if args.warmup is not None else max(cfg.warmup_bars, cfg.ltf_window)
    total = len(df_ltf)
    need = warmup + 5
    if total < need:
        die(f"Not enough bars. Have={total}, need>={need}. Reduce --warmup or expand date range.")

    # Import engine and inject shim
    try:
        import engine  # type: ignore
        from engine import TradingEngine  # type: ignore
    except Exception as e:
        die(f"Cannot import engine/TradingEngine: {type(e).__name__}: {str(e)}")

    tf_map: Dict[int, "pd.DataFrame"] = {tf_ltf: df_ltf}
    if tf_mtf is not None and df_mtf is not None and len(df_mtf) > 0:
        tf_map[tf_mtf] = df_mtf
    if tf_htf is not None and df_htf is not None and len(df_htf) > 0:
        tf_map[tf_htf] = df_htf

    shim = MT5Shim(real_mt5=mt5, symbol=cfg.symbol, tf_map=tf_map, state_dir=args.state_dir)

    # Swap engine.mt5 to shim
    try:
        engine.mt5 = shim  # type: ignore
    except Exception:
        pass

    # Create engine instance under shim
    try:
        eng = TradingEngine(args.config)
    except Exception as e:
        die(f"TradingEngine init failed under shim: {type(e).__name__}: {str(e)}")

    adapter = EngineReplayAdapter(engine_obj=eng, state_dir=args.state_dir)

    print("============================================================")
    print("CANDIDATE REPLAY RUNNER v1.0.8")
    print(f"Symbol: {cfg.symbol} | Mode: {cfg.mode}")
    print(f"LTF: {cfg.ltf} (bars={len(df_ltf)}) | MTF: {cfg.mtf} | HTF: {cfg.htf}")
    print(f"Range: {args.start} to {args.end}")
    print(f"Warmup bars: {warmup}")
    print(f"Output: {dataset_path}")
    print("============================================================")

    import numpy as np

    def end_index_by_time(df, t_in: Any) -> int:
        t_py = as_py_datetime(t_in)
        t64 = np.datetime64(t_py)
        ts = df["time"].values
        pos = int(np.searchsorted(ts, t64, side="right") - 1)
        return max(pos, -1)

    exported = 0
    eval_errors = 0
    max_events = int(args.max_events)
    unlimited = (max_events == 0)

    for i in range(total):
        if i < warmup:
            continue

        t_ltf = as_py_datetime(df_ltf.iloc[i]["time"])
        shim.current_time = t_ltf

        is_candidate, pkg, has_error = adapter.evaluate()
        if has_error:
            eval_errors += 1

        if is_candidate:
            ltf_slice = slice_window(df_ltf, i, cfg.ltf_window)

            mtf_slice = None
            if df_mtf is not None and len(df_mtf) > 0:
                j = end_index_by_time(df_mtf, t_ltf)
                if j >= 0:
                    mtf_slice = slice_window(df_mtf, j, cfg.mtf_window)

            htf_slice = None
            if df_htf is not None and len(df_htf) > 0:
                k = end_index_by_time(df_htf, t_ltf)
                if k >= 0:
                    htf_slice = slice_window(df_htf, k, cfg.htf_window)

            snapshot = {
                "meta": {
                    "symbol": cfg.symbol,
                    "mode": cfg.mode,
                    "timestamp": iso(t_ltf),
                    "timeframes": {"LTF": cfg.ltf, "MTF": cfg.mtf, "HTF": cfg.htf},
                },
                "market": {
                    "LTF": ltf_slice.tail(min(len(ltf_slice), 200)).to_dict(orient="records"),
                    "MTF": mtf_slice.tail(min(len(mtf_slice), 120)).to_dict(orient="records") if mtf_slice is not None else [],
                    "HTF": htf_slice.tail(min(len(htf_slice), 80)).to_dict(orient="records") if htf_slice is not None else [],
                },
                "signal_package": pkg,
            }

            event = {
                "event_id": f"{cfg.symbol}:{cfg.ltf}:{iso(t_ltf)}",
                "timestamp": iso(t_ltf),
                "symbol": cfg.symbol,
                "mode": cfg.mode,
                "ltf": cfg.ltf,
                "candidate": True,
                "snapshot": snapshot,
            }

            jsonl_append(dataset_path, event)
            exported += 1
            save_json(resume_path, {"last_idx": i + 1, "exported": exported, "dataset_path": dataset_path})

            if (not unlimited) and exported >= max_events:
                print(f"[STOP] Reached max-events={max_events}")
                break

        if args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)

        if args.progress_every > 0 and (i % args.progress_every == 0):
            pct = (i / (total - 1)) * 100.0
            print(f"[PROGRESS] idx={i}/{total} ({pct:.1f}%) | exported={exported} | eval_errors={eval_errors}")

    print("============================================================")
    print(f"DONE. Exported candidate events: {exported}")
    print(f"Eval errors: {eval_errors}")
    print(f"Dataset: {dataset_path}")
    print(f"Engine error log: {os.path.join(args.state_dir, 'engine_eval_errors.log')}")
    print("============================================================")


if __name__ == "__main__":
    main()