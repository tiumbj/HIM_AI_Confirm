# ==========================================================
# Candidate Replay Runner (Hybrid Validation)
# File: C:\Hybrid_Intelligence_Mentor\candidate_replay_runner_v1_0_1.py
# Version: 1.0.1
# Changelog:
# - v1.0.1:
#   * Fix end_index_by_time(): support python datetime by converting to numpy.datetime64
#   * Harden time conversion paths (Timestamp/datetime)
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
from typing import Any, Dict, Optional, Tuple

# --- Optional deps (safe fallback) ---
try:
    import pandas as pd
except Exception:
    pd = None  # type: ignore

try:
    import MetaTrader5 as mt5
except Exception:
    mt5 = None  # type: ignore


# -----------------------------
# Utilities
# -----------------------------
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


def to_mt5_tf(tf_str: str) -> int:
    """
    Map common config strings to MT5 timeframe constants.
    Supports: M1, M5, M15, M30, H1, H4, D1
    """
    if mt5 is None:
        die("MetaTrader5 module not available. Install MetaTrader5 and ensure MT5 terminal is running.")

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


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, obj: Dict[str, Any]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def jsonl_append(path: str, row: Dict[str, Any]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def as_py_datetime(x: Any) -> dt.datetime:
    """
    Normalize pandas Timestamp / numpy datetime64 / python datetime to python datetime.
    """
    if isinstance(x, dt.datetime):
        return x
    if pd is not None:
        try:
            if isinstance(x, pd.Timestamp):
                return x.to_pydatetime()
        except Exception:
            pass
    # best-effort string parse
    try:
        return dt.datetime.fromisoformat(str(x))
    except Exception:
        return dt.datetime.utcnow()


# -----------------------------
# Config Adapter
# -----------------------------
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
    require_confirmation: bool
    min_rr: float
    max_spread_points: Optional[int]
    max_deviation_points: Optional[int]
    adx_max: Optional[float]
    bb_width_atr_max: Optional[float]


def resolve_effective_config(config_path: str) -> EffectiveConfig:
    raw = load_json(config_path)

    symbol = str(raw.get("symbol", raw.get("Symbol", "GOLD")))
    mode = str(raw.get("mode", raw.get("Mode", "sideway_scalp")))

    tfs = raw.get("timeframes", {}) if isinstance(raw.get("timeframes", {}), dict) else {}
    ltf = str(tfs.get("LTF", raw.get("ltf", "M5"))).upper()
    mtf_val = tfs.get("MTF", raw.get("mtf", "M15"))
    htf_val = tfs.get("HTF", raw.get("htf", "H1"))
    mtf = str(mtf_val).upper() if mtf_val else None
    htf = str(htf_val).upper() if htf_val else None

    min_rr = float(raw.get("min_rr", 1.5))
    require_confirmation = bool(raw.get("require_confirmation", True))

    profiles = raw.get("profiles", {})
    exec_cfg = None
    try:
        exec_cfg = profiles.get(mode, {}).get("execution", None) if isinstance(profiles, dict) else None
    except Exception:
        exec_cfg = None

    max_spread_points = None
    max_deviation_points = None
    if isinstance(exec_cfg, dict):
        max_spread_points = exec_cfg.get("max_spread_points")
        max_deviation_points = exec_cfg.get("max_deviation_points")

    side = raw.get("sideway_scalp", {}) if isinstance(raw.get("sideway_scalp", {}), dict) else {}
    adx_max = side.get("adx_max")
    bb_width_atr_max = side.get("bb_width_atr_max")

    replay = raw.get("replay", {}) if isinstance(raw.get("replay", {}), dict) else {}
    warmup_bars = int(replay.get("warmup_bars", 500))
    ltf_window = int(replay.get("ltf_window", 200))
    mtf_window = int(replay.get("mtf_window", 120))
    htf_window = int(replay.get("htf_window", 80))

    return EffectiveConfig(
        symbol=symbol,
        mode=mode,
        ltf=ltf,
        mtf=mtf,
        htf=htf,
        warmup_bars=warmup_bars,
        ltf_window=ltf_window,
        mtf_window=mtf_window,
        htf_window=htf_window,
        require_confirmation=require_confirmation,
        min_rr=min_rr,
        max_spread_points=max_spread_points,
        max_deviation_points=max_deviation_points,
        adx_max=float(adx_max) if adx_max is not None else None,
        bb_width_atr_max=float(bb_width_atr_max) if bb_width_atr_max is not None else None,
    )


# -----------------------------
# Engine Adapter (Best-effort)
# -----------------------------
class EngineAdapter:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self._engine_obj = None
        self._engine_mod = None

        try:
            import importlib
            self._engine_mod = importlib.import_module("engine")
        except Exception:
            self._engine_mod = None

        if self._engine_mod is not None:
            try:
                if hasattr(self._engine_mod, "TradingEngine"):
                    self._engine_obj = getattr(self._engine_mod, "TradingEngine")(config_path)
            except Exception:
                self._engine_obj = None

    def evaluate(self, ltf_df) -> Tuple[bool, Dict[str, Any]]:
        if self._engine_obj is not None:
            for meth in ["generate_signal_package", "generate_signal", "analyze", "run_once", "compute"]:
                if hasattr(self._engine_obj, meth):
                    try:
                        out = getattr(self._engine_obj, meth)(ltf_df)
                        pkg = self._normalize(out)
                        return self._decide_candidate(pkg), pkg
                    except Exception:
                        pass

        if self._engine_mod is not None:
            for fn in ["generate_signal_package", "compute_signal", "analyze_market", "build_signal"]:
                if hasattr(self._engine_mod, fn):
                    try:
                        out = getattr(self._engine_mod, fn)(ltf_df)
                        pkg = self._normalize(out)
                        return self._decide_candidate(pkg), pkg
                    except Exception:
                        pass

        last = ltf_df.iloc[-1]
        rng = float(last["high"]) - float(last["low"])
        is_candidate = rng > 0 and float(last["close"]) != float(last["open"])
        pkg = {
            "fallback": True,
            "rule": "range>0 and close!=open",
            "bar": {
                "time": str(last["time"]),
                "open": float(last["open"]),
                "high": float(last["high"]),
                "low": float(last["low"]),
                "close": float(last["close"]),
                "tick_volume": int(last.get("tick_volume", 0)),
            },
        }
        return is_candidate, pkg

    def _normalize(self, out: Any) -> Dict[str, Any]:
        if out is None:
            return {}
        if isinstance(out, dict):
            return out
        if hasattr(out, "__dict__"):
            return dict(out.__dict__)
        return {"raw": str(out)}

    def _decide_candidate(self, pkg: Dict[str, Any]) -> bool:
        for k in ["is_candidate", "candidate"]:
            if k in pkg and isinstance(pkg[k], bool):
                return pkg[k]
        for k in ["Signal", "signal", "direction", "dir"]:
            if k in pkg:
                try:
                    v = float(pkg[k])
                    return v != 0.0
                except Exception:
                    if isinstance(pkg[k], str):
                        return pkg[k].strip().lower() in ["buy", "sell", "long", "short"]
        return False


# -----------------------------
# Snapshot Builder (No-lookahead)
# -----------------------------
def df_from_mt5_rates(rates) -> "pd.DataFrame":
    if pd is None:
        die("pandas not available. Install pandas.")
    if rates is None or len(rates) == 0:
        return pd.DataFrame()
    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s")
    return df


def slice_window(df, end_idx: int, window: int) -> "pd.DataFrame":
    start = max(0, end_idx - window + 1)
    return df.iloc[start : end_idx + 1].copy()


# แก้ไขฟังก์ชัน build_snapshot ในไฟล์ candidate_replay_runner_v1_0_0.py
# หาบรรทัดประมาณ 370-400 และแก้ไขเป็น:

def build_snapshot(
    symbol: str,
    cfg: EffectiveConfig,
    t_ltf: dt.datetime,
    ltf_slice,
    mtf_slice,
    htf_slice,
    signal_pkg: Dict[str, Any],
    spread_points_proxy: Optional[int],
) -> Dict[str, Any]:
    
    # ฟังก์ชันช่วยแปลง Timestamp เป็น string
    def convert_timestamps(obj):
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: convert_timestamps(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_timestamps(item) for item in obj]
        return obj
    
    # สร้าง snapshot ปกติ
    snap = {
        "meta": {
            "symbol": symbol,
            "mode": cfg.mode,
            "timestamp": iso(t_ltf),
            "timeframes": {"LTF": cfg.ltf, "MTF": cfg.mtf, "HTF": cfg.htf},
            "windows": {
                "ltf": int(len(ltf_slice)),
                "mtf": int(len(mtf_slice)) if mtf_slice is not None else 0,
                "htf": int(len(htf_slice)) if htf_slice is not None else 0,
            },
            "spread_points_proxy": spread_points_proxy,
            "execution_constraints": {
                "max_spread_points": cfg.max_spread_points,
                "max_deviation_points": cfg.max_deviation_points,
            },
            "sideway_gates": {"adx_max": cfg.adx_max, "bb_width_atr_max": cfg.bb_width_atr_max},
        },
        "market": {
            "LTF": ltf_slice.tail(min(len(ltf_slice), 200)).to_dict(orient="records"),
            "MTF": mtf_slice.tail(min(len(mtf_slice), 120)).to_dict(orient="records") if mtf_slice is not None else [],
            "HTF": htf_slice.tail(min(len(htf_slice), 80)).to_dict(orient="records") if htf_slice is not None else [],
        },
        "signal_package": signal_pkg,
    }
    
    # แปลง Timestamps ทั้งหมดใน snap
    snap = convert_timestamps(snap)
    return snap


# -----------------------------
# Resume State
# -----------------------------
def load_resume_state(path: str) -> Dict[str, Any]:
    if os.path.exists(path):
        try:
            return load_json(path)
        except Exception:
            return {}
    return {}


def save_resume_state(path: str, st: Dict[str, Any]) -> None:
    save_json(path, st)


# -----------------------------
# Main Runner
# -----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config.json")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--max-events", type=int, default=5000, help="Max candidate events to export (0 = unlimited)")
    ap.add_argument("--outdir", default="backtests", help="Output directory for dataset")
    ap.add_argument("--state-dir", default=".state", help="State directory for resume")
    ap.add_argument("--resume", action="store_true", help="Resume from saved replay index")
    ap.add_argument("--sleep-ms", type=int, default=0, help="Optional sleep between iterations (debug)")
    args = ap.parse_args()

    cfg = resolve_effective_config(args.config)

    if mt5 is None:
        die("MetaTrader5 module not available. Install MetaTrader5 and ensure MT5 terminal is running.")

    if not mt5.initialize():
        die(f"MT5 initialize failed: {mt5.last_error()}")

    symbol = cfg.symbol
    if not mt5.symbol_select(symbol, True):
        die(f"MT5 cannot select symbol '{symbol}'")

    tf_ltf = to_mt5_tf(cfg.ltf)
    tf_mtf = to_mt5_tf(cfg.mtf) if cfg.mtf else None
    tf_htf = to_mt5_tf(cfg.htf) if cfg.htf else None

    start_dt = parse_yyyy_mm_dd(args.start)
    end_dt = parse_yyyy_mm_dd(args.end) + dt.timedelta(days=1)

    rates_ltf = mt5.copy_rates_range(symbol, tf_ltf, start_dt, end_dt)
    if rates_ltf is None or len(rates_ltf) == 0:
        die("No LTF rates returned. Check date range / symbol / MT5 history download.")

    df_ltf = df_from_mt5_rates(rates_ltf)

    df_mtf = None
    df_htf = None
    if tf_mtf is not None:
        df_mtf = df_from_mt5_rates(mt5.copy_rates_range(symbol, tf_mtf, start_dt, end_dt))
    if tf_htf is not None:
        df_htf = df_from_mt5_rates(mt5.copy_rates_range(symbol, tf_htf, start_dt, end_dt))

    ensure_dir(args.outdir)
    ensure_dir(args.state_dir)

    dataset_path = os.path.join(
        args.outdir,
        f"candidates_{symbol}_{cfg.ltf}_{args.start}_to_{args.end}.jsonl"
    )
    resume_path = os.path.join(args.state_dir, f"replay_resume_{symbol}_{cfg.ltf}.json")

    resume = load_resume_state(resume_path) if args.resume else {}
    start_idx = int(resume.get("last_idx", 0)) if args.resume else 0
    exported = int(resume.get("exported", 0)) if args.resume else 0

    engine = EngineAdapter(args.config)

    warmup = max(cfg.warmup_bars, cfg.ltf_window)
    total = len(df_ltf)
    if total <= warmup + 5:
        die(f"Not enough bars. Have={total}, need>={warmup + 5}. Expand date range.")

    print("============================================================")
    print("CANDIDATE REPLAY RUNNER v1.0.1")
    print(f"Symbol: {symbol} | Mode: {cfg.mode}")
    print(f"LTF: {cfg.ltf} (bars={len(df_ltf)}) | MTF: {cfg.mtf} | HTF: {cfg.htf}")
    print(f"Range: {args.start} to {args.end}")
    print(f"Warmup bars: {warmup}")
    print(f"Output: {dataset_path}")
    print(f"Resume: {args.resume} (start_idx={start_idx})")
    print("============================================================")

    # Pre-index helper: find last index where df.time <= t (no lookahead)
    def end_index_by_time(df, t_in) -> int:
        import numpy as np
        import datetime as dt
        import pandas as pd

        # convert to python datetime first
        if isinstance(t_in, dt.datetime):
            t_py = t_in
        else:
            try:
                t_py = pd.Timestamp(t_in).to_pydatetime()
            except Exception:
                t_py = dt.datetime.utcnow()

        # convert to numpy datetime64
        t64 = np.datetime64(t_py)

        ts = df["time"].values
        pos = int(np.searchsorted(ts, t64, side="right") - 1)
        return max(pos, -1)

    max_events = args.max_events
    unlimited = (max_events == 0)

    for i in range(start_idx, total):
        if i < warmup:
            continue

        t_ltf = as_py_datetime(df_ltf.iloc[i]["time"])
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

        spread_proxy = None
        try:
            si = mt5.symbol_info(symbol)
            if si is not None and hasattr(si, "spread"):
                spread_proxy = int(si.spread)
        except Exception:
            spread_proxy = None

        try:
            is_candidate, pkg = engine.evaluate(ltf_slice)
        except Exception as e:
            is_candidate, pkg = False, {"error": f"engine_evaluate_failed: {str(e)}"}

        if is_candidate:
            snap = build_snapshot(
                symbol=symbol,
                cfg=cfg,
                t_ltf=t_ltf,
                ltf_slice=ltf_slice,
                mtf_slice=mtf_slice,
                htf_slice=htf_slice,
                signal_pkg=pkg,
                spread_points_proxy=spread_proxy,
            )

            event = {
                "event_id": f"{symbol}:{cfg.ltf}:{iso(t_ltf)}",
                "timestamp": iso(t_ltf),
                "symbol": symbol,
                "mode": cfg.mode,
                "ltf": cfg.ltf,
                "candidate": True,
                "snapshot": snap,
            }

            jsonl_append(dataset_path, event)
            exported += 1
            save_resume_state(resume_path, {"last_idx": i + 1, "exported": exported, "dataset_path": dataset_path})

            if (not unlimited) and exported >= max_events:
                print(f"[STOP] Reached max-events={max_events}")
                break

        if args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)

        if i % 2000 == 0 and i != start_idx:
            pct = (i / (total - 1)) * 100.0
            print(f"[PROGRESS] idx={i}/{total} ({pct:.1f}%) | exported={exported}")

    print("============================================================")
    print(f"DONE. Exported candidate events: {exported}")
    print(f"Dataset: {dataset_path}")
    print(f"Resume state: {resume_path}")
    print("Next: Phase B (AI Schema Validator) then Phase C (A/B Benchmark Runner)")
    print("============================================================")

    mt5.shutdown()


if __name__ == "__main__":
    main()