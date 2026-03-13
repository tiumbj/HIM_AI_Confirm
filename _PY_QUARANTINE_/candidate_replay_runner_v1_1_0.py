# ============================================================
# CANDIDATE REPLAY RUNNER v1.2.2
# File: candidate_replay_runner_v1_1_0.py
# Path: C:\Hybrid_Intelligence_Mentor\candidate_replay_runner_v1_1_0.py
#
# CHANGELOG
# - v1.2.2 (2026-02-28):
#   * ADD: Trace export (all bars) -> backtests/trace_{symbol}_{ltf}_{start}_to_{end}.jsonl
#       - write every pkg (BUY/SELL/NONE) with meta fields
#   * KEEP: v1.2.1 hard bind multi-timeframe dataframe binding by SHIM timeframe IDs
#   * KEEP: verify registration (fatal if required TF missing)
#   * KEEP: proper MT5 module injection via sys.modules["MetaTrader5"]
#
# NOTE
# - candidates_*.jsonl : BUY/SELL only
# - trace_*.jsonl      : ALL bars (for blocked_by distribution)
# ============================================================

from __future__ import annotations

import argparse
import importlib
import json
import sys
import types
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


# -----------------------------
# Utilities
# -----------------------------
def die(msg: str) -> None:
    print(f"[FATAL] {msg}")
    raise SystemExit(2)


def parse_dt(s: str) -> datetime:
    return datetime.strptime(s.strip(), "%Y-%m-%d")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def epoch_to_utc_str(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def to_mt5_struct(df: pd.DataFrame) -> np.ndarray:
    out = np.zeros(
        len(df),
        dtype=[
            ("time", np.int64),
            ("open", np.float64),
            ("high", np.float64),
            ("low", np.float64),
            ("close", np.float64),
            ("tick_volume", np.int64),
            ("spread", np.int64),
            ("real_volume", np.int64),
        ],
    )

    out["time"] = df["time"].astype(np.int64).to_numpy()
    out["open"] = df["open"].astype(float).to_numpy()
    out["high"] = df["high"].astype(float).to_numpy()
    out["low"] = df["low"].astype(float).to_numpy()
    out["close"] = df["close"].astype(float).to_numpy()

    if "tick_volume" in df.columns:
        out["tick_volume"] = df["tick_volume"].fillna(0).astype(np.int64).to_numpy()
    else:
        out["tick_volume"] = 0

    if "spread" in df.columns:
        out["spread"] = df["spread"].fillna(0).astype(np.int64).to_numpy()
    else:
        out["spread"] = 0

    if "real_volume" in df.columns:
        out["real_volume"] = df["real_volume"].fillna(0).astype(np.int64).to_numpy()
    else:
        out["real_volume"] = 0

    return out


def resample_minutes(df_m1: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df_m1 is None or len(df_m1) == 0:
        return pd.DataFrame()

    df = df_m1.copy()
    dt_index = pd.to_datetime(df["time"], unit="s", utc=True)
    df.index = dt_index

    rule = f"{int(minutes)}min"
    o = df["open"].resample(rule).first()
    h = df["high"].resample(rule).max()
    l = df["low"].resample(rule).min()
    c = df["close"].resample(rule).last()

    tv = df["tick_volume"].resample(rule).sum() if "tick_volume" in df.columns else None
    sp = df["spread"].resample(rule).last() if "spread" in df.columns else None
    rv = df["real_volume"].resample(rule).sum() if "real_volume" in df.columns else None

    out = pd.DataFrame({"open": o, "high": h, "low": l, "close": c}).dropna()
    out["tick_volume"] = tv.reindex(out.index).fillna(0).astype(np.int64) if tv is not None else 0
    out["spread"] = sp.reindex(out.index).fillna(0).astype(np.int64) if sp is not None else 0
    out["real_volume"] = rv.reindex(out.index).fillna(0).astype(np.int64) if rv is not None else 0

    out = out.reset_index(drop=False)
    out.rename(columns={"index": "dt"}, inplace=True)
    out["time"] = (out["dt"].astype("int64") // 10**9).astype(np.int64)
    out.drop(columns=["dt"], inplace=True)
    return out.sort_values("time").reset_index(drop=True)


# -----------------------------
# Config handling
# -----------------------------
@dataclass
class TFConfig:
    ltf: str
    mtf: Optional[str]
    htf: Optional[str]


@dataclass
class RunnerConfig:
    symbol: str
    mode: str
    tf: TFConfig


def load_runner_config(config_path: str) -> RunnerConfig:
    with open(config_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    symbol = str(raw.get("symbol", "GOLD"))
    mode = str(raw.get("mode", "sideway_scalp"))

    profiles = raw.get("profiles", {}) or {}
    prof = profiles.get(mode, {}) if isinstance(profiles, dict) else {}

    tf_block = prof.get("timeframes", {}) if isinstance(prof, dict) else {}
    ltf = str(tf_block.get("ltf", raw.get("ltf", "M5")))
    mtf = tf_block.get("mtf", raw.get("mtf", "M15"))
    htf = tf_block.get("htf", raw.get("htf", "H1"))

    mtf = str(mtf) if mtf else None
    htf = str(htf) if htf else None

    return RunnerConfig(symbol=symbol, mode=mode, tf=TFConfig(ltf=ltf, mtf=mtf, htf=htf))


# -----------------------------
# MT5Shim (Replay)
# -----------------------------
class MT5Shim:
    TIMEFRAME_M1 = 1
    TIMEFRAME_M2 = -2
    TIMEFRAME_M3 = -3
    TIMEFRAME_M4 = -4
    TIMEFRAME_M5 = 5
    TIMEFRAME_M6 = -6
    TIMEFRAME_M10 = -10
    TIMEFRAME_M12 = -12
    TIMEFRAME_M15 = 15
    TIMEFRAME_M20 = -20
    TIMEFRAME_M30 = 30
    TIMEFRAME_H1 = 60
    TIMEFRAME_H2 = -120
    TIMEFRAME_H3 = -180
    TIMEFRAME_H4 = 240
    TIMEFRAME_H6 = -360
    TIMEFRAME_H8 = -480
    TIMEFRAME_H12 = -720
    TIMEFRAME_D1 = 1440
    TIMEFRAME_W1 = -10080
    TIMEFRAME_MN1 = -43200

    MIN_BARS = 50

    def __init__(self, symbol: str, tf_dfs: Dict[int, pd.DataFrame]):
        self._symbol = symbol
        self._tf_dfs = tf_dfs
        self.current_time: Optional[int] = None
        self._tick_bid: float = float("nan")
        self._tick_ask: float = float("nan")

        self.debug_enabled: bool = False
        self.debug_until_time: Optional[int] = None
        self.debug_force_on_none: bool = True
        self._dbg_call_seq: int = 0

    def initialize(self) -> bool:
        return True

    def shutdown(self) -> None:
        return None

    def symbol_select(self, symbol: str, enable: bool) -> bool:
        return True

    def symbol_info_tick(self, symbol: str):
        class _T:
            pass

        t = _T()
        t.bid = float(self._tick_bid)
        t.ask = float(self._tick_ask)
        return t

    def _get_df(self, timeframe: int) -> Optional[pd.DataFrame]:
        return self._tf_dfs.get(int(timeframe))

    def _clip_to_current(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or len(df) == 0:
            return df
        if self.current_time is None:
            return df
        return df[df["time"] <= int(self.current_time)]

    def _should_debug_now(self) -> bool:
        if not self.debug_enabled:
            return False
        if self.current_time is None:
            return True
        if self.debug_until_time is None:
            return True
        return int(self.current_time) <= int(self.debug_until_time)

    def _shim_dbg(self, msg: str) -> None:
        print(f"[SHIM] {msg}")

    def copy_rates_from_pos(self, symbol: str, timeframe: int, start_pos: int, count: int):
        self._dbg_call_seq += 1

        df = self._get_df(int(timeframe))
        if df is None or len(df) == 0:
            if self._should_debug_now() or self.debug_force_on_none:
                self._shim_dbg(f"call={self._dbg_call_seq} tf={timeframe} pos={start_pos} count={count} -> None reason=no_df")
            return None

        df2 = self._clip_to_current(df)
        if df2 is None or len(df2) == 0:
            if self._should_debug_now() or self.debug_force_on_none:
                self._shim_dbg(f"call={self._dbg_call_seq} tf={timeframe} pos={start_pos} count={count} ct={self.current_time} -> None reason=empty_after_clip")
            return None

        if len(df2) < int(self.MIN_BARS):
            if self._should_debug_now() or self.debug_force_on_none:
                self._shim_dbg(f"call={self._dbg_call_seq} tf={timeframe} pos={start_pos} count={count} len_df2={len(df2)} -> None reason=len_df2<MIN_BARS({self.MIN_BARS})")
            return None

        pos = int(start_pos)
        cnt = int(count)
        if cnt <= 0:
            if self._should_debug_now() or self.debug_force_on_none:
                self._shim_dbg(f"call={self._dbg_call_seq} tf={timeframe} pos={start_pos} count={count} -> None reason=bad_count")
            return None

        end_idx = len(df2) - 1 - pos
        if end_idx < 0:
            if self._should_debug_now() or self.debug_force_on_none:
                self._shim_dbg(f"call={self._dbg_call_seq} tf={timeframe} pos={start_pos} count={count} len_df2={len(df2)} end_idx={end_idx} -> None reason=end_idx<0")
            return None

        start_idx = max(0, end_idx - cnt + 1)
        window = df2.iloc[start_idx : end_idx + 1].copy()

        if len(window) < int(self.MIN_BARS):
            if self._should_debug_now() or self.debug_force_on_none:
                self._shim_dbg(
                    f"call={self._dbg_call_seq} tf={timeframe} pos={start_pos} count={count} "
                    f"len_df2={len(df2)} start_idx={start_idx} end_idx={end_idx} window_len={len(window)} "
                    f"-> None reason=window_len<MIN_BARS({self.MIN_BARS})"
                )
            return None

        if self._should_debug_now():
            self._shim_dbg(
                f"call={self._dbg_call_seq} tf={timeframe} pos={start_pos} count={count} ct={self.current_time} "
                f"len_df2={len(df2)} start_idx={start_idx} end_idx={end_idx} window_len={len(window)} -> OK"
            )

        return to_mt5_struct(window)


# -----------------------------
# Injection
# -----------------------------
def inject_mt5_module(shim: MT5Shim) -> None:
    mt5_mod = types.ModuleType("MetaTrader5")

    for name in dir(shim):
        if name.startswith("TIMEFRAME_"):
            setattr(mt5_mod, name, getattr(shim, name))

    mt5_mod.initialize = shim.initialize
    mt5_mod.shutdown = shim.shutdown
    mt5_mod.symbol_select = shim.symbol_select
    mt5_mod.symbol_info_tick = shim.symbol_info_tick
    mt5_mod.copy_rates_from_pos = shim.copy_rates_from_pos

    sys.modules["MetaTrader5"] = mt5_mod


# -----------------------------
# Real MT5 fetch (before injection)
# -----------------------------
def fetch_mt5_rates(symbol: str, timeframe: int, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    import MetaTrader5 as mt5

    if not mt5.initialize():
        die("MT5 initialize() failed. Please open MT5 terminal and login.")

    mt5.symbol_select(symbol, True)

    rates = mt5.copy_rates_range(symbol, timeframe, start_dt, end_dt)
    if rates is None or len(rates) == 0:
        return pd.DataFrame()

    df = pd.DataFrame(rates)
    if "time" not in df.columns:
        return pd.DataFrame()

    df["time"] = df["time"].astype(np.int64)
    return df.sort_values("time").reset_index(drop=True)


def tf_str_to_shim_id(tf: str) -> int:
    s = (tf or "").strip().upper()
    mapping = {
        "M1": MT5Shim.TIMEFRAME_M1,
        "M2": MT5Shim.TIMEFRAME_M2,
        "M3": MT5Shim.TIMEFRAME_M3,
        "M4": MT5Shim.TIMEFRAME_M4,
        "M5": MT5Shim.TIMEFRAME_M5,
        "M6": MT5Shim.TIMEFRAME_M6,
        "M10": MT5Shim.TIMEFRAME_M10,
        "M12": MT5Shim.TIMEFRAME_M12,
        "M15": MT5Shim.TIMEFRAME_M15,
        "M20": MT5Shim.TIMEFRAME_M20,
        "M30": MT5Shim.TIMEFRAME_M30,
        "H1": MT5Shim.TIMEFRAME_H1,
        "H2": MT5Shim.TIMEFRAME_H2,
        "H3": MT5Shim.TIMEFRAME_H3,
        "H4": MT5Shim.TIMEFRAME_H4,
        "H6": MT5Shim.TIMEFRAME_H6,
        "H8": MT5Shim.TIMEFRAME_H8,
        "H12": MT5Shim.TIMEFRAME_H12,
        "D1": MT5Shim.TIMEFRAME_D1,
        "W1": MT5Shim.TIMEFRAME_W1,
        "MN1": MT5Shim.TIMEFRAME_MN1,
    }
    if s in mapping:
        return int(mapping[s])

    if s.startswith("M"):
        try:
            mins = int(s[1:])
            return -mins
        except Exception:
            return 0
    return 0


def fetch_df_for_tf(symbol: str, tf_str: str, df_m1: pd.DataFrame, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    s = (tf_str or "").strip().upper()
    shim_id = tf_str_to_shim_id(s)

    if shim_id < 0 and s.startswith("M"):
        mins = abs(int(shim_id))
        return resample_minutes(df_m1, mins)

    try:
        import MetaTrader5 as mt5
        native_id = getattr(mt5, f"TIMEFRAME_{s}", None)
        if native_id is None:
            if s.startswith("M"):
                mins = int(s[1:])
                return resample_minutes(df_m1, mins)
            return pd.DataFrame()

        df = fetch_mt5_rates(symbol, int(native_id), start_dt, end_dt)
        if df is not None and len(df) > 0:
            return df

        if s.startswith("M"):
            mins = int(s[1:])
            return resample_minutes(df_m1, mins)

        return pd.DataFrame()
    except Exception:
        if s.startswith("M"):
            try:
                mins = int(s[1:])
                return resample_minutes(df_m1, mins)
            except Exception:
                return pd.DataFrame()
        return pd.DataFrame()


def register_tf(tf_dfs: Dict[int, pd.DataFrame], tf_str: Optional[str], df: Optional[pd.DataFrame]) -> int:
    if not tf_str:
        return 0
    shim_id = tf_str_to_shim_id(tf_str)
    if shim_id == 0:
        return 0
    if df is None or len(df) == 0:
        return shim_id
    tf_dfs[int(shim_id)] = df
    return int(shim_id)


def print_registered(tf_dfs: Dict[int, pd.DataFrame]) -> None:
    keys = sorted(tf_dfs.keys())
    parts = [f"{k}:{len(tf_dfs[k])}" for k in keys]
    print(f"[INFO] Registered timeframes (shim_id:bars) => {', '.join(parts)}")


def verify_required(tf_dfs: Dict[int, pd.DataFrame], required_ids: Dict[str, int]) -> None:
    missing = []
    for label, tf_id in required_ids.items():
        if tf_id == 0:
            continue
        if int(tf_id) not in tf_dfs:
            missing.append(f"{label}={tf_id}")
    if missing:
        die(f"Missing required timeframe dfs: {', '.join(missing)}")


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config.json")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--max-events", type=int, default=200)
    ap.add_argument("--warmup", type=int, default=200)
    ap.add_argument("--debug-first", type=int, default=0)
    ap.add_argument("--shim-debug", action="store_true")
    ap.add_argument("--export-trace", action="store_true", help="Export ALL bars trace jsonl")
    args = ap.parse_args()

    cfg = load_runner_config(args.config)
    symbol = cfg.symbol
    mode = cfg.mode
    ltf_str = cfg.tf.ltf
    mtf_str = cfg.tf.mtf
    htf_str = cfg.tf.htf

    start_dt = parse_dt(args.start)
    end_dt = parse_dt(args.end) + timedelta(days=1) - timedelta(seconds=1)

    ensure_parent(Path("backtests") / "x.txt")

    candidates_path = Path("backtests") / f"candidates_{symbol}_{ltf_str}_{args.start}_to_{args.end}.jsonl"
    trace_path = Path("backtests") / f"trace_{symbol}_{ltf_str}_{args.start}_to_{args.end}.jsonl"

    buffer_days = 5
    start_buf = start_dt - timedelta(days=buffer_days)
    end_buf = end_dt + timedelta(days=buffer_days)

    # Real MT5 fetch BEFORE injection
    df_m1 = fetch_mt5_rates(symbol, 1, start_buf, end_buf)
    if df_m1 is None or len(df_m1) == 0:
        die("No M1 rates fetched.")

    df_ltf = fetch_df_for_tf(symbol, ltf_str, df_m1, start_dt, end_dt)
    if df_ltf is None or len(df_ltf) == 0:
        die(f"No LTF rates for {ltf_str}.")

    df_mtf = fetch_df_for_tf(symbol, mtf_str, df_m1, start_dt, end_dt) if mtf_str else None
    df_htf = fetch_df_for_tf(symbol, htf_str, df_m1, start_dt, end_dt) if htf_str else None

    total = len(df_ltf)
    warmup = int(args.warmup)
    if total < warmup + 5:
        die(f"Not enough bars. Have={total}, need>={warmup+5}.")

    print("============================================================")
    print("CANDIDATE REPLAY RUNNER v1.2.2")
    print(f"Symbol: {symbol} | Mode: {mode}")
    print(f"LTF: {ltf_str} (bars={total}) | MTF: {mtf_str or 'None'} | HTF: {htf_str or 'None'}")
    print(f"Range: {args.start} to {args.end}")
    print(f"Warmup: {warmup} | Max events: {args.max_events}")
    print(f"Candidates: {candidates_path.as_posix()}")
    print(f"Trace: {trace_path.as_posix()} (enabled={bool(args.export_trace)})")
    print("============================================================")

    # Hard register by SHIM IDs
    tf_dfs: Dict[int, pd.DataFrame] = {}
    ltf_id = register_tf(tf_dfs, ltf_str, df_ltf)
    mtf_id = register_tf(tf_dfs, mtf_str, df_mtf) if mtf_str else 0
    htf_id = register_tf(tf_dfs, htf_str, df_htf) if htf_str else 0

    print_registered(tf_dfs)
    verify_required(tf_dfs, {"LTF": ltf_id, "MTF": mtf_id, "HTF": htf_id})

    shim = MT5Shim(symbol=symbol, tf_dfs=tf_dfs)

    if args.shim_debug and args.debug_first:
        idx_cut = min(total - 1, warmup + int(args.debug_first) - 1)
        shim.debug_enabled = True
        shim.debug_until_time = int(df_ltf.iloc[idx_cut]["time"])
    elif args.shim_debug:
        shim.debug_enabled = True
        shim.debug_until_time = None

    inject_mt5_module(shim)

    # import/reload engine AFTER injection
    if "engine" in sys.modules:
        importlib.reload(sys.modules["engine"])
        engine_mod = sys.modules["engine"]
    else:
        engine_mod = importlib.import_module("engine")

    TradingEngine = engine_mod.TradingEngine
    eng = TradingEngine(args.config)

    exported = 0
    eval_errors = 0

    candidates_path.parent.mkdir(parents=True, exist_ok=True)

    f_trace = trace_path.open("w", encoding="utf-8") if args.export_trace else None
    with candidates_path.open("w", encoding="utf-8") as f_cand:
        for i in range(warmup, total):
            t_ltf = int(df_ltf.iloc[i]["time"])
            shim.current_time = t_ltf

            close = float(df_ltf.iloc[i]["close"])
            shim._tick_bid = close
            shim._tick_ask = close

            try:
                pkg = eng.generate_signal_package()
                has_error = False
            except Exception as e:
                pkg = {
                    "symbol": symbol,
                    "direction": "NONE",
                    "context": {"blocked_by": "engine_exception", "error": str(e)},
                }
                has_error = True
                eval_errors += 1

            # write trace (all bars)
            if f_trace is not None:
                meta = {
                    "i": i,
                    "t_epoch": t_ltf,
                    "t_utc": epoch_to_utc_str(t_ltf),
                    "ltf": ltf_str,
                    "mode": mode,
                }
                if isinstance(pkg, dict):
                    pkg_out: Dict[str, Any] = dict(pkg)
                    pkg_out["meta"] = meta
                else:
                    pkg_out = {"raw": str(pkg), "meta": meta}
                f_trace.write(json.dumps(pkg_out, ensure_ascii=False) + "\n")

            direction = str(pkg.get("direction", "NONE")) if isinstance(pkg, dict) else "NONE"
            ctx = pkg.get("context", {}) if isinstance(pkg, dict) and isinstance(pkg.get("context"), dict) else {}
            blocked_by = ctx.get("blocked_by")

            if args.debug_first and i < warmup + int(args.debug_first):
                dt_utc = datetime.fromtimestamp(t_ltf, tz=timezone.utc)
                print(f"[DBG] i={i} t={dt_utc} dir={direction} blocked_by={blocked_by} has_error={has_error}")

            # write candidates (BUY/SELL only)
            if direction in ("BUY", "SELL"):
                f_cand.write(json.dumps(pkg, ensure_ascii=False) + "\n")
                exported += 1
                if exported >= int(args.max_events):
                    break

            if i == warmup or (i - warmup) % 200 == 0:
                pct = (i / total) * 100.0
                print(f"[PROGRESS] idx={i}/{total} ({pct:.1f}%) | exported={exported} | eval_errors={eval_errors}")

    if f_trace is not None:
        f_trace.close()

    print("============================================================")
    print(f"DONE. Exported candidate events: {exported}")
    print(f"Eval errors: {eval_errors}")
    print(f"Candidates: {candidates_path.as_posix()}")
    if args.export_trace:
        print(f"Trace: {trace_path.as_posix()}")
    print("============================================================")


if __name__ == "__main__":
    main()