# ==========================================================
# Candidate Replay Runner (Hybrid Validation) - Engine Binding
# File: C:\Hybrid_Intelligence_Mentor\candidate_replay_runner_v1_0_5.py
# Version: 1.0.5
# Changelog:
# - v1.0.5:
#   * Engine call is adaptive based on inspect.signature()
#   * Capture first N engine exceptions + short traceback to .state/engine_eval_errors.log
#   * Optional --debug-engine prints signature and first error summary
#   * Keep JSON-safe conversion for snapshots (Timestamp -> ISO)
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


def to_mt5_tf(tf_str: str) -> int:
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
    mtf = str(mtf_val).upper() if mtf_val else None
    htf = str(htf_val).upper() if htf_val else None

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
    )


class EngineAdapter:
    """
    Deterministic binding:
    - We discovered TradingEngine.generate_signal_package exists.
    - But signature is unknown in advance. We inspect signature and pass arguments accordingly.
    - We log exceptions for inspection.
    """

    def __init__(self, config_path: str, debug_engine: bool, state_dir: str):
        self.config_path = config_path
        self.debug_engine = debug_engine
        self.state_dir = state_dir
        ensure_dir(state_dir)

        self._engine_obj = None
        self._sig = None
        self._sig_txt = None

        try:
            from engine import TradingEngine  # type: ignore
            self._engine_obj = TradingEngine(config_path)
        except Exception as e:
            self._engine_obj = None
            self._log_error("ENGINE_INIT", e)

        if self._engine_obj is not None:
            try:
                import inspect
                fn = getattr(self._engine_obj, "generate_signal_package", None)
                if fn is None:
                    raise AttributeError("TradingEngine has no generate_signal_package")
                self._sig = inspect.signature(fn)
                self._sig_txt = str(self._sig)
                if self.debug_engine:
                    print(f"[ENGINE] generate_signal_package signature = {self._sig_txt}")
            except Exception as e:
                self._log_error("ENGINE_SIGNATURE", e)

    def _log_error(self, tag: str, e: Exception) -> None:
        path = os.path.join(self.state_dir, "engine_eval_errors.log")
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"\n--- {tag} ---\n")
            f.write(f"{type(e).__name__}: {str(e)}\n")
            f.write(traceback.format_exc(limit=8))

    def _call_generate_signal_package(
        self,
        symbol: str,
        timeframe: str,
        ltf_df,
        cfg_obj: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Adaptive call by parameter names.
        Common patterns handled:
        - (df) or (bars) or (data)
        - (symbol, df)
        - (symbol, timeframe, df)
        - (cfg, df)
        - (payload)
        """
        if self._engine_obj is None:
            return {"error": "engine_not_initialized"}

        fn = getattr(self._engine_obj, "generate_signal_package")

        # Build candidate payloads
        df_records = None
        try:
            df_records = ltf_df.to_dict(orient="records")
        except Exception:
            df_records = None

        payload = {
            "symbol": symbol,
            "timeframe": timeframe,
            "config": cfg_obj,
            "bars_df": ltf_df,
            "bars": df_records,
        }

        # If we know signature, match by param names
        if self._sig is not None:
            params = [p for p in self._sig.parameters.values() if p.name != "self"]

            # 0 params
            if len(params) == 0:
                out = fn()
                return out if isinstance(out, dict) else {"raw": str(out)}

            # 1 param
            if len(params) == 1:
                name = params[0].name.lower()
                if name in ("df", "bars_df", "dataframe", "data"):
                    out = fn(ltf_df)
                elif name in ("bars", "rates", "ohlc", "candles"):
                    out = fn(df_records if df_records is not None else ltf_df)
                elif name in ("payload", "input", "req", "request"):
                    out = fn({"symbol": symbol, "timeframe": timeframe, "bars": df_records})
                else:
                    # best-effort: df first
                    out = fn(ltf_df)
                return out if isinstance(out, dict) else {"raw": str(out)}

            # 2+ params: fill by names when possible
            kwargs: Dict[str, Any] = {}
            args_list = []

            # Prefer kwargs if names match
            for p in params:
                n = p.name.lower()
                if n in payload:
                    kwargs[p.name] = payload[n]
                elif n in ("symbol",):
                    kwargs[p.name] = symbol
                elif n in ("tf", "timeframe", "period"):
                    kwargs[p.name] = timeframe
                elif n in ("cfg", "config", "settings"):
                    kwargs[p.name] = cfg_obj
                elif n in ("df", "bars_df", "dataframe", "data"):
                    kwargs[p.name] = ltf_df
                elif n in ("bars", "rates", "ohlc", "candles"):
                    kwargs[p.name] = df_records if df_records is not None else ltf_df

            # If kwargs didn't cover enough (positional fallback)
            if len(kwargs) < len(params):
                for p in params:
                    if p.name in kwargs:
                        continue
                    n = p.name.lower()
                    if n in ("symbol",):
                        args_list.append(symbol)
                    elif n in ("tf", "timeframe", "period"):
                        args_list.append(timeframe)
                    elif n in ("cfg", "config", "settings"):
                        args_list.append(cfg_obj)
                    else:
                        args_list.append(ltf_df)

            out = fn(*args_list, **kwargs)
            return out if isinstance(out, dict) else {"raw": str(out)}

        # Unknown signature: try safe call order
        try:
            out = fn(ltf_df)
            return out if isinstance(out, dict) else {"raw": str(out)}
        except Exception:
            out = fn(symbol, ltf_df)
            return out if isinstance(out, dict) else {"raw": str(out)}

    def evaluate(
        self,
        symbol: str,
        timeframe: str,
        ltf_df,
        cfg_obj: Dict[str, Any],
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Returns (is_candidate, pkg)
        Candidate decision rule:
        - If pkg contains 'Signal'/'signal' non-zero OR 'direction' buy/sell => candidate
        - Else not candidate
        """
        try:
            pkg = self._call_generate_signal_package(symbol, timeframe, ltf_df, cfg_obj)
        except Exception as e:
            self._log_error("ENGINE_CALL", e)
            return False, {"error": f"engine_call_failed: {type(e).__name__}", "message": str(e)}

        # Decide candidate
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

        for k in ("Signal", "signal", "direction", "dir"):
            if k in pkg:
                d = as_dir(pkg.get(k))
                if d is not None:
                    return True, pkg

        # Some engines nest under keys like 'dry_run_order' or 'signal'
        for container_key in ("dry_run_order", "order", "result", "payload", "signal"):
            sub = pkg.get(container_key)
            if isinstance(sub, dict):
                for k in ("Signal", "signal", "direction", "dir"):
                    if k in sub:
                        d = as_dir(sub.get(k))
                        if d is not None:
                            return True, pkg

        return False, pkg


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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--max-events", type=int, default=5000)
    ap.add_argument("--outdir", default="backtests")
    ap.add_argument("--state-dir", default=".state")
    ap.add_argument("--sleep-ms", type=int, default=0)
    ap.add_argument("--progress-every", type=int, default=200)
    ap.add_argument("--warmup", type=int, default=None)
    ap.add_argument("--debug-engine", action="store_true")
    args = ap.parse_args()

    cfg = resolve_effective_config(args.config)
    cfg_obj = load_json(args.config)

    if mt5 is None:
        die("MetaTrader5 module not available.")
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
        die("No LTF rates returned.")
    df_ltf = df_from_mt5_rates(rates_ltf)

    df_mtf = None
    df_htf = None
    if tf_mtf is not None:
        df_mtf = df_from_mt5_rates(mt5.copy_rates_range(symbol, tf_mtf, start_dt, end_dt))
    if tf_htf is not None:
        df_htf = df_from_mt5_rates(mt5.copy_rates_range(symbol, tf_htf, start_dt, end_dt))

    ensure_dir(args.outdir)
    ensure_dir(args.state_dir)

    dataset_path = os.path.join(args.outdir, f"candidates_{symbol}_{cfg.ltf}_{args.start}_to_{args.end}.jsonl")
    resume_path = os.path.join(args.state_dir, f"replay_resume_{symbol}_{cfg.ltf}.json")

    # fresh run: clear old dataset to avoid confusion
    if os.path.exists(dataset_path):
        os.remove(dataset_path)

    warmup = int(args.warmup) if args.warmup is not None else max(cfg.warmup_bars, cfg.ltf_window)
    total = len(df_ltf)
    need = warmup + 5
    if total < need:
        die(f"Not enough bars. Have={total}, need>={need}. Reduce --warmup or expand date range.")

    print("============================================================")
    print("CANDIDATE REPLAY RUNNER v1.0.5")
    print(f"Symbol: {symbol} | Mode: {cfg.mode}")
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

    engine = EngineAdapter(args.config, debug_engine=args.debug_engine, state_dir=args.state_dir)

    exported = 0
    eval_errors = 0
    max_events = args.max_events
    unlimited = (max_events == 0)

    for i in range(total):
        if i < warmup:
            continue

        t_ltf = as_py_datetime(df_ltf.iloc[i]["time"])
        ltf_slice = slice_window(df_ltf, i, cfg.ltf_window)

        # MTF/HTF alignment for snapshot only (no look-ahead)
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

        # Evaluate via engine
        try:
            is_candidate, pkg = engine.evaluate(symbol, cfg.ltf, ltf_slice, cfg_obj)
        except Exception as e:
            eval_errors += 1
            # also logged inside adapter in most cases
            pkg = {"error": f"engine_eval_failed: {type(e).__name__}", "message": str(e)}
            is_candidate = False

        if is_candidate:
            spread_proxy = None
            try:
                si = mt5.symbol_info(symbol)
                if si is not None and hasattr(si, "spread"):
                    spread_proxy = int(si.spread)
            except Exception:
                spread_proxy = None

            snapshot = {
                "meta": {
                    "symbol": symbol,
                    "mode": cfg.mode,
                    "timestamp": iso(t_ltf),
                    "timeframes": {"LTF": cfg.ltf, "MTF": cfg.mtf, "HTF": cfg.htf},
                    "spread_points_proxy": spread_proxy,
                },
                "market": {
                    "LTF": ltf_slice.tail(min(len(ltf_slice), 200)).to_dict(orient="records"),
                    "MTF": mtf_slice.tail(min(len(mtf_slice), 120)).to_dict(orient="records") if mtf_slice is not None else [],
                    "HTF": htf_slice.tail(min(len(htf_slice), 80)).to_dict(orient="records") if htf_slice is not None else [],
                },
                "signal_package": pkg,
            }

            event = {
                "event_id": f"{symbol}:{cfg.ltf}:{iso(t_ltf)}",
                "timestamp": iso(t_ltf),
                "symbol": symbol,
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

    mt5.shutdown()


if __name__ == "__main__":
    main()