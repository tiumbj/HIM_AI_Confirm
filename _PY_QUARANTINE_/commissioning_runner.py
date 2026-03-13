"""
Hybrid Intelligence Mentor (HIM)
File: commissioning_runner.py
Version: v2.12.6 (Event-Based Commissioning + Gate Observability + MT5 Time Compatibility)
Date: 2026-03-02 (Asia/Bangkok)

CHANGELOG
- v2.12.6:
  - FIX: MetaTrader5 module in some environments has no mt5.time_current().
    Implement server_time_epoch() compatibility:
      * if mt5.time_current exists -> use it
      * else fallback to int(time.time()) (UTC epoch)
    tick_age_sec = max(0, server_time - tick.time)
  - Keep ALL features from v2.12.5:
      * Event gating: NEW_BAR + SIGNAL_CHANGE
      * Persistent state: .commission_event_state.json
      * Logs: logs/commissioning_events.jsonl
      * Gate Observability: details.signal.obs (best-effort, schema-agnostic)
      * One-shot mentor_executor.py call on EVENT only

RATIONALE
- Restore runtime without downgrading features.
- Make commissioning stable across MT5 package variants.

SAFETY
- Fail-closed behavior: stale tick -> no event processing
"""

from __future__ import annotations

import json
import os
import sys
import time
import hashlib
import traceback
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    import MetaTrader5 as mt5
except Exception as e:
    print(f"[FATAL] MetaTrader5 import failed: {e}")
    raise SystemExit(1)


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_dir_for_file(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def json_dumps_compact(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def atomic_write_json(path: str, obj: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def append_jsonl(path: str, obj: Dict[str, Any]) -> None:
    ensure_dir_for_file(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json_dumps_compact(obj) + "\n")


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

def load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"config not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    if not isinstance(cfg, dict):
        raise ValueError("config.json must be a JSON object")
    return cfg


def cfg_get(cfg: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    cur: Any = cfg
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


# -----------------------------------------------------------------------------
# MT5: Time compatibility + Tick freshness + bar time
# -----------------------------------------------------------------------------

def server_time_epoch() -> int:
    """
    Compatibility for MT5 variants.
    Preferred: mt5.time_current() (server time, epoch seconds)
    Fallback: time.time() (system UTC epoch seconds)
    """
    try:
        fn = getattr(mt5, "time_current", None)
        if callable(fn):
            return int(fn())
    except Exception:
        pass
    return int(time.time())


@dataclass
class TickSnapshot:
    bid: float
    ask: float
    last: float
    tick_time: int
    server_time: int
    tick_age_sec: float


def get_tick_snapshot(symbol: str) -> Optional[TickSnapshot]:
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return None

    # tick.time is epoch seconds (UTC) from server
    tick_time = int(getattr(tick, "time", 0))
    server_time = server_time_epoch()

    age = server_time - tick_time
    if age < 0:
        age = 0

    return TickSnapshot(
        bid=float(getattr(tick, "bid", 0.0) or 0.0),
        ask=float(getattr(tick, "ask", 0.0) or 0.0),
        last=float(getattr(tick, "last", 0.0) or 0.0),
        tick_time=tick_time,
        server_time=server_time,
        tick_age_sec=float(age),
    )


def tick_is_fresh(tick: TickSnapshot, max_age_sec: float) -> bool:
    return tick.tick_age_sec <= max_age_sec


TF_MAP = {
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


def get_latest_bar_time(symbol: str, tf_name: str) -> Optional[int]:
    tf_code = TF_MAP.get(tf_name)
    if tf_code is None:
        return None
    try:
        rates = mt5.copy_rates_from_pos(symbol, tf_code, 0, 1)
        if rates is None or len(rates) == 0:
            return None
        return int(rates[0]["time"])
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Event State (persistent)
# -----------------------------------------------------------------------------

@dataclass
class EventState:
    last_bar_time: Dict[str, int]          # tf_name -> bar_time
    last_signature: Optional[str]          # stable signal signature


def load_event_state(path: str) -> EventState:
    obj = load_json(path, {})
    if not isinstance(obj, dict):
        obj = {}
    lbt = obj.get("last_bar_time", {})
    if not isinstance(lbt, dict):
        lbt = {}
    lbt2: Dict[str, int] = {}
    for k, v in lbt.items():
        try:
            lbt2[str(k).upper()] = int(v)
        except Exception:
            continue
    sig = obj.get("last_signature", None)
    if sig is not None:
        sig = str(sig)
    return EventState(last_bar_time=lbt2, last_signature=sig)


def save_event_state(path: str, st: EventState) -> None:
    atomic_write_json(path, {
        "last_bar_time": st.last_bar_time,
        "last_signature": st.last_signature,
        "ts": utc_iso_now(),
        "version": "v2.12.6",
    })


# -----------------------------------------------------------------------------
# Observability extraction (best-effort, schema-agnostic)
# -----------------------------------------------------------------------------

def _dig(d: Any, path: List[str]) -> Any:
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur


def _first_present(d: Dict[str, Any], candidates: List[List[str]]) -> Any:
    for p in candidates:
        v = _dig(d, p)
        if v is not None:
            return v
    return None


def build_observability(d: Dict[str, Any]) -> Dict[str, Any]:
    htf_dir = _first_present(d, [
        ["structure", "htf", "dir"],
        ["structure", "htf", "direction"],
        ["htf", "dir"],
        ["htf_dir"],
        ["htf_direction"],
    ])
    mtf_dir = _first_present(d, [
        ["structure", "mtf", "dir"],
        ["structure", "mtf", "direction"],
        ["mtf", "dir"],
        ["mtf_dir"],
        ["mtf_direction"],
    ])
    ltf_dir = _first_present(d, [
        ["structure", "ltf", "dir"],
        ["structure", "ltf", "direction"],
        ["ltf", "dir"],
        ["ltf_dir"],
        ["ltf_direction"],
    ])

    adx_val = _first_present(d, [
        ["adx"],
        ["indicators", "adx"],
        ["breakout", "adx"],
        ["sideway_scalp", "adx"],
        ["metrics", "adx"],
    ])
    adx_val = safe_float(adx_val, None)

    bb_width_atr = _first_present(d, [
        ["bb_width_atr"],
        ["indicators", "bb_width_atr"],
        ["breakout", "bb_width_atr"],
        ["metrics", "bb_width_atr"],
        ["vol", "bb_width_atr"],
    ])
    bb_width_atr = safe_float(bb_width_atr, None)

    st_dir = _first_present(d, [
        ["supertrend", "dir"],
        ["supertrend", "direction"],
        ["indicators", "supertrend_dir"],
        ["supertrend_dir"],
    ])

    blocked_by = d.get("blocked_by", None)
    gates = d.get("gates", None)
    reasons = d.get("reasons", None)

    if blocked_by is None:
        blocked_by = _first_present(d, [["decision", "blocked_by"], ["audit", "blocked_by"]])
    if gates is None:
        gates = _first_present(d, [["decision", "gates"], ["audit", "gates"]])
    if reasons is None:
        reasons = _first_present(d, [["decision", "reasons"], ["audit", "reasons"]])

    if isinstance(blocked_by, (list, dict)):
        blocked_by = json_dumps_compact(blocked_by)
    if isinstance(gates, (list, dict)):
        gates = json_dumps_compact(gates)
    if isinstance(reasons, (list, dict)):
        reasons = json_dumps_compact(reasons)

    decision = _first_present(d, [["decision"], ["action"], ["signal"], ["side"]])

    return {
        "htf_dir": htf_dir,
        "mtf_dir": mtf_dir,
        "ltf_dir": ltf_dir,
        "adx": adx_val,
        "bb_width_atr": bb_width_atr,
        "supertrend_dir": st_dir,
        "decision": decision,
        "blocked_by": blocked_by,
        "gates": gates,
        "reasons": reasons,
    }


# -----------------------------------------------------------------------------
# Signal payload + signature (best-effort)
# -----------------------------------------------------------------------------

def try_eval_signal_payload(config_path: str) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    dbg: Dict[str, Any] = {"ok": False, "method": None, "error": None}
    try:
        from engine import TradingEngine  # type: ignore

        e = TradingEngine(config_path)

        candidates = ["generate_signal", "build_signal", "run_once", "evaluate", "get_signal"]
        used = None
        out: Any = None

        for name in candidates:
            if hasattr(e, name) and callable(getattr(e, name)):
                used = name
                out = getattr(e, name)()
                break

        if used is None:
            dbg["error"] = "TradingEngine has no known eval method"
            return None, dbg

        dbg["ok"] = True
        dbg["method"] = used

        if isinstance(out, dict):
            d = out
        elif hasattr(out, "to_dict") and callable(getattr(out, "to_dict")):
            d = out.to_dict()
        elif hasattr(out, "__dict__"):
            d = dict(out.__dict__)
        else:
            d = {"repr": repr(out)}

        if not isinstance(d, dict):
            d = {"repr": repr(d)}

        dbg["top_keys"] = list(d.keys())[:80]
        return d, dbg

    except Exception as ex:
        dbg["error"] = f"{type(ex).__name__}: {ex}"
        dbg["trace"] = traceback.format_exc(limit=2)
        return None, dbg


def build_stable_signature(signal_dict: Dict[str, Any]) -> str:
    keep = [
        "symbol", "mode",
        "direction", "side", "signal", "decision", "action",
        "blocked_by", "gates", "reasons",
        "entry", "sl", "tp", "rr",
        "confidence",
    ]
    compact: Dict[str, Any] = {}
    for k in keep:
        if k in signal_dict:
            compact[k] = signal_dict[k]

    if "blocked_by" in compact and isinstance(compact["blocked_by"], (list, dict)):
        compact["blocked_by"] = json_dumps_compact(compact["blocked_by"])
    if "gates" in compact and isinstance(compact["gates"], (list, dict)):
        compact["gates"] = json_dumps_compact(compact["gates"])
    if "reasons" in compact and isinstance(compact["reasons"], (list, dict)):
        compact["reasons"] = json_dumps_compact(compact["reasons"])

    base = compact if compact else {"repr": repr(signal_dict)}
    return sha1_hex(json_dumps_compact(base))


def detect_events(
    symbol: str,
    tfs: List[str],
    st: EventState,
    config_path: str,
) -> Tuple[bool, List[str], Dict[str, Any]]:
    reasons: List[str] = []
    details: Dict[str, Any] = {"bars": {}, "signal": {}}

    # NEW_BAR
    new_bar = False
    for tf in tfs:
        bt = get_latest_bar_time(symbol, tf)
        details["bars"][tf] = bt
        if bt is None:
            continue
        prev = st.last_bar_time.get(tf)
        if prev is None:
            st.last_bar_time[tf] = bt
        elif bt != prev:
            st.last_bar_time[tf] = bt
            new_bar = True

    if new_bar:
        reasons.append("NEW_BAR")

    # SIGNAL_CHANGE + OBS (best-effort)
    sig_dict, sig_dbg = try_eval_signal_payload(config_path)
    obs = None
    signature = None

    if isinstance(sig_dict, dict):
        obs = build_observability(sig_dict)
        signature = build_stable_signature(sig_dict)

    details["signal"] = {
        "signature": signature,
        "dbg": sig_dbg,
        "obs": obs
    }

    if signature is not None:
        if st.last_signature is None:
            st.last_signature = signature
        elif signature != st.last_signature:
            st.last_signature = signature
            reasons.append("SIGNAL_CHANGE")

    return (len(reasons) > 0), reasons, details


# -----------------------------------------------------------------------------
# Executor invocation (one-shot)
# -----------------------------------------------------------------------------

def run_mentor_executor(py_exe: str, config_path: str) -> Tuple[int, str]:
    cmd = [py_exe, "mentor_executor.py"]
    env = os.environ.copy()
    env["HIM_CONFIG"] = config_path

    try:
        p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", env=env)
        out = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
        out = out.strip()
        if len(out) > 4000:
            out = out[-4000:]
        return int(p.returncode), out
    except Exception as e:
        return 99, f"executor_call_failed: {type(e).__name__}: {e}"


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> int:
    config_path = os.environ.get("HIM_CONFIG", "config.json")
    py_exe = sys.executable

    try:
        cfg = load_config(config_path)
    except Exception as e:
        print(f"[FATAL] load_config failed: {e}")
        return 2

    symbol = str(cfg.get("symbol", "GOLD"))
    commissioning_enabled = bool(cfg_get(cfg, ["commissioning", "enabled"], True))

    poll_sec = safe_float(cfg_get(cfg, ["commissioning", "poll_sec"], 1.0), 1.0) or 1.0
    events_log_path = str(cfg_get(cfg, ["commissioning", "events_log_path"], "logs/commissioning_events.jsonl"))
    tick_max_age = safe_float(
        cfg_get(cfg, ["execution", "tick_max_age_sec"], cfg.get("tick_max_age_sec", 15)),
        15
    ) or 15.0

    # TF for NEW_BAR:
    tfs_cfg = cfg_get(cfg, ["commissioning", "event_timeframes"], None)
    if isinstance(tfs_cfg, list) and tfs_cfg:
        tfs = [str(x).upper() for x in tfs_cfg]
    else:
        ltf = cfg_get(cfg, ["timeframes", "ltf"], "M5")
        tfs = [str(ltf).upper()]

    tfs = [tf for tf in tfs if tf in TF_MAP]
    if not tfs:
        tfs = ["M5"]

    state_path = str(cfg_get(cfg, ["commissioning", "state_path"], ".commission_event_state.json"))
    st = load_event_state(state_path)

    print(json_dumps_compact({
        "ts": utc_iso_now(),
        "msg": "commissioning_runner_start",
        "version": "v2.12.6",
        "config_path": config_path,
        "symbol": symbol,
        "enabled": commissioning_enabled,
        "poll_sec": poll_sec,
        "tick_max_age_sec": tick_max_age,
        "event_timeframes": tfs,
        "events_log_path": events_log_path,
        "state_path": state_path
    }))

    if not commissioning_enabled:
        print("[INFO] commissioning.enabled=false -> exit")
        return 0

    if not mt5.initialize():
        print("[FATAL] mt5.initialize() failed")
        return 3

    sym = mt5.symbol_info(symbol)
    if sym is None:
        print(f"[FATAL] symbol_info None: {symbol}")
        return 4
    if not sym.visible:
        mt5.symbol_select(symbol, True)

    try:
        while True:
            try:
                tick = get_tick_snapshot(symbol)
                if tick is None:
                    append_jsonl(events_log_path, {
                        "ts": utc_iso_now(),
                        "type": "HEARTBEAT",
                        "ok": False,
                        "reason": "NO_TICK",
                        "symbol": symbol
                    })
                    time.sleep(poll_sec)
                    continue

                if not tick_is_fresh(tick, tick_max_age):
                    append_jsonl(events_log_path, {
                        "ts": utc_iso_now(),
                        "type": "HEARTBEAT",
                        "ok": False,
                        "reason": "STALE_TICK",
                        "symbol": symbol,
                        "tick_age_sec": tick.tick_age_sec,
                        "tick_time": tick.tick_time,
                        "server_time": tick.server_time
                    })
                    time.sleep(poll_sec)
                    continue

                fire, reasons, details = detect_events(symbol, tfs, st, config_path)

                save_event_state(state_path, st)

                if not fire:
                    time.sleep(poll_sec)
                    continue

                append_jsonl(events_log_path, {
                    "ts": utc_iso_now(),
                    "type": "EVENT",
                    "symbol": symbol,
                    "reasons": reasons,
                    "tick": {
                        "bid": tick.bid,
                        "ask": tick.ask,
                        "last": tick.last,
                        "tick_time": tick.tick_time,
                        "server_time": tick.server_time,
                        "tick_age_sec": tick.tick_age_sec
                    },
                    "details": details
                })

                rc, out = run_mentor_executor(py_exe, config_path)
                append_jsonl(events_log_path, {
                    "ts": utc_iso_now(),
                    "type": "EXECUTOR_RESULT",
                    "symbol": symbol,
                    "returncode": rc,
                    "output_tail": out
                })

                time.sleep(poll_sec)

            except KeyboardInterrupt:
                print(json_dumps_compact({"ts": utc_iso_now(), "msg": "stopped_by_user"}))
                break
            except Exception as e:
                append_jsonl(events_log_path, {
                    "ts": utc_iso_now(),
                    "type": "ERROR",
                    "symbol": symbol,
                    "error": f"{type(e).__name__}: {e}",
                    "trace": traceback.format_exc(limit=3)
                })
                time.sleep(max(poll_sec, 1.0))

    finally:
        try:
            mt5.shutdown()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())