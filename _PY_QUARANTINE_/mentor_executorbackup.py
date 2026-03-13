# mentor_executor.py
# =============================================================================
# Hybrid Intelligence Mentor (HIM) - Mentor Executor (Confirm-only)
# Version: v2.13.3
# Changelog:
# - Pass-through context.metrics to obs.metrics for metric visibility
# - Add runtime.event_timeframe from config.commissioning.event_timeframes (if single)
# - Keep one-line JSON output, confirm-only, schema stable
# Evidence note:
# - Required to diagnose blockers on M1 (no_vol_expansion, supertrend_conflict)
#   by exposing score/threshold/margin + distance metrics.
# =============================================================================

from __future__ import annotations

import json
import sys
import time
from typing import Any, Dict, List, Optional

from engine import TradingEngine


VERSION = "v2.13.3"


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _normalize_blocked_by(s: Any) -> List[str]:
    if s is None:
        return []
    if isinstance(s, list):
        # already list[str]
        out = []
        for x in s:
            if x is None:
                continue
            t = str(x).strip()
            if t:
                out.append(t)
        return sorted(list(dict.fromkeys(out)))
    # string case
    txt = str(s).replace(";", ",")
    parts = [p.strip() for p in txt.split(",") if p.strip()]
    # dedup preserving order
    seen = set()
    out = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _get_event_timeframe_from_config(cfg: Dict[str, Any]) -> Optional[str]:
    commissioning = cfg.get("commissioning") or {}
    etf = commissioning.get("event_timeframes")
    if isinstance(etf, list) and len(etf) == 1:
        return str(etf[0])
    return None


def main() -> int:
    config_path = "config.json"
    try:
        raw_cfg = _load_json(config_path)
    except Exception as e:
        print(json.dumps({"version": VERSION, "error": f"config_load_failed: {type(e).__name__}: {e}"}), flush=True)
        return 2

    event_tf = _get_event_timeframe_from_config(raw_cfg)

    # Confirm-only: we compute and report; no trading actions here.
    try:
        engine = TradingEngine(config_path)
        pkg = engine.generate_signal_package()
    except Exception as e:
        out = {
            "version": VERSION,
            "ts": time.time(),
            "decision": "NONE",
            "error": f"engine_failed: {type(e).__name__}: {e}",
            "obs": {"runtime": {"event_timeframe": event_tf}},
        }
        print(json.dumps(out, ensure_ascii=False), flush=True)
        return 3

    ctx = pkg.get("context") or {}
    gates = (ctx.get("gates") or {}) if isinstance(ctx.get("gates"), dict) else {}
    metrics = (ctx.get("metrics") or {}) if isinstance(ctx.get("metrics"), dict) else {}

    blocked_list = _normalize_blocked_by(ctx.get("blocked_by"))

    # Decision policy (confirm-only output):
    # - If there are blockers => NONE
    # - Else => output direction as "BUY"/"SELL" but still no order placement here
    direction = str(pkg.get("direction") or "NONE").upper()
    decision = "NONE"
    if direction in ("BUY", "SELL") and len(blocked_list) == 0:
        decision = direction

    out = {
        "version": VERSION,
        "ts": time.time(),
        "symbol": pkg.get("symbol"),
        "decision": decision,  # confirm-only
        "plan": {
            "direction": direction,
            "entry": pkg.get("entry_candidate"),
            "sl": pkg.get("stop_candidate"),
            "tp": pkg.get("tp_candidate"),
            "rr": pkg.get("rr"),
        },
        "blocked_by": blocked_list,
        "obs": {
            "runtime": {"event_timeframe": event_tf},
            "engine_version": ctx.get("engine_version"),
            "trends": {
                "HTF": ctx.get("HTF_trend"),
                "MTF": ctx.get("MTF_trend"),
                "LTF": ctx.get("LTF_trend"),
            },
            "gates": {
                "bias_ok": gates.get("bias_ok"),
                "supertrend_ok": gates.get("supertrend_ok"),
                "vol_expansion_ok": gates.get("vol_expansion_ok"),
                "bos_ok": gates.get("bos_ok"),
                "bos_break_ok": gates.get("bos_break_ok"),
                "retest_ok": gates.get("retest_ok"),
                "proximity_ok": gates.get("proximity_ok"),
                "rr_ok": gates.get("rr_ok"),
            },
            # Metrics pass-through (bounded to metrics dict prepared by engine)
            "metrics": metrics,
        },
    }

    # One-line JSON output (machine readable)
    print(json.dumps(out, ensure_ascii=False, separators=(",", ":")), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())