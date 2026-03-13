"""
Hybrid Intelligence Mentor (HIM)
Module: Strategy Router (Adaptive)
File: strategy_router.py
Version: 1.0.0
Date: 2026-03-01 (Asia/Bangkok)

CHANGELOG
- 1.0.0
  - Add adaptive regime->mode routing with hysteresis.
  - Single source-of-truth: select mode, then build effective config from profiles.
  - Transition state degrades to DRY_RUN (enable_execution=false) by default.

RATIONALE (ภาษาคน + ศัพท์เทคนิค)
- Hysteresis (ฮิสเทอรีซิส): ใช้ threshold 2 เส้น (เข้า trend / ออก trend) กันโหมดสลับไปมาถี่ (mode flapping)
- Transition: ช่วงก้ำกึ่ง (trend building) ให้ลดความเสี่ยง โดยบังคับ DRY_RUN
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class RouterPolicy:
    # Hysteresis thresholds
    trend_on_adx: float = 28.0     # เข้า TREND เมื่อ ADX >= 28 ต่อเนื่อง confirm_bars
    trend_off_adx: float = 22.0    # กลับ SIDEWAY เมื่อ ADX <= 22 ต่อเนื่อง confirm_bars
    confirm_bars: int = 3          # จำนวนแท่งยืนยัน (ใช้ state memory ข้ามรอบรัน)

    # Extra gating (optional)
    sideway_bb_width_atr_max: Optional[float] = None  # ถ้าตั้งค่า: SIDEWAY ต้อง bb_width_atr <= ค่านี้

    # Mode mapping
    mode_sideway: str = "sideway_scalp"
    mode_trend: str = "breakout"

    # Transition handling
    transition_degrade_to_dry_run: bool = True


@dataclass
class RouterState:
    last_regime: str = "TRANSITION"  # SIDEWAY | TREND | TRANSITION
    trend_on_count: int = 0
    trend_off_count: int = 0
    updated_ts: str = ""


def _deep_merge(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in (patch or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out.get(k, {}), v)
        else:
            out[k] = v
    return out


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def _extract_decision_fields(decision: Any) -> Tuple[str, float, float]:
    """
    Support both:
    - object with attrs: decision.regime, decision.adx, decision.bb_width_atr
    - dict-like
    """
    regime = ""
    adx = 0.0
    bbw = 0.0

    # attrs
    if hasattr(decision, "regime"):
        regime = str(getattr(decision, "regime", "")).upper()
        adx = _safe_float(getattr(decision, "adx", 0.0), 0.0)
        bbw = _safe_float(getattr(decision, "bb_width_atr", 0.0), 0.0)
        return regime, adx, bbw

    # dict
    if isinstance(decision, dict):
        regime = str(decision.get("regime", "")).upper()
        adx = _safe_float(decision.get("adx", 0.0), 0.0)
        bbw = _safe_float(decision.get("bb_width_atr", 0.0), 0.0)
        return regime, adx, bbw

    return regime, adx, bbw


def _load_state(path: str) -> RouterState:
    try:
        if not os.path.exists(path):
            return RouterState()
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f) or {}
        st = RouterState()
        st.last_regime = str(d.get("last_regime", st.last_regime)).upper()
        st.trend_on_count = int(d.get("trend_on_count", st.trend_on_count))
        st.trend_off_count = int(d.get("trend_off_count", st.trend_off_count))
        st.updated_ts = str(d.get("updated_ts", st.updated_ts))
        return st
    except Exception:
        return RouterState()


def _save_state(path: str, st: RouterState) -> None:
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "last_regime": st.last_regime,
                    "trend_on_count": st.trend_on_count,
                    "trend_off_count": st.trend_off_count,
                    "updated_ts": st.updated_ts,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except Exception:
        pass


def _policy_from_config(cfg: Dict[str, Any]) -> RouterPolicy:
    """
    Optional config block:
      "strategy_router": {
         "trend_on_adx": 28,
         "trend_off_adx": 22,
         "confirm_bars": 3,
         "sideway_bb_width_atr_max": 6.0,
         "mode_sideway": "sideway_scalp",
         "mode_trend": "breakout",
         "transition_degrade_to_dry_run": true
      }
    """
    p = RouterPolicy()
    blk = cfg.get("strategy_router", {})
    if isinstance(blk, dict):
        p = RouterPolicy(
            trend_on_adx=_safe_float(blk.get("trend_on_adx", p.trend_on_adx), p.trend_on_adx),
            trend_off_adx=_safe_float(blk.get("trend_off_adx", p.trend_off_adx), p.trend_off_adx),
            confirm_bars=max(1, int(blk.get("confirm_bars", p.confirm_bars))),
            sideway_bb_width_atr_max=(None if blk.get("sideway_bb_width_atr_max", None) is None else _safe_float(blk.get("sideway_bb_width_atr_max"), 0.0)),
            mode_sideway=str(blk.get("mode_sideway", p.mode_sideway)),
            mode_trend=str(blk.get("mode_trend", p.mode_trend)),
            transition_degrade_to_dry_run=bool(blk.get("transition_degrade_to_dry_run", p.transition_degrade_to_dry_run)),
        )
    return p


def decide_adaptive_regime(
    cfg: Dict[str, Any],
    decision: Any,
    *,
    state_path: str = ".state/strategy_router_state.json",
) -> Tuple[str, RouterPolicy, RouterState, Dict[str, Any]]:
    """
    Returns:
      adaptive_regime: SIDEWAY | TREND | TRANSITION
      policy, state, metrics
    """
    policy = _policy_from_config(cfg)
    st = _load_state(state_path)

    base_regime, adx, bbw = _extract_decision_fields(decision)

    # Optional: sideway bb width gating
    sideway_ok = True
    if policy.sideway_bb_width_atr_max is not None:
        sideway_ok = bbw <= float(policy.sideway_bb_width_atr_max)

    # Update hysteresis counters
    if adx >= policy.trend_on_adx:
        st.trend_on_count += 1
    else:
        st.trend_on_count = 0

    if adx <= policy.trend_off_adx:
        st.trend_off_count += 1
    else:
        st.trend_off_count = 0

    adaptive = "TRANSITION"

    # State machine with hysteresis
    if st.last_regime == "TREND":
        if st.trend_off_count >= policy.confirm_bars:
            adaptive = "SIDEWAY" if sideway_ok else "TRANSITION"
        else:
            adaptive = "TREND"
    elif st.last_regime == "SIDEWAY":
        if st.trend_on_count >= policy.confirm_bars:
            adaptive = "TREND"
        else:
            adaptive = "SIDEWAY" if sideway_ok else "TRANSITION"
    else:
        # TRANSITION -> choose by counts
        if st.trend_on_count >= policy.confirm_bars:
            adaptive = "TREND"
        elif st.trend_off_count >= policy.confirm_bars and sideway_ok:
            adaptive = "SIDEWAY"
        else:
            adaptive = "TRANSITION"

    st.last_regime = adaptive
    st.updated_ts = datetime.utcnow().isoformat() + "Z"
    _save_state(state_path, st)

    metrics = {
        "base_regime": base_regime,
        "adx": adx,
        "bb_width_atr": bbw,
        "trend_on_count": st.trend_on_count,
        "trend_off_count": st.trend_off_count,
        "trend_on_adx": policy.trend_on_adx,
        "trend_off_adx": policy.trend_off_adx,
        "confirm_bars": policy.confirm_bars,
        "sideway_ok": sideway_ok,
    }
    return adaptive, policy, st, metrics


def select_mode_for_regime(adaptive_regime: str, policy: RouterPolicy) -> str:
    r = str(adaptive_regime).upper()
    if r == "SIDEWAY":
        return policy.mode_sideway
    if r == "TREND":
        return policy.mode_trend
    return "transition"


def build_effective_config_adaptive(
    base_cfg: Dict[str, Any],
    decision: Any,
    *,
    state_path: str = ".state/strategy_router_state.json",
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Build effective config:
    - Choose adaptive regime with hysteresis
    - Select mode: SIDEWAY->sideway_scalp, TREND->breakout
    - Merge profile config if exists (profiles[mode])
    - Transition: degrade to DRY_RUN (enable_execution=false) by default
    Returns (effective_cfg, selection_info)
    """
    adaptive, policy, st, metrics = decide_adaptive_regime(base_cfg, decision, state_path=state_path)
    mode = select_mode_for_regime(adaptive, policy)

    eff = dict(base_cfg)

    # Merge selected profile if exists
    profiles = eff.get("profiles", {})
    if isinstance(profiles, dict) and mode in profiles and isinstance(profiles.get(mode), dict):
        eff = _deep_merge(eff, profiles.get(mode, {}))

    # Set selected mode for engine
    if mode != "transition":
        eff["mode"] = mode
    else:
        # keep mode as-is but mark transition
        eff["mode"] = str(eff.get("mode", "manual"))

    # Transition risk handling
    if adaptive == "TRANSITION" and policy.transition_degrade_to_dry_run:
        eff["enable_execution"] = False

    # Normalize execution mapping (profile.execution -> top-level execution)
    # so executor/riskguard can read cfg["execution"]
    if isinstance(eff.get("execution", {}), dict):
        execution = eff.get("execution", {}) or {}
    else:
        execution = {}
    prof_exec = None
    if isinstance(profiles, dict) and mode in profiles and isinstance(profiles.get(mode), dict):
        prof_exec = (profiles[mode].get("execution", None) if isinstance(profiles[mode], dict) else None)
    if isinstance(prof_exec, dict):
        execution = _deep_merge(execution, prof_exec)
    eff["execution"] = execution

    # Volume/lot mapping
    if eff.get("lot", None) is None:
        v = (execution.get("volume", None) if isinstance(execution, dict) else None)
        if v is not None:
            eff["lot"] = v

    info = {
        "adaptive_regime": adaptive,
        "mode_selected": (policy.mode_sideway if adaptive == "SIDEWAY" else policy.mode_trend if adaptive == "TREND" else "transition"),
        "mode_effective": eff.get("mode"),
        "transition_dry_run": bool(adaptive == "TRANSITION" and policy.transition_degrade_to_dry_run),
        "metrics": metrics,
    }
    return eff, info