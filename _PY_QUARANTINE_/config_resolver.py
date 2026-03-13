"""
HIM Config Resolver
Version: 1.0.1

Purpose:
- Create "effective config" by deep-merging: base(root) + profiles[mode] overrides
- Enforce consistent runtime behavior across engine/executor/dashboard/etc.

Rules:
- If mode missing => use base config only
- If profiles[mode] missing => use base config only
- Deep-merge dicts, overwrite scalars/lists
- Profile key lookup is case-insensitive (fix: mode upper vs profiles lower)

Changelog:
- 1.0.1 (2026-03-01): Fix case-insensitive lookup for profiles keys.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = deepcopy(v)
    return out


def _get_profile_case_insensitive(profiles: Dict[str, Any], mode_upper: str) -> Dict[str, Any] | None:
    if not isinstance(profiles, dict):
        return None
    # direct hit
    prof = profiles.get(mode_upper)
    if isinstance(prof, dict):
        return prof
    # try lower
    prof = profiles.get(mode_upper.lower())
    if isinstance(prof, dict):
        return prof
    # try exact case-insensitive scan
    for k, v in profiles.items():
        if isinstance(k, str) and k.strip().upper() == mode_upper and isinstance(v, dict):
            return v
    return None


def resolve_effective_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(cfg, dict):
        return {}

    mode_upper = str(cfg.get("mode", "")).strip().upper()
    profiles = cfg.get("profiles", {}) if isinstance(cfg.get("profiles", {}), dict) else {}

    base = deepcopy(cfg)
    base.pop("profiles", None)

    if not mode_upper:
        return base

    prof = _get_profile_case_insensitive(profiles, mode_upper)
    if not isinstance(prof, dict):
        # Keep base.mode as-is if present
        return base

    effective = _deep_merge(base, prof)
    effective["mode"] = mode_upper
    return effective


def summarize_effective_config(effective: Dict[str, Any]) -> Dict[str, Any]:
    tf = effective.get("timeframes", {}) or {}
    risk = effective.get("risk", {}) or {}
    return {
        "mode": effective.get("mode"),
        "symbol": effective.get("symbol"),
        "enable_execution": effective.get("enable_execution"),
        "confidence_threshold": effective.get("confidence_threshold"),
        "min_score": effective.get("min_score"),
        "min_rr": effective.get("min_rr"),
        "lot": effective.get("lot"),
        "timeframes": {"htf": tf.get("htf"), "mtf": tf.get("mtf"), "ltf": tf.get("ltf")},
        "atr_period": risk.get("atr_period"),
        "atr_sl_mult": risk.get("atr_sl_mult"),
    }