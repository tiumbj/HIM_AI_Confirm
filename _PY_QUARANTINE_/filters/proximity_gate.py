from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ProximityGatePolicy:
    adx_proximity_override: float = 30.0


@dataclass(frozen=True)
class ProximityBypassDecision:
    bypass_proximity_gate: bool
    reason: str


def decide_proximity_bypass(
    *,
    adx_mtf: Optional[float],
    regime: str,
    mode: str,
    policy: ProximityGatePolicy,
) -> ProximityBypassDecision:
    if adx_mtf is None:
        return ProximityBypassDecision(False, "no_adx_mtf")
    if not isinstance(adx_mtf, (int, float)) or math.isnan(float(adx_mtf)):
        return ProximityBypassDecision(False, "invalid_adx_mtf")

    if str(regime).upper() != "TREND":
        return ProximityBypassDecision(False, "not_trend_regime")

    if str(mode).lower() != "breakout":
        return ProximityBypassDecision(False, "not_breakout_mode")

    thr = float(policy.adx_proximity_override)
    if float(adx_mtf) > thr:
        return ProximityBypassDecision(True, "adx_mtf_above_threshold")
    return ProximityBypassDecision(False, "adx_mtf_not_above_threshold")

