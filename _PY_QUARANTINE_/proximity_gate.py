"""
Hybrid Intelligence Mentor (HIM)
Module: filters.proximity_gate
Version: v2.12.0
Date: 2026-03-01 (Asia/Bangkok)

Changelog:
- v2.12.0: Add Adaptive Proximity Gate based on MTF ADX override.
  Rule: If regime == TREND and mode == breakout and ADX_MTF > adx_proximity_override,
        then bypass proximity gate (only proximity gate; other gates still enforced).

Design Notes:
- This module is deterministic and side-effect free.
- Intended to be called inside engine breakout pipeline right before proximity gate enforcement.
- Does NOT bypass BOS / Supertrend / VolExpansion / RR floor / RiskGuard / Validator.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Regime(str, Enum):
    SIDEWAY = "SIDEWAY"
    TRANSITION = "TRANSITION"
    TREND = "TREND"


@dataclass(frozen=True)
class ProximityGatePolicy:
    """
    adx_proximity_override:
        - Use MTF ADX value (e.g., M15) for override decision
        - If ADX_MTF > threshold => bypass proximity gate
    """
    adx_proximity_override: float = 30.0

    # When True, require strict context match to allow bypass
    require_trend_and_breakout: bool = True


@dataclass(frozen=True)
class ProximityGateDecision:
    bypass_proximity_gate: bool
    reason: str
    adx_mtf: Optional[float]
    threshold: float
    regime: Optional[str]
    mode: Optional[str]


def decide_proximity_bypass(
    *,
    adx_mtf: Optional[float],
    regime: Optional[str],
    mode: Optional[str],
    policy: ProximityGatePolicy = ProximityGatePolicy(),
) -> ProximityGateDecision:
    """
    Decide whether to bypass proximity gate based on MTF ADX.

    Parameters
    ----------
    adx_mtf:
        MTF ADX value (float). If None => cannot bypass.
    regime:
        Expected values: "TREND", "SIDEWAY", "TRANSITION" (string to avoid tight coupling).
    mode:
        Expected value for bypass: "breakout".

    Returns
    -------
    ProximityGateDecision
        Contains bypass decision + audit fields for logs/telemetry.
    """

    # Fail-safe defaults (fail-closed): if we can't prove conditions, do NOT bypass.
    if adx_mtf is None:
        return ProximityGateDecision(
            bypass_proximity_gate=False,
            reason="no_adx_mtf",
            adx_mtf=None,
            threshold=policy.adx_proximity_override,
            regime=regime,
            mode=mode,
        )

    # Defensive: NaN/inf should not enable bypass
    if not (adx_mtf == adx_mtf) or adx_mtf == float("inf") or adx_mtf == float("-inf"):
        return ProximityGateDecision(
            bypass_proximity_gate=False,
            reason="invalid_adx_mtf",
            adx_mtf=adx_mtf,
            threshold=policy.adx_proximity_override,
            regime=regime,
            mode=mode,
        )

    if policy.require_trend_and_breakout:
        if (regime or "").upper() != Regime.TREND.value:
            return ProximityGateDecision(
                bypass_proximity_gate=False,
                reason="not_trend_regime",
                adx_mtf=adx_mtf,
                threshold=policy.adx_proximity_override,
                regime=regime,
                mode=mode,
            )
        if (mode or "").lower() != "breakout":
            return ProximityGateDecision(
                bypass_proximity_gate=False,
                reason="not_breakout_mode",
                adx_mtf=adx_mtf,
                threshold=policy.adx_proximity_override,
                regime=regime,
                mode=mode,
            )

    if adx_mtf > policy.adx_proximity_override:
        return ProximityGateDecision(
            bypass_proximity_gate=True,
            reason="adx_mtf_above_threshold",
            adx_mtf=adx_mtf,
            threshold=policy.adx_proximity_override,
            regime=regime,
            mode=mode,
        )

    return ProximityGateDecision(
        bypass_proximity_gate=False,
        reason="adx_mtf_not_above_threshold",
        adx_mtf=adx_mtf,
        threshold=policy.adx_proximity_override,
        regime=regime,
        mode=mode,
    )