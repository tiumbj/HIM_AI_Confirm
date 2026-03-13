"""
Hybrid Intelligence Mentor (HIM)
Tests: Adaptive Proximity Gate (MTF ADX Override)
Version: v2.12.0
"""

from filters.proximity_gate import decide_proximity_bypass, ProximityGatePolicy


def test_bypass_when_trend_breakout_and_adx_above_threshold():
    policy = ProximityGatePolicy(adx_proximity_override=30.0)
    d = decide_proximity_bypass(adx_mtf=32.0, regime="TREND", mode="breakout", policy=policy)
    assert d.bypass_proximity_gate is True
    assert d.reason == "adx_mtf_above_threshold"


def test_no_bypass_when_adx_equal_threshold():
    policy = ProximityGatePolicy(adx_proximity_override=30.0)
    d = decide_proximity_bypass(adx_mtf=30.0, regime="TREND", mode="breakout", policy=policy)
    assert d.bypass_proximity_gate is False
    assert d.reason == "adx_mtf_not_above_threshold"


def test_no_bypass_when_adx_below_threshold():
    policy = ProximityGatePolicy(adx_proximity_override=30.0)
    d = decide_proximity_bypass(adx_mtf=25.0, regime="TREND", mode="breakout", policy=policy)
    assert d.bypass_proximity_gate is False
    assert d.reason == "adx_mtf_not_above_threshold"


def test_no_bypass_when_not_trend_regime():
    policy = ProximityGatePolicy(adx_proximity_override=30.0)
    d = decide_proximity_bypass(adx_mtf=40.0, regime="SIDEWAY", mode="breakout", policy=policy)
    assert d.bypass_proximity_gate is False
    assert d.reason == "not_trend_regime"


def test_no_bypass_when_not_breakout_mode():
    policy = ProximityGatePolicy(adx_proximity_override=30.0)
    d = decide_proximity_bypass(adx_mtf=40.0, regime="TREND", mode="sideway_scalp", policy=policy)
    assert d.bypass_proximity_gate is False
    assert d.reason == "not_breakout_mode"


def test_fail_closed_when_adx_none():
    policy = ProximityGatePolicy(adx_proximity_override=30.0)
    d = decide_proximity_bypass(adx_mtf=None, regime="TREND", mode="breakout", policy=policy)
    assert d.bypass_proximity_gate is False
    assert d.reason == "no_adx_mtf"


def test_fail_closed_when_adx_nan():
    policy = ProximityGatePolicy(adx_proximity_override=30.0)
    d = decide_proximity_bypass(adx_mtf=float("nan"), regime="TREND", mode="breakout", policy=policy)
    assert d.bypass_proximity_gate is False
    assert d.reason == "invalid_adx_mtf"