"""
File: validator_integration_test.py
Path: C:\\Hybrid_Intelligence_Mentor\\validator_integration_test.py
Version: 1.0.0

Purpose:
Test engine package + validator_v1_0 schema integration deterministically.
"""

import json
from engine import TradingEngine
from validator_v1_0 import validate_ai_response_v1_0

CONFIG_PATH = r".state\effective_config.regime.json"


def main():
    print("=== LOAD CONFIG ===")
    cfg = json.load(open(CONFIG_PATH, "r", encoding="utf-8"))

    print("=== GENERATE ENGINE PACKAGE ===")
    eng = TradingEngine(CONFIG_PATH)
    pkg = eng.generate_signal_package()
    ctx = pkg.get("context", {})

    direction = pkg.get("direction")
    entry = pkg.get("entry_candidate")
    sl = pkg.get("stop_candidate")
    tp = pkg.get("tp_candidate")
    atr = ctx.get("atr")
    mode = ctx.get("mode") or cfg.get("mode")

    # Volume resolution
    profiles = cfg.get("profiles", {})
    vol = None
    if mode in profiles:
        vol = profiles.get(mode, {}).get("execution", {}).get("volume")

    if vol is None:
        vol = cfg.get("execution", {}).get("volume", 0.01)

    engine_order = {
        "direction": direction,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "lot": vol,
        "mode": mode,
        "atr": atr,
    }

    ai_payload = {
        "schema_version": "1.0",
        "decision": "CONFIRM",
        "confidence": 0.81,
        "direction": direction,
        "lot": vol,
        "mode": mode,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "note": "local schema test",
    }

    print("\n=== ENGINE ORDER ===")
    print(engine_order)

    print("\n=== AI PAYLOAD ===")
    print(ai_payload)

    print("\n=== VALIDATOR RESULT ===")
    result = validate_ai_response_v1_0(ai_payload, engine_order)
    print(result)


if __name__ == "__main__":
    main()