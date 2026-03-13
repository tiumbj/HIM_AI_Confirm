"""
File: ai_bridge_v1.py
Path: C:\\Hybrid_Intelligence_Mentor\\ai_bridge_v1.py
Version: 1.2.0

Changelog:
- v1.2.0:
  - Use schema v1.0 end-to-end for AI confirm (top-level baseline fields).
  - Enforce confirm-only via validator_v1_0.validate_ai_response_v1_0 (fail-closed).
  - Provide helper: build_engine_order_from_signal_package() and confirm_via_api().
  - Keep AIConfirmDecision dataclass for backward compatibility (but produced from validated schema).

Design (TH):
- Bridge ต้องไม่ให้ AI สร้างสัญญาณเอง
- ส่ง baseline (direction/entry/sl/tp/lot/mode/atr) ไป /api/ai_confirm
- รับ response schema v1.0 แล้วเติม field ที่ API ไม่ส่ง (direction/lot/mode) จาก engine_order
- เรียก validator: ok=True เท่านั้นถึงถือว่า CONFIRM ได้
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Tuple

import requests

from validator_v1_0 import validate_ai_response_v1_0

LOG = logging.getLogger("ai_bridge_v1")


@dataclass
class AIConfirmDecision:
    final_confirm: bool
    side: str            # "BUY" / "SELL" / "NONE"
    entry: float
    sl: float
    tp: float
    confidence: float    # 0..100 (เพื่อความเข้ากันกับ output เดิมของคุณ)
    mentor_hint: str


def _load_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_engine_order_from_signal_package(
    pkg: Dict[str, Any],
    *,
    lot: float,
    mode: str,
) -> Dict[str, Any]:
    ctx = pkg.get("context", {}) if isinstance(pkg.get("context", {}), dict) else {}
    return {
        "direction": pkg.get("direction", "NONE"),
        "entry": pkg.get("entry_candidate"),
        "sl": pkg.get("stop_candidate"),
        "tp": pkg.get("tp_candidate"),
        "lot": lot,
        "mode": mode,
        "atr": ctx.get("atr"),
    }


def _post_ai_confirm(api_url: str, baseline: Dict[str, Any], timeout_sec: float) -> Dict[str, Any]:
    r = requests.post(api_url, json=baseline, timeout=timeout_sec)
    r.raise_for_status()
    return r.json()


def confirm_via_api(
    *,
    api_url: str,
    timeout_sec: float,
    engine_order: Dict[str, Any],
) -> Tuple[AIConfirmDecision, Dict[str, Any]]:
    """
    Returns:
      - AIConfirmDecision (legacy-friendly)
      - validated_payload (schema v1.0 normalized fields, good for executor logging)
    """
    # 1) Build baseline request (top-level fields) for API
    baseline_req = {
        "schema_version": "1.0",
        "direction": engine_order.get("direction"),
        "entry": engine_order.get("entry"),
        "sl": engine_order.get("sl"),
        "tp": engine_order.get("tp"),
        "lot": engine_order.get("lot"),
        "mode": engine_order.get("mode"),
        "atr": engine_order.get("atr"),
        "note": "baseline_from_engine",
    }

    # 2) Call API
    try:
        api_raw = _post_ai_confirm(api_url, baseline_req, timeout_sec)
    except Exception as e:
        # Fail-closed: return REJECT decision
        msg = f"ai exception: {e}"
        return (
            AIConfirmDecision(
                final_confirm=False,
                side="NONE",
                entry=0.0,
                sl=0.0,
                tp=0.0,
                confidence=0.0,
                mentor_hint=msg,
            ),
            {
                "schema_version": "1.0",
                "decision": "REJECT",
                "confidence": 0.0,
                "direction": engine_order.get("direction"),
                "lot": engine_order.get("lot"),
                "mode": engine_order.get("mode"),
                "entry": engine_order.get("entry"),
                "sl": engine_order.get("sl"),
                "tp": engine_order.get("tp"),
                "note": msg,
            },
        )

    # 3) Build ai_payload schema v1.0 (fill missing fields from engine_order)
    ai_payload = {
        "schema_version": api_raw.get("schema_version", "1.0"),
        "decision": api_raw.get("decision"),
        "confidence": api_raw.get("confidence", 0.0),
        "direction": engine_order.get("direction"),  # lock direction (confirm-only)
        "lot": engine_order.get("lot"),
        "mode": engine_order.get("mode"),
        "entry": api_raw.get("entry", engine_order.get("entry")),
        "sl": api_raw.get("sl", engine_order.get("sl")),
        "tp": api_raw.get("tp", engine_order.get("tp")),
        "note": api_raw.get("note"),
    }

    # 4) Validate (fail-closed)
    res = validate_ai_response_v1_0(ai_payload, engine_order)

    if not getattr(res, "ok", False):
        hint = f"validator_reject:{getattr(res,'errors',None)}|{getattr(res,'reasons',None)}"
        return (
            AIConfirmDecision(
                final_confirm=False,
                side="NONE",
                entry=0.0,
                sl=0.0,
                tp=0.0,
                confidence=0.0,
                mentor_hint=hint,
            ),
            ai_payload,
        )

    # 5) CONFIRM path
    decision = (ai_payload.get("decision") or "REJECT").upper()
    final_confirm = decision == "CONFIRM"

    # confidence to 0..100 for compatibility
    conf = ai_payload.get("confidence", 0.0)
    if isinstance(conf, (int, float)):
        conf_pct = float(conf) * 100.0 if conf <= 1.0 else float(conf)
    else:
        conf_pct = 0.0

    side = engine_order.get("direction", "NONE") if final_confirm else "NONE"

    return (
        AIConfirmDecision(
            final_confirm=final_confirm,
            side=str(side),
            entry=float(ai_payload.get("entry") or 0.0),
            sl=float(ai_payload.get("sl") or 0.0),
            tp=float(ai_payload.get("tp") or 0.0),
            confidence=conf_pct,
            mentor_hint=str(ai_payload.get("note") or ""),
        ),
        ai_payload,
    )


def main():
    import argparse
    from engine import TradingEngine

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--sample", action="store_true")
    args = ap.parse_args()

    cfg = _load_config(args.config)
    ai_cfg = cfg.get("ai", {})
    api_url = ai_cfg.get("api_url", "http://127.0.0.1:5000/api/ai_confirm")
    timeout_sec = float(ai_cfg.get("timeout_sec", 10))

    mode = cfg.get("mode", "sideway_scalp")
    lot = float(cfg.get("execution", {}).get("volume", 0.01))
    eng = TradingEngine(args.config)
    pkg = eng.generate_signal_package()
    engine_order = build_engine_order_from_signal_package(pkg, lot=lot, mode=mode)

    d, payload = confirm_via_api(api_url=api_url, timeout_sec=timeout_sec, engine_order=engine_order)
    print("AIConfirmDecision:", d)
    LOG.info("ai_payload=%s", payload)


if __name__ == "__main__":
    main()
