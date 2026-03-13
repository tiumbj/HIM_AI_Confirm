"""
Hybrid Intelligence Mentor (HIM)
File: tools/blocker_stats.py
Version: v1.0.0 (Blocked_by frequency + gate breakdown from commissioning_events.jsonl)
Date: 2026-03-02 (Asia/Bangkok)

PURPOSE
- Compute evidence-driven blocker statistics from logs/commissioning_events.jsonl
- Uses EXECUTOR_RESULT.output_tail (JSON string from mentor_executor) as source-of-truth per event

INPUT
- logs/commissioning_events.jsonl (JSONL)
  - We read only lines where type == "EXECUTOR_RESULT"
  - We parse output_tail as JSON (mentor_executor output)

OUTPUT
- Console report:
  - total samples
  - decision distribution
  - blocked_by frequency (%)
  - gate booleans frequency (if present)
  - top blocker combinations

SAFETY
- Read-only tool (no trading, no MT5 access required)
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


def load_jsonl(path: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                # skip corrupted lines
                continue
    return out


def safe_json_loads(s: str) -> Optional[Dict[str, Any]]:
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def normalize_blocked_by(x: Any) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(i).strip() for i in x if str(i).strip()]
    # sometimes stored as comma-separated string
    s = str(x).strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split(",")]
    # de-dup preserve order
    seen = set()
    out: List[str] = []
    for p in parts:
        if p and p not in seen:
            out.append(p)
            seen.add(p)
    return out


@dataclass
class Sample:
    ts: str
    version: str
    decision: Optional[str]
    blocked_by: List[str]
    gates: Dict[str, Any]


def extract_samples(rows: List[Dict[str, Any]]) -> List[Sample]:
    samples: List[Sample] = []
    for r in rows:
        if r.get("type") != "EXECUTOR_RESULT":
            continue
        tail = r.get("output_tail")
        if not isinstance(tail, str) or not tail.strip().startswith("{"):
            continue
        payload = safe_json_loads(tail.strip())
        if not payload:
            continue

        if payload.get("type") != "MENTOR_EXECUTOR":
            continue
        if payload.get("ok") is not True:
            continue

        version = str(payload.get("version", "unknown"))
        ts = str(payload.get("ts", r.get("ts", "")))

        decision = payload.get("decision")
        decision_s = None if decision is None else str(decision)

        blocked_by = normalize_blocked_by(payload.get("blocked_by"))

        # gates may be at payload.obs.gates (v2.13.2) or absent (older)
        gates: Dict[str, Any] = {}
        obs = payload.get("obs")
        if isinstance(obs, dict):
            g = obs.get("gates")
            if isinstance(g, dict):
                gates = g

        samples.append(Sample(
            ts=ts,
            version=version,
            decision=decision_s,
            blocked_by=blocked_by,
            gates=gates,
        ))
    return samples


def pct(a: int, b: int) -> float:
    if b <= 0:
        return 0.0
    return (a * 100.0) / b


def print_report(samples: List[Sample], top_k: int) -> None:
    n = len(samples)
    print(f"TOTAL_SAMPLES: {n}")
    if n == 0:
        print("No usable EXECUTOR_RESULT samples found. Ensure runner is writing mentor_executor JSON into output_tail.")
        return

    # Versions distribution
    ver = Counter(s.version for s in samples)
    print("\nVERSIONS:")
    for k, v in ver.most_common():
        print(f"  {k}: {v} ({pct(v, n):.1f}%)")

    # Decision distribution
    dec = Counter((s.decision or "NULL") for s in samples)
    print("\nDECISIONS:")
    for k, v in dec.most_common():
        print(f"  {k}: {v} ({pct(v, n):.1f}%)")

    # Blocked_by frequency
    bb = Counter()
    combo = Counter()
    for s in samples:
        if not s.blocked_by:
            bb["<none>"] += 1
            combo["<none>"] += 1
        else:
            for b in s.blocked_by:
                bb[b] += 1
            combo[" + ".join(s.blocked_by)] += 1

    print("\nBLOCKED_BY (frequency per sample):")
    for k, v in bb.most_common(top_k):
        print(f"  {k}: {v} ({pct(v, n):.1f}%)")

    print("\nTOP_BLOCKER_COMBOS:")
    for k, v in combo.most_common(top_k):
        print(f"  {k}: {v} ({pct(v, n):.1f}%)")

    # Gate booleans breakdown (if present)
    gate_keys = [
        "bias_ok",
        "supertrend_ok",
        "vol_expansion_ok",
        "bos_ok",
        "rr_ok",
        "retest_ok",
        "proximity_ok",
    ]
    gate_counts: Dict[str, Counter] = {g: Counter() for g in gate_keys}

    any_gate_data = False
    for s in samples:
        if not s.gates:
            continue
        any_gate_data = True
        for g in gate_keys:
            val = s.gates.get(g, None)
            if val is True:
                gate_counts[g]["true"] += 1
            elif val is False:
                gate_counts[g]["false"] += 1
            elif val is None:
                gate_counts[g]["null"] += 1
            else:
                gate_counts[g][f"other({type(val).__name__})"] += 1

    print("\nGATES (boolean breakdown):")
    if not any_gate_data:
        print("  No gate data found in samples (obs.gates missing).")
    else:
        for g in gate_keys:
            c = gate_counts[g]
            # show true/false/null if present
            parts = []
            for key in ["true", "false", "null"]:
                if c.get(key, 0) > 0:
                    parts.append(f"{key}={c[key]}({pct(c[key], n):.1f}%)")
            # include others if any
            other = [(k, v) for k, v in c.items() if k not in ("true", "false", "null")]
            if other:
                parts.append("other=" + ",".join([f"{k}:{v}" for k, v in other]))
            print(f"  {g}: " + (" | ".join(parts) if parts else "no_data"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default="logs/commissioning_events.jsonl", help="Path to commissioning_events.jsonl")
    ap.add_argument("--last", type=int, default=100, help="Use last N EXECUTOR_RESULT samples (after parsing)")
    ap.add_argument("--top", type=int, default=10, help="Top-K items to display")
    args = ap.parse_args()

    rows = load_jsonl(args.log)
    samples = extract_samples(rows)

    if args.last > 0 and len(samples) > args.last:
        samples = samples[-args.last:]

    print_report(samples, top_k=args.top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())