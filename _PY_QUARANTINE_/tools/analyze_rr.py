# tools/analyze_rr.py
# Version: v1.1 (Production-safe, filtered)
# Purpose: Analyze RR distribution from commissioning log (Windows-safe)
# Usage:
#   python -u tools/analyze_rr.py --n 200 --engine v2.12.8

import argparse
import json
import math
from pathlib import Path

LOG_PATH = "logs/commissioning_events.jsonl"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=200, help="max samples to scan (from newest backward)")
    ap.add_argument("--engine", type=str, default="", help="filter by engine_version (e.g. v2.12.8). empty=all")
    args = ap.parse_args()

    path = Path(LOG_PATH)
    if not path.exists():
        print("Log not found:", LOG_PATH)
        return

    lines = path.read_text(encoding="utf-8").splitlines()

    rr = []
    scanned = 0
    matched = 0

    for ln in reversed(lines):
        if '"type":"EXECUTOR_RESULT"' not in ln:
            continue

        scanned += 1
        j = json.loads(ln)
        ot = j.get("output_tail", "")
        if not ot:
            continue

        try:
            d = json.loads(ot)
        except Exception:
            continue

        # filter by engine version (inside obs.engine_version)
        if args.engine:
            eng = ((d.get("obs") or {}).get("engine_version") or "")
            if eng != args.engine:
                continue

        plan = d.get("plan") or {}
        v = plan.get("rr")

        if isinstance(v, (int, float)) and not math.isnan(v):
            rr.append(float(v))

        matched += 1
        if matched >= args.n:
            break

    print("Scanned EXECUTOR_RESULT:", scanned)
    print("Matched samples:", matched)
    print("RR samples:", len(rr))
    if rr:
        print("RR min:", min(rr))
        print("RR max:", max(rr))
        # show simple counts
        zeros = sum(1 for x in rr if abs(x) < 1e-12)
        print("RR zeros:", zeros)
    else:
        print("No RR values found")

if __name__ == "__main__":
    main()