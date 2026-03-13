# tools/analyze_bos.py
# Version: v1.0 (Production-safe, Windows-safe)
# Purpose: Analyze BOS break metrics distribution & pass-rate vs thresholds
# Usage:
#   python -u tools/analyze_bos.py --n 200 --engine v2.12.8

import argparse
import json
import math
from pathlib import Path

LOG_PATH = "logs/commissioning_events.jsonl"

THRESHOLDS = [0.40, 0.30, 0.25, 0.20, 0.15, 0.10]


def pct(vals, k):
    vals = [v for v in vals if isinstance(v, (int, float)) and not math.isnan(v)]
    if not vals:
        return None
    vals = sorted(vals)
    idx = int(round((k / 100) * (len(vals) - 1)))
    return vals[idx]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=200, help="max matched samples (newest backward)")
    ap.add_argument("--engine", type=str, default="", help="filter by obs.engine_version (e.g. v2.12.8). empty=all")
    args = ap.parse_args()

    path = Path(LOG_PATH)
    if not path.exists():
        print("Log not found:", LOG_PATH)
        return

    lines = path.read_text(encoding="utf-8").splitlines()

    up = []
    dn = []
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

        if args.engine:
            eng = ((d.get("obs") or {}).get("engine_version") or "")
            if eng != args.engine:
                continue

        metrics = ((d.get("obs") or {}).get("metrics") or {})
        bu = metrics.get("bos_break_up_atr")
        bd = metrics.get("bos_break_dn_atr")

        if isinstance(bu, (int, float)) and not math.isnan(bu):
            up.append(float(bu))
        if isinstance(bd, (int, float)) and not math.isnan(bd):
            dn.append(float(bd))

        matched += 1
        if matched >= args.n:
            break

    print("Scanned EXECUTOR_RESULT:", scanned)
    print("Matched samples:", matched)
    print("bos_break_up_atr samples:", len(up))
    print("bos_break_dn_atr samples:", len(dn))

    # Percentiles
    print("\nBOS break UP ATR percentiles")
    print("p50:", pct(up, 50))
    print("p80:", pct(up, 80))
    print("p90:", pct(up, 90))

    print("\nBOS break DN ATR percentiles")
    print("p50:", pct(dn, 50))
    print("p80:", pct(dn, 80))
    print("p90:", pct(dn, 90))

    # Pass rates by threshold:
    # BOS ok if (up >= thr) OR (dn >= thr)
    print("\nPASS RATE by threshold (up>=thr OR dn>=thr)")
    if matched == 0:
        print("No matched samples")
        return

    # Align lengths by using per-sample availability:
    # If either metric is missing for a sample, treat missing as -inf (fail side).
    # We approximate by using lists collected; since we append only numeric, this undercounts missing.
    # Therefore compute pass-rate from decoded samples again (more accurate).
    pass_counts = {thr: 0 for thr in THRESHOLDS}
    total = 0

    # re-scan matched samples accurately
    total = 0
    matched2 = 0
    for ln in reversed(lines):
        if '"type":"EXECUTOR_RESULT"' not in ln:
            continue

        j = json.loads(ln)
        ot = j.get("output_tail", "")
        if not ot:
            continue

        try:
            d = json.loads(ot)
        except Exception:
            continue

        if args.engine:
            eng = ((d.get("obs") or {}).get("engine_version") or "")
            if eng != args.engine:
                continue

        metrics = ((d.get("obs") or {}).get("metrics") or {})
        bu = metrics.get("bos_break_up_atr")
        bd = metrics.get("bos_break_dn_atr")

        buv = float(bu) if isinstance(bu, (int, float)) and not math.isnan(bu) else float("-inf")
        bdv = float(bd) if isinstance(bd, (int, float)) and not math.isnan(bd) else float("-inf")

        total += 1
        for thr in THRESHOLDS:
            if (buv >= thr) or (bdv >= thr):
                pass_counts[thr] += 1

        matched2 += 1
        if matched2 >= matched:
            break

    for thr in THRESHOLDS:
        rate = (pass_counts[thr] / total * 100.0) if total else 0.0
        print(f"thr={thr:.2f}: pass={pass_counts[thr]}/{total} ({rate:.1f}%)")


if __name__ == "__main__":
    main()