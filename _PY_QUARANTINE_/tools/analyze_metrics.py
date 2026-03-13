# tools/analyze_metrics.py
# Version: v1.0
# Purpose: Analyze commissioning metrics distribution safely (Windows-friendly)

import json
import math
from pathlib import Path

LOG_PATH = "logs/commissioning_events.jsonl"
MAX_SAMPLES = 200


def pct(vals, k):
    vals = [v for v in vals if isinstance(v, (int, float)) and not math.isnan(v)]
    if not vals:
        return None
    vals = sorted(vals)
    idx = int(round((k / 100) * (len(vals) - 1)))
    return vals[idx]


def main():
    path = Path(LOG_PATH)
    if not path.exists():
        print("Log not found:", LOG_PATH)
        return

    lines = path.read_text(encoding="utf-8").splitlines()

    data = []

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

        obs = (d.get("obs") or {}).get("metrics") or {}

        data.append((
            obs.get("supertrend_distance_atr"),
            obs.get("bb_width_atr"),
            obs.get("rr")
        ))

        if len(data) >= MAX_SAMPLES:
            break

    print("Samples:", len(data))

    sd = [x[0] for x in data]
    bw = [x[1] for x in data]
    rr = [x[2] for x in data if isinstance(x[2], (int, float))]

    print("\nSupertrend Distance ATR percentiles")
    print("p50:", pct(sd, 50))
    print("p80:", pct(sd, 80))
    print("p90:", pct(sd, 90))

    print("\nBB Width ATR percentiles")
    print("p50:", pct(bw, 50))
    print("p80:", pct(bw, 80))
    print("p90:", pct(bw, 90))

    if rr:
        print("\nRR range:", min(rr), "→", max(rr))
    else:
        print("\nRR range: None")


if __name__ == "__main__":
    main()