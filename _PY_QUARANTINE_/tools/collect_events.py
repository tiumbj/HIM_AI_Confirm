"""
Hybrid Intelligence Mentor (HIM)
File: tools/collect_events.py
Version: v1.0.1 (Robust JSON extraction from output_tail)
Date: 2026-03-02

FIX
- Extract JSON safely from output_tail even if trailing characters exist
"""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Dict, Optional


def safe_extract_json(s: str) -> Optional[Dict[str, Any]]:
    if not isinstance(s, str):
        return None

    start = s.find("{")
    end = s.rfind("}")

    if start == -1 or end == -1 or end <= start:
        return None

    try:
        return json.loads(s[start:end+1])
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default="logs/commissioning_events.jsonl")
    ap.add_argument("--out", default="logs/_samples_executor_results.jsonl")
    ap.add_argument("--n", type=int, default=100)
    ap.add_argument("--poll", type=float, default=0.5)
    ap.add_argument("--require-version", default="")
    args = ap.parse_args()

    if not os.path.exists(args.log):
        print("log not found")
        return 2

    # clear output
    with open(args.out, "w", encoding="utf-8") as f:
        pass

    collected = 0

    with open(args.log, "r", encoding="utf-8", errors="replace") as f:
        f.seek(0, os.SEEK_END)

        print(f"COLLECT_START log={args.log} target_n={args.n}")

        while collected < args.n:
            pos = f.tell()
            line = f.readline()

            if not line:
                f.seek(pos)
                time.sleep(args.poll)
                continue

            try:
                row = json.loads(line)
            except Exception:
                continue

            if row.get("type") != "EXECUTOR_RESULT":
                continue

            payload = safe_extract_json(row.get("output_tail", ""))
            if not payload:
                continue

            if payload.get("ok") is not True:
                continue

            if args.require_version:
                if payload.get("version") != args.require_version:
                    continue

            with open(args.out, "a", encoding="utf-8") as out:
                out.write(line)

            collected += 1
            print(f"[{collected}/{args.n}] {payload.get('version')} {payload.get('blocked_by')}")

        print(f"COLLECT_DONE collected={collected}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())