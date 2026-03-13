# ============================================================
# ANALYZER v1.0.1
# File: analyzer_v1_0.py
# Path: C:\Hybrid_Intelligence_Mentor\analyzer_v1_0.py
#
# CHANGELOG
# - v1.0.1:
#   * FIX: add die() function
#   * deterministic error handling
#   * stable trace file validation
# ============================================================

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


# -----------------------------
# Core Utility
# -----------------------------
def die(msg: str) -> None:
    print(f"[FATAL] {msg}")
    sys.exit(2)


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                die(f"Invalid JSON at {path} line {ln}")
    return rows


def to_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def epoch_to_str(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return ""


def fmt_pct(a: int, b: int) -> str:
    if b == 0:
        return "0.0%"
    return f"{(a/b)*100:.1f}%"


def write_csv(path: Path, header: List[str], rows: List[List[Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)


# -----------------------------
# Candidate Analysis
# -----------------------------
def analyze_candidates(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(rows)
    dir_counter = Counter()
    rr_vals = []
    conf_vals = []

    parsed_rows = []

    for r in rows:
        direction = r.get("direction", "NONE")
        dir_counter[direction] += 1

        rr = to_float(r.get("rr"))
        confidence = to_float(r.get("confidence"))

        if rr is not None:
            rr_vals.append(rr)
        if confidence is not None:
            conf_vals.append(confidence)

        parsed_rows.append([
            epoch_to_str(r.get("time")),
            direction,
            r.get("entry"),
            r.get("sl"),
            r.get("tp"),
            rr,
            confidence
        ])

    summary = {
        "total_candidates": total,
        "by_direction": dict(dir_counter),
        "buy_pct": fmt_pct(dir_counter.get("BUY", 0), total),
        "sell_pct": fmt_pct(dir_counter.get("SELL", 0), total),
        "rr_mean": sum(rr_vals)/len(rr_vals) if rr_vals else None,
        "confidence_mean": sum(conf_vals)/len(conf_vals) if conf_vals else None
    }

    return summary, parsed_rows


# -----------------------------
# Trace Analysis
# -----------------------------
def analyze_trace(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(rows)
    blocked_counter = Counter()

    for r in rows:
        ctx = r.get("context", {})
        blocked = ctx.get("blocked_by", "")
        blocked_counter[str(blocked)] += 1

    summary = {
        "total_bars": total,
        "blocked_by_distribution": {
            k: {"count": v, "pct": fmt_pct(v, total)}
            for k, v in blocked_counter.most_common()
        }
    }

    return summary


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--candidates", required=True)
    ap.add_argument("--trace", default="")
    ap.add_argument("--outdir", default="analysis_out")
    args = ap.parse_args()

    candidates_path = Path(args.candidates)
    if not candidates_path.exists():
        die(f"Candidates file not found: {candidates_path}")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    cand_rows = read_jsonl(candidates_path)
    cand_summary, cand_csv_rows = analyze_candidates(cand_rows)

    write_csv(
        outdir / "candidates_stats.csv",
        ["time_utc", "direction", "entry", "sl", "tp", "rr", "confidence"],
        cand_csv_rows
    )

    trace_summary = None
    if args.trace:
        trace_path = Path(args.trace)
        if not trace_path.exists():
            die(f"Trace file not found: {trace_path}")

        trace_rows = read_jsonl(trace_path)
        trace_summary = analyze_trace(trace_rows)

        write_csv(
            outdir / "blocked_by.csv",
            ["blocked_by", "count", "pct"],
            [[k, v["count"], v["pct"]] for k, v in trace_summary["blocked_by_distribution"].items()]
        )

    report = {
        "candidates_summary": cand_summary,
        "trace_summary": trace_summary
    }

    with (outdir / "summary.json").open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print("============================================================")
    print("ANALYZER v1.0.1 DONE")
    print(f"Output directory: {outdir.resolve()}")
    print("============================================================")


if __name__ == "__main__":
    main()