"""
HIM
File: mentor_executor_json.py
Version: v2.10.2-j1 (JSON Wrapper for mentor_executor.py)
Date: 2026-03-02 (Asia/Bangkok)

Purpose
- Run mentor_executor.py as a subprocess (no coupling to internal code).
- Capture stdout/stderr text.
- Extract key metrics with robust regex (best-effort).
- Emit exactly ONE JSON line to stdout (machine-readable).

Why wrapper instead of editing mentor_executor.py directly?
- Avoid breaking a working orchestrator.
- Provide stable interface for commissioning_runner / dashboard / analytics.
"""

from __future__ import annotations

import os
import re
import sys
import json
import time
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple


def utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def run_executor(py_exe: str) -> Tuple[int, str]:
    env = os.environ.copy()
    cmd = [py_exe, "mentor_executor.py"]
    p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", env=env)
    out = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
    return int(p.returncode), out.strip()


def first_float(pattern: str, text: str) -> Optional[float]:
    m = re.search(pattern, text)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def first_str(pattern: str, text: str) -> Optional[str]:
    m = re.search(pattern, text)
    if not m:
        return None
    return m.group(1)


def parse_metrics(text: str) -> Dict[str, Any]:
    """
    Best-effort regex extraction from current logs like:
      ROUTER | adaptive_regime=TREND | mode_selected=breakout | ... | adx=29.89 bbw=4.456 ...
      DEBUG_SKIP | ... | blocked_by=supertrend_conflict,supertrend_conflict
    """
    reg = first_str(r"adaptive_regime=([A-Z_]+)", text)
    mode = first_str(r"mode_selected=([A-Za-z0-9_]+)", text)
    adx = first_float(r"adx=([0-9]+(?:\.[0-9]+)?)", text)
    bbw = first_float(r"bbw=([0-9]+(?:\.[0-9]+)?)", text)
    blocked_by = first_str(r"blocked_by=([A-Za-z0-9_,]+)", text)

    live = first_str(r"\bLIVE=([A-Za-z]+)\b", text)
    lot = first_float(r"\blot=([0-9]+(?:\.[0-9]+)?)\b", text)
    symbol = first_str(r"\bsymbol=([A-Za-z0-9_]+)\b", text)

    return {
        "adaptive_regime": reg,
        "mode_selected": mode,
        "adx": adx,
        "bbw": bbw,
        "blocked_by": blocked_by,
        "live": (live.lower() == "true") if isinstance(live, str) else None,
        "lot": lot,
        "symbol": symbol,
    }


def main() -> int:
    py_exe = sys.executable

    t0 = time.time()
    rc, txt = run_executor(py_exe)
    dt_ms = int((time.time() - t0) * 1000)

    payload: Dict[str, Any] = {
        "ts": utc_iso_now(),
        "type": "EXECUTOR_JSON",
        "ok": (rc == 0),
        "returncode": rc,
        "duration_ms": dt_ms,
        "metrics": parse_metrics(txt),
        "raw_tail": (txt[-4000:] if len(txt) > 4000 else txt),
        "version": "v2.10.2-j1",
    }

    # Print ONE JSON line (machine readable)
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    return 0 if rc == 0 else 10


if __name__ == "__main__":
    raise SystemExit(main())