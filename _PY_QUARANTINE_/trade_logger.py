"""
Trade Logger - Append-only JSON event log (Production Safe)
Version: 1.0.1

Changelog:
- 1.0.1 (2026-02-26)
  - FIX: Hard guard to prevent writing into config.json (catastrophic overwrite).
  - ADD: Default log path = logs/trade_log.json (never config.json)
  - KEEP: Atomic write; list of dict events.

Design:
- Accept cfg dict.
- Determine log_path from cfg["execution"]["trade_log_path"] if present.
- Fail-closed if resolved path basename == "config.json".
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List


def _atomic_write_json(path: str, data: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _safe_get(d: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


class TradeLogger:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        base_dir = os.path.dirname(os.path.abspath(__file__))

        # Configurable path (recommended)
        cfg_path = _safe_get(cfg, ["execution", "trade_log_path"], None)

        if isinstance(cfg_path, str) and cfg_path.strip():
            # allow relative or absolute
            path = cfg_path.strip()
            if not os.path.isabs(path):
                path = os.path.join(base_dir, path)
        else:
            path = os.path.join(base_dir, "logs", "trade_log.json")

        # HARD GUARD: never allow writing to config.json
        if os.path.basename(path).lower() == "config.json":
            raise RuntimeError("CRITICAL: TradeLogger log_path resolved to config.json (refuse to overwrite)")

        self.path = path

        # Ensure file exists as list
        if not os.path.exists(self.path):
            _atomic_write_json(self.path, [])

    def append(self, event: Dict[str, Any]) -> None:
        if not isinstance(event, dict):
            raise ValueError("event must be dict")

        # Minimal enrichment
        event = dict(event)
        event.setdefault("ts_iso", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))

        # Load current log list
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, list):
                # fail closed: don't overwrite unknown structure
                raise RuntimeError("trade log file is not a list; refuse to overwrite")
        except FileNotFoundError:
            data = []
        except Exception:
            # fail closed
            raise

        data.append(event)
        _atomic_write_json(self.path, data)