# ============================================================
#  watchdog_supervisor.py — HIM Auto Crash Recovery Supervisor
#  Version: v1.0.0
#  Created: 2026-03-05
#
#  Strategy Header (Project Rule):
#   - Purpose: Run + monitor critical HIM processes and auto-restart on crash.
#   - Production Model: No changes to trading core; supervisor only manages processes.
#   - Safety: Respects KILL_SWITCH.txt to hard-stop trading (forces DRY_RUN=1).
#
#  Managed processes:
#   1) python api_server.py
#   2) python mentor_executor.py   (forced DRY_RUN=1 if KILL_SWITCH exists)
#   3) streamlit run intelligent_dashboard.py --server.port <port>
#
#  Changelog:
#   - v1.0.0:
#       * Auto-restart with exponential backoff
#       * Kill-switch integration (forces DRY_RUN=1 on mentor_executor)
#       * Writes logs/watchdog_supervisor.jsonl
# ============================================================

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Dict, Optional, List, Any, Tuple

APP_VERSION = "v1.0.0"

PROJECT_ROOT = os.getcwd()
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
WATCHDOG_LOG = os.path.join(LOG_DIR, "watchdog_supervisor.jsonl")

KILL_SWITCH_PATH = os.path.join(PROJECT_ROOT, "KILL_SWITCH.txt")
CONFIG_PATH = os.environ.get("HIM_CONFIG_PATH", "config.json").strip() or "config.json"


def _ensure_logs_dir() -> None:
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
    except Exception:
        pass


def _append_jsonl(path: str, obj: Dict[str, Any]) -> None:
    _ensure_logs_dir()
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _ts() -> int:
    return int(time.time())


def _load_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _kill_switch_active() -> Tuple[bool, str]:
    if not os.path.exists(KILL_SWITCH_PATH):
        return False, ""
    try:
        with open(KILL_SWITCH_PATH, "r", encoding="utf-8", errors="replace") as f:
            note = f.read().strip()
        return True, note[:500]
    except Exception:
        return True, "KILL_SWITCH present"


@dataclass
class ManagedProc:
    name: str
    cmd: List[str]
    cwd: str
    env: Dict[str, str]
    popen: Optional[subprocess.Popen] = None
    restart_count: int = 0
    next_restart_ts: int = 0


def _make_env(base: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    env = dict(os.environ)
    if base:
        env.update(base)
    return env


def _start_process(p: ManagedProc) -> None:
    # NOTE: Using CREATE_NEW_PROCESS_GROUP improves Ctrl+C handling on Windows
    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]

    p.popen = subprocess.Popen(
        p.cmd,
        cwd=p.cwd,
        env=p.env,
        stdout=None,   # inherit console
        stderr=None,   # inherit console
        creationflags=creationflags,
    )
    p.restart_count += 1
    _append_jsonl(WATCHDOG_LOG, {
        "ts": _ts(),
        "event": "process_start",
        "version": APP_VERSION,
        "name": p.name,
        "cmd": p.cmd,
        "pid": p.popen.pid if p.popen else None,
        "restart_count": p.restart_count,
    })


def _stop_process(p: ManagedProc, reason: str) -> None:
    if not p.popen:
        return
    try:
        _append_jsonl(WATCHDOG_LOG, {
            "ts": _ts(),
            "event": "process_stop",
            "version": APP_VERSION,
            "name": p.name,
            "pid": p.popen.pid,
            "reason": reason,
        })

        if os.name == "nt":
            # Try gentle signal then kill
            try:
                p.popen.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
                time.sleep(1.0)
            except Exception:
                pass

        p.popen.terminate()
        try:
            p.popen.wait(timeout=3.0)
        except Exception:
            p.popen.kill()
    except Exception:
        pass
    finally:
        p.popen = None


def _compute_backoff_sec(restart_count: int) -> int:
    # Exponential backoff with cap
    # 1st restart: 1s, then 2,4,8,16,30...
    seq = [1, 2, 4, 8, 16, 30, 30, 30]
    idx = min(max(restart_count - 1, 0), len(seq) - 1)
    return seq[idx]


def main() -> int:
    cfg = _load_config()

    # Dashboard port (avoid 8501 collision)
    dash_port = int(cfg.get("dashboard", {}).get("port", 8502)) if isinstance(cfg.get("dashboard", {}), dict) else 8502
    dash_host = str(cfg.get("dashboard", {}).get("host", "127.0.0.1")) if isinstance(cfg.get("dashboard", {}), dict) else "127.0.0.1"

    # API base used by dashboard
    api_host = str(cfg.get("api", {}).get("host", "127.0.0.1")) if isinstance(cfg.get("api", {}), dict) else "127.0.0.1"
    api_port = int(cfg.get("api", {}).get("port", 5000)) if isinstance(cfg.get("api", {}), dict) else 5000
    api_base = f"http://{api_host}:{api_port}"

    # Default DRY_RUN for mentor (can override by env)
    default_dry_run = str(cfg.get("commissioning", {}).get("dry_run", 0)) if isinstance(cfg.get("commissioning", {}), dict) else "0"

    procs: List[ManagedProc] = [
        ManagedProc(
            name="api_server",
            cmd=[sys.executable, "api_server.py"],
            cwd=PROJECT_ROOT,
            env=_make_env({}),
        ),
        ManagedProc(
            name="risk_guard",
            cmd=[sys.executable, "risk_guard_hardstop.py"],
            cwd=PROJECT_ROOT,
            env=_make_env({}),
        ),
        ManagedProc(
            name="trade_analytics",
            cmd=[sys.executable, "trade_analytics.py", "--daemon"],
            cwd=PROJECT_ROOT,
            env=_make_env({}),
        ),
        ManagedProc(
            name="mentor_executor",
            cmd=[sys.executable, "mentor_executor.py"],
            cwd=PROJECT_ROOT,
            env=_make_env({
                "SIGNAL_URL": f"{api_base}/api/signal_preview",
                "AI_CONFIRM_URL": f"{api_base}/api/ai_confirm",
                "DRY_RUN": default_dry_run,
            }),
        ),
       # ManagedProc(
       #    name="dashboard",
    #       cmd=["streamlit", "run", "intelligent_dashboard.py", "--server.port", str(dash_port), "--server.address", dash_host],
         #  cwd=PROJECT_ROOT,
    #     env=_make_env({
        #        "HIM_API_BASE": api_base,
        #    }),
        #),
    ]

    running = True

    def _handle_exit(sig_num, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _handle_exit)
    signal.signal(signal.SIGTERM, _handle_exit)

    _append_jsonl(WATCHDOG_LOG, {
        "ts": _ts(),
        "event": "watchdog_start",
        "version": APP_VERSION,
        "project_root": PROJECT_ROOT,
        "config_path": CONFIG_PATH,
        "api_base": api_base,
        "dashboard": {"host": dash_host, "port": dash_port},
    })

    # Start all processes once
    for p in procs:
        p.next_restart_ts = 0
        _start_process(p)

    # Main loop
    while running:
        kill_on, kill_note = _kill_switch_active()

        for p in procs:
            # Special policy: mentor_executor forced DRY_RUN=1 when kill switch active
            if p.name == "mentor_executor":
                desired_dry_run = "1" if kill_on else (os.environ.get("DRY_RUN", default_dry_run) or default_dry_run)
                if p.env.get("DRY_RUN") != desired_dry_run:
                    p.env["DRY_RUN"] = desired_dry_run
                    _append_jsonl(WATCHDOG_LOG, {
                        "ts": _ts(),
                        "event": "mentor_dry_run_update",
                        "version": APP_VERSION,
                        "dry_run": desired_dry_run,
                        "kill_switch": kill_on,
                        "note": kill_note,
                    })
                    # restart mentor to apply env change
                    _stop_process(p, reason="apply_dry_run_policy")
                    p.next_restart_ts = _ts() + 1

            # Restart if dead
            if p.popen and p.popen.poll() is not None:
                code = p.popen.returncode
                p.popen = None
                backoff = _compute_backoff_sec(p.restart_count + 1)
                p.next_restart_ts = _ts() + backoff
                _append_jsonl(WATCHDOG_LOG, {
                    "ts": _ts(),
                    "event": "process_exit",
                    "version": APP_VERSION,
                    "name": p.name,
                    "returncode": code,
                    "restart_in_sec": backoff,
                })

            if p.popen is None and _ts() >= p.next_restart_ts:
                _start_process(p)

        time.sleep(1.0)

    # Shutdown
    _append_jsonl(WATCHDOG_LOG, {"ts": _ts(), "event": "watchdog_stop", "version": APP_VERSION})

    # stop children
    for p in procs[::-1]:
        _stop_process(p, reason="watchdog_shutdown")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())