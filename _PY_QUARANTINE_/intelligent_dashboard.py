# ============================================================
#  intelligent_dashboard.py — HIM Live Performance Dashboard
#  Version: v1.0.1
#  Updated: 2026-03-05
#
#  Strategy Header (Project Rule):
#   - Purpose: Live observability for HIM pipeline (Engine -> AI confirm -> MT5 Executor)
#   - Production Model: Read-only dashboard (no trading actions)
#   - Evidence / Backtest numbers: N/A (observability; uses live logs & API)
#
#  Changelog:
#   - v1.0.1:
#       * FIX: Auto-refresh no longer blocks rendering (replace st.rerun loop with JS reload)
#       * Use existing repo logs: execution_orders.jsonl, api_server.jsonl, mentor_executor.jsonl
# ============================================================

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import urllib.request


# -----------------------------
# Defaults / Paths
# -----------------------------
PROJECT_ROOT = os.getcwd()

LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
EXEC_LOG = os.path.join(LOG_DIR, "execution_orders.jsonl")
API_LOG = os.path.join(LOG_DIR, "api_server.jsonl")
MENTOR_LOG = os.path.join(LOG_DIR, "mentor_executor.jsonl")  # exists in your repo

DEFAULT_API_BASE = os.environ.get("HIM_API_BASE", "http://127.0.0.1:5000").strip()
DEFAULT_SYMBOL = os.environ.get("HIM_SYMBOL", "GOLD").strip()


# -----------------------------
# Utilities
# -----------------------------
def http_get_json(url: str, timeout_sec: float = 5.0) -> Tuple[bool, Any, str]:
    try:
        req = urllib.request.Request(url=url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        try:
            return True, json.loads(raw), "ok"
        except Exception:
            return False, raw[:800], "non_json_response"
    except Exception as e:
        return False, None, f"connect_failed: {e}"


def read_jsonl_tail(path: str, max_lines: int = 2000) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "rb") as f:
            lines = f.readlines()[-max_lines:]
        out: List[Dict[str, Any]] = []
        for b in lines:
            s = b.decode("utf-8", errors="replace").strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                if isinstance(obj, dict):
                    out.append(obj)
            except Exception:
                continue
        return out
    except Exception:
        return []


def to_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.json_normalize(rows)


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def fmt_local_time(ts: Any) -> str:
    t = safe_int(ts, 0)
    if t <= 0:
        return ""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))


def file_stat(path: str) -> str:
    if not os.path.exists(path):
        return "missing"
    try:
        st_ = os.stat(path)
        return f"ok | size={st_.st_size} | mtime={time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(st_.st_mtime))}"
    except Exception:
        return "unknown"


def inject_auto_reload(seconds: int) -> None:
    # Reload the whole page after N seconds (safe, does not block render)
    ms = max(1, seconds) * 1000
    components.html(
        f"""
        <script>
          setTimeout(function() {{
            window.location.reload();
          }}, {ms});
        </script>
        """,
        height=0,
        width=0,
    )


# -----------------------------
# KPI Builders
# -----------------------------
@dataclass
class ExecKPI:
    total: int
    order_sent: int
    skipped: int
    top_reasons: pd.DataFrame
    recent: pd.DataFrame


def build_exec_kpi(df: pd.DataFrame, recent_n: int = 200) -> ExecKPI:
    if df.empty:
        return ExecKPI(0, 0, 0, pd.DataFrame(), pd.DataFrame())

    total = len(df)
    order_sent = int((df["status"] == "ORDER_SENT").sum()) if "status" in df.columns else 0
    skipped = int((df["status"] == "SKIP").sum()) if "status" in df.columns else 0

    top_reasons = pd.DataFrame()
    if "reason" in df.columns:
        top_reasons = df["reason"].fillna("unknown").value_counts().head(12).reset_index()
        top_reasons.columns = ["reason", "count"]

    recent = df.copy()
    if "ts" in recent.columns:
        recent = recent.sort_values("ts", ascending=False)
        recent["time"] = recent["ts"].apply(fmt_local_time)
    recent = recent.head(recent_n)

    keep_cols = [
        c
        for c in [
            "time",
            "status",
            "reason",
            "request_id",
            "symbol",
            "price",
            "plan.entry",
            "plan.sl",
            "plan.tp",
            "ai_confirm.approved",
            "ai_confirm.reason",
        ]
        if c in recent.columns
    ]
    if keep_cols:
        recent = recent[keep_cols]

    return ExecKPI(total, order_sent, skipped, top_reasons, recent)


@dataclass
class ApiKPI:
    total: int
    top_events: pd.DataFrame
    recent: pd.DataFrame


def build_api_kpi(df: pd.DataFrame, recent_n: int = 200) -> ApiKPI:
    if df.empty:
        return ApiKPI(0, pd.DataFrame(), pd.DataFrame())

    total = len(df)

    top_events = pd.DataFrame()
    if "event" in df.columns:
        top_events = df["event"].fillna("unknown").value_counts().head(12).reset_index()
        top_events.columns = ["event", "count"]

    recent = df.copy()
    if "ts" in recent.columns:
        recent = recent.sort_values("ts", ascending=False)
        recent["time"] = recent["ts"].apply(fmt_local_time)
    recent = recent.head(recent_n)

    keep_cols = [c for c in ["time", "event", "ok", "reason", "latency_ms", "request_id", "approved", "path"] if c in recent.columns]
    if keep_cols:
        recent = recent[keep_cols]

    return ApiKPI(total, top_events, recent)


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="HIM Live Dashboard", layout="wide")
st.title("HIM Live Performance Dashboard")
st.caption("Read-only observability: API v4 + live logs. (Safe: no trading actions here)")

with st.sidebar:
    st.subheader("Connection")
    api_base = st.text_input("API Base URL", value=DEFAULT_API_BASE)

    st.subheader("Refresh")
    auto_refresh = st.toggle("Auto-refresh", value=True)
    refresh_sec = st.slider("Refresh interval (sec)", 2, 60, 5)
    max_lines = st.slider("Log tail lines", 200, 10000, 2000, step=200)
    recent_rows = st.slider("Recent rows shown", 50, 500, 200, step=50)

    st.subheader("Filters")
    symbol_filter = st.text_input("Symbol filter", value=DEFAULT_SYMBOL)

    st.subheader("Files (logs)")
    st.text(f"logs/: {LOG_DIR}")
    st.text(f"execution_orders.jsonl: {file_stat(EXEC_LOG)}")
    st.text(f"api_server.jsonl: {file_stat(API_LOG)}")
    st.text(f"mentor_executor.jsonl: {file_stat(MENTOR_LOG)}")

    manual_reload = st.button("Reload now")

# Auto refresh (non-blocking)
if auto_refresh and not manual_reload:
    inject_auto_reload(refresh_sec)

# -----------------------------
# Fetch API: status + signal
# -----------------------------
colA, colB = st.columns(2)

with colA:
    st.subheader("API Status")
    ok_status, status_json, status_reason = http_get_json(f"{api_base}/api/status", timeout_sec=5.0)
    if ok_status and isinstance(status_json, dict) and status_json.get("ok") is True:
        st.success("API reachable")
        st.json(status_json)
    else:
        st.error(f"API status failed: {status_reason}")
        if status_json is not None:
            st.write(status_json)

with colB:
    st.subheader("Latest Signal Preview")
    ok_sig, sig_json, sig_reason = http_get_json(f"{api_base}/api/signal_preview", timeout_sec=12.0)
    if ok_sig and isinstance(sig_json, dict):
        decision = str(sig_json.get("decision", "")).upper()
        blocked_by = sig_json.get("blocked_by", [])
        latency_ms = None
        try:
            latency_ms = sig_json.get("api_meta", {}).get("latency_ms")
        except Exception:
            latency_ms = None

        if decision in ("BUY", "SELL"):
            st.success(f"Decision: {decision}")
        else:
            st.info(f"Decision: {decision or 'N/A'}")

        st.write({"latency_ms": latency_ms, "blocked_by": blocked_by})
        st.json(sig_json)
    else:
        st.error(f"Signal preview failed: {sig_reason}")
        if sig_json is not None:
            st.write(sig_json)

st.divider()

tabs = st.tabs(["MT5 Execution KPIs", "API Audit KPIs", "Mentor Executor KPIs", "Raw Tail Preview"])

# -----------------------------
# Tab 1: Execution
# -----------------------------
with tabs[0]:
    st.subheader("MT5 Execution KPIs (logs/execution_orders.jsonl)")

    exec_rows = read_jsonl_tail(EXEC_LOG, max_lines=max_lines)
    exec_df = to_df(exec_rows)

    if not exec_df.empty and symbol_filter and "symbol" in exec_df.columns:
        exec_df = exec_df[exec_df["symbol"].astype(str).str.upper() == symbol_filter.strip().upper()]

    kpi = build_exec_kpi(exec_df, recent_n=recent_rows)

    c1, c2, c3 = st.columns(3)
    c1.metric("Total events", kpi.total)
    c2.metric("ORDER_SENT", kpi.order_sent)
    c3.metric("SKIP", kpi.skipped)

    st.markdown("### Top SKIP reasons")
    if not kpi.top_reasons.empty:
        st.dataframe(kpi.top_reasons, width="stretch")
    else:
        st.write("No SKIP reasons yet.")

    st.markdown("### Recent events")
    if not kpi.recent.empty:
        st.dataframe(kpi.recent, width="stretch")
    else:
        st.write("No execution events yet.")

# -----------------------------
# Tab 2: API audit
# -----------------------------
with tabs[1]:
    st.subheader("API Audit KPIs (logs/api_server.jsonl)")

    api_rows = read_jsonl_tail(API_LOG, max_lines=max_lines)
    api_df = to_df(api_rows)

    api_kpi = build_api_kpi(api_df, recent_n=recent_rows)

    st.metric("Total audit events", api_kpi.total)

    st.markdown("### Top API events")
    if not api_kpi.top_events.empty:
        st.dataframe(api_kpi.top_events, width="stretch")
    else:
        st.write("No api events yet.")

    st.markdown("### Recent audit rows")
    if not api_kpi.recent.empty:
        st.dataframe(api_kpi.recent, width="stretch")
    else:
        st.write("No api audit rows yet.")

# -----------------------------
# Tab 3: Mentor KPIs
# -----------------------------
with tabs[2]:
    st.subheader("Mentor Executor KPIs (logs/mentor_executor.jsonl)")

    mentor_rows = read_jsonl_tail(MENTOR_LOG, max_lines=max_lines)
    mentor_df = to_df(mentor_rows)

    if mentor_df.empty:
        st.write("mentor_executor.jsonl missing/empty (but your repo likely has it).")
    else:
        if "ts" in mentor_df.columns:
            mentor_df = mentor_df.sort_values("ts", ascending=False)
            mentor_df["time"] = mentor_df["ts"].apply(fmt_local_time)

        # Try to extract common fields (best-effort)
        keep_cols = [c for c in ["time", "event", "status", "reason", "request_id", "signal_url", "ai_confirm_url"] if c in mentor_df.columns]
        if keep_cols:
            st.dataframe(mentor_df[keep_cols].head(recent_rows), width="stretch")
        else:
            st.dataframe(mentor_df.head(recent_rows), width="stretch")

# -----------------------------
# Tab 4: Raw tail preview
# -----------------------------
with tabs[3]:
    st.subheader("Raw Tail Preview (last 50 lines)")

    def show_tail(path: str, n: int = 50):
        st.markdown(f"**{path}**")
        if not os.path.exists(path):
            st.write("missing")
            return
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()[-n:]
            st.code("".join(lines), language="json")
        except Exception as e:
            st.write(f"read_failed: {e}")

    show_tail(EXEC_LOG, n=50)
    show_tail(API_LOG, n=50)
    show_tail(MENTOR_LOG, n=50)

st.caption("Tip: Set HIM_API_BASE if API runs elsewhere. Example: set HIM_API_BASE=http://127.0.0.1:5000")
