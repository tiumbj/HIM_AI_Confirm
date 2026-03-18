"""
ชื่อโค้ด: HIM Terminal Dashboard
ที่อยู่ไฟล์: c:\Data\Bot\HIM_AI_Confirm\dashboard_terminal.py
คำสั่งรัน: python dashboard_terminal.py
เวอร์ชัน: v1.6.0
"""

from __future__ import annotations

import zlib
import re
import json
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


VERSION = "v1.6.0"

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=False)
except Exception:
    pass

RUNTIME_DIR = os.path.join(PROJECT_ROOT, "runtime")
STATE_PATH = os.environ.get("DASH_STATE_PATH") or os.path.join(RUNTIME_DIR, "dashboard_state.json")

STATE_STALE_SECONDS = float(os.environ.get("STATE_STALE_SECONDS", "8") or "8")
REFRESH_SECONDS = float(os.environ.get("DASH_REFRESH_SECONDS", "1") or "1")
MAX_EVENT_LINES = int(float(os.environ.get("MAX_EVENT_LINES", "30") or "30"))
EFFECT_INTERVAL_SECONDS = float(os.environ.get("DASH_EFFECT_INTERVAL_SECONDS", "25") or "25")
PANEL_WIDTH = int(float(os.environ.get("DASH_PANEL_WIDTH", "104") or "104"))
COL_GAP = int(float(os.environ.get("DASH_COL_GAP", "2") or "2"))
COL_WIDTH = int(float(os.environ.get("DASH_COL_WIDTH", "96") or "96"))
BAR_WIDTH = int(float(os.environ.get("DASH_BAR_WIDTH", "18") or "18"))
DASH_ANSI = os.environ.get("DASH_ANSI", "1").strip() in ("1", "true", "TRUE", "yes", "YES")
DASH_LAYOUT = (os.environ.get("DASH_LAYOUT", "auto") or "auto").strip().lower()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_ANSI_RESET = "\x1b[0m"
_ANSI_GREEN = "\x1b[32m"
_ANSI_BRIGHT_GREEN = "\x1b[92m"
_ANSI_DIM = "\x1b[90m"


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(float(v))
    except Exception:
        return None


def _load_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def _clear_screen() -> None:
    try:
        os.system("cls" if os.name == "nt" else "clear")
    except Exception:
        pass


def _fmt_num(v: Any, digits: int = 2) -> str:
    f = _safe_float(v)
    if f is None:
        return "--"
    return f"{f:.{digits}f}"


def _fmt_int(v: Any) -> str:
    i = _safe_int(v)
    return str(i) if i is not None else "--"


def _clip(s: Any, n: int) -> str:
    try:
        t = str(s)
    except Exception:
        t = ""
    if len(t) <= n:
        return t
    return t[: max(0, n - 1)] + "…"


def _crc32_seed(*parts: Any) -> int:
    s = "|".join([str(p) for p in parts])
    return int(zlib.crc32(s.encode("utf-8", errors="ignore")) & 0xFFFFFFFF)


def _noise_pct(seed: int, key: str, lo: int = 0, hi: int = 100) -> int:
    x = _crc32_seed(seed, key)
    span = max(1, int(hi) - int(lo) + 1)
    return int(lo) + int(x % span)


def _ansi_on() -> bool:
    return bool(DASH_ANSI)


def _ansi_wrap(s: str, code: str) -> str:
    if not _ansi_on():
        return s
    return f"{code}{s}{_ANSI_RESET}"


def _visible_len(s: str) -> int:
    return len(_ANSI_RE.sub("", s))


def _clip_visible(s: str, max_visible: int) -> str:
    if max_visible <= 0:
        return ""
    if _visible_len(s) <= max_visible:
        return s
    out: List[str] = []
    visible = 0
    i = 0
    while i < len(s) and visible < max_visible:
        if s[i] == "\x1b":
            m = _ANSI_RE.match(s, i)
            if m:
                out.append(m.group(0))
                i = m.end()
                continue
        out.append(s[i])
        visible += 1
        i += 1
    return "".join(out)


def _pad_visible(s: str, width: int) -> str:
    pad = int(width) - _visible_len(s)
    if pad <= 0:
        return s
    return s + (" " * pad)


def _bar(pct: Any, width: int) -> str:
    p = _safe_float(pct)
    if p is None:
        p = 0.0
    if p < 0:
        p = 0.0
    if p > 100:
        p = 100.0
    w = max(8, int(width))
    filled = int(round((p / 100.0) * w))
    if filled < 0:
        filled = 0
    if filled > w:
        filled = w
    head = int(time.time() * 7.0) % w
    chars: List[str] = []
    for i in range(w):
        if i == head:
            c = "▣" if i < filled else "▢"
            chars.append(_ansi_wrap(c, _ANSI_BRIGHT_GREEN))
            continue
        if i < filled:
            chars.append(_ansi_wrap("▮", _ANSI_GREEN))
        else:
            chars.append(_ansi_wrap("·", _ANSI_DIM))
    return "".join(chars)


def _scanline(width: int) -> str:
    w = max(10, int(width))
    pos = int(time.time() * 6.0) % w
    left = _ansi_wrap("░" * pos, _ANSI_DIM)
    mid = _ansi_wrap("█", _ANSI_BRIGHT_GREEN)
    right = _ansi_wrap("░" * (w - pos - 1), _ANSI_DIM)
    return left + mid + right


def _panel(title: str, body_lines: List[str], width: int) -> str:
    w = max(40, int(width))
    t = f" {title} "
    top = "┌" + t + "─" * max(0, w - len(t) - 2) + "┐"
    out = [top]
    for ln in body_lines:
        s = str(ln)
        if _visible_len(s) > w - 2:
            s = _clip_visible(s, w - 3) + "…"
        out.append("│" + _pad_visible(s, w - 2) + "│")
    out.append("└" + "─" * (w - 2) + "┘")
    return "\n".join(out)


def _merge_columns(left: List[str], right: List[str], left_width: int, right_width: int, gap: int) -> List[str]:
    lw = max(20, int(left_width))
    rw = max(20, int(right_width))
    g = " " * max(1, int(gap))
    n = max(len(left), len(right))
    out: List[str] = []
    for i in range(n):
        left_str = left[i] if i < len(left) else ""
        right_str = right[i] if i < len(right) else ""
        out.append(_pad_visible(left_str, lw) + g + _pad_visible(right_str, rw))
    return out



@dataclass
class DashboardState:
    raw: Dict[str, Any]
    mtime: float

    def age_sec(self) -> float:
        return max(0.0, time.time() - float(self.mtime))


class JsonStateProvider:
    def __init__(self, path: str):
        self.path = path
        self._last_mtime: Optional[float] = None
        self._snapshot: Dict[str, Any] = self._default_state()
        self._events: List[str] = ["provider ready"]

    def _default_state(self) -> Dict[str, Any]:
        return {
            "header": {"system": "DEGRADED", "symbol": "--", "time_utc": _utc_iso(), "position": 0, "cascade_exit_enabled": False},
            "data_ingest": {"feed": "MISSING", "ticks": "NO_DATA"},
            "mt5_status": {"ok": False, "connected": None, "trade_allowed": None, "reason": "no_state"},
            "ai_llm_status": {"enabled": False, "mode": "DISABLED", "provider": None, "model": None, "key_set": False},
            "market_view": {"state": "UNKNOWN", "lines": []},
            "live_analytics": {"price": None, "bid": None, "ask": None, "equity": None, "balance": None, "profit": None},
            "execution_guard": {"action": "WAIT", "reason": "no production state file detected"},
            "position_monitor": {"status": "UNKNOWN", "positions": [], "last_cascade_event": None},
            "daily_report": {"ok": False, "date": datetime.now().strftime("%Y-%m-%d"), "trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "net_pnl": 0.0},
            "event_stream": {"lines": []},
        }

    def get_state(self) -> DashboardState:
        if not os.path.exists(self.path):
            s = dict(self._snapshot)
            s["header"] = {**s.get("header", {}), "system": "DEGRADED"}
            s["execution_guard"] = {**s.get("execution_guard", {}), "action": "WAIT", "reason": "missing dashboard_state.json"}
            s["event_stream"] = {"lines": (self._events + ["MISSING FILE STATE"])[:MAX_EVENT_LINES]}
            return DashboardState(raw=s, mtime=0.0)

        try:
            st = os.stat(self.path)
            mtime = float(st.st_mtime)
        except Exception:
            mtime = 0.0

        if self._last_mtime is not None and mtime == self._last_mtime:
            return DashboardState(raw=self._snapshot, mtime=mtime)

        loaded = _load_json(self.path)
        if loaded is None:
            s = dict(self._snapshot)
            s["header"] = {**s.get("header", {}), "system": "DEGRADED"}
            self._events.append("JSON LOAD ERROR")
            s["event_stream"] = {"lines": (self._events + ["JSON ERROR STATE"])[:MAX_EVENT_LINES]}
            return DashboardState(raw=s, mtime=mtime)

        merged = dict(self._snapshot)
        for k in (
            "header",
            "data_ingest",
            "mt5_status",
            "ai_llm_status",
            "market_view",
            "live_analytics",
            "execution_guard",
            "position_monitor",
            "daily_report",
            "event_stream",
        ):
            if isinstance(loaded.get(k), dict):
                merged[k] = loaded.get(k)

        self._snapshot = merged
        self._last_mtime = mtime
        return DashboardState(raw=merged, mtime=mtime)


class DashboardRenderer:
    def render(self, ds: DashboardState) -> str:
        s = ds.raw
        header = s.get("header", {}) if isinstance(s.get("header"), dict) else {}
        ingest = s.get("data_ingest", {}) if isinstance(s.get("data_ingest"), dict) else {}
        mt5s = s.get("mt5_status", {}) if isinstance(s.get("mt5_status"), dict) else {}
        ais = s.get("ai_llm_status", {}) if isinstance(s.get("ai_llm_status"), dict) else {}
        mkt = s.get("market_view", {}) if isinstance(s.get("market_view"), dict) else {}
        live = s.get("live_analytics", {}) if isinstance(s.get("live_analytics"), dict) else {}
        guard = s.get("execution_guard", {}) if isinstance(s.get("execution_guard"), dict) else {}
        posm = s.get("position_monitor", {}) if isinstance(s.get("position_monitor"), dict) else {}
        report = s.get("daily_report", {}) if isinstance(s.get("daily_report"), dict) else {}
        evs = s.get("event_stream", {}) if isinstance(s.get("event_stream"), dict) else {}

        age = ds.age_sec()
        system = str(header.get("system") or "DEGRADED").upper()
        if age > STATE_STALE_SECONDS:
            system = "STALE"

        term_cols, term_rows = shutil.get_terminal_size(fallback=(PANEL_WIDTH, 50))
        layout = DASH_LAYOUT
        if layout not in ("auto", "one", "two"):
            layout = "auto"
        if layout == "one":
            use_two_col = False
        elif layout == "two":
            use_two_col = True
        else:
            use_two_col = term_cols >= 160

        symbol = _clip(header.get("symbol"), 12)
        pos_count = _safe_int(header.get("position")) or 0
        magic = _fmt_int(header.get("magic"))
        cascade_on = bool(header.get("cascade_exit_enabled"))
        mt5_latency = _fmt_int(header.get("mt5_latency_ms"))

        spread_pts = ingest.get("spread_points")
        spread_pts_s = _fmt_int(spread_pts)

        bid = live.get("bid")
        ask = live.get("ask")
        price = live.get("price")
        eq = live.get("equity")
        bal = live.get("balance")
        pnl = live.get("profit")

        exit_risk = _clip(posm.get("exit_risk"), 18)

        effect_bucket = int(time.time() // max(5.0, EFFECT_INTERVAL_SECONDS))
        base_seed = _crc32_seed(symbol, magic, system, effect_bucket)

        data_flow = 90 if system == "NOMINAL" else (60 if system == "STALE" else 25)
        market_energy = 70 if system == "NOMINAL" else (45 if system == "STALE" else 20)
        trend_power = 65 if (cascade_on and system == "NOMINAL") else (40 if system == "NOMINAL" else 20)
        signal_power = 55 if pos_count > 0 else 35
        ai_lock = 30 if system == "NOMINAL" else 15

        data_flow = max(0, min(100, data_flow + (_noise_pct(base_seed, "df", -6, 6))))
        market_energy = max(0, min(100, market_energy + (_noise_pct(base_seed, "me", -9, 9))))
        trend_power = max(0, min(100, trend_power + (_noise_pct(base_seed, "tp", -8, 8))))
        signal_power = max(0, min(100, signal_power + (_noise_pct(base_seed, "sp", -7, 7))))
        ai_lock = max(0, min(100, ai_lock + (_noise_pct(base_seed, "ai", -5, 10))))

        if use_two_col:
            col_w = max(56, min(int(COL_WIDTH), int((term_cols - COL_GAP) // 2)))
            full_w = min(int(term_cols), max(int(PANEL_WIDTH), int(col_w * 2 + COL_GAP)))
            bar_w = min(int(BAR_WIDTH), max(10, int(col_w / 6)))
        else:
            col_w = max(56, min(int(PANEL_WIDTH), int(term_cols)))
            full_w = col_w
            bar_w = min(int(BAR_WIDTH), max(10, int(full_w / 6)))

        scan = _scanline(bar_w)
        heartbeat_hz = (1.0 / REFRESH_SECONDS) if REFRESH_SECONDS > 0 else 1.0

        header_lines = [
            f"SYSTEM={system:<8}  SYMBOL={symbol:<12}  POS={pos_count:<3}  MAGIC={magic:<8}  CASCADE_EXIT={str(cascade_on):<5}",
            f"UTC={_utc_iso()}  HEARTBEAT_HZ={heartbeat_hz:>5.2f}  AGE={age:>5.1f}s  MT5_LATENCY_MS={mt5_latency:>4}",
            f"SCAN [{scan}]",
        ]

        ingest_lines = [
            f"feed={_clip(ingest.get('feed'), 10):<10} ticks={_clip(ingest.get('ticks'), 10):<10} spread_pts={spread_pts_s:<5}",
            f"DATA_FLOW     [ {_bar(data_flow, bar_w)} ] {_ansi_wrap(f'{data_flow:>3}%', _ANSI_BRIGHT_GREEN)}",
            f"MARKET_ENERGY  [ {_bar(market_energy, bar_w)} ] {_ansi_wrap(f'{market_energy:>3}%', _ANSI_BRIGHT_GREEN)}",
        ]

        live_lines = [
            f"price={_fmt_num(price, 2):>10}  bid={_fmt_num(bid, 2):>10}  ask={_fmt_num(ask, 2):>10}",
            f"equity={_fmt_num(eq, 2):>10}  balance={_fmt_num(bal, 2):>10}  pnl={_fmt_num(pnl, 2):>10}",
            f"SPREAD_METER   [ {_bar(min(100, (_safe_int(spread_pts) or 0) * 2), bar_w)} ] {_ansi_wrap(f'{spread_pts_s:>4} pts', _ANSI_BRIGHT_GREEN)}",
        ]

        guard_lines = [
            f"action={_clip(guard.get('action'), 18):<18} reason={_clip(guard.get('reason'), 58)}",
            f"TREND_POWER    [ {_bar(trend_power, bar_w)} ] {_ansi_wrap(f'{trend_power:>3}%', _ANSI_BRIGHT_GREEN)}",
            f"SIGNAL_POWER   [ {_bar(signal_power, bar_w)} ] {_ansi_wrap(f'{signal_power:>3}%', _ANSI_BRIGHT_GREEN)}",
            f"AI_LOCK        [ {_bar(ai_lock, bar_w)} ] {_ansi_wrap(f'{ai_lock:>3}%', _ANSI_BRIGHT_GREEN)}   exit_risk={exit_risk}",
        ]

        mt5_lines = [
            f"ok={str(bool(mt5s.get('ok'))):<5} connected={str(mt5s.get('connected')):<5} trade_allowed={str(mt5s.get('trade_allowed')):<5}",
            f"reason={_clip(mt5s.get('reason'), 80)}",
        ]

        ai_lines = [
            f"enabled={str(bool(ais.get('enabled'))):<5} mode={_clip(ais.get('mode'), 10):<10} key_set={str(bool(ais.get('key_set'))):<5}",
            f"provider={_clip(ais.get('provider'), 18):<18} model={_clip(ais.get('model'), 32)}",
        ]

        mkt_lines: List[str] = []
        mkt_lines.append(f"state={_clip(mkt.get('state'), 16):<16} exit_risk={exit_risk}")
        mv = mkt.get("lines", [])
        if isinstance(mv, list):
            for ln in mv[:4]:
                mkt_lines.append(f"- {_clip(ln, 92)}")
        else:
            mkt_lines.append("- (no commentary)")

        rep_lines = [
            f"date={_clip(report.get('date'), 12):<12} ok={str(bool(report.get('ok'))):<5} trades={_fmt_int(report.get('trades')):<4}",
            f"wins={_fmt_int(report.get('wins')):<4} losses={_fmt_int(report.get('losses')):<4} win_rate={_fmt_num(report.get('win_rate'), 2):>6}%",
            f"net_pnl={_fmt_num(report.get('net_pnl'), 2):>10}",
        ]

        pos_lines: List[str] = []
        positions = posm.get("positions", [])
        if not isinstance(positions, list):
            positions = []
        pos_lines.append(f"status={_clip(posm.get('status'), 10):<10} count={len(positions):<3} exit_risk={exit_risk}")
        for p in positions[:10]:
            if not isinstance(p, dict):
                continue
            pos_lines.append(
                f"ticket={_fmt_int(p.get('ticket')):<8} side={_clip(p.get('type'), 4):<4} vol={_fmt_num(p.get('volume'), 2):>6} "
                f"open={_fmt_num(p.get('price_open'), 2):>10} sl={_fmt_num(p.get('sl'), 2):>10} tp={_fmt_num(p.get('tp'), 2):>10} pnl={_fmt_num(p.get('profit'), 2):>9}"
            )
        last_c = posm.get("last_cascade_event")
        if isinstance(last_c, dict):
            pos_lines.append(
                f"cascade_last event={_clip(last_c.get('event'), 16):<16} action={_clip(last_c.get('action'), 16):<16} "
                f"ticket={_fmt_int(last_c.get('ticket')):<8} reason={_clip(last_c.get('reason'), 40)}"
            )

        ev_lines = evs.get("lines", [])
        if not isinstance(ev_lines, list):
            ev_lines = []
        stream_lines_all = [str(x)[:240] for x in ev_lines[-MAX_EVENT_LINES:]] if ev_lines else []
        if not stream_lines_all:
            stream_lines_all = ["(no events yet)"]

        if not use_two_col:
            blocks = [
                _panel("HIM — CONTROL CORE", header_lines, full_w),
                _panel("LIVE ANALYTICS", live_lines, full_w),
                _panel("DATA INGEST", ingest_lines, full_w),
                _panel("EXECUTION GUARD", guard_lines, full_w),
                _panel("MT5 STATUS", mt5_lines, full_w),
                _panel("AI/LLM STATUS", ai_lines, full_w),
                _panel("MARKET VIEW", mkt_lines, full_w),
                _panel("DAILY REPORT", rep_lines, full_w),
                _panel("POSITION MONITOR", pos_lines, full_w),
            ]
            top_lines: List[str] = []
            for b in blocks:
                top_lines.extend(b.splitlines())
            remaining = int(term_rows) - len(top_lines) - 5
            if remaining < 6:
                remaining = 6
            stream_lines = stream_lines_all[-min(int(MAX_EVENT_LINES), int(remaining)) :]
            top_lines.append("")
            top_lines.extend(_panel("EVENT STREAM", stream_lines, full_w).splitlines())
            return "\n".join(top_lines)

        left_col_blocks = [
            _panel("HIM — CONTROL CORE", header_lines, col_w),
            _panel("DATA INGEST", ingest_lines, col_w),
            _panel("EXECUTION GUARD", guard_lines, col_w),
            _panel("POSITION MONITOR", pos_lines, col_w),
        ]
        right_col_blocks = [
            _panel("LIVE ANALYTICS", live_lines, col_w),
            _panel("MT5 STATUS", mt5_lines, col_w),
            _panel("AI/LLM STATUS", ai_lines, col_w),
            _panel("MARKET VIEW", mkt_lines, col_w),
            _panel("DAILY REPORT", rep_lines, col_w),
        ]

        left_lines: List[str] = []
        for b in left_col_blocks:
            left_lines.extend(b.splitlines())
        right_lines: List[str] = []
        for b in right_col_blocks:
            right_lines.extend(b.splitlines())

        grid = _merge_columns(left_lines, right_lines, col_w, col_w, COL_GAP)
        remaining = int(term_rows) - len(grid) - 6
        if remaining < 6:
            remaining = 6
        stream_lines = stream_lines_all[-min(int(MAX_EVENT_LINES), int(remaining)) :]
        grid.append("")
        grid.append(_panel("EVENT STREAM", stream_lines, full_w))
        return "\n".join(grid)


def main() -> int:
    os.makedirs(RUNTIME_DIR, exist_ok=True)
    provider = JsonStateProvider(STATE_PATH)
    renderer = DashboardRenderer()

    while True:
        ds = provider.get_state()
        _clear_screen()
        print(renderer.render(ds))
        time.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
