from __future__ import annotations

import argparse
import json
import os
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _as_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _normalize_decision(d: Any) -> str:
    return str(d or "").upper().strip()


@dataclass
class TfData:
    tf_str: str
    tf_id: int
    df: Any


class MT5ReplayShim:
    def __init__(self, *, symbol: str, tf_data: Dict[int, TfData]):
        self.symbol = symbol
        self._tf_data = tf_data
        self._current_ts: Optional[int] = None

        for td in tf_data.values():
            setattr(self, f"TIMEFRAME_{td.tf_str}", td.tf_id)

    def set_time(self, unix_ts_sec: int) -> None:
        self._current_ts = int(unix_ts_sec)

    def initialize(self) -> bool:
        return True

    def shutdown(self) -> bool:
        return True

    def last_error(self) -> Tuple[int, str]:
        return (0, "")

    def copy_rates_from_pos(self, symbol: str, timeframe: int, start_pos: int, count: int):
        if symbol != self.symbol:
            return None
        if self._current_ts is None:
            return None
        if timeframe not in self._tf_data:
            return None
        if start_pos != 0:
            return None

        df = self._tf_data[timeframe].df
        sub = df[df["time"] <= int(self._current_ts)]
        if len(sub) == 0:
            return None
        tail = sub.tail(int(count))
        records = tail.to_dict(orient="records")
        return records


def _fetch_last_n(symbol: str, tf_id: int, n: int):
    import MetaTrader5 as mt5
    import pandas as pd

    rates = mt5.copy_rates_from_pos(symbol, tf_id, 0, int(n))
    if rates is None or len(rates) == 0:
        return pd.DataFrame()
    df = pd.DataFrame(rates)
    if "time" not in df.columns:
        return pd.DataFrame()
    df["time"] = df["time"].astype(int)
    df = df.sort_values("time").reset_index(drop=True)
    return df


def _ensure_mt5_ready(symbol: str) -> None:
    import MetaTrader5 as mt5

    if not mt5.initialize():
        raise RuntimeError(f"mt5.initialize failed: {mt5.last_error()}")
    mt5.symbol_select(symbol, True)


def _pick_tf_id(tf: str) -> int:
    import MetaTrader5 as mt5

    m = {
        "M1": mt5.TIMEFRAME_M1,
        "M5": mt5.TIMEFRAME_M5,
        "M15": mt5.TIMEFRAME_M15,
        "M30": mt5.TIMEFRAME_M30,
        "H1": mt5.TIMEFRAME_H1,
        "H4": mt5.TIMEFRAME_H4,
        "D1": mt5.TIMEFRAME_D1,
    }
    t = str(tf).upper().strip()
    if t not in m:
        raise ValueError(f"Unsupported timeframe: {tf}")
    return int(m[t])


def analyze(
    *,
    config_path: Path,
    symbol: str,
    event_tf: str,
    steps: int,
    extra_history: int,
) -> Dict[str, Any]:
    import MetaTrader5 as mt5
    import engine as eng

    cfg = _load_json(config_path)
    tfs = cfg.get("timeframes", {}) if isinstance(cfg.get("timeframes", {}), dict) else {}
    htf = str(tfs.get("htf", cfg.get("htf", "H1"))).upper()
    mtf = str(tfs.get("mtf", cfg.get("mtf", "M15"))).upper()
    ltf = str(tfs.get("ltf", cfg.get("ltf", "M5"))).upper()

    rates_lookback = _as_int(cfg.get("rates_lookback", 600), 600)
    need = int(rates_lookback) + int(extra_history) + int(steps) + 20

    _ensure_mt5_ready(symbol)

    tf_list = sorted(set([htf, mtf, ltf, event_tf]))
    tf_data: Dict[int, TfData] = {}
    for tf_str in tf_list:
        tf_id = _pick_tf_id(tf_str)
        df = _fetch_last_n(symbol, tf_id, need)
        if df.empty:
            raise RuntimeError(f"No MT5 rates for {symbol} {tf_str}")
        tf_data[tf_id] = TfData(tf_str=tf_str, tf_id=tf_id, df=df)

    event_id = _pick_tf_id(event_tf)
    df_event = tf_data[event_id].df
    if len(df_event) < 50:
        raise RuntimeError("Not enough event bars")

    start_idx = max(0, len(df_event) - int(steps))
    times = [int(x) for x in df_event["time"].iloc[start_idx:].tolist()]

    shim = MT5ReplayShim(symbol=symbol, tf_data=tf_data)
    eng.mt5 = shim  # type: ignore[attr-defined]

    engine_obj = eng.TradingEngine(config=str(config_path))

    blocked_counter: Counter[str] = Counter()
    decision_counter: Counter[str] = Counter()
    tradeable = 0
    samples: List[Dict[str, Any]] = []

    t0 = time.time()
    for i, ts in enumerate(times):
        shim.set_time(ts)
        pkg = engine_obj.generate_signal_package(symbol=symbol, event_timeframe=event_tf)
        decision = _normalize_decision(pkg.get("decision"))
        blocked = pkg.get("blocked_by", [])
        if not isinstance(blocked, list):
            blocked = []
        decision_counter[decision or ""] += 1
        for b in blocked:
            blocked_counter[str(b)] += 1
        can_trade = decision in ("BUY", "SELL") and len(blocked) == 0
        if can_trade:
            tradeable += 1

        if i >= max(0, len(times) - 25):
            samples.append(
                {
                    "ts": int(ts),
                    "decision": decision,
                    "blocked_by": blocked,
                    "metrics": pkg.get("metrics", {}),
                    "gates": pkg.get("gates", {}),
                    "plan": pkg.get("plan", {}),
                    "price": pkg.get("price", {}),
                }
            )

    elapsed_ms = int((time.time() - t0) * 1000)

    mt5.shutdown()

    top_blockers = [{"reason": k, "count": int(v)} for k, v in blocked_counter.most_common(15)]
    decisions = [{"decision": k, "count": int(v)} for k, v in decision_counter.most_common()]

    return {
        "ts_utc": _utc_now_iso(),
        "symbol": symbol,
        "event_timeframe": event_tf,
        "timeframes": {"htf": htf, "mtf": mtf, "ltf": ltf},
        "steps": int(len(times)),
        "rates_lookback": int(rates_lookback),
        "elapsed_ms": int(elapsed_ms),
        "decision_counts": decisions,
        "tradeable_count": int(tradeable),
        "top_blockers": top_blockers,
        "tail_samples": samples,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--symbol", default=os.environ.get("SYMBOL", "GOLD"))
    ap.add_argument("--event-tf", default=os.environ.get("EVENT_TF", "M15"))
    ap.add_argument("--steps", default="300")
    ap.add_argument("--extra-history", default="300")
    ap.add_argument("--out", default="analysis_no_trade_report.json")
    args = ap.parse_args()

    rep = analyze(
        config_path=Path(str(args.config)).resolve(),
        symbol=str(args.symbol).strip(),
        event_tf=str(args.event_tf).strip().upper(),
        steps=_as_int(args.steps, 300),
        extra_history=_as_int(args.extra_history, 300),
    )
    out_path = Path(str(args.out)).resolve()
    _safe_write_json(out_path, rep)
    print(out_path.as_posix())
    print(json.dumps({"tradeable_count": rep["tradeable_count"], "top_blockers": rep["top_blockers"][:5]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

