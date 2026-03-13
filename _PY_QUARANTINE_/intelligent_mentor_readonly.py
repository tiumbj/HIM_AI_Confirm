from __future__ import annotations

import json
import os
import time
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


def load_env_file(env_path: str, override: bool = True) -> None:
    try:
        if not os.path.exists(env_path):
            return
        with open(env_path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                key = k.strip()
                val = v.strip().strip("'").strip('"')
                if not key:
                    continue
                if (not override) and (key in os.environ):
                    continue
                os.environ[key] = val
    except Exception:
        return


def telegram_send_text(text: str, chat_id_env: str, token_env: str = "TELEGRAM_BOT_TOKEN") -> Tuple[bool, int, str]:
    token = os.environ.get(token_env)
    chat_id = os.environ.get(chat_id_env)
    if not token or not chat_id:
        return False, 0, f"missing token/chat_id env ({token_env}/{chat_id_env})"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    data = _safe_json(payload).encode("utf-8")
    req = Request(url, method="POST", data=data, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            status = int(getattr(resp, "status", 0) or 0)
            return (status == 200), status, body
    except HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = str(e)
        code = int(getattr(e, "code", 0) or 0)
        return False, code, body
    except (URLError, TimeoutError) as e:
        return False, 0, str(e)


def http_get_json(url: str, timeout_sec: float = 8.0) -> Tuple[bool, Any]:
    try:
        req = Request(url, method="GET", headers={"Accept": "application/json"})
        with urlopen(req, timeout=timeout_sec) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        return True, json.loads(body)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as e:
        return False, str(e)


def http_post_json(url: str, payload: Dict[str, Any], timeout_sec: float = 15.0) -> Tuple[bool, Any]:
    try:
        data = _safe_json(payload).encode("utf-8")
        req = Request(url, method="POST", data=data, headers={"Content-Type": "application/json", "Accept": "application/json"})
        with urlopen(req, timeout=timeout_sec) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        return True, json.loads(body)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as e:
        return False, str(e)


def _nan_to_none(x: Any) -> Any:
    try:
        f = float(x)
        if math.isfinite(f):
            return f
        return None
    except Exception:
        return None


def _fmt_num(x: Any, digits: int = 2) -> str:
    v = _nan_to_none(x)
    if v is None:
        return "-"
    try:
        return f"{float(v):.{digits}f}"
    except Exception:
        return "-"

def _atr_level(tf: str, atr: Optional[float]) -> str:
    if atr is None:
        return "ไม่ทราบ"
    v = float(atr)
    t = str(tf or "").upper()
    if t == "M1":
        return "สูง" if v >= 4 else ("กลาง" if v >= 2 else "ต่ำ")
    if t == "M5":
        return "สูง" if v >= 10 else ("กลาง" if v >= 5 else "ต่ำ")
    if t == "M15":
        return "สูง" if v >= 20 else ("กลาง" if v >= 10 else "ต่ำ")
    if t == "M30":
        return "สูง" if v >= 25 else ("กลาง" if v >= 12 else "ต่ำ")
    if t == "H1":
        return "สูง" if v >= 40 else ("กลาง" if v >= 20 else "ต่ำ")
    if t == "H4":
        return "สูง" if v >= 90 else ("กลาง" if v >= 50 else "ต่ำ")
    if t == "D1":
        return "สูง" if v >= 180 else ("กลาง" if v >= 100 else "ต่ำ")
    return "สูง" if v >= 25 else ("กลาง" if v >= 10 else "ต่ำ")


def _bb_level(bb_width_atr: Optional[float]) -> str:
    if bb_width_atr is None:
        return "ไม่ทราบ"
    v = float(bb_width_atr)
    if v >= 1.8:
        return "กว้าง"
    if v <= 0.9:
        return "แคบ"
    return "กลาง"


def _st_dist_level(st_distance_atr: Optional[float]) -> str:
    if st_distance_atr is None:
        return "ไม่ทราบ"
    v = abs(float(st_distance_atr))
    if v >= 2.0:
        return "ไกล"
    if v <= 0.7:
        return "ใกล้"
    return "กลาง"


def _rsi_level(rsi: Optional[float]) -> str:
    if rsi is None:
        return "ไม่ทราบ"
    v = float(rsi)
    if v >= 60:
        return "โมเมนตัมขาขึ้นแรง"
    if v <= 40:
        return "โมเมนตัมขาลงแรง"
    return "โมเมนตัมกลาง"


def _adx_level(adx: Optional[float]) -> str:
    if adx is None:
        return "ไม่ทราบ"
    v = float(adx)
    if v >= 25:
        return "เทรนด์ค่อนข้างชัด"
    if v <= 18:
        return "เทรนด์ไม่ชัด/เสี่ยงไซด์เวย์"
    return "เทรนด์กำลังก่อตัว"


def _ema(values: List[float], period: int) -> Optional[float]:
    if period <= 1 or not values or len(values) < period:
        return None
    k = 2.0 / (float(period) + 1.0)
    ema = float(sum(values[:period])) / float(period)
    for v in values[period:]:
        ema = (float(v) * k) + (ema * (1.0 - k))
    return float(ema)


def _rsi_wilder(values: List[float], period: int = 14) -> Optional[float]:
    if period <= 1 or not values or len(values) < (period + 1):
        return None
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, period + 1):
        d = float(values[i]) - float(values[i - 1])
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = sum(gains) / float(period)
    avg_loss = sum(losses) / float(period)
    for i in range(period + 1, len(values)):
        d = float(values[i]) - float(values[i - 1])
        gain = max(d, 0.0)
        loss = max(-d, 0.0)
        avg_gain = ((avg_gain * (period - 1)) + gain) / float(period)
        avg_loss = ((avg_loss * (period - 1)) + loss) / float(period)
    if avg_loss == 0.0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _adx_wilder(df: Any, period: int = 14) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    try:
        highs = [float(x) for x in list(df["high"].values)]
        lows = [float(x) for x in list(df["low"].values)]
        closes = [float(x) for x in list(df["close"].values)]
    except Exception:
        return None, None, None

    n = len(closes)
    if period <= 1 or n < (period * 2 + 2):
        return None, None, None

    tr: List[float] = []
    pdm: List[float] = []
    ndm: List[float] = []
    for i in range(1, n):
        up_move = highs[i] - highs[i - 1]
        down_move = lows[i - 1] - lows[i]
        plus_dm = up_move if (up_move > down_move and up_move > 0) else 0.0
        minus_dm = down_move if (down_move > up_move and down_move > 0) else 0.0
        tr_i = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        pdm.append(plus_dm)
        ndm.append(minus_dm)
        tr.append(float(tr_i))

    tr14 = sum(tr[:period])
    pdm14 = sum(pdm[:period])
    ndm14 = sum(ndm[:period])

    di_plus = 0.0 if tr14 == 0 else (100.0 * (pdm14 / tr14))
    di_minus = 0.0 if tr14 == 0 else (100.0 * (ndm14 / tr14))
    dx = 0.0 if (di_plus + di_minus) == 0 else (100.0 * abs(di_plus - di_minus) / (di_plus + di_minus))

    dxs: List[float] = [dx]

    for i in range(period, len(tr)):
        tr14 = tr14 - (tr14 / float(period)) + tr[i]
        pdm14 = pdm14 - (pdm14 / float(period)) + pdm[i]
        ndm14 = ndm14 - (ndm14 / float(period)) + ndm[i]

        di_plus = 0.0 if tr14 == 0 else (100.0 * (pdm14 / tr14))
        di_minus = 0.0 if tr14 == 0 else (100.0 * (ndm14 / tr14))
        dx = 0.0 if (di_plus + di_minus) == 0 else (100.0 * abs(di_plus - di_minus) / (di_plus + di_minus))
        dxs.append(dx)

    if len(dxs) < period:
        return None, None, None

    adx = sum(dxs[:period]) / float(period)
    for v in dxs[period:]:
        adx = ((adx * (period - 1)) + float(v)) / float(period)

    return float(adx), float(di_plus), float(di_minus)


def _explain_blocker_th(reason: str) -> str:
    r = str(reason or "").strip()
    if r == "supertrend_conflict":
        return "เทรนด์จากกรอบใหญ่กับ SuperTrend ของกรอบเหตุการณ์ขัดแย้งกัน (ยังไม่ควรสวนเทรนด์)"
    if r == "no_bos_break":
        return "ยังไม่เกิด Break of Structure (BOS) ที่แรงพอตามเกณฑ์"
    if r == "no_vol_expansion":
        return "ความผันผวนยังไม่ขยาย (BB width/ATR ต่ำ) โอกาสหลอกสูง"
    if r == "rr_too_low":
        return "RR ยังต่ำกว่าเกณฑ์ขั้นต่ำ"
    if r == "data_not_ready":
        return "ข้อมูลราคาไม่พอ/ยังโหลดไม่ครบ"
    return r


def _explain_blocker_en(reason: str) -> str:
    r = str(reason or "").strip()
    if r == "supertrend_conflict":
        return "Higher-timeframe bias conflicts with event SuperTrend (avoid counter-trend entries)."
    if r == "no_bos_break":
        return "No valid Break of Structure (BOS) yet per threshold."
    if r == "no_vol_expansion":
        return "Volatility is not expanding (BB width/ATR too low); higher fakeout risk."
    if r == "rr_too_low":
        return "Reward-to-risk is below the minimum threshold."
    if r == "data_not_ready":
        return "Insufficient market data / feed not ready."
    return r


def _ai_mentor_narrative(context: Dict[str, Any]) -> Tuple[bool, str]:
    use_ai = os.environ.get("MENTOR_USE_AI", "0").strip() in ("1", "true", "TRUE", "yes", "YES")
    if not use_ai:
        return False, "disabled"

    api_url = (os.environ.get("MENTOR_AI_API_URL") or "https://api.deepseek.com/v1/chat/completions").strip()
    key = (os.environ.get("DEEPSEEK_API_KEY") or os.environ.get("MENTOR_AI_API_KEY") or "").strip()
    if not api_url or not key:
        return False, "missing_api_key"

    prompt = (
        "You are an elite trading mentor. "
        "Explain the multi-timeframe analysis for GOLD using the provided JSON. "
        "Output MUST be bilingual Thai + English. "
        "Be actionable and educational: trend/bias per timeframe, key blockers, what must happen next, and how to read the chart. "
        "If decision is HOLD, provide conditional scenarios (BUY setup / SELL setup) referencing BOS, SuperTrend alignment, volatility, and RR. "
        "Do NOT invent indicators not present in the JSON. "
        "Use clear bullet points. Keep it concise but useful.\n\n"
        f"JSON:\n{_safe_json(context)}"
    )

    payload = {
        "model": os.environ.get("MENTOR_AI_MODEL", "deepseek-chat"),
        "messages": [
            {"role": "system", "content": "You are a professional trading mentor."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 900,
    }

    data = _safe_json(payload).encode("utf-8")
    req = Request(
        api_url,
        method="POST",
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json", "Authorization": f"Bearer {key}"},
    )
    try:
        with urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        j = json.loads(body)
        choices = j.get("choices")
        if isinstance(choices, list) and choices:
            msg = choices[0].get("message", {})
            if isinstance(msg, dict):
                content = msg.get("content")
                if isinstance(content, str) and content.strip():
                    return True, content.strip()
        return False, "bad_ai_response"
    except Exception as e:
        return False, f"ai_error:{e}"


def _midpoint(a: float, b: float) -> float:
    return (float(a) + float(b)) / 2.0


def _mk_plan(entry: float, atr: float, sl_atr: float, tp_atr: float, side: str) -> Dict[str, float]:
    e = float(entry)
    a = float(atr)
    s = float(sl_atr)
    t = float(tp_atr)
    if side == "BUY":
        return {"entry": e, "sl": e - (a * s), "tp": e + (a * t)}
    return {"entry": e, "sl": e + (a * s), "tp": e - (a * t)}


def _ema_bias(ema20: Optional[float], ema50: Optional[float]) -> str:
    if ema20 is None or ema50 is None:
        return "UNKNOWN"
    if float(ema20) > float(ema50):
        return "BULLISH"
    if float(ema20) < float(ema50):
        return "BEARISH"
    return "NEUTRAL"


def _trend_bias_label(st_dir_label: Optional[str]) -> str:
    lab = (st_dir_label or "").upper().strip()
    if lab in ("UP", "DOWN"):
        return "BULLISH" if lab == "UP" else "BEARISH"
    return "UNKNOWN"


def _mtf_ratio(tf_rows: List[Dict[str, Any]]) -> Tuple[Optional[float], Optional[float]]:
    ups = 0
    dns = 0
    for tf in tf_rows:
        if not isinstance(tf, dict) or not bool(tf.get("ok")):
            continue
        lab = str(tf.get("st_dir_label") or "").upper().strip()
        if lab == "UP":
            ups += 1
        elif lab == "DOWN":
            dns += 1
    total = ups + dns
    if total <= 0:
        return None, None
    return (100.0 * float(ups) / float(total)), (100.0 * float(dns) / float(total))


def _mt5_spread_points(symbol: str) -> Optional[float]:
    try:
        import MetaTrader5 as mt5  # type: ignore

        if not mt5.initialize():
            return None
        info = mt5.symbol_info(symbol)
        tick = mt5.symbol_info_tick(symbol)
        if info is None or tick is None:
            return None
        point = float(getattr(info, "point", 0.0) or 0.0)
        ask = float(getattr(tick, "ask", 0.0) or 0.0)
        bid = float(getattr(tick, "bid", 0.0) or 0.0)
        if point <= 0.0 or ask <= 0.0 or bid <= 0.0:
            return None
        return (ask - bid) / point
    except Exception:
        return None


def _format_beginner_th(
    *,
    symbol: str,
    ts: int,
    decision: str,
    top_bias: str,
    major_bias: Optional[str],
    blocked_list: List[str],
    plan: Dict[str, Any],
    buy_plan: Optional[Dict[str, float]],
    sell_plan: Optional[Dict[str, float]],
    trigger_buy: Optional[float],
    trigger_sell: Optional[float],
    current_price: Optional[float],
    event_timeframe: str,
    atr: Optional[float],
    bb_width_atr: Optional[float],
    st_dir_event: Optional[int],
    st_distance_atr: Optional[float],
    alignment_score: Optional[int],
    regime: Optional[str],
    rr: Optional[float],
    min_rr: Optional[float],
    rsi14_event: Optional[float],
    ema20_event: Optional[float],
    ema50_event: Optional[float],
    ema200_event: Optional[float],
    adx14_event: Optional[float],
    di_plus_event: Optional[float],
    di_minus_event: Optional[float],
    rsi14_h1: Optional[float],
    ema200_h1: Optional[float],
    adx14_h1: Optional[float],
    m15_bias: Optional[str],
    ema_bias_m15: Optional[str],
    mtf_up_pct: Optional[float],
    mtf_dn_pct: Optional[float],
    spread_points: Optional[float],
    m15_last: Optional[float],
    ema20_m15: Optional[float],
    ema50_m15: Optional[float],
    zone_high_20_m15: Optional[float],
    zone_low_20_m15: Optional[float],
    trader_mentor_ai: Optional[str],
) -> str:
    lines: List[str] = []
    lines.append("HIM INTELLIGENT MENTOR (READ-ONLY)")
    lines.append(f"time_utc={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(int(ts or 0)))}")
    lines.append(f"symbol={symbol}")
    lines.append("")

    if current_price is not None:
        lines.append(f"ราคาล่าสุดประมาณ: {_fmt_num(current_price)}")
        lines.append("")

    lines.append("1) มองภาพใหญ่ (Top-down)")
    if major_bias == "UP":
        lines.append("- D1/H4 เอนไปทางขึ้น: โฟกัสหาจังหวะ BUY มากกว่า SELL")
    elif major_bias == "DOWN":
        lines.append("- D1/H4 เอนไปทางลง: โฟกัสหาจังหวะ SELL มากกว่า BUY")
    else:
        lines.append("- D1/H4 ยังไม่ชัด: ลดความเสี่ยง/รอจังหวะให้ชัดขึ้น")

    if top_bias == "UP":
        lines.append("- ภาพรวมหลาย TF ตอนนี้เอนไปทางขึ้น")
    elif top_bias == "DOWN":
        lines.append("- ภาพรวมหลาย TF ตอนนี้เอนไปทางลง")
    elif top_bias == "mixed":
        lines.append("- ภาพรวมหลาย TF ยังผสม: ระวังโดนหลอกในกรอบย่อย")
    else:
        lines.append("- ภาพรวมหลาย TF: ยังไม่มีข้อมูลพอ")

    if blocked_list:
        lines.append("")
        lines.append("2) ตอนนี้ควรทำอะไร")
        lines.append("- ตอนนี้ “ยังไม่เข้า” เพราะเงื่อนไขยังไม่ครบ:")
        for b in blocked_list[:3]:
            lines.append(f"  • {_explain_blocker_th(b)}")
    else:
        lines.append("")
        lines.append("2) ตอนนี้ควรทำอะไร")
        lines.append("- เงื่อนไขครบมากขึ้น สามารถใช้แผนด้านล่างเพื่อเข้าเทรดได้")

    lines.append("")
    lines.append("3) แผนเข้าเทรด (แนะนำให้ทำตามแบบง่าย ๆ)")

    direct_plan = False
    if decision in ("BUY", "SELL") and all(k in plan for k in ("entry", "sl", "tp")):
        direct_plan = True
        side_th = "ซื้อ (BUY)" if decision == "BUY" else "ขาย (SELL)"
        lines.append(f"- สัญญาณตอนนี้: {side_th}")
        lines.append(f"- แนะนำเข้า: Entry {_fmt_num(plan.get('entry'))} | SL {_fmt_num(plan.get('sl'))} | TP {_fmt_num(plan.get('tp'))}")
        mid = _midpoint(float(plan.get("entry", 0.0)), float(plan.get("tp", 0.0)))
        lines.append("")
        lines.append("4) ถ้าราคาไปทางที่ถูก (วิธีปรับแผน)")
        lines.append(f"- ถ้าราคาแตะ {_fmt_num(mid)} (ครึ่งทางไป TP) ให้เลื่อน SL มาใกล้ Entry เพื่อกันกำไรหาย")
        lines.append("- ถ้าราคาย่อลงแรงหลังจากแตะครึ่งทางแล้ว ให้ระวังกลับตัว อย่าเพิ่มไม้")
        lines.append("")
        lines.append("5) ถ้าราคาผิดทาง (ข้อห้ามสำคัญ)")
        lines.append("- ถ้าราคาหลุด SL ให้ตัดทันที ห้ามถัวเฉลี่ยขาดทุน/ห้ามเพิ่มไม้แก้มือ")

    if (not direct_plan) and buy_plan and trigger_buy is not None:
        lines.append("A) แผนฝั่งซื้อ (BUY) — ใช้เมื่อราคายืนยันขึ้น")
        lines.append(f"- เข้าเมื่อ: ราคา “ยืนเหนือ” {_fmt_num(trigger_buy)}")
        lines.append(f"- แนะนำเข้า: Entry {_fmt_num(buy_plan['entry'])} | SL {_fmt_num(buy_plan['sl'])} | TP {_fmt_num(buy_plan['tp'])}")
        mid = _midpoint(buy_plan["entry"], buy_plan["tp"])
        lines.append(f"- ถ้าราคาแตะ {_fmt_num(mid)} ให้เลื่อน SL มาใกล้ Entry")
        lines.append(f"- ถ้าราคาแตะ {_fmt_num(trigger_buy)} แล้ว “หลุดกลับลงมาเร็ว” ให้ระวังหลอก (อย่าไล่ราคา)")
        lines.append("")

    if (not direct_plan) and sell_plan and trigger_sell is not None:
        lines.append("B) แผนฝั่งขาย (SELL) — ใช้เมื่อราคายืนยันลง")
        lines.append(f"- เข้าเมื่อ: ราคา “หลุดต่ำกว่า” {_fmt_num(trigger_sell)}")
        lines.append(f"- แนะนำเข้า: Entry {_fmt_num(sell_plan['entry'])} | SL {_fmt_num(sell_plan['sl'])} | TP {_fmt_num(sell_plan['tp'])}")
        mid = _midpoint(sell_plan["entry"], sell_plan["tp"])
        lines.append(f"- ถ้าราคาแตะ {_fmt_num(mid)} ให้เลื่อน SL มาใกล้ Entry")
        lines.append(f"- ถ้าราคาแตะ {_fmt_num(trigger_sell)} แล้ว “เด้งกลับขึ้นเร็ว” ให้ระวังหลอก (อย่าไล่ราคา)")
        lines.append("")

    if (not direct_plan) and (not buy_plan) and (not sell_plan):
        lines.append("- ตอนนี้ยังให้จุดเข้า/ออกแบบชัด ๆ ไม่ได้ เพราะโหมดนี้ไม่ได้ข้อมูลครบทุก TF")
        lines.append("- แนะนำ: ใช้ MENTOR_MODE=engine เพื่อให้ระบบคำนวณแนวรับ/แนวต้าน (swing_high/low) แล้วจะได้จุดเข้า/SL/TP ที่ชัดเจน")
        lines.append("")

    lines.append("6) ข้อควรระวัง (อ่านง่าย ๆ)")
    if major_bias == "DOWN" and top_bias == "UP":
        lines.append("- กรอบใหญ่ลง แต่กรอบเล็กเด้งขึ้น: ระวังเด้งหลอก")
    elif major_bias == "UP" and top_bias == "DOWN":
        lines.append("- กรอบใหญ่ขึ้น แต่กรอบเล็กย่อลง: ระวังหลุดเป็นแค่ย่อหรือกลับตัวจริง")
    lines.append("- ถ้าราคาแกว่งแรงใกล้จุดเข้า: ลดขนาดไม้ลงก่อน")
    lines.append("- ถ้าไม่มั่นใจ: รอได้ ไม่ต้องเทรดทุกแท่ง")

    lines.append("")
    lines.append("7) วิธีดูอินดิเคเตอร์แบบมือโปร (อ้างอิงข้อมูลจริงตอนนี้)")
    st_label = "UP" if (isinstance(st_dir_event, int) and st_dir_event > 0) else ("DOWN" if isinstance(st_dir_event, int) and st_dir_event < 0 else "NA")
    st_dist_tag = _st_dist_level(st_distance_atr)
    lines.append(f"- SuperTrend ({event_timeframe}): ตอนนี้เทรนด์={st_label}")
    if st_dist_tag == "ไกล":
        lines.append("  • ห่างเส้นมาก: มักวิ่งมาไกลแล้ว ระวังไล่ราคา รอจังหวะย่อ/เด้งค่อยเข้า")
    elif st_dist_tag == "ใกล้":
        lines.append("  • ใกล้เส้น: จุดเข้า/SL มักคุมง่ายขึ้น แต่ต้องรอแท่งยืนยันก่อน")
    elif st_dist_tag == "กลาง":
        lines.append("  • ระยะห่างปานกลาง: ใช้โครงสร้างราคา + แผน A/B เป็นตัวตัดสินใจ")
    if major_bias == "DOWN" and st_label == "UP":
        lines.append("  • หมายเหตุ: กรอบใหญ่ลง แต่กรอบสั้นเป็นขาขึ้น ระวังสวนเทรนด์ใหญ่")
    if major_bias == "UP" and st_label == "DOWN":
        lines.append("  • หมายเหตุ: กรอบใหญ่ขึ้น แต่กรอบสั้นเป็นขาลง ระวังย่อหลอก/กลับตัว")

    bb_tag = _bb_level(bb_width_atr)
    lines.append("- Bollinger Bands:")
    if bb_tag == "กว้าง":
        lines.append("  • แบนด์กว้าง: ตลาดผันผวน/มีแรง (เหมาะกับแผนตามเทรนด์/เบรกเอาท์)")
    elif bb_tag == "แคบ":
        lines.append("  • แบนด์แคบ: ตลาดเงียบ/ไซด์เวย์ (ระวังเบรกหลอก)")
    elif bb_tag == "กลาง":
        lines.append("  • กลาง ๆ: รอให้ราคาเลือกทางชัด ๆ ก่อน")

    lines.append(f"- ATR ({event_timeframe}): ผันผวน={_atr_level(event_timeframe, atr)}")

    if trigger_buy is not None or trigger_sell is not None:
        lines.append("- BOS / โครงสร้างราคา:")
        if trigger_buy is not None:
            lines.append(f"  • แนวต้านสำคัญ≈{_fmt_num(trigger_buy)} (ถ้าทะลุแล้ว ‘ยืนได้’ โอกาสไปต่อสูงขึ้น)")
        if trigger_sell is not None:
            lines.append(f"  • แนวรับสำคัญ≈{_fmt_num(trigger_sell)} (ถ้าหลุดแล้ว ‘กลับไม่ขึ้น’ โอกาสลงต่อสูงขึ้น)")
        lines.append("  • วิธีมือโปร: รอแท่งปิด (close) ยืนยัน + ดูการย่อ/รีเทสต์ ไม่เข้าเพราะไส้เทียนอย่างเดียว")

    if alignment_score is None:
        align_tag = "ไม่ทราบ"
    else:
        s = int(alignment_score)
        align_tag = "สูง" if s >= 3 else ("กลาง" if s == 2 else "ต่ำ")
    lines.append(f"- Regime: {regime or '-'} | Alignment: {align_tag}")

    if (rr is not None) and (min_rr is not None):
        rr_ok = float(rr) >= float(min_rr)
        lines.append(f"- RR: {'ผ่านเกณฑ์' if rr_ok else 'ต่ำกว่าเกณฑ์'}")
        if not rr_ok:
            lines.append("  • ถ้า RR ต่ำกว่าเกณฑ์: มือโปรจะไม่ฝืนเข้า หรือจะปรับแผนให้ RR ดีขึ้นก่อน")
    if direct_plan:
        lines.append("  • ถ้าจะเข้าแผนนี้: อย่าลืม ‘ทำตาม SL’ คือเงื่อนไขสำคัญที่สุด")

    lines.append("")
    lines.append("8) อินดิเคเตอร์ (สรุปแนวโน้มแบบกระชับ)")
    lines.append(f"- RSI(14) ({event_timeframe}): {_rsi_level(rsi14_event)}")

    lines.append(f"- EMA ({event_timeframe}): แนวโน้มจากเส้นเฉลี่ย")
    if ema20_event is not None and ema50_event is not None:
        if float(ema20_event) > float(ema50_event):
            lines.append("  • ระยะสั้น: EMA20 อยู่เหนือ EMA50 (เอนไปทางขึ้น)")
        elif float(ema20_event) < float(ema50_event):
            lines.append("  • ระยะสั้น: EMA20 อยู่ใต้ EMA50 (เอนไปทางลง)")
    if current_price is not None and ema200_event is not None:
        if float(current_price) > float(ema200_event):
            lines.append("  • ระยะยาว: ราคาอยู่เหนือ EMA200 (ฝั่ง BUY จะปลอดภัยขึ้นเมื่อมีสัญญาณยืนยัน)")
        elif float(current_price) < float(ema200_event):
            lines.append("  • ระยะยาว: ราคาอยู่ใต้ EMA200 (ฝั่ง SELL ได้เปรียบกว่าเมื่อมีสัญญาณยืนยัน)")

    lines.append(f"- ADX(14) ({event_timeframe}): {_adx_level(adx14_event)}")
    if di_plus_event is not None and di_minus_event is not None:
        if float(di_plus_event) > float(di_minus_event):
            lines.append("  • Direction: แรงซื้อ (DI+) มากกว่าแรงขาย (DI-)")
        elif float(di_plus_event) < float(di_minus_event):
            lines.append("  • Direction: แรงขาย (DI-) มากกว่าแรงซื้อ (DI+)")

    lines.append("- เคล็ดลับแบบมือโปร: ให้ H1 เป็นตัวกรอง")
    h1_rsi_txt = _rsi_level(rsi14_h1)
    h1_adx_txt = _adx_level(adx14_h1)
    if current_price is not None and ema200_h1 is not None:
        h1_ema200_txt = "ราคาอยู่เหนือ EMA200" if float(current_price) > float(ema200_h1) else "ราคาอยู่ใต้ EMA200"
    else:
        h1_ema200_txt = "EMA200 ไม่ทราบ"
    lines.append(f"  • H1: {h1_rsi_txt} | {h1_ema200_txt} | {h1_adx_txt}")
    lines.append("  • ถ้า H1 เป็นขาลงและเทรนด์เริ่มชัด: อย่าฝืน BUY ใน M1 จนกว่าจะมีการกลับตัวชัดเจน")

    lines.append("")
    lines.append("TRADER MENTOR")
    lines.append("วิเคราะห์สภาวะตลาดตอนนี้")
    m15_bias_txt = str(m15_bias or "UNKNOWN")
    ema_bias_txt = str(ema_bias_m15 or "UNKNOWN")
    mtf_txt = "-" if (mtf_up_pct is None or mtf_dn_pct is None) else f"{_fmt_num(mtf_up_pct,1)}/{_fmt_num(mtf_dn_pct,1)}"
    spread_txt = "-" if spread_points is None else f"{_fmt_num(spread_points,0)}"
    last_txt = "-" if m15_last is None else _fmt_num(m15_last)
    ema20_txt = "-" if ema20_m15 is None else _fmt_num(ema20_m15)
    ema50_txt = "-" if ema50_m15 is None else _fmt_num(ema50_m15)
    zh_txt = "-" if zone_high_20_m15 is None else _fmt_num(zone_high_20_m15)
    zl_txt = "-" if zone_low_20_m15 is None else _fmt_num(zone_low_20_m15)
    lines.append(
        f"• Regime: {regime or '-'} | M15 bias: {m15_bias_txt} | EMA bias: {ema_bias_txt} | "
        f"MTF: {mtf_txt} | Spread ~{spread_txt} pts | Last {last_txt} | EMA20 {ema20_txt} / EMA50 {ema50_txt} | "
        f"โซน 20แท่ง: High {zh_txt} / Low {zl_txt}"
    )

    if zone_low_20_m15 is not None or ema20_m15 is not None:
        low_ref = zl_txt
        ema20_ref = ema20_txt
        lines.append(f"• แผน: รอให้ราคาหลุด low โซนล่าสุด ({low_ref})/ปิดต่ำกว่า EMA20(M15) ({ema20_ref}) แล้วค่อยพิจารณาเติมไม้ (ถ้ายังไม่ครบ 5 และพ้น cooldown)")
        lines.append(f"• ถ้าไม่: ถ้าราคาปิดเหนือ EMA20(M15) ({ema20_ref}) ต่อเนื่อง/ยืนกลับขึ้นแรง ให้หยุดเติมไม้ และระวังกลับตัวสวน")
    else:
        lines.append("• แผน: รอให้ราคายืนยันหลุด low โซนล่าสุด/ปิดต่ำกว่า EMA20(M15) แล้วค่อยพิจารณาเติมไม้ (ถ้ายังไม่ครบ 5 และพ้น cooldown)")
        lines.append("• ถ้าไม่: ถ้าราคาปิดเหนือ EMA20(M15) ต่อเนื่อง/ยืนกลับขึ้นแรง ให้หยุดเติมไม้ และระวังกลับตัวสวน")

    lines.append("• อย่า: ห้ามเฉลี่ยขาดทุน และห้ามขยับ SL ให้ไกลกว่าเดิม")
    lines.append("• ระวัง: ช่วงข่าว/rollover ถ้า spread กว้างผิดปกติให้พักก่อน")

    lines.append("ระวัง “ตลาดกลับตัวเร็ว”")
    lines.append("• ถ้าเพิ่งเข้าแล้วราคากลับสวนแรงใน 1-3 แท่ง M5 (กลับเข้ากรอบเดิมเร็ว) → มักเป็น whipsaw/false break")
    lines.append("• ถ้า spread กว้างผิดปกติช่วงข่าว/rollover → ราคาอาจกระชากไปมาและโดน SL ได้ง่ายขึ้น")
    lines.append("• ถ้าเริ่มมีสัญญาณฝั่งตรงข้ามที่ MTF สูงกว่าอย่างชัดเจน → โอกาสกลับตัวเพิ่มขึ้น")
    lines.append("• ต้องทำอะไร: ห้ามเพิ่มไม้/เฉลี่ยขาดทุน และห้ามขยับ SL ให้ไกลกว่าเดิม; ถ้าจะจัดการให้ทำเพื่อ “ลดความเสี่ยง” เท่านั้น (ปิดบางส่วน/ปิดทั้งไม้) แล้วรอระบบประเมินใหม่")

    if trader_mentor_ai:
        lines.append("")
        lines.append("AI Add-on:")
        lines.append(trader_mentor_ai)

    return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class TimeframeView:
    tf: str
    ok: bool
    close: float
    atr: float
    bb_width_atr: float
    st_dir: int
    st_distance_atr_signed: float


def _to_float(x: Any, default: float = float("nan")) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def _tf_label(st_dir: int) -> str:
    return "UP" if int(st_dir) > 0 else "DOWN"


class IntelligentMentorReadOnly:
    def __init__(
        self,
        *,
        config_path: str = "config.json",
        symbol: Optional[str] = None,
        event_timeframe: str = "M1",
        tfs: Sequence[str] = ("D1", "H4", "H1", "M30", "M15", "M5", "M1"),
        signal_url: Optional[str] = None,
    ) -> None:
        self.config_path = str(config_path)
        self.symbol_override = symbol.strip() if isinstance(symbol, str) and symbol.strip() else None
        self.event_timeframe = str(event_timeframe).upper().strip()
        self.tfs = [str(tf).upper().strip() for tf in tfs if str(tf).strip()]
        self.signal_url = (signal_url or os.environ.get("MENTOR_SIGNAL_URL") or "http://127.0.0.1:5000/api/signal_preview").strip()

        self.engine = None
        self.mode = "api"
        prefer = (os.environ.get("MENTOR_MODE") or "").strip().lower()
        if prefer not in ("", "api", "engine"):
            prefer = ""

        if prefer in ("", "engine"):
            try:
                from engine import TradingEngine  # type: ignore

                self.engine = TradingEngine(self.config_path)
                self.mode = "engine"
            except Exception:
                self.engine = None
                self.mode = "api"

    def analyze(self) -> Dict[str, Any]:
        symbol = self.symbol_override or "GOLD"

        if self.mode == "engine" and self.engine is not None:
            symbol = self.symbol_override or str(getattr(self.engine.cfg, "symbol", "GOLD"))
            pkg = self.engine.generate_signal_package(symbol=symbol, event_timeframe=self.event_timeframe)

            tf_rows: List[Dict[str, Any]] = []
            for tf in self.tfs:
                try:
                    b = self.engine._compute_tf_bundle(symbol, tf)
                except Exception:
                    b = {"tf": tf, "ok": False}

                ok = bool(b.get("ok"))

                df = b.get("df") if ok else None
                rsi14 = None
                ema20 = None
                ema50 = None
                ema200 = None
                adx14 = None
                di_p = None
                di_m = None
                try:
                    if df is not None:
                        closes = [float(x) for x in list(df["close"].values)]
                        rsi14 = _rsi_wilder(closes, 14)
                        ema20 = _ema(closes, 20)
                        ema50 = _ema(closes, 50)
                        ema200 = _ema(closes, 200)
                        adx14, di_p, di_m = _adx_wilder(df, 14)
                except Exception:
                    rsi14 = None
                    ema20 = None
                    ema50 = None
                    ema200 = None
                    adx14 = None
                    di_p = None
                    di_m = None

                zone_high_20 = None
                zone_low_20 = None
                try:
                    if df is not None and ("high" in df.columns) and ("low" in df.columns) and len(df) >= 5:
                        tail = df.tail(20)
                        zone_high_20 = float(tail["high"].astype(float).max())
                        zone_low_20 = float(tail["low"].astype(float).min())
                except Exception:
                    zone_high_20 = None
                    zone_low_20 = None

                st_dir = int(b.get("st_dir") or 0) if ok else 0
                tf_rows.append(
                    {
                        "tf": tf,
                        "ok": ok,
                        "close": _to_float(b.get("close")),
                        "atr": _to_float(b.get("atr")),
                        "bb_width_atr": _to_float(b.get("bb_width_atr")),
                        "st_dir": st_dir,
                        "st_dir_label": _tf_label(st_dir) if ok else "NA",
                        "st_distance_atr_signed": _to_float(b.get("st_distance_atr_signed")),
                        "rsi14": _nan_to_none(rsi14),
                        "ema20": _nan_to_none(ema20),
                        "ema50": _nan_to_none(ema50),
                        "ema200": _nan_to_none(ema200),
                        "adx14": _nan_to_none(adx14),
                        "di_plus": _nan_to_none(di_p),
                        "di_minus": _nan_to_none(di_m),
                        "zone_high_20": _nan_to_none(zone_high_20),
                        "zone_low_20": _nan_to_none(zone_low_20),
                    }
                )

            return {
                "ts": int(time.time()),
                "mode": "engine",
                "symbol": symbol,
                "event_timeframe": self.event_timeframe,
                "decision": pkg.get("decision"),
                "plan": pkg.get("plan"),
                "bias": pkg.get("bias"),
                "status": pkg.get("status"),
                "blocked_by": pkg.get("blocked_by"),
                "metrics": pkg.get("metrics"),
                "spread_points": _mt5_spread_points(symbol),
                "cfg": {
                    "bos_break_atr_min": getattr(self.engine.cfg, "bos_break_atr_min", None),
                    "min_rr": getattr(self.engine.cfg, "min_rr", None),
                    "st_relax_dist_atr_m1": getattr(self.engine.cfg, "st_relax_dist_atr_m1", None),
                    "bb_width_atr_min_m1": getattr(self.engine.cfg, "bb_width_atr_min_m1", None),
                    "bb_width_atr_min": getattr(self.engine.cfg, "bb_width_atr_min", None),
                },
                "timeframes": tf_rows,
            }

        ok, sig = http_get_json(self.signal_url)
        if not ok or not isinstance(sig, dict):
            return {
                "ts": int(time.time()),
                "mode": "api",
                "symbol": symbol,
                "status": "ERROR",
                "reason": "engine_import_failed_or_missing_deps",
                "detail": sig if isinstance(sig, str) else "signal_fetch_failed",
                "hint": {
                    "option_1_install": "python -m pip install numpy pandas MetaTrader5",
                    "option_2_run_api": "python api_server.py  (then MENTOR_SIGNAL_URL=http://127.0.0.1:5000/api/signal_preview)",
                },
            }

        symbol = self.symbol_override or str(sig.get("symbol") or "GOLD")
        return {
            "ts": int(time.time()),
            "mode": "api",
            "symbol": symbol,
            "event_timeframe": sig.get("event_timeframe"),
            "decision": sig.get("decision"),
            "plan": sig.get("plan"),
            "bias": sig.get("bias"),
            "status": sig.get("status"),
            "blocked_by": sig.get("blocked_by"),
            "metrics": sig.get("metrics"),
            "cfg": sig.get("cfg"),
            "timeframes": sig.get("timeframes"),
            "api_meta": sig.get("api_meta"),
        }

    def format_message(self, result: Dict[str, Any]) -> str:
        style = (os.environ.get("MENTOR_STYLE") or "beginner").strip().lower()
        mode = str(result.get("mode") or "engine")
        decision = str(result.get("decision") or "HOLD")
        bias = str(result.get("bias") or "unknown")
        status = str(result.get("status") or "")
        blocked_by = result.get("blocked_by")
        if isinstance(blocked_by, list):
            blocked_list = [str(x) for x in blocked_by if x]
        else:
            blocked_list = [str(blocked_by)] if blocked_by else []

        plan = result.get("plan") if isinstance(result.get("plan"), dict) else {}
        entry = plan.get("entry", "-")
        sl = plan.get("sl", "-")
        tp = plan.get("tp", "-")

        metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
        cfg = result.get("cfg") if isinstance(result.get("cfg"), dict) else {}
        align = metrics.get("alignment_score")
        bbwa = metrics.get("bb_width_atr")
        st_dir_ev = metrics.get("supertrend_dir_event")
        st_dist = metrics.get("supertrend_distance_atr")
        bos_up = metrics.get("bos_break_up_atr")
        bos_dn = metrics.get("bos_break_dn_atr")
        rr = metrics.get("rr")
        regime = metrics.get("regime")
        bos_thr = cfg.get("bos_break_atr_min", None)
        min_rr = cfg.get("min_rr", None)
        st_relax = cfg.get("st_relax_dist_atr_m1", None)

        tf_rows: List[Dict[str, Any]] = []
        tfs = result.get("timeframes")
        if isinstance(tfs, list):
            for tf in tfs:
                if isinstance(tf, dict):
                    tf_rows.append(tf)

        top_trend = {"UP": 0, "DOWN": 0}
        for tf in tf_rows:
            if not bool(tf.get("ok")):
                continue
            lab = str(tf.get("st_dir_label") or "")
            if lab in top_trend:
                top_trend[lab] += 1

        major_bias: Optional[str] = None
        if tf_rows:
            majors = [tf for tf in tf_rows if str(tf.get("tf") or "").upper() in ("D1", "H4") and bool(tf.get("ok"))]
            up = sum(1 for tf in majors if str(tf.get("st_dir_label") or "") == "UP")
            dn = sum(1 for tf in majors if str(tf.get("st_dir_label") or "") == "DOWN")
            if up > dn:
                major_bias = "UP"
            elif dn > up:
                major_bias = "DOWN"
            else:
                major_bias = "mixed" if majors else None

        if not tf_rows:
            top_bias = "N/A"
        else:
            top_bias = "mixed"
            if top_trend["UP"] > top_trend["DOWN"]:
                top_bias = "UP"
            elif top_trend["DOWN"] > top_trend["UP"]:
                top_bias = "DOWN"

        ev_close = None
        ev_atr = None
        for tf in tf_rows:
            if str(tf.get("tf") or "").upper() == str(result.get("event_timeframe") or "M1").upper() and bool(tf.get("ok")):
                ev_close = _nan_to_none(tf.get("close"))
                ev_atr = _nan_to_none(tf.get("atr"))
                break
        if ev_close is None or ev_atr is None:
            for tf in tf_rows:
                if str(tf.get("tf") or "").upper() == "M1" and bool(tf.get("ok")):
                    ev_close = _nan_to_none(tf.get("close"))
                    ev_atr = _nan_to_none(tf.get("atr"))
                    break

        swing_high = None
        swing_low = None
        if ev_close is not None and ev_atr is not None:
            bu = _nan_to_none(bos_up)
            bd = _nan_to_none(bos_dn)
            if bu is not None:
                swing_high = float(ev_close) - (float(bu) * float(ev_atr))
            if bd is not None:
                swing_low = float(ev_close) + (float(bd) * float(ev_atr))

        sl_atr = _nan_to_none(metrics.get("sl_atr"))
        tp_atr = _nan_to_none(metrics.get("tp_atr"))
        buy_setup: Optional[Dict[str, float]] = None
        sell_setup: Optional[Dict[str, float]] = None
        if ev_close is not None and ev_atr is not None and sl_atr is not None and tp_atr is not None:
            buy_setup = {
                "entry": float(ev_close),
                "sl": float(ev_close) - (float(ev_atr) * float(sl_atr)),
                "tp": float(ev_close) + (float(ev_atr) * float(tp_atr)),
            }
            sell_setup = {
                "entry": float(ev_close),
                "sl": float(ev_close) + (float(ev_atr) * float(sl_atr)),
                "tp": float(ev_close) - (float(ev_atr) * float(tp_atr)),
            }

        trigger_buy = swing_high
        trigger_sell = swing_low
        buy_plan = None
        sell_plan = None
        if trigger_buy is not None and ev_atr is not None and sl_atr is not None and tp_atr is not None:
            buy_plan = _mk_plan(float(trigger_buy), float(ev_atr), float(sl_atr), float(tp_atr), "BUY")
        if trigger_sell is not None and ev_atr is not None and sl_atr is not None and tp_atr is not None:
            sell_plan = _mk_plan(float(trigger_sell), float(ev_atr), float(sl_atr), float(tp_atr), "SELL")

        event_tf_name = str(result.get("event_timeframe") or "M1").upper()
        ev_row = None
        h1_row = None
        for tf in tf_rows:
            if not isinstance(tf, dict) or not bool(tf.get("ok")):
                continue
            tf_name = str(tf.get("tf") or "").upper()
            if tf_name == event_tf_name:
                ev_row = tf
            if tf_name == "H1":
                h1_row = tf

        rsi14_event = _nan_to_none(ev_row.get("rsi14")) if isinstance(ev_row, dict) else None
        ema20_event = _nan_to_none(ev_row.get("ema20")) if isinstance(ev_row, dict) else None
        ema50_event = _nan_to_none(ev_row.get("ema50")) if isinstance(ev_row, dict) else None
        ema200_event = _nan_to_none(ev_row.get("ema200")) if isinstance(ev_row, dict) else None
        adx14_event = _nan_to_none(ev_row.get("adx14")) if isinstance(ev_row, dict) else None
        di_plus_event = _nan_to_none(ev_row.get("di_plus")) if isinstance(ev_row, dict) else None
        di_minus_event = _nan_to_none(ev_row.get("di_minus")) if isinstance(ev_row, dict) else None

        rsi14_h1 = _nan_to_none(h1_row.get("rsi14")) if isinstance(h1_row, dict) else None
        ema200_h1 = _nan_to_none(h1_row.get("ema200")) if isinstance(h1_row, dict) else None
        adx14_h1 = _nan_to_none(h1_row.get("adx14")) if isinstance(h1_row, dict) else None

        if style != "verbose":
            m15_row = None
            for tf in tf_rows:
                if isinstance(tf, dict) and bool(tf.get("ok")) and str(tf.get("tf") or "").upper() == "M15":
                    m15_row = tf
                    break

            m15_bias = _trend_bias_label(str(m15_row.get("st_dir_label")) if isinstance(m15_row, dict) else None)
            ema_bias_m15 = _ema_bias(
                _nan_to_none(m15_row.get("ema20")) if isinstance(m15_row, dict) else None,
                _nan_to_none(m15_row.get("ema50")) if isinstance(m15_row, dict) else None,
            )
            mtf_up_pct, mtf_dn_pct = _mtf_ratio(tf_rows)

            spread_points = _nan_to_none(result.get("spread_points"))
            if spread_points is None:
                spread_points = _mt5_spread_points(str(result.get("symbol") or "GOLD"))

            m15_last = _nan_to_none(m15_row.get("close")) if isinstance(m15_row, dict) else None
            ema20_m15 = _nan_to_none(m15_row.get("ema20")) if isinstance(m15_row, dict) else None
            ema50_m15 = _nan_to_none(m15_row.get("ema50")) if isinstance(m15_row, dict) else None
            zone_high_20_m15 = _nan_to_none(m15_row.get("zone_high_20")) if isinstance(m15_row, dict) else None
            zone_low_20_m15 = _nan_to_none(m15_row.get("zone_low_20")) if isinstance(m15_row, dict) else None

            trader_mentor_ai = None
            if os.environ.get("MENTOR_TRADER_MENTOR_USE_AI", "0").strip() in ("1", "true", "TRUE", "yes", "YES"):
                ctx_for_ai = {
                    "symbol": result.get("symbol"),
                    "mode": mode,
                    "decision": decision,
                    "blocked_by": blocked_list,
                    "metrics": metrics,
                    "trader_mentor": {
                        "regime": regime,
                        "m15_bias": m15_bias,
                        "ema_bias_m15": ema_bias_m15,
                        "mtf_up_pct": mtf_up_pct,
                        "mtf_dn_pct": mtf_dn_pct,
                        "spread_points": spread_points,
                        "m15_last": m15_last,
                        "ema20_m15": ema20_m15,
                        "ema50_m15": ema50_m15,
                        "zone_high_20_m15": zone_high_20_m15,
                        "zone_low_20_m15": zone_low_20_m15,
                    },
                }
                ok_ai2, ai_text2 = _ai_mentor_narrative(ctx_for_ai)
                if ok_ai2 and isinstance(ai_text2, str) and ai_text2.strip():
                    trader_mentor_ai = ai_text2.strip()

            return _format_beginner_th(
                symbol=str(result.get("symbol") or "GOLD"),
                ts=int(result.get("ts") or 0),
                decision=decision,
                top_bias=top_bias,
                major_bias=major_bias,
                blocked_list=blocked_list,
                plan=plan,
                buy_plan=buy_plan,
                sell_plan=sell_plan,
                trigger_buy=trigger_buy,
                trigger_sell=trigger_sell,
                current_price=(float(ev_close) if ev_close is not None else None),
                event_timeframe=str(result.get("event_timeframe") or "M1"),
                atr=(float(ev_atr) if ev_atr is not None else None),
                bb_width_atr=(_nan_to_none(bbwa)),
                st_dir_event=(int(st_dir_ev) if isinstance(st_dir_ev, (int, float)) else None),
                st_distance_atr=(_nan_to_none(st_dist)),
                alignment_score=(int(_nan_to_none(align)) if _nan_to_none(align) is not None else None),
                regime=(str(regime) if regime is not None else None),
                rr=(_nan_to_none(rr)),
                min_rr=(_nan_to_none(min_rr)),
                rsi14_event=(_nan_to_none(rsi14_event)),
                ema20_event=(_nan_to_none(ema20_event)),
                ema50_event=(_nan_to_none(ema50_event)),
                ema200_event=(_nan_to_none(ema200_event)),
                adx14_event=(_nan_to_none(adx14_event)),
                di_plus_event=(_nan_to_none(di_plus_event)),
                di_minus_event=(_nan_to_none(di_minus_event)),
                rsi14_h1=(_nan_to_none(rsi14_h1)),
                ema200_h1=(_nan_to_none(ema200_h1)),
                adx14_h1=(_nan_to_none(adx14_h1)),
                m15_bias=m15_bias,
                ema_bias_m15=ema_bias_m15,
                mtf_up_pct=mtf_up_pct,
                mtf_dn_pct=mtf_dn_pct,
                spread_points=spread_points,
                m15_last=m15_last,
                ema20_m15=ema20_m15,
                ema50_m15=ema50_m15,
                zone_high_20_m15=zone_high_20_m15,
                zone_low_20_m15=zone_low_20_m15,
                trader_mentor_ai=trader_mentor_ai,
            )

        ctx_for_ai = {
            "symbol": result.get("symbol"),
            "mode": mode,
            "decision": decision,
            "bias": bias,
            "status": status,
            "plan": plan,
            "blocked_by": blocked_list,
            "metrics": metrics,
            "cfg": cfg,
            "timeframes": tf_rows,
        }
        ok_ai, ai_text = _ai_mentor_narrative(ctx_for_ai)
        ai_requested = os.environ.get("MENTOR_USE_AI", "0").strip() in ("1", "true", "TRUE", "yes", "YES")

        lines = [
            "HIM INTELLIGENT MENTOR (READ-ONLY)",
            f"time_utc={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(int(result.get('ts') or 0)))}",
            f"symbol={result.get('symbol')}  mode={mode}",
        ]
        if status == "ERROR":
            lines.append("TH: ระบบอ่าน engine โดยตรงไม่ได้ หรือ API ไม่พร้อม")
            lines.append("EN: Engine import failed or API is unavailable")
            lines.append(f"reason={result.get('reason')}")
            detail = str(result.get("detail") or "")
            if detail:
                lines.append(f"detail={detail[:220]}")
            hint = result.get("hint")
            if isinstance(hint, dict):
                if hint.get("option_1_install"):
                    lines.append(f"install={hint.get('option_1_install')}")
                if hint.get("option_2_run_api"):
                    lines.append(f"run_api={hint.get('option_2_run_api')}")
            return "\n".join(lines)

        lines.append("")
        lines.append("TH: สรุปภาพรวม (สอนดูกราฟ)")
        lines.append(f"- แนวโน้มรวมหลาย TF: {top_bias} | Alignment={_fmt_num(align,0)} | Regime={regime or '-'}")
        lines.append(f"- สัญญาณระบบ: decision={decision} | bias={bias or '-'} | RR={_fmt_num(rr,2)} (min_rr={_fmt_num(min_rr,2)})")
        if decision in ("BUY", "SELL"):
            lines.append(f"- แผนเข้าเทรด: เข้า={_fmt_num(entry)} SL={_fmt_num(sl)} TP={_fmt_num(tp)}")
        else:
            lines.append("- แผน (ยังไม่เข้า): รอเงื่อนไขผ่านก่อน แล้วค่อยเข้าแบบตามเทรนด์ (Top-down)")
            if buy_setup and sell_setup:
                lines.append(f"  • ถ้า BUY setup ผ่าน: entry≈{_fmt_num(buy_setup['entry'])} SL≈{_fmt_num(buy_setup['sl'])} TP≈{_fmt_num(buy_setup['tp'])}")
                lines.append(f"  • ถ้า SELL setup ผ่าน: entry≈{_fmt_num(sell_setup['entry'])} SL≈{_fmt_num(sell_setup['sl'])} TP≈{_fmt_num(sell_setup['tp'])}")
        if blocked_list:
            lines.append("- ตัวบล็อกหลัก:")
            for b in blocked_list[:5]:
                lines.append(f"  • {b}: {_explain_blocker_th(b)}")
            if "no_bos_break" in blocked_list and swing_high is not None and swing_low is not None:
                lines.append(f"  • ระดับ BOS โดยประมาณ: swing_high≈{_fmt_num(swing_high)} / swing_low≈{_fmt_num(swing_low)} (threshold={_fmt_num(bos_thr,2)} ATR)")
        lines.append(f"- BOS metric: up_atr={_fmt_num(bos_up,2)} dn_atr={_fmt_num(bos_dn,2)}")
        lines.append(f"- SuperTrend(event): dir={st_dir_ev} dist_atr={_fmt_num(st_dist,2)} (relax_m1={_fmt_num(st_relax,2)})")
        lines.append(f"- Volatility(event): bb_width_atr={_fmt_num(bbwa,2)}")

        lines.append("")
        lines.append("EN: Executive Summary")
        lines.append(f"- Multi-TF bias: {top_bias} | Alignment={_fmt_num(align,0)} | Regime={regime or '-'}")
        lines.append(f"- System call: decision={decision} | bias={bias or '-'} | RR={_fmt_num(rr,2)} (min_rr={_fmt_num(min_rr,2)})")
        if decision in ("BUY", "SELL"):
            lines.append(f"- Trade plan: entry={_fmt_num(entry)} SL={_fmt_num(sl)} TP={_fmt_num(tp)}")
        else:
            lines.append("- Plan (no entry yet): wait for blockers to clear, then follow the top-down trend")
            if buy_setup and sell_setup:
                lines.append(f"  • If BUY setup clears: entry≈{_fmt_num(buy_setup['entry'])} SL≈{_fmt_num(buy_setup['sl'])} TP≈{_fmt_num(buy_setup['tp'])}")
                lines.append(f"  • If SELL setup clears: entry≈{_fmt_num(sell_setup['entry'])} SL≈{_fmt_num(sell_setup['sl'])} TP≈{_fmt_num(sell_setup['tp'])}")
        if blocked_list:
            lines.append("- Key blockers:")
            for b in blocked_list[:5]:
                lines.append(f"  • {b}: {_explain_blocker_en(b)}")
            if "no_bos_break" in blocked_list and swing_high is not None and swing_low is not None:
                lines.append(f"  • Approx BOS levels: swing_high≈{_fmt_num(swing_high)} / swing_low≈{_fmt_num(swing_low)} (threshold={_fmt_num(bos_thr,2)} ATR)")
        lines.append(f"- BOS metric: up_atr={_fmt_num(bos_up,2)} dn_atr={_fmt_num(bos_dn,2)}")
        lines.append(f"- SuperTrend(event): dir={st_dir_ev} dist_atr={_fmt_num(st_dist,2)} (relax_m1={_fmt_num(st_relax,2)})")
        lines.append(f"- Volatility(event): bb_width_atr={_fmt_num(bbwa,2)}")

        lines.append("")
        lines.append("TH: Multi-Timeframe (D1/H4/H1/M30/M5/M1)")
        if tf_rows:
            for tf in tf_rows:
                tf_name = tf.get("tf", "?")
                ok = bool(tf.get("ok"))
                if not ok:
                    lines.append(f"- {tf_name}: NA")
                    continue
                st_label = tf.get("st_dir_label", "NA")
                dist = tf.get("st_distance_atr_signed", float("nan"))
                bbw = tf.get("bb_width_atr", float("nan"))
                atr = tf.get("atr", float("nan"))
                close = tf.get("close", float("nan"))
                lines.append(
                    f"- {tf_name}: เทรนด์={st_label} | close={_fmt_num(close)} | ATR={_fmt_num(atr)} | dist_ATR={_fmt_num(dist,2)} | BBw/ATR={_fmt_num(bbw,2)}"
                )
        else:
            lines.append("- (โหมด API ไม่มีรายละเอียดราย TF) ติดตั้ง engine deps + ใช้ MENTOR_MODE=engine เพื่อดูครบทุก TF")

        lines.append("")
        lines.append("EN: Multi-Timeframe (D1/H4/H1/M30/M5/M1)")
        if tf_rows:
            for tf in tf_rows:
                tf_name = tf.get("tf", "?")
                ok = bool(tf.get("ok"))
                if not ok:
                    lines.append(f"- {tf_name}: NA")
                    continue
                st_label = tf.get("st_dir_label", "NA")
                dist = tf.get("st_distance_atr_signed", float("nan"))
                bbw = tf.get("bb_width_atr", float("nan"))
                atr = tf.get("atr", float("nan"))
                close = tf.get("close", float("nan"))
                lines.append(
                    f"- {tf_name}: trend={st_label} | close={_fmt_num(close)} | ATR={_fmt_num(atr)} | dist_ATR={_fmt_num(dist,2)} | BBw/ATR={_fmt_num(bbw,2)}"
                )
        else:
            lines.append("- (API mode has no per-timeframe details) Install engine deps + set MENTOR_MODE=engine for full TF view")

        if ok_ai and ai_text:
            lines.append("")
            lines.append("AI Mentor (TH+EN):")
            lines.append(ai_text)
        elif ai_requested and not ok_ai:
            lines.append("")
            lines.append(f"AI Mentor: unavailable ({ai_text})")

        return "\n".join(lines)

    def send_to_mentor_room(self, text: str) -> Tuple[bool, int, str]:
        return telegram_send_text(text=text, chat_id_env="TELEGRAM_MENTOR_CHAT_ID")


def main() -> int:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    load_env_file(os.path.join(base_dir, ".env"), override=True)

    cfg_path = os.environ.get("MENTOR_CONFIG_PATH", "config.json")
    symbol = os.environ.get("MENTOR_SYMBOL", None)
    event_tf = os.environ.get("MENTOR_EVENT_TF", "M1")
    send_tg = os.environ.get("MENTOR_SEND_TELEGRAM", "1").strip() in ("1", "true", "TRUE", "yes", "YES")
    signal_url = os.environ.get("MENTOR_SIGNAL_URL", None)

    mentor = IntelligentMentorReadOnly(config_path=cfg_path, symbol=symbol, event_timeframe=event_tf, signal_url=signal_url)
    result = mentor.analyze()
    text = mentor.format_message(result)

    print(text)
    print(_safe_json(result))

    if send_tg:
        ok, status, body = mentor.send_to_mentor_room(text)
        if not ok:
            print(_safe_json({"telegram_ok": ok, "status": status, "detail": body[:400]}))
            return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
