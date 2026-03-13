"""
Hybrid Intelligence Mentor (HIM)
Trading Engine

Version: 2.0.5

Changelog:
- 2.0.5: Breakout proximity alert (config-only, no architecture change)
         - Add near_bos_buy / near_bos_sell flags
         - Add breakout_proximity_score (0..100)
         - Add eta_candles_to_bos estimate using ATR on LTF
         - Keep BOS distance fields (bos_ref_high/low, distance_to_bos_buy/sell, distance_to_bos)
         - Keep MTF fallback when HTF ranging
         - Keep hard gates: BOS required + SuperTrend must confirm
- 2.0.4: Add MTF fallback when HTF is ranging (HTF authority, MTF fallback)
- 2.0.3: Load config.json directly (no RuntimeConfig dependency)
"""

import json
import numpy as np
import MetaTrader5 as mt5


class TradingEngine:
    def __init__(self, config_path: str = "config.json"):
        self.config_path = config_path
        self.cfg = self.load_config()

    # ==========================
    # CONFIG
    # ==========================
    def load_config(self):
        with open(self.config_path, "r", encoding="utf-8") as f:
            return json.load(f)

    # ==========================
    # MT5 TIMEFRAMES
    # ==========================
    def tf(self, name: str) -> int:
        mapping = {
            "M5": mt5.TIMEFRAME_M5,
            "M15": mt5.TIMEFRAME_M15,
            "M30": mt5.TIMEFRAME_M30,
            "H1": mt5.TIMEFRAME_H1,
            "H4": mt5.TIMEFRAME_H4,
            "D1": mt5.TIMEFRAME_D1,
        }
        key = str(name).upper().strip()
        if key not in mapping:
            raise ValueError(f"Unsupported timeframe name: {name}")
        return mapping[key]

    def get_data(self, symbol: str, tf: int, bars: int = 600):
        rates = mt5.copy_rates_from_pos(symbol, tf, 0, int(bars))
        if rates is None or len(rates) == 0:
            raise RuntimeError(f"No MT5 data for {symbol}, tf={tf}, bars={bars}")
        return rates

    # ==========================
    # STRUCTURE (PIVOT/SWING)
    # ==========================
    def pivots(self, high, low, n: int):
        hi = []
        lo = []
        n = max(1, int(n))
        for i in range(n, len(high) - n):
            if high[i] == max(high[i - n : i + n + 1]):
                hi.append(i)
            if low[i] == min(low[i - n : i + n + 1]):
                lo.append(i)
        return hi, lo

    def structure(self, data, sens: int):
        high = data["high"]
        low = data["low"]

        piv_hi, piv_lo = self.pivots(high, low, sens)

        swing_highs = [float(high[i]) for i in piv_hi]
        swing_lows = [float(low[i]) for i in piv_lo]

        last_high = swing_highs[-1] if swing_highs else None
        last_low = swing_lows[-1] if swing_lows else None

        trend = "ranging"
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            if swing_highs[-1] > swing_highs[-2] and swing_lows[-1] > swing_lows[-2]:
                trend = "bullish"
            elif swing_highs[-1] < swing_highs[-2] and swing_lows[-1] < swing_lows[-2]:
                trend = "bearish"

        return trend, last_high, last_low

    # ==========================
    # ATR
    # ==========================
    def atr(self, high, low, close, period: int):
        period = max(1, int(period))
        prev_close = np.roll(close, 1)
        prev_close[0] = close[0]
        tr = np.maximum(
            high - low,
            np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)),
        )
        atr = np.zeros(len(close), dtype=float)
        alpha = 1.0 / period
        for i in range(1, len(close)):
            atr[i] = atr[i - 1] * (1 - alpha) + tr[i] * alpha
        return atr

    # ==========================
    # SUPERTREND (direction only)
    # ==========================
    def supertrend(self, high, low, close, period: int, mult: float):
        atr = self.atr(high, low, close, period)
        mid = (high + low) / 2.0
        upper = mid + float(mult) * atr
        lower = mid - float(mult) * atr

        direction = np.ones(len(close), dtype=int)
        for i in range(1, len(close)):
            if close[i] > upper[i - 1]:
                direction[i] = 1
            elif close[i] < lower[i - 1]:
                direction[i] = -1
            else:
                direction[i] = direction[i - 1]
        return direction

    # ==========================
    # BOS
    # ==========================
    def bos_check(self, direction: str, price: float, bos_high, bos_low) -> bool:
        if direction == "BUY" and bos_high is not None:
            return float(price) > float(bos_high)
        if direction == "SELL" and bos_low is not None:
            return float(price) < float(bos_low)
        return False

    # ==========================
    # DIRECTION (HTF + MTF FALLBACK)
    # ==========================
    def decide_direction(self, htf_trend: str, mtf_trend: str):
        # Returns (direction, bias_source, blocked_reason_if_any)
        if htf_trend == "bullish":
            return "BUY", "HTF", None
        if htf_trend == "bearish":
            return "SELL", "HTF", None

        # HTF ranging -> fallback to MTF
        if mtf_trend == "bullish":
            return "BUY", "MTF_FALLBACK", None
        if mtf_trend == "bearish":
            return "SELL", "MTF_FALLBACK", None

        return "NONE", "NO_CLEAR_BIAS", "no_clear_bias"

    # ==========================
    # PROXIMITY / ETA
    # ==========================
    def proximity_score(self, dist_buy, dist_sell, atr_value: float, threshold: float):
        """
        Score 0..100. Closer to BOS => higher score.
        Uses normalized distance by ATR and also absolute threshold (points).
        """
        atr_value = float(atr_value) if atr_value is not None else 0.0
        atr_value = max(atr_value, 1e-9)
        threshold = max(float(threshold), 1e-9)

        # distance in points (positive means "still not broken")
        best = None
        side = None

        if dist_buy is not None and float(dist_buy) >= 0:
            best = float(dist_buy)
            side = "BUY"
        if dist_sell is not None and float(dist_sell) >= 0:
            if best is None or float(dist_sell) < best:
                best = float(dist_sell)
                side = "SELL"

        if best is None:
            return 0.0, None  # cannot score

        # normalize by ATR: 0 ATR away => 100, 2 ATR away => low
        norm = best / atr_value

        # combine two heuristics:
        # - absolute threshold gate: if within threshold => strong score
        # - normalized distance: if within 0.5 ATR => high score
        score_abs = 100.0 * max(0.0, min(1.0, (threshold - best) / threshold))
        score_atr = 100.0 * max(0.0, min(1.0, (0.5 - norm) / 0.5))

        score = max(score_abs, score_atr)
        return float(max(0.0, min(score, 100.0))), side

    def eta_candles(self, dist: float, atr_value: float):
        """
        Rough ETA in candles: distance / ATR_per_candle.
        """
        if dist is None:
            return None
        atr_value = float(atr_value) if atr_value is not None else None
        if atr_value is None or atr_value <= 1e-9:
            return None
        d = float(dist)
        if d < 0:
            return 0.0
        return float(d / atr_value)

    # ==========================
    # MAIN
    # ==========================
    def generate_signal_package(self):
        cfg = self.cfg

        symbol = str(cfg.get("symbol", "GOLD"))

        tf_cfg = cfg.get("timeframes", {}) or {}
        sens_cfg = cfg.get("structure_sensitivity", {}) or {}
        st_cfg = cfg.get("supertrend", {}) or {}
        risk_cfg = cfg.get("risk", {}) or {}

        # Proximity settings (optional; safe defaults)
        prox_cfg = cfg.get("breakout_proximity", {}) or {}
        prox_threshold = float(prox_cfg.get("threshold", 0.5))  # points
        prox_min_score = float(prox_cfg.get("min_score", 70.0))  # 0..100

        htf_name = str(tf_cfg.get("htf", "H1")).upper()
        mtf_name = str(tf_cfg.get("mtf", "M30")).upper()
        ltf_name = str(tf_cfg.get("ltf", "M5")).upper()

        sens_htf = int(sens_cfg.get("htf", 4))
        sens_mtf = int(sens_cfg.get("mtf", 3))
        sens_ltf = int(sens_cfg.get("ltf", 1))

        st_period = int(st_cfg.get("period", 21))
        st_mult = float(st_cfg.get("multiplier", 4.0))

        atr_period = int(risk_cfg.get("atr_period", 14))

        # Fetch data
        htf = self.get_data(symbol, self.tf(htf_name), bars=600)
        mtf = self.get_data(symbol, self.tf(mtf_name), bars=800)
        ltf = self.get_data(symbol, self.tf(ltf_name), bars=1200)

        # Structure
        htf_trend, _, _ = self.structure(htf, sens_htf)
        mtf_trend, _, _ = self.structure(mtf, sens_mtf)
        ltf_trend, bos_ref_high, bos_ref_low = self.structure(ltf, sens_ltf)

        # Decide direction using HTF then fallback MTF
        direction, bias_source, bias_block = self.decide_direction(htf_trend, mtf_trend)

        # Arrays
        close_arr = np.asarray(ltf["close"], dtype=float)
        high_arr = np.asarray(ltf["high"], dtype=float)
        low_arr = np.asarray(ltf["low"], dtype=float)

        price_now = float(close_arr[-1])

        # ATR for ETA / normalization
        atr_arr = self.atr(high_arr, low_arr, close_arr, atr_period)
        atr_value = float(atr_arr[-1]) if len(atr_arr) > 0 else None

        # SuperTrend on LTF (always compute; ok even when bias unclear)
        st_dir = self.supertrend(high_arr, low_arr, close_arr, st_period, st_mult)[-1]
        st_state = "bullish" if int(st_dir) == 1 else "bearish"

        st_ok = False
        if direction in ("BUY", "SELL"):
            st_ok = ((direction == "BUY") and (st_state == "bullish")) or ((direction == "SELL") and (st_state == "bearish"))

        # BOS on LTF reference
        bos = False
        if direction in ("BUY", "SELL"):
            bos = self.bos_check(direction, price_now, bos_ref_high, bos_ref_low)

        # Distances
        dist_buy = None if bos_ref_high is None else float(bos_ref_high) - price_now
        dist_sell = None if bos_ref_low is None else price_now - float(bos_ref_low)

        dist = None
        if direction == "BUY":
            dist = dist_buy
        elif direction == "SELL":
            dist = dist_sell

        # Proximity flags (independent from direction)
        near_buy = (dist_buy is not None) and (float(dist_buy) >= 0.0) and (float(dist_buy) <= prox_threshold)
        near_sell = (dist_sell is not None) and (float(dist_sell) >= 0.0) and (float(dist_sell) <= prox_threshold)

        prox_score, prox_side = self.proximity_score(dist_buy, dist_sell, atr_value, prox_threshold)

        # ETA to BOS (per side + chosen side)
        eta_buy = self.eta_candles(dist_buy, atr_value) if dist_buy is not None else None
        eta_sell = self.eta_candles(dist_sell, atr_value) if dist_sell is not None else None
        eta = self.eta_candles(dist, atr_value) if dist is not None else None

        # Proximity alert (non-blocking)
        proximity_alert = bool(prox_score >= prox_min_score)

        # HARD GATES (final spec)
        blocked = []
        if bias_block is not None:
            blocked.append(bias_block)

        # Enforce BOS/ST gates only if direction decided (BUY/SELL)
        if direction in ("BUY", "SELL"):
            if not bos:
                blocked.append("no_bos")
            if not st_ok:
                blocked.append("supertrend_contradiction")

        direction_out = "NONE" if blocked else direction

        # Context package
        return {
            "symbol": symbol,
            "direction": direction_out,
            "context": {
                "HTF_trend": htf_trend,
                "MTF_trend": mtf_trend,
                "LTF_trend": ltf_trend,

                "bias_source": bias_source,

                "bos": bool(bos),
                "bos_ref_high": bos_ref_high,
                "bos_ref_low": bos_ref_low,

                "distance_to_bos": dist,
                "distance_to_bos_buy": dist_buy,
                "distance_to_bos_sell": dist_sell,

                "near_bos_buy": bool(near_buy),
                "near_bos_sell": bool(near_sell),

                "atr": atr_value,
                "eta_candles_to_bos": eta,
                "eta_candles_to_bos_buy": eta_buy,
                "eta_candles_to_bos_sell": eta_sell,

                "breakout_proximity_threshold": prox_threshold,
                "breakout_proximity_min_score": prox_min_score,
                "breakout_proximity_score": prox_score,
                "breakout_proximity_side": prox_side,
                "breakout_proximity_alert": proximity_alert,

                # Always show ST direction; gate uses st_ok when direction decided
                "supertrend": st_state,
                "supertrend_ok": bool(st_ok) if direction in ("BUY", "SELL") else False,

                "blocked_by": ",".join(blocked) if blocked else None,

                "timeframes": {"htf": htf_name, "mtf": mtf_name, "ltf": ltf_name},
                "structure_sensitivity": {"htf": sens_htf, "mtf": sens_mtf, "ltf": sens_ltf},
                "supertrend_params": {"period": st_period, "multiplier": st_mult},
            },
        }