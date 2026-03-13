"""
Mentor Executor - Execution Controller (FINAL SPEC)
Version: 2.0.0
Changelog:
- 2.0.0: Integrate engine v2 flow + strict AI JSON validation gate + config.json hot reload.
Rules enforced:
- Execute ONLY if: approved == true AND confidence >= threshold AND enable_execution == true
- BOS required is handled upstream (engine+ai), executor is final safety gate.
- Never freeze silently: log critical failures.
"""

from __future__ import annotations

import os
import json
import time
import logging
from typing import Any, Dict

import MetaTrader5 as mt5

from engine import TradingEngine
from ai_mentor import AIMentor


# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("him_system.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("HIM")


class HotConfig:
    def __init__(self, path: str):
        self.path = path
        self._mtime = 0.0
        self._cache: Dict[str, Any] = {}

    def load(self) -> Dict[str, Any]:
        try:
            if not os.path.exists(self.path):
                return self._cache or {}
            m = os.path.getmtime(self.path)
            if m <= self._mtime and self._cache:
                return self._cache
            with open(self.path, "r", encoding="utf-8") as f:
                self._cache = json.load(f) or {}
            self._mtime = m
            return self._cache
        except Exception as e:
            logger.error(f"CRITICAL: config.json load failed: {e}")
            return self._cache or {}


class MentorExecutor:
    def __init__(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_path = os.path.join(base_dir, "config.json")
        self.hcfg = HotConfig(self.config_path)

        self.engine = TradingEngine(config_path=self.config_path)
        self.ai = AIMentor()

        self._ensure_mt5()

    def _ensure_mt5(self) -> None:
        if mt5.terminal_info() is not None:
            return
        if not mt5.initialize():
            err = mt5.last_error()
            raise RuntimeError(f"MT5 initialize failed: {err}")
        logger.info("MT5 Connected")

    def _get_tick(self, symbol: str):
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            raise RuntimeError(f"Cannot get tick for symbol: {symbol}")
        return tick

    def _place_market_order(self, symbol: str, direction: str, lot: float, sl: float, tp: float) -> bool:
        """
        Minimal MT5 execution. Uses market order.
        Safety: relies on enable_execution gate.
        """
        info = mt5.symbol_info(symbol)
        if info is None:
            logger.error(f"Symbol not found: {symbol}")
            return False
        if not info.visible:
            if not mt5.symbol_select(symbol, True):
                logger.error(f"Cannot select symbol: {symbol}")
                return False

        tick = self._get_tick(symbol)
        price = tick.ask if direction == "BUY" else tick.bid
        order_type = mt5.ORDER_TYPE_BUY if direction == "BUY" else mt5.ORDER_TYPE_SELL

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(lot),
            "type": order_type,
            "price": float(price),
            "sl": float(sl),
            "tp": float(tp),
            "deviation": 30,
            "magic": 20260225,
            "comment": "HIM v2",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_FOK,
        }

        result = mt5.order_send(request)
        if result is None:
            logger.error("order_send returned None")
            return False

        if result.retcode != mt5.TRADE_RETCODE_DONE:
            logger.error(f"Order failed retcode={result.retcode} comment={getattr(result, 'comment', '')}")
            return False

        logger.info(f"ORDER OK: ticket={result.order} price={price}")
        return True

    @staticmethod
    def _sanity(direction: str, entry: float, sl: float, tp: float) -> bool:
        if direction == "BUY":
            return sl < entry < tp
        if direction == "SELL":
            return tp < entry < sl
        return False

    def run_once(self) -> None:
        cfg = self.hcfg.load()

        symbol = str(cfg.get("symbol", "GOLD"))
        enable_execution = bool(cfg.get("enable_execution", False))
        conf_th = int(cfg.get("confidence_threshold", 75))

        # Optional lot control (simple)
        lot = float(cfg.get("lot", 0.01))

        # 1) Engine signal package
        try:
            pkg = self.engine.generate_signal_package()
        except Exception as e:
            logger.error(f"CRITICAL: engine failed: {e}")
            return

        direction = str(pkg.get("direction", "NONE"))
        score = float(pkg.get("score", 0.0))
        rr = float(pkg.get("rr", 0.0))
        logger.info(f"SIGNAL: {direction} score={score:.1f} rr={rr:.2f}")

        # No trade if NONE
        if direction not in ("BUY", "SELL"):
            logger.info("No clear bias. Skip.")
            return

        # 2) AI approval (strict JSON only)
        ai_resp = self.ai.approve_trade(pkg)
        if not AIMentor.validate_response(ai_resp):
            logger.error("CRITICAL: AI response invalid shape. Skip execution.")
            return

        approved = bool(ai_resp["approved"])
        confidence = int(ai_resp["confidence"])
        entry = float(ai_resp["entry"])
        sl = float(ai_resp["sl"])
        tp = float(ai_resp["tp"])

        logger.info(f"AI: approved={approved} confidence={confidence}%")
        if ai_resp["reasoning"]:
            logger.info("REASONING:\n" + ai_resp["reasoning"])
        if ai_resp["warnings"]:
            logger.info("WARNINGS:\n" + ai_resp["warnings"])

        # 3) Final safety gate
        if not approved:
            logger.info("Gate: rejected by AI.")
            return
        if confidence < conf_th:
            logger.info(f"Gate: confidence below threshold ({confidence} < {conf_th}).")
            return
        if not enable_execution:
            logger.info("Gate: enable_execution=false (dry-run).")
            return
        if not self._sanity(direction, entry, sl, tp):
            logger.error("CRITICAL: price sanity check failed. Skip execution.")
            return

        # 4) Execute
        ok = self._place_market_order(symbol=symbol, direction=direction, lot=lot, sl=sl, tp=tp)
        if ok:
            logger.info("EXECUTED: success")
        else:
            logger.info("EXECUTED: failed")

    def run_loop(self, interval_sec: int = 15) -> None:
        logger.info("HIM MentorExecutor loop started.")
        while True:
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"CRITICAL LOOP ERROR: {e}")
            time.sleep(interval_sec)


if __name__ == "__main__":
    ex = MentorExecutor()
    # default loop
    ex.run_loop(interval_sec=15)