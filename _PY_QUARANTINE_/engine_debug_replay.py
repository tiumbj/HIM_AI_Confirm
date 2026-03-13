# =========================================
# Engine Debug Replay Probe v1.0.0
# Purpose: Inspect raw engine outputs
# =========================================

import MetaTrader5 as mt5
from engine import TradingEngine
import datetime as dt

CONFIG_PATH = "config.json"
SYMBOL = "GOLD"
LTF_TF = mt5.TIMEFRAME_M5
BARS = 300

def main():
    if not mt5.initialize():
        print("MT5 init failed")
        return

    mt5.symbol_select(SYMBOL, True)

    engine = TradingEngine(CONFIG_PATH)

    rates = mt5.copy_rates_from_pos(SYMBOL, LTF_TF, 0, BARS)

    if rates is None or len(rates) < 100:
        print("Not enough bars from MT5")
        return

    print("Bars:", len(rates))
    print("=====================================")

    for i in range(200, min(len(rates), 230)):
        pkg = engine.generate_signal_package()

        ctx = pkg.get("context", {})

        print(
            f"{i} | "
            f"dir={pkg.get('direction')} | "
            f"blocked={ctx.get('blocked_by')} | "
            f"bias={ctx.get('direction_bias')} | "
            f"watch={ctx.get('watch_state')}"
        )

    print("=====================================")

if __name__ == "__main__":
    main()