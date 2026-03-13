"""
MT5 Trade Tester - ทดสอบส่งคำสั่ง Buy/Sell ไปยัง MT5
"""

import MetaTrader5 as mt5
import time
from datetime import datetime
import sys

# ============================================================================
# CONFIGURATION
# ============================================================================
MT5_LOGIN = 168021026
MT5_PASS = "Tium@232520"
MT5_SERVER = "XMGlobal-MT5 2"
SYMBOL = "GOLD"  # หรือ "GOLD" ตามที่ MT5 ใช้
VOLUME = 0.01  # ขนาด Lot (0.01 = micro lot)
DEVIATION = 20  # Slippage ที่ยอมรับได้

def connect_mt5():
    """เชื่อมต่อกับ MT5"""
    print("\n🔌 Connecting to MT5...")
    
    # ปิด connection เก่า
    mt5.shutdown()
    
    # เชื่อมต่อ
    initialized = mt5.initialize(
        login=MT5_LOGIN,
        password=MT5_PASS,
        server=MT5_SERVER,
        timeout=30000
    )
    
    if not initialized:
        print(f"❌ MT5 Connection failed: {mt5.last_error()}")
        return False
    
    print("✅ MT5 Connected successfully")
    
    # แสดงข้อมูลบัญชี
    account = mt5.account_info()
    if account:
        print(f"   Account: {account.login}")
        print(f"   Balance: ${account.balance:,.2f}")
        print(f"   Equity: ${account.equity:,.2f}")
        print(f"   Currency: {account.currency}")
    
    return True

def get_symbol_info(symbol):
    """ตรวจสอบข้อมูล Symbol"""
    symbol_info = mt5.symbol_info(symbol)
    
    if symbol_info is None:
        print(f"❌ Symbol {symbol} not found")
        print(f"   Available symbols: {mt5.symbols_get()[:5]}...")
        return None
    
    if not symbol_info.visible:
        print(f"📢 Symbol {symbol} not visible - selecting...")
        if not mt5.symbol_select(symbol, True):
            print(f"❌ Cannot select {symbol}")
            return None
        print(f"✅ Symbol {symbol} selected")
    
    print(f"\n📊 Symbol Info:")
    print(f"   Bid: {symbol_info.bid}")
    print(f"   Ask: {symbol_info.ask}")
    print(f"   Spread: {symbol_info.spread} points")
    print(f"   Min Volume: {symbol_info.volume_min}")
    print(f"   Max Volume: {symbol_info.volume_max}")
    print(f"   Volume Step: {symbol_info.volume_step}")
    
    return symbol_info

def send_market_order(symbol, order_type, volume, sl_points=None, tp_points=None):
    """
    ส่งคำสั่ง Market Order
    
    Args:
        symbol: ชื่อ Symbol
        order_type: 'BUY' หรือ 'SELL'
        volume: ขนาด Lot
        sl_points: ระยะ Stop Loss (จุด)
        tp_points: ระยะ Take Profit (จุด)
    """
    
    print(f"\n{'='*60}")
    print(f"📤 Sending {order_type} Order")
    print(f"{'='*60}")
    
    # เตรียมราคา
    tick = mt5.symbol_info_tick(symbol)
    if not tick:
        print("❌ Cannot get tick")
        return None
    
    if order_type.upper() == 'BUY':
        price = tick.ask
        order_type_mt5 = mt5.ORDER_TYPE_BUY
        sl = price - sl_points if sl_points else 0
        tp = price + tp_points if tp_points else 0
    else:  # SELL
        price = tick.bid
        order_type_mt5 = mt5.ORDER_TYPE_SELL
        sl = price + sl_points if sl_points else 0
        tp = price - tp_points if tp_points else 0
    
    # แสดงรายละเอียดคำสั่ง
    print(f"\n📋 Order Details:")
    print(f"   Symbol: {symbol}")
    print(f"   Type: {order_type}")
    print(f"   Volume: {volume} lots")
    print(f"   Price: {price}")
    print(f"   SL: {sl if sl > 0 else 'No SL'}")
    print(f"   TP: {tp if tp > 0 else 'No TP'}")
    
    # สร้าง request
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": float(volume),
        "type": order_type_mt5,
        "price": price,
        "sl": sl if sl > 0 else 0,
        "tp": tp if tp > 0 else 0,
        "deviation": DEVIATION,
        "magic": 2345600,
        "comment": "Test Order from Script",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    
    # ส่งคำสั่ง
    print("\n⏳ Sending order...")
    result = mt5.order_send(request)
    
    # ตรวจสอบผลลัพธ์
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"\n❌ Order failed!")
        print(f"   Error Code: {result.retcode}")
        print(f"   Comment: {result.comment}")
        
        # อธิบาย Error Code
        error_codes = {
            10004: "Requote",
            10006: "Request rejected",
            10007: "Request canceled by trader",
            10008: "Order placed",
            10009: "Modified",
            10010: "Canceled",
            10011: "Partially filled",
            10012: "Filled",
            10013: "Expired",
            10014: "Order accepted",
            10015: "Order pending",
            10016: "Invalid stops",
            10017: "Invalid volume",
            10018: "Market closed",
            10019: "Insufficient money"
        }
        
        if result.retcode in error_codes:
            print(f"   Reason: {error_codes[result.retcode]}")
        
        return None
    
    # แสดงผลสำเร็จ
    print(f"\n✅ Order successful!")
    print(f"   Ticket: #{result.order}")
    print(f"   Volume: {result.volume} lots")
    print(f"   Price: {result.price}")
    print(f"   Comment: {result.comment}")
    
    return result

def check_positions():
    """ตรวจสอบ Positions ที่เปิดอยู่"""
    positions = mt5.positions_get()
    
    if not positions:
        print("\n📭 No open positions")
        return []
    
    print(f"\n📍 Open Positions ({len(positions)}):")
    print("-" * 80)
    
    for pos in positions:
        profit_color = "\033[92m" if pos.profit >= 0 else "\033[91m"
        print(f"   #{pos.ticket} | {pos.symbol} | {'BUY' if pos.type==0 else 'SELL'} | "
              f"{pos.volume} lots | Entry: {pos.price_open} | Current: {pos.price_current} | "
              f"P/L: {profit_color}${pos.profit:.2f}\033[0m")
    
    return positions

def close_position(ticket):
    """ปิด Position ตาม Ticket"""
    position = mt5.positions_get(ticket=ticket)
    
    if not position:
        print(f"❌ Position #{ticket} not found")
        return False
    
    position = position[0]
    tick = mt5.symbol_info_tick(position.symbol)
    
    # สร้าง request เพื่อปิด
    close_request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": position.symbol,
        "volume": position.volume,
        "type": mt5.ORDER_TYPE_BUY if position.type == 1 else mt5.ORDER_TYPE_SELL,
        "position": ticket,
        "price": tick.ask if position.type == 1 else tick.bid,
        "deviation": DEVIATION,
        "magic": 2345600,
        "comment": "Close from Tester",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    
    result = mt5.order_send(close_request)
    
    if result.retcode == mt5.TRADE_RETCODE_DONE:
        print(f"✅ Position #{ticket} closed")
        return True
    else:
        print(f"❌ Close failed: {result.comment}")
        return False

def close_all_positions():
    """ปิดทุก Position"""
    positions = mt5.positions_get()
    
    if not positions:
        print("📭 No positions to close")
        return
    
    print(f"\n🔄 Closing {len(positions)} positions...")
    
    for pos in positions:
        close_position(pos.ticket)
        time.sleep(1)  # รอ 1 วินาทีระหว่างการปิด

def test_buy():
    """ทดสอบส่งคำสั่ง BUY"""
    print("\n" + "="*60)
    print("🚀 TESTING BUY ORDER")
    print("="*60)
    
    # คำนวณ SL/TP จาก ATR
    print("\n📊 Calculating ATR...")
    rates = mt5.copy_rates_from_pos(SYMBOL, mt5.TIMEFRAME_H1, 0, 20)
    if rates is not None:
        import numpy as np
        high = np.array([r['high'] for r in rates])
        low = np.array([r['low'] for r in rates])
        close = np.array([r['close'] for r in rates])
        
        # ATR อย่างง่าย
        tr = np.maximum(high[1:] - low[1:], 
                       np.abs(high[1:] - close[:-1]),
                       np.abs(low[1:] - close[:-1]))
        atr = np.mean(tr)
        print(f"   ATR: {atr:.2f}")
        
        sl_points = atr * 1.5
        tp_points = atr * 3.0
    else:
        sl_points = 200  # Default สำหรับ Gold
        tp_points = 400
        print(f"   Using default SL/TP: {sl_points}/{tp_points}")
    
    return send_market_order(SYMBOL, 'BUY', VOLUME, sl_points, tp_points)

def test_sell():
    """ทดสอบส่งคำสั่ง SELL"""
    print("\n" + "="*60)
    print("🚀 TESTING SELL ORDER")
    print("="*60)
    
    # คำนวณ SL/TP จาก ATR
    rates = mt5.copy_rates_from_pos(SYMBOL, mt5.TIMEFRAME_H1, 0, 20)
    if rates is not None:
        import numpy as np
        high = np.array([r['high'] for r in rates])
        low = np.array([r['low'] for r in rates])
        close = np.array([r['close'] for r in rates])
        
        tr = np.maximum(high[1:] - low[1:], 
                       np.abs(high[1:] - close[:-1]),
                       np.abs(low[1:] - close[:-1]))
        atr = np.mean(tr)
        print(f"   ATR: {atr:.2f}")
        
        sl_points = atr * 1.5
        tp_points = atr * 3.0
    else:
        sl_points = 200
        tp_points = 400
    
    return send_market_order(SYMBOL, 'SELL', VOLUME, sl_points, tp_points)

# ============================================================================
# MAIN MENU
# ============================================================================
if __name__ == "__main__":
    print("\n" + "="*60)
    print("💰 MT5 TRADE TESTER")
    print("="*60)
    print(f"Symbol: {SYMBOL}")
    print(f"Volume: {VOLUME} lots")
    print("="*60)
    
    # เชื่อมต่อ MT5
    if not connect_mt5():
        sys.exit(1)
    
    # ตรวจสอบ Symbol
    symbol_info = get_symbol_info(SYMBOL)
    if not symbol_info:
        mt5.shutdown()
        sys.exit(1)
    
    while True:
        print("\n" + "="*60)
        print("📋 MENU")
        print("="*60)
        print("1. 🟢 TEST BUY ORDER")
        print("2. 🔴 TEST SELL ORDER")
        print("3. 📍 Check Open Positions")
        print("4. ❌ Close All Positions")
        print("5. 🔄 Get Current Price")
        print("6. 📊 Account Info")
        print("0. 🚪 Exit")
        print("="*60)
        
        choice = input("\nSelect option: ").strip()
        
        if choice == '1':
            result = test_buy()
            if result:
                print("\n✅ BUY order sent successfully!")
        
        elif choice == '2':
            result = test_sell()
            if result:
                print("\n✅ SELL order sent successfully!")
        
        elif choice == '3':
            check_positions()
        
        elif choice == '4':
            confirm = input("Close ALL positions? (y/n): ").lower()
            if confirm == 'y':
                close_all_positions()
        
        elif choice == '5':
            tick = mt5.symbol_info_tick(SYMBOL)
            if tick:
                print(f"\n📊 {SYMBOL} Price:")
                print(f"   Bid: {tick.bid}")
                print(f"   Ask: {tick.ask}")
                print(f"   Spread: {tick.ask - tick.bid:.2f}")
        
        elif choice == '6':
            account = mt5.account_info()
            if account:
                print(f"\n💼 Account Info:")
                print(f"   Login: {account.login}")
                print(f"   Server: {account.server}")
                print(f"   Balance: ${account.balance:,.2f}")
                print(f"   Equity: ${account.equity:,.2f}")
                print(f"   Margin: ${account.margin:,.2f}")
                print(f"   Free Margin: ${account.margin_free:,.2f}")
                print(f"   Leverage: 1:{account.leverage}")
        
        elif choice == '0':
            print("\n👋 Closing connections...")
            mt5.shutdown()
            print("✅ Done")
            break
        
        else:
            print("❌ Invalid option")
        
        input("\nPress Enter to continue...")
