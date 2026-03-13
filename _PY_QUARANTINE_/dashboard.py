"""HIM System Dashboard"""
import MetaTrader5 as mt5
from datetime import datetime, timedelta
import os

def show_dashboard():
    """Display system status dashboard"""
    
    # Clear screen
    os.system('cls' if os.name == 'nt' else 'clear')
    
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║          HIM SYSTEM DASHBOARD - สถานะระบบ                  ║")
    print("╚══════════════════════════════════════════════════════════════╝\n")
    
    # MT5 Connection
    if mt5.initialize():
        print("✅ MT5 Connection: CONNECTED")
        
        account_info = mt5.account_info()
        if account_info:
            print(f"   Account: {account_info.login}")
            print(f"   Balance: ${account_info.balance:,.2f}")
            print(f"   Equity: ${account_info.equity:,.2f}")
            print(f"   Margin: ${account_info.margin:,.2f}")
            print(f"   Free Margin: ${account_info.margin_free:,.2f}")
            
            # Profit/Loss
            profit = account_info.profit
            profit_emoji = "📈" if profit >= 0 else "📉"
            print(f"   {profit_emoji} P/L: ${profit:,.2f}")
        
        # Open Positions
        positions = mt5.positions_get()
        print(f"\n📊 Open Positions: {len(positions) if positions else 0}")
        
        if positions:
            for pos in positions:
                pos_type = "🟢 BUY" if pos.type == mt5.ORDER_TYPE_BUY else "🔴 SELL"
                pos_profit = pos.profit
                profit_emoji = "💰" if pos_profit >= 0 else "💸"
                
                print(f"\n   Ticket: #{pos.ticket}")
                print(f"   Type: {pos_type}")
                print(f"   Symbol: {pos.symbol}")
                print(f"   Volume: {pos.volume} lots")
                print(f"   Entry: {pos.price_open:.2f}")
                print(f"   Current: {pos.price_current:.2f}")
                print(f"   SL: {pos.sl:.2f}")
                print(f"   TP: {pos.tp:.2f}")
                print(f"   {profit_emoji} Profit: ${pos_profit:.2f}")
        
        # Today's Orders
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        deals = mt5.history_deals_get(today, datetime.now())
        
        if deals:
            print(f"\n📈 Today's Trades: {len(deals)}")
            
            total_profit = sum(deal.profit for deal in deals)
            profit_emoji = "💰" if total_profit >= 0 else "💸"
            print(f"   {profit_emoji} Total P/L: ${total_profit:.2f}")
        
        mt5.shutdown()
    else:
        print("❌ MT5 Connection: DISCONNECTED")
    
    # Log File Status
    print("\n📝 Log File:")
    if os.path.exists('him_system.log'):
        log_size = os.path.getsize('him_system.log') / 1024  # KB
        print(f"   Size: {log_size:.2f} KB")
        
        # Last 5 lines
        with open('him_system.log', 'r', encoding='utf-8') as f:
            lines = f.readlines()
            print("\n   Last 5 Events:")
            for line in lines[-5:]:
                print(f"   {line.strip()}")
    else:
        print("   ⚠️ Log file not found")
    
    print("\n" + "="*60)
    print(f"🕐 Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)


if __name__ == "__main__":
    import time
    
    try:
        while True:
            show_dashboard()
            print("\n⏳ Refreshing in 30 seconds... (Ctrl+C to exit)")
            time.sleep(30)
    except KeyboardInterrupt:
        print("\n👋 Dashboard closed")