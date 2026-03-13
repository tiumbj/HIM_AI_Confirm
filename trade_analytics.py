"""
trade_analytics.py - Trade Analytics Daemon
Version: v1.0.0
Purpose: Analyze trading performance and generate insights
"""

import os
import sys
import time
import json
import signal
import logging
from datetime import datetime
from typing import Dict, Any, List
from collections import defaultdict

# Configuration
VERSION = "v1.0.0"
PROJECT_ROOT = os.getcwd()
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
ANALYTICS_LOG = os.path.join(LOG_DIR, "trade_analytics.jsonl")
TRADES_LOG = os.path.join(LOG_DIR, "trades.jsonl")
CONFIG_PATH = os.environ.get("HIM_CONFIG_PATH", "config.json")

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "trade_analytics.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TradeAnalytics")

class TradeAnalytics:
    """Trade Analytics Class for analyzing trading performance"""
    
    def __init__(self):
        self.running = True
        self.config = self._load_config()
        self.analysis_interval = self.config.get("analytics", {}).get("analysis_interval", 60.0)
        self.trade_history_days = self.config.get("analytics", {}).get("trade_history_days", 7)
        self.min_trades_for_analysis = self.config.get("analytics", {}).get("min_trades_for_analysis", 10)
        self.analytics_enabled = self.config.get("analytics", {}).get("enabled", True)
        
        # Cache for analytics results
        self.last_analysis = None
        self.last_analysis_time = 0
        self.trade_cache: List[Dict[str, Any]] = []
        
        logger.info(f"Trade Analytics initialized v{VERSION}")
        logger.info(f"Analysis interval: {self.analysis_interval}s")
        logger.info(f"Trade history: {self.trade_history_days} days")
        logger.info(f"Analytics enabled: {self.analytics_enabled}")
        
        self._log_event("initialized", {
            "analysis_interval": self.analysis_interval,
            "trade_history_days": self.trade_history_days,
            "min_trades_for_analysis": self.min_trades_for_analysis,
            "enabled": self.analytics_enabled
        })
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file"""
        try:
            if os.path.exists(CONFIG_PATH):
                with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
        return {}
    
    def _log_event(self, event: str, data: Dict[str, Any]) -> None:
        """Log event to JSONL file"""
        try:
            log_entry = {
                "ts": int(time.time()),
                "datetime": datetime.utcnow().isoformat(),
                "event": event,
                "version": VERSION,
                **data
            }
            with open(ANALYTICS_LOG, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry) + "\n")
        except Exception:
            pass
    
    def load_recent_trades(self) -> List[Dict[str, Any]]:
        """Load recent trades from trades log"""
        trades = []
        cutoff_time = time.time() - (self.trade_history_days * 24 * 3600)
        
        try:
            if os.path.exists(TRADES_LOG):
                with open(TRADES_LOG, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            trade = json.loads(line.strip())
                            if trade.get("ts", 0) >= cutoff_time:
                                trades.append(trade)
                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            logger.error(f"Failed to load trades: {e}")
        
        logger.info(f"Loaded {len(trades)} recent trades")
        return trades
    
    def calculate_win_rate(self, trades: List[Dict[str, Any]]) -> float:
        """Calculate win rate from trades"""
        if not trades:
            return 0.0
        
        winning_trades = sum(1 for t in trades if t.get("profit", 0) > 0)
        return (winning_trades / len(trades)) * 100
    
    def calculate_profit_factor(self, trades: List[Dict[str, Any]]) -> float:
        """Calculate profit factor (gross profit / gross loss)"""
        gross_profit = sum(t.get("profit", 0) for t in trades if t.get("profit", 0) > 0)
        gross_loss = abs(sum(t.get("profit", 0) for t in trades if t.get("profit", 0) < 0))
        
        if gross_loss == 0:
            return float('inf') if gross_profit > 0 else 0.0
        
        return gross_profit / gross_loss
    
    def calculate_sharpe_ratio(self, trades: List[Dict[str, Any]]) -> float:
        """Calculate approximate Sharpe ratio from trades"""
        if len(trades) < 2:
            return 0.0
        
        profits = [t.get("profit", 0) for t in trades]
        avg_profit = sum(profits) / len(profits)
        
        if avg_profit <= 0:
            return 0.0
        
        # Calculate standard deviation
        variance = sum((p - avg_profit) ** 2 for p in profits) / len(profits)
        std_dev = variance ** 0.5
        
        if std_dev == 0:
            return 0.0
        
        # Rough Sharpe ratio (assuming risk-free rate = 0)
        return avg_profit / std_dev
    
    def calculate_max_drawdown(self, trades: List[Dict[str, Any]]) -> float:
        """Calculate maximum drawdown from trades"""
        if not trades:
            return 0.0
        
        cumulative = 0
        peak = 0
        max_dd = 0
        
        for trade in trades:
            cumulative += trade.get("profit", 0)
            if cumulative > peak:
                peak = cumulative
            dd = peak - cumulative
            if dd > max_dd:
                max_dd = dd
        
        return max_dd
    
    def analyze_by_symbol(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze performance by symbol"""
        symbol_stats = defaultdict(lambda: {"trades": 0, "profit": 0.0, "wins": 0})
        
        for trade in trades:
            symbol = trade.get("symbol", "UNKNOWN")
            profit = trade.get("profit", 0)
            
            symbol_stats[symbol]["trades"] += 1
            symbol_stats[symbol]["profit"] += profit
            if profit > 0:
                symbol_stats[symbol]["wins"] += 1
        
        # Calculate win rates
        result = {}
        for symbol, stats in symbol_stats.items():
            win_rate = (stats["wins"] / stats["trades"] * 100) if stats["trades"] > 0 else 0
            result[symbol] = {
                "trades": stats["trades"],
                "total_profit": round(stats["profit"], 2),
                "win_rate": round(win_rate, 2),
                "avg_profit": round(stats["profit"] / stats["trades"], 2) if stats["trades"] > 0 else 0
            }
        
        return result
    
    def analyze_by_hour(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze performance by hour of day"""
        hour_stats = defaultdict(lambda: {"trades": 0, "profit": 0.0, "wins": 0})
        
        for trade in trades:
            ts = trade.get("ts", 0)
            if ts:
                dt = datetime.fromtimestamp(ts)
                hour = dt.hour
                profit = trade.get("profit", 0)
                
                hour_stats[hour]["trades"] += 1
                hour_stats[hour]["profit"] += profit
                if profit > 0:
                    hour_stats[hour]["wins"] += 1
        
        # Calculate best/worst hours
        result = {}
        for hour in range(24):
            if hour in hour_stats:
                stats = hour_stats[hour]
                win_rate = (stats["wins"] / stats["trades"] * 100) if stats["trades"] > 0 else 0
                result[str(hour)] = {
                    "trades": stats["trades"],
                    "total_profit": round(stats["profit"], 2),
                    "win_rate": round(win_rate, 2),
                    "avg_profit": round(stats["profit"] / stats["trades"], 2) if stats["trades"] > 0 else 0
                }
        
        return result
    
    def generate_analytics(self) -> Dict[str, Any]:
        """Generate comprehensive trade analytics"""
        trades = self.load_recent_trades()
        
        if len(trades) < self.min_trades_for_analysis:
            logger.info(f"Not enough trades for analysis: {len(trades)} < {self.min_trades_for_analysis}")
            return {
                "status": "insufficient_data",
                "trade_count": len(trades),
                "min_required": self.min_trades_for_analysis,
                "timestamp": time.time()
            }
        
        # Calculate metrics
        total_profit = sum(t.get("profit", 0) for t in trades)
        win_rate = self.calculate_win_rate(trades)
        profit_factor = self.calculate_profit_factor(trades)
        sharpe_ratio = self.calculate_sharpe_ratio(trades)
        max_drawdown = self.calculate_max_drawdown(trades)
        
        # Calculate additional stats
        winning_trades = [t for t in trades if t.get("profit", 0) > 0]
        losing_trades = [t for t in trades if t.get("profit", 0) < 0]
        
        avg_win = sum(t.get("profit", 0) for t in winning_trades) / len(winning_trades) if winning_trades else 0
        avg_loss = sum(abs(t.get("profit", 0)) for t in losing_trades) / len(losing_trades) if losing_trades else 0
        
        # Generate comprehensive analytics
        analytics = {
            "status": "success",
            "timestamp": time.time(),
            "datetime": datetime.utcnow().isoformat(),
            "period_days": self.trade_history_days,
            "trade_count": len(trades),
            "total_profit": round(total_profit, 2),
            "win_rate": round(win_rate, 2),
            "profit_factor": round(profit_factor, 2),
            "sharpe_ratio": round(sharpe_ratio, 2),
            "max_drawdown": round(max_drawdown, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "win_loss_ratio": round(abs(avg_win / avg_loss), 2) if avg_loss != 0 else 0,
            "by_symbol": self.analyze_by_symbol(trades),
            "by_hour": self.analyze_by_hour(trades),
            "best_trade": max(trades, key=lambda x: x.get("profit", 0)) if trades else None,
            "worst_trade": min(trades, key=lambda x: x.get("profit", 0)) if trades else None,
        }
        
        # Log analytics
        self._log_event("analytics_generated", {
            "trade_count": len(trades),
            "total_profit": total_profit,
            "win_rate": win_rate,
            "profit_factor": profit_factor
        })
        
        return analytics
    
    def run_analysis_cycle(self) -> None:
        """Run one analysis cycle"""
        if not self.analytics_enabled:
            return
        
        current_time = time.time()
        if current_time - self.last_analysis_time >= self.analysis_interval:
            logger.info("Running trade analysis...")
            self.last_analysis = self.generate_analytics()
            self.last_analysis_time = current_time
            
            # Print summary
            if self.last_analysis.get("status") == "success":
                logger.info(f"Analysis complete: {self.last_analysis['trade_count']} trades, " +
                           f"Profit: ${self.last_analysis['total_profit']}, " +
                           f"Win Rate: {self.last_analysis['win_rate']}%")
    
    def get_current_analytics(self) -> Dict[str, Any]:
        """Get current analytics (cached or run new)"""
        if self.last_analysis is None:
            self.last_analysis = self.generate_analytics()
            self.last_analysis_time = time.time()
        
        return self.last_analysis
    
    def run(self) -> None:
        """Main run loop"""
        logger.info("Trade Analytics daemon started")
        self._log_event("started", {})
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        while self.running:
            try:
                self.run_analysis_cycle()
                time.sleep(1.0)  # Check every second for interval
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(5)
        
        logger.info("Trade Analytics shutting down")
        self._log_event("stopped", {})
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

def main():
    """Main entry point"""
    analytics = TradeAnalytics()
    
    # Parse command line arguments
    daemon_mode = "--daemon" in sys.argv
    if daemon_mode:
        logger.info("Running in daemon mode")
    
    try:
        analytics.run()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
