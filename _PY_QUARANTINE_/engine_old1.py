"""
Hybrid Intelligence Mentor (HIM) - Trading Engine Module
High-performance data processing & technical analysis using Polars
"""

import MetaTrader5 as mt5
import polars as pl
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import requests
from dataclasses import dataclass
import pytz

from config import (
    SYMBOL, SYMBOL_NEWS, TIMEFRAMES, INDICATORS, RISK, SYSTEM,
    NEWS_API_URL, NEWS_API_KEY, NEWS_LOOKBACK_HOURS,
    CONFLUENCE_WEIGHTS, TIMEFRAME_NAMES
)


# ============================================================================
# DATA CLASSES
# ============================================================================
@dataclass
class MarketStructure:
    """Market structure detection results"""
    trend: str  # "bullish", "bearish", "ranging"
    swing_highs: List[float]
    swing_lows: List[float]
    support_levels: List[float]
    resistance_levels: List[float]
    order_blocks: List[Dict[str, Any]]
    fvgs: List[Dict[str, Any]]
    bos_detected: bool
    choch_detected: bool


@dataclass
class PriceAction:
    """Price action pattern detection"""
    pinbar_bullish: bool
    pinbar_bearish: bool
    liquidity_sweep_high: bool
    liquidity_sweep_low: bool
    pattern_strength: float


@dataclass
class TechnicalSnapshot:
    """Complete technical analysis snapshot"""
    timeframe: str
    trend: str
    ema_fast: float
    ema_slow: float
    ema_signal: str  # "bullish_cross", "bearish_cross", "neutral"
    macd_value: float
    macd_signal: float
    macd_histogram: float
    rsi: float
    atr: float
    sar: float
    sar_trend: str  # "bullish", "bearish"
    market_structure: MarketStructure
    price_action: PriceAction
    current_price: float
    timestamp: datetime


@dataclass
class ConfluenceAnalysis:
    """Confluence score and breakdown"""
    total_score: float
    components: Dict[str, float]
    signal_type: str  # "BUY", "SELL", "NONE"
    entry_price: float
    stop_loss: float
    take_profit: float
    risk_reward: float
    explanation: str


# ============================================================================
# TRADING ENGINE CLASS
# ============================================================================
class TradingEngine:
    """High-performance trading engine using Polars"""
    
    def __init__(self):
        self.timezone = pytz.timezone(SYSTEM.TIMEZONE)
        self.cache: Dict[str, pl.LazyFrame] = {}
    
    # ========================================================================
    # DATA INGESTION
    # ========================================================================
    
    def get_mt5_data(self, symbol: str, timeframe: int, n_bars: int = SYSTEM.DATA_LOOKBACK) -> pl.LazyFrame:
        """
        Fetch OHLCV data from MT5 and convert to Polars LazyFrame
        
        Args:
            symbol: MT5 symbol name
            timeframe: MT5 timeframe constant
            n_bars: Number of bars to fetch
            
        Returns:
            Polars LazyFrame with OHLCV data
        """
        # Fetch data from MT5
        rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, n_bars)
        
        if rates is None or len(rates) == 0:
            raise ValueError(f"Failed to fetch data for {symbol} on {TIMEFRAME_NAMES.get(timeframe, timeframe)}")
        
        # Convert to Polars DataFrame with proper timezone
        df = pl.DataFrame({
            'time': pl.Series([datetime.fromtimestamp(r['time'], tz=self.timezone) for r in rates]),
            'open': pl.Series(rates['open'], dtype=pl.Float64),
            'high': pl.Series(rates['high'], dtype=pl.Float64),
            'low': pl.Series(rates['low'], dtype=pl.Float64),
            'close': pl.Series(rates['close'], dtype=pl.Float64),
            'tick_volume': pl.Series(rates['tick_volume'], dtype=pl.Int64),
            'spread': pl.Series(rates['spread'], dtype=pl.Int32),
            'real_volume': pl.Series(rates['real_volume'], dtype=pl.Int64)
        })
        
        # Return as LazyFrame for optimized computation
        return df.lazy()
    
    def get_news_sentiment(self, symbol: str = SYMBOL_NEWS) -> Dict[str, Any]:
        """
        Fetch latest news        sentiment from MarketAux API
        
        Args:
            symbol: News symbol (e.g., "XAU" for gold)
            
        Returns:
            Dictionary with sentiment analysis
        """
        try:
            # Calculate time range
            end_time = datetime.now(self.timezone)
            start_time = end_time - timedelta(hours=NEWS_LOOKBACK_HOURS)
            
            # Prepare API request
            params = {
                'api_token': NEWS_API_KEY,
                'symbols': symbol,
                'filter_entities': 'true',
                'language': 'en',
                'published_after': start_time.isoformat(),
                'limit': 50
            }
            
            response = requests.get(NEWS_API_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'data' not in data or len(data['data']) == 0:
                return {
                    'sentiment': 'neutral',
                    'score': 0.0,
                    'news_count': 0,
                    'headlines': []
                }
            
            # Analyze sentiment from news
            news_items = data['data']
            sentiment_scores = []
            headlines = []
            
            for item in news_items:
                title = item.get('title', '')
                description = item.get('description', '')
                entities = item.get('entities', [])
                
                # Extract sentiment score from entities
                for entity in entities:
                    if entity.get('symbol') == symbol:
                        sentiment = entity.get('sentiment_score', 0.0)
                        sentiment_scores.append(sentiment)
                        headlines.append({
                            'title': title,
                            'sentiment': sentiment,
                            'published': item.get('published_at', '')
                        })
                        break
            
            # Calculate aggregate sentiment
            if sentiment_scores:
                avg_sentiment = np.mean(sentiment_scores)
                if avg_sentiment > 0.1:
                    sentiment_label = 'bullish'
                elif avg_sentiment < -0.1:
                    sentiment_label = 'bearish'
                else:
                    sentiment_label = 'neutral'
            else:
                avg_sentiment = 0.0
                sentiment_label = 'neutral'
            
            return {
                'sentiment': sentiment_label,
                'score': float(avg_sentiment),
                'news_count': len(headlines),
                'headlines': headlines[:5]  # Top 5 headlines
            }
            
        except Exception as e:
            print(f"[WARNING] News API Error: {str(e)}")
            return {
                'sentiment': 'neutral',
                'score': 0.0,
                'news_count': 0,
                'headlines': [],
                'error': str(e)
            }
    
    # ========================================================================
    # TECHNICAL INDICATORS
    # ========================================================================
    
    def apply_technical_indicators(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply all technical indicators using Polars expressions
        
        Args:
            df: LazyFrame with OHLCV data
            
        Returns:
            LazyFrame with all technical indicators
        """
        df = df.with_columns([
            # ============ EMA Indicators ============
            pl.col('close').ewm_mean(span=INDICATORS.EMA_FAST, adjust=False).alias('ema_fast'),
            pl.col('close').ewm_mean(span=INDICATORS.EMA_SLOW, adjust=False).alias('ema_slow'),
            
            # ============ RSI Calculation ============
            # Calculate price changes
            (pl.col('close') - pl.col('close').shift(1)).alias('price_change')
        ]).with_columns([
            # Separate gains and losses
            pl.when(pl.col('price_change') > 0)
              .then(pl.col('price_change'))
              .otherwise(0.0).alias('gain'),
            pl.when(pl.col('price_change') < 0)
              .then(-pl.col('price_change'))
              .otherwise(0.0).alias('loss')
        ]).with_columns([
            # Average gains and losses
            pl.col('gain').ewm_mean(span=INDICATORS.RSI_PERIOD, adjust=False).alias('avg_gain'),
            pl.col('loss').ewm_mean(span=INDICATORS.RSI_PERIOD, adjust=False).alias('avg_loss')
        ]).with_columns([
            # RSI calculation
            (100 - (100 / (1 + (pl.col('avg_gain') / pl.col('avg_loss'))))).alias('rsi')
        ])
        
        # ============ ATR Calculation ============
        df = df.with_columns([
            (pl.col('high') - pl.col('low')).alias('tr1'),
            (pl.col('high') - pl.col('close').shift(1)).abs().alias('tr2'),
            (pl.col('low') - pl.col('close').shift(1)).abs().alias('tr3')
        ]).with_columns([
            pl.max_horizontal(['tr1', 'tr2', 'tr3']).alias('true_range')
        ]).with_columns([
            pl.col('true_range').ewm_mean(span=INDICATORS.ATR_PERIOD, adjust=False).alias('atr')
        ])
        
        # ============ MACD Calculation ============
        df = df.with_columns([
            pl.col('close').ewm_mean(span=INDICATORS.MACD_FAST, adjust=False).alias('ema_macd_fast'),
            pl.col('close').ewm_mean(span=INDICATORS.MACD_SLOW, adjust=False).alias('ema_macd_slow')
        ]).with_columns([
            (pl.col('ema_macd_fast') - pl.col('ema_macd_slow')).alias('macd_line')
        ]).with_columns([
            pl.col('macd_line').ewm_mean(span=INDICATORS.MACD_SIGNAL, adjust=False).alias('macd_signal')
        ]).with_columns([
            (pl.col('macd_line') - pl.col('macd_signal')).alias('macd_histogram')
        ])
        
        # ============ High/Low for structure ============
        df = df.with_columns([
            pl.col('high').rolling_max(window_size=INDICATORS.SWING_LOOKBACK).alias('swing_high'),
            pl.col('low').rolling_min(window_size=INDICATORS.SWING_LOOKBACK).alias('swing_low')
        ])
        
        return df
    
    def calculate_parabolic_sar(self, df_collected: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate Parabolic SAR indicator
        
        Args:
            df_collected: Collected DataFrame with OHLC data
            
        Returns:
            DataFrame with SAR column added
        """
        high = df_collected['high'].to_numpy()
        low = df_collected['low'].to_numpy()
        close = df_collected['close'].to_numpy()
        
        n = len(high)
        sar = np.zeros(n)
        trend = np.ones(n, dtype=int)  # 1 = bullish, -1 = bearish
        ep = np.zeros(n)  # Extreme point
        af = np.full(n, INDICATORS.SAR_STEP)  # Acceleration factor
        
        # Initialize
        sar[0] = low[0]
        trend[0] = 1
        ep[0] = high[0]
        
        for i in range(1, n):
            # Calculate SAR
            sar[i] = sar[i-1] + af[i-1] * (ep[i-1] - sar[i-1])
            
            # Check for trend reversal
            if trend[i-1] == 1:  # Bullish trend
                if low[i] < sar[i]:
                    # Reverse to bearish
                    trend[i] = -1
                    sar[i] = ep[i-1]
                    ep[i] = low[i]
                    af[i] = INDICATORS.SAR_STEP
                else:
                    trend[i] = 1
                    if high[i] > ep[i-1]:
                        ep[i] = high[i]
                        af[i] = min(af[i-1] + INDICATORS.SAR_STEP, INDICATORS.SAR_MAX)
                    else:
                        ep[i] = ep[i-1]
                        af[i] = af[i-1]
            else:  # Bearish trend
                if high[i] > sar[i]:
                    # Reverse to bullish
                    trend[i] = 1
                    sar[i] = ep[i-1]
                    ep[i] = high[i]
                    af[i] = INDICATORS.SAR_STEP
                else:
                    trend[i] = -1
                    if low[i] < ep[i-1]:
                        ep[i] = low[i]
                        af[i] = min(af[i-1] + INDICATORS.SAR_STEP, INDICATORS.SAR_MAX)
                    else:
                        ep[i] = ep[i-1]
                        af[i] = af[i-1]
        
        # Add to DataFrame
        df_with_sar = df_collected.with_columns([
            pl.Series('sar', sar),
            pl.Series('sar_trend', trend)
        ])
        
        return df_with_sar
    
    # ========================================================================
    # MARKET STRUCTURE DETECTION
    # ========================================================================
    
    def detect_market_structure(self, df: pl.DataFrame) -> MarketStructure:
        """
        Detect market structure using Dow Theory principles
        
        Args:
            df: Collected DataFrame with indicators
            
        Returns:
            MarketStructure object
        """
        # Extract data
        high = df['high'].to_numpy()
        low = df['low'].to_numpy()
        close = df['close'].to_numpy()
        
        # ============ Detect Swing Points ============
        swing_highs = []
        swing_lows = []
        
        for i in range(INDICATORS.SWING_LOOKBACK, len(high) - INDICATORS.SWING_LOOKBACK):
            # Swing high: highest point in window
            if high[i] == max(high[i-INDICATORS.SWING_LOOKBACK:i+INDICATORS.SWING_LOOKBACK+1]):
                swing_highs.append(high[i])
            
            # Swing low: lowest point in window
            if low[i] == min(low[i-INDICATORS.SWING_LOOKBACK:i+INDICATORS.SWING_LOOKBACK+1]):
                swing_lows.append(low[i])
        
        # ============ Determine Trend ============
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            higher_highs = swing_highs[-1] > swing_highs[-2]
            higher_lows = swing_lows[-1] > swing_lows[-2]
            lower_highs = swing_highs[-1] < swing_highs[-2]
            lower_lows = swing_lows[-1] < swing_lows[-2]
            
            if higher_highs and higher_lows:
                trend = "bullish"
            elif lower_highs and lower_lows:
                trend = "bearish"
            else:
                trend = "ranging"
        else:
            trend = "ranging"
        
        # ============ Support & Resistance Levels ============
        support_levels = sorted(set([round(sl, 1) for sl in swing_lows[-5:]]))
        resistance_levels = sorted(set([round(rh, 1) for rh in swing_highs[-5:]]))
        
        # ============ Order Block Detection ============
        order_blocks = self._detect_order_blocks(df)
        
        # ============ Fair Value Gap Detection ============
        fvgs = self._detect_fvg(df)
        
        # ============ BOS & CHoCH Detection ============
        bos_detected, choch_detected = self._detect_structure_breaks(df, swing_highs, swing_lows)
        
        return MarketStructure(
            trend=trend,
            swing_highs=swing_highs,
            swing_lows=swing_lows,
            support_levels=support_levels,
            resistance_levels=resistance_levels,
            order_blocks=order_blocks,
            fvgs=fvgs,
            bos_detected=bos_detected,
            choch_detected=choch_detected
        )
    
    def _detect_order_blocks(self, df: pl.DataFrame) -> List[Dict[str, Any]]:
        """Detect Order Blocks (last opposite candle before impulse)"""
        order_blocks = []
        
        open_arr = df['open'].to_numpy()
        close_arr = df['close'].to_numpy()
        high_arr = df['high'].to_numpy()
        low_arr = df['low'].to_numpy()
        
        lookback = min(INDICATORS.OB_LOOKBACK, len(df) - 1)
        
        for i in range(lookback, len(df) - 3):
            # Bullish Order Block: Bearish candle before bullish impulse
            if close_arr[i] < open_arr[i]:  # Bearish candle
                # Check for bullish impulse (3+ bullish candles)
                bullish_impulse = all(close_arr[i+j] > open_arr[i+j] for j in range(1, 4))
                if bullish_impulse:
                    order_blocks.append({
                        'type': 'demand',
                        'high': high_arr[i],
                        'low': low_arr[i],
                        'index': i
                    })
            
            # Bearish Order Block: Bullish candle before bearish impulse
            if close_arr[i] > open_arr[i]:  # Bullish candle
                # Check for bearish impulse (3+ bearish candles)
                bearish_impulse = all(close_arr[i+j] < open_arr[i+j] for j in range(1, 4))
                if bearish_impulse:
                    order_blocks.append({
                        'type': 'supply',
                        'high': high_arr[i],
                        'low': low_arr[i],
                        'index': i
                    })
        
        # Return most recent 5 order blocks
        return order_blocks[-5:]
    
    def _detect_fvg(self, df: pl.DataFrame) -> List[Dict[str, Any]]:
        """Detect Fair Value Gaps (price imbalance between candle 1 and 3)"""
        fvgs = []
        
        high_arr = df['high'].to_numpy()
        low_arr = df['low'].to_numpy()
        
        for i in range(2, len(df)):
            # Bullish FVG: Gap between candle[i-2] high and candle[i] low
            if low_arr[i] - high_arr[i-2] > INDICATORS.FVG_MIN_GAP:
                fvgs.append({
                    'type': 'bullish',
                    'top': low_arr[i],
                    'bottom': high_arr[i-2],
                    'gap': low_arr[i] - high_arr[i-2],
                    'index': i
                })
            
            # Bearish FVG: Gap between candle[i-2] low and candle[i] high
            if low_arr[i-2] - high_arr[i] > INDICATORS.FVG_MIN_GAP:
                fvgs.append({
                    'type': 'bearish',
                    'top': low_arr[i-2],
                    'bottom': high_arr[i],
                    'gap': low_arr[i-2] - high_arr[i],
                    'index': i
                })
        
        # Return most recent 5 FVGs
        return fvgs[-5:]
    
    def _detect_structure_breaks(self, df: pl.DataFrame, swing_highs: List[float], 
                                  swing_lows: List[float]) -> Tuple[bool, bool]:
        """Detect Break of Structure (BOS) and Change of Character (CHoCH)"""
        if len(swing_highs) < 3 or len(swing_lows) < 3:
            return False, False
        
        close = df['close'].to_list()
        current_price = close[-1]
        
        # BOS Detection (continuation pattern)
        bos_detected = False
        if swing_highs[-1] > swing_highs[-2] and swing_lows[-1] > swing_lows[-2]:
            # Bullish BOS: Breaking previous high in uptrend
            if current_price > swing_highs[-2]:
                bos_detected = True
        elif swing_highs[-1] < swing_highs[-2] and swing_lows[-1] < swing_lows[-2]:
            # Bearish BOS: Breaking previous low in downtrend
            if current_price < swing_lows[-2]:
                bos_detected = True
        
        # CHoCH Detection (reversal pattern)
        choch_detected = False
        # Bullish CHoCH: Price breaks structure high after making lower lows
        if swing_lows[-2] < swing_lows[-3] and current_price > swing_highs[-2]:
            choch_detected = True
        # Bearish CHoCH: Price breaks structure low after making higher highs
        elif swing_highs[-2] > swing_highs[-3] and current_price < swing_lows[-2]:
            choch_detected = True
        
        return bos_detected, choch_detected
    
    # ========================================================================
    # PRICE ACTION PATTERNS
    # ========================================================================
    
    def detect_price_action(self, df: pl.DataFrame, structure: MarketStructure) -> PriceAction:
        """
        Detect price action patterns
        
        Args:
            df: Collected DataFrame
            structure: Market structure object
            
        Returns:
            PriceAction object
        """
        # Get last few candles
        last_candles = df.tail(10)
        last_candle = df.tail(1)
        
        open_val = last_candle['open'][0]
        high_val = last_candle['high'][0]
        low_val = last_candle['low'][0]
        close_val = last_candle['close'][0]
        
        # ============ Pin Bar Detection ============
        total_range = high_val - low_val
        body_size = abs(close_val - open_val)
        
        pinbar_bullish = False
        pinbar_bearish = False
        
        if total_range > 0:
            body_ratio = body_size / total_range
            
            # Bullish Pin Bar: Long lower wick at support
            lower_wick = min(open_val, close_val) - low_val
            if (lower_wick / total_range >= INDICATORS.PINBAR_WICK_RATIO and 
                body_ratio <= INDICATORS.PINBAR_BODY_RATIO):
                # Check if at swing low or support
                if structure.swing_lows and close_val <= structure.swing_lows[-1] * 1.001:
                    pinbar_bullish = True
            
            # Bearish Pin Bar: Long upper wick at resistance
            upper_wick = high_val - max(open_val, close_val)
            if (upper_wick / total_range >= INDICATORS.PINBAR_WICK_RATIO and 
                body_ratio <= INDICATORS.PINBAR_BODY_RATIO):
                # Check if at swing high or resistance
                if structure.swing_highs and close_val >= structure.swing_highs[-1] * 0.999:
                    pinbar_bearish = True
        
        # ============ Liquidity Sweep Detection ============
        liquidity_sweep_high = False
        liquidity_sweep_low = False
        
        if len(last_candles) >= INDICATORS.LIQUIDITY_SWEEP_THRESHOLD + 1:
            recent_high = last_candles['high'].max()
            recent_low = last_candles['low'].min()
            
            # Check last few candles for sweep pattern
            for i in range(len(last_candles) - INDICATORS.LIQUIDITY_SWEEP_THRESHOLD, len(last_candles)):
                candle = last_candles[i]
                
                # Liquidity sweep high: Brief break above high with quick rejection
                if (candle['high'][-1] > recent_high * 0.9999 and
                    candle['close'][-1] > candle['open'][-1] and
                    recent_high in structure.swing_highs):
                    liquidity_sweep_high = True
                
                # Liquidity sweep low: Brief break below low with quick rejection
                if (candle['low'][-1] < recent_low * 1.0001 and 
                    candle['close'][-1] > candle['open'][-1] and
                    recent_low in structure.swing_lows):
                    liquidity_sweep_low = True
        
        # ============ Calculate Pattern Strength ============
        pattern_strength = 0.0
        if pinbar_bullish or pinbar_bearish:
            pattern_strength += 0.4
        if liquidity_sweep_high or liquidity_sweep_low:
            pattern_strength += 0.3
        if structure.bos_detected:
            pattern_strength += 0.2
        if structure.choch_detected:
            pattern_strength += 0.1
        
        return PriceAction(
            pinbar_bullish=pinbar_bullish,
            pinbar_bearish=pinbar_bearish,
            liquidity_sweep_high=liquidity_sweep_high,
            liquidity_sweep_low=liquidity_sweep_low,
            pattern_strength=min(pattern_strength, 1.0)
        )
    
    # ========================================================================
    # TECHNICAL SNAPSHOT GENERATION
    # ========================================================================
    
    def generate_technical_snapshot(self, symbol: str, timeframe: int) -> TechnicalSnapshot:
        """
        Generate complete technical analysis snapshot for a timeframe
        
        Args:
            symbol: MT5 symbol
            timeframe: MT5 timeframe constant
            
        Returns:
            TechnicalSnapshot object
        """
        # Get data
        df_lazy = self.get_mt5_data(symbol, timeframe)
        df_lazy = self.apply_technical_indicators(df_lazy)
        df = df_lazy.collect()
        
        # Add Parabolic SAR
        df = self.calculate_parabolic_sar(df)
        
        # Get last row
        last = df.tail(1)
        
        # Detect market structure
        structure = self.detect_market_structure(df)
        
        # Detect price action
        price_action = self.detect_price_action(df, structure)
        
        # Determine EMA signal
        ema_fast = last['ema_fast'][0]
        ema_slow = last['ema_slow'][0]
        ema_fast_prev = df['ema_fast'][-2]
        ema_slow_prev = df['ema_slow'][-2]
        
        if ema_fast > ema_slow and ema_fast_prev <= ema_slow_prev:
            ema_signal = "bullish_cross"
        elif ema_fast < ema_slow and ema_fast_prev >= ema_slow_prev:
            ema_signal = "bearish_cross"
        elif ema_fast > ema_slow:
            ema_signal = "bullish"
        elif ema_fast < ema_slow:
            ema_signal = "bearish"
        else:
            ema_signal = "neutral"
        
        # Determine SAR trend
        sar_trend = "bullish" if last['sar_trend'][0] == 1 else "bearish"
        
        return TechnicalSnapshot(
            timeframe=TIMEFRAME_NAMES.get(timeframe, str(timeframe)),
            trend=structure.trend,
            ema_fast=ema_fast,
            ema_slow=ema_slow,
            ema_signal=ema_signal,
            macd_value=last['macd_line'][0],
            macd_signal=last['macd_signal'][0],
            macd_histogram=last['macd_histogram'][0],
            rsi=last['rsi'][0],
            atr=last['atr'][0],
            sar=last['sar'][0],
            sar_trend=sar_trend,
            market_structure=structure,
            price_action=price_action,
            current_price=last['close'][0],
            timestamp=last['time'][0]
        )
    
    # ========================================================================
    # CONFLUENCE ANALYSIS
    # ========================================================================
    
    def check_confluence(self, df_htf: TechnicalSnapshot, df_mtf: TechnicalSnapshot, 
                        df_ltf: TechnicalSnapshot, news_sentiment: Dict[str, Any]) -> ConfluenceAnalysis:
        """
        Aggregate multi-timeframe analysis and calculate confluence score
        
        Args:
            df_htf: High timeframe snapshot
            df_mtf: Medium timeframe snapshot
            df_ltf: Low timeframe snapshot
            news_sentiment: News sentiment data
            
        Returns:
            ConfluenceAnalysis object
        """
        components = {}
        signal_type = "NONE"
        entry_price = df_ltf.current_price
        stop_loss = 0.0
        take_profit = 0.0
        
        # ============ Trend Alignment ============
        bullish_trends = sum([
            df_htf.trend == "bullish",
            df_mtf.trend == "bullish",
            df_ltf.trend == "bullish"
        ])
        bearish_trends = sum([
            df_htf.trend == "bearish",
            df_mtf.trend == "bearish",
            df_ltf.trend == "bearish"
        ])
        
        if bullish_trends >= 2:
            components['trend_alignment'] = CONFLUENCE_WEIGHTS['trend_alignment']
        elif bearish_trends >= 2:
            components['trend_alignment'] = -CONFLUENCE_WEIGHTS['trend_alignment']
        else:
            components['trend_alignment'] = 0.0
        
        # ============ EMA Cross ============
        if df_ltf.ema_signal == "bullish_cross":
            components['ema_cross'] = CONFLUENCE_WEIGHTS['ema_cross']
        elif df_ltf.ema_signal == "bearish_cross":
            components['ema_cross'] = -CONFLUENCE_WEIGHTS['ema_cross']
        elif df_ltf.ema_signal == "bullish":
            components['ema_cross'] = CONFLUENCE_WEIGHTS['ema_cross'] * 0.5
        elif df_ltf.ema_signal == "bearish":
            components['ema_cross'] = -CONFLUENCE_WEIGHTS['ema_cross'] * 0.5
        else:
            components['ema_cross'] = 0.0
        
        # ============ MACD Signal ============
        if df_ltf.macd_histogram > 0 and df_ltf.macd_value > df_ltf.macd_signal:
            components['macd_signal'] = CONFLUENCE_WEIGHTS['macd_signal']
        elif df_ltf.macd_histogram < 0 and df_ltf.macd_value < df_ltf.macd_signal:
            components['macd_signal'] = -CONFLUENCE_WEIGHTS['macd_signal']
        else:
            components['macd_signal'] = 0.0
        
        # ============ RSI Zone ============
        if df_ltf.rsi < 30:
            components['rsi_zone'] = CONFLUENCE_WEIGHTS['rsi_zone']  # Oversold - bullish
        elif df_ltf.rsi > 70:
            components['rsi_zone'] = -CONFLUENCE_WEIGHTS['rsi_zone']  # Overbought - bearish
        else:
            components['rsi_zone'] = 0.0
        
        # ============ SAR Flip ============
        if df_ltf.sar_trend == "bullish" and df_ltf.sar < df_ltf.current_price:
            components['sar_flip'] = CONFLUENCE_WEIGHTS['sar_flip']
        elif df_ltf.sar_trend == "bearish" and df_ltf.sar > df_ltf.current_price:
            components['sar_flip'] = -CONFLUENCE_WEIGHTS['sar_flip']
        else:
            components['sar_flip'] = 0.0
        
        # ============ Support/Resistance ============
        at_support = any(abs(entry_price - level) / entry_price < 0.001 
                        for level in df_ltf.market_structure.support_levels)
        at_resistance = any(abs(entry_price - level) / entry_price < 0.001 
                           for level in df_ltf.market_structure.resistance_levels)
        
        if at_support:
            components['support_resistance'] = CONFLUENCE_WEIGHTS['support_resistance']
        elif at_resistance:
            components['support_resistance'] = -CONFLUENCE_WEIGHTS['support_resistance']
        else:
            components['support_resistance'] = 0.0
        
        # ============ Order Block ============
        in_demand_zone = any(
            ob['type'] == 'demand' and ob['low'] <= entry_price <= ob['high']
            for ob in df_ltf.market_structure.order_blocks
        )
        in_supply_zone = any(
            ob['type'] == 'supply' and ob['low'] <= entry_price <= ob['high']
            for ob in df_ltf.market_structure.order_blocks
        )
        
        if in_demand_zone:
            components['order_block'] = CONFLUENCE_WEIGHTS['order_block']
        elif in_supply_zone:
            components['order_block'] = -CONFLUENCE_WEIGHTS['order_block']
        else:
            components['order_block'] = 0.0
        
        # ============ Fair Value Gap ============
        in_bullish_fvg = any(
            fvg['type'] == 'bullish' and fvg['bottom'] <= entry_price <= fvg['top']
            for fvg in df_ltf.market_structure.fvgs
        )
        in_bearish_fvg = any(
            fvg['type'] == 'bearish' and fvg['bottom'] <= entry_price <= fvg['top']
            for fvg in df_ltf.market_structure.fvgs
        )
        
        if in_bullish_fvg:
            components['fvg'] = CONFLUENCE_WEIGHTS['fvg']
        elif in_bearish_fvg:
            components['fvg'] = -CONFLUENCE_WEIGHTS['fvg']
        else:
            components['fvg'] = 0.0
        
        # ============ Pin Bar Pattern ============
        if df_ltf.price_action.pinbar_bullish:
            components['pinbar'] = CONFLUENCE_WEIGHTS['pinbar']
        elif df_ltf.price_action.pinbar_bearish:
            components['pinbar'] = -CONFLUENCE_WEIGHTS['pinbar']
        else:
            components['pinbar'] = 0.0
        
        # ============ Liquidity Sweep ============
        if df_ltf.price_action.liquidity_sweep_low:
            components['liquidity_sweep'] = CONFLUENCE_WEIGHTS['liquidity_sweep']
        elif df_ltf.price_action.liquidity_sweep_high:
            components['liquidity_sweep'] = -CONFLUENCE_WEIGHTS['liquidity_sweep']
        else:
            components['liquidity_sweep'] = 0.0
        
        # ============ Break of Structure ============
        if df_ltf.market_structure.bos_detected:
            if df_ltf.trend == "bullish":
                components['bos'] = CONFLUENCE_WEIGHTS['bos']
            elif df_ltf.trend == "bearish":
                components['bos'] = -CONFLUENCE_WEIGHTS['bos']
            else:
                components['bos'] = 0.0
        else:
            components['bos'] = 0.0
        
        # ============ Change of Character ============
        if df_ltf.market_structure.choch_detected:
            # CHoCH indicates reversal
            if df_htf.trend == "bearish" and df_ltf.trend == "bullish":
                components['choch'] = CONFLUENCE_WEIGHTS['choch']
            elif df_htf.trend == "bullish" and df_ltf.trend == "bearish":
                components['choch'] = -CONFLUENCE_WEIGHTS['choch']
            else:
                components['choch'] = 0.0
        else:
            components['choch'] = 0.0
        
        # ============ News Sentiment ============
        if news_sentiment['sentiment'] == 'bullish':
            components['news_sentiment'] = CONFLUENCE_WEIGHTS['news_sentiment'] * abs(news_sentiment['score'])
        elif news_sentiment['sentiment'] == 'bearish':
            components['news_sentiment'] = -CONFLUENCE_WEIGHTS['news_sentiment'] * abs(news_sentiment['score'])
        else:
            components['news_sentiment'] = 0.0
        
        # ============ Calculate Total Score ============
        total_score = sum(components.values())
        
        # ============ Determine Signal Type ============
        if total_score >= RISK.CONFLUENCE_THRESHOLD:
            signal_type = "BUY"
            
            # Calculate SL and TP for BUY
            atr = df_ltf.atr
            stop_loss = entry_price - (1.5 * atr)
            
            # Use nearest support as secondary SL reference
            if df_ltf.market_structure.support_levels:
                nearest_support = max([s for s in df_ltf.market_structure.support_levels if s < entry_price], 
                                     default=stop_loss)
                stop_loss = max(stop_loss, nearest_support - (0.5 * atr))
            
            risk = entry_price - stop_loss
            take_profit = entry_price + (risk * RISK.MIN_RR_RATIO)
            
        elif total_score <= -RISK.CONFLUENCE_THRESHOLD:
            signal_type = "SELL"
            
            # Calculate SL and TP for SELL
            atr = df_ltf.atr
            stop_loss = entry_price + (1.5 * atr)
            
            # Use nearest resistance as secondary SL reference
            if df_ltf.market_structure.resistance_levels:
                nearest_resistance = min([r for r in df_ltf.market_structure.resistance_levels if r > entry_price], 
                                        default=stop_loss)
                stop_loss = min(stop_loss, nearest_resistance + (0.5 * atr))
            
            risk = stop_loss - entry_price
            take_profit = entry_price - (risk * RISK.MIN_RR_RATIO)
        
        # ============ Calculate Risk:Reward ============
        if signal_type != "NONE":
            risk = abs(entry_price - stop_loss)
            reward = abs(take_profit - entry_price)
            risk_reward = reward / risk if risk > 0 else 0.0
        else:
            risk_reward = 0.0
        
        # ============ Generate Explanation ============
        explanation = self._generate_confluence_explanation(
            signal_type, components, df_htf, df_mtf, df_ltf, news_sentiment
        )
        
        return ConfluenceAnalysis(
            total_score=total_score,
            components=components,
            signal_type=signal_type,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            risk_reward=risk_reward,
            explanation=explanation
        )
    
    def _generate_confluence_explanation(self, signal_type: str, components: Dict[str, float],
                                         df_htf: TechnicalSnapshot, df_mtf: TechnicalSnapshot,
                                         df_ltf: TechnicalSnapshot, news_sentiment: Dict[str, Any]) -> str:
        """Generate human-readable explanation of confluence analysis"""
        
        if signal_type == "NONE":
            return "No clear trading opportunity detected. Confluence score below threshold."
        
        explanation_parts = []
        
        # Header
        explanation_parts.append(f"[ANALYSIS] **{signal_type} Signal Detected**\n")
        
        # Trend Analysis
        explanation_parts.append(f"[SEARCH] **Multi-Timeframe Trend:**")
        explanation_parts.append(f"  • HTF ({df_htf.timeframe}): {df_htf.trend.upper()}")
        explanation_parts.append(f"  • MTF ({df_mtf.timeframe}): {df_mtf.trend.upper()}")
        explanation_parts.append(f"  • LTF ({df_ltf.timeframe}): {df_ltf.trend.upper()}\n")
        
        # Key Indicators
        explanation_parts.append(f"[BUY] **Technical Indicators:**")
        explanation_parts.append(f"  • EMA: {df_ltf.ema_signal.replace('_', ' ').title()}")
        explanation_parts.append(f"  • RSI: {df_ltf.rsi:.2f} ({'Oversold' if df_ltf.rsi < 30 else 'Overbought' if df_ltf.rsi > 70 else 'Neutral'})")
        explanation_parts.append(f"  • MACD: {'Bullish' if df_ltf.macd_histogram > 0 else 'Bearish'}")
        explanation_parts.append(f"  • SAR: {df_ltf.sar_trend.title()}\n")
        
        # Market Structure
        structure_features = []
        if df_ltf.market_structure.bos_detected:
            structure_features.append("Break of Structure")
        if df_ltf.market_structure.choch_detected:
            structure_features.append("Change of Character")
        if df_ltf.market_structure.order_blocks:
            structure_features.append(f"{len(df_ltf.market_structure.order_blocks)} Order Blocks")
        if df_ltf.market_structure.fvgs:
            structure_features.append(f"{len(df_ltf.market_structure.fvgs)} Fair Value Gaps")
        
        if structure_features:
            explanation_parts.append(f"🏗️ **Market Structure:**")
            for feature in structure_features:
                explanation_parts.append(f"  • {feature}")
            explanation_parts.append("")
        
        # Price Action
        price_action_signals = []
        if df_ltf.price_action.pinbar_bullish:
            price_action_signals.append("Bullish Pin Bar")
        if df_ltf.price_action.pinbar_bearish:
            price_action_signals.append("Bearish Pin Bar")
        if df_ltf.price_action.liquidity_sweep_low:
            price_action_signals.append("Liquidity Sweep Low (Bullish)")
        if df_ltf.price_action.liquidity_sweep_high:
            price_action_signals.append("Liquidity Sweep High (Bearish)")
        
        if price_action_signals:
            explanation_parts.append(f"[SIGNAL] **Price Action Patterns:**")
            for signal in price_action_signals:
                explanation_parts.append(f"  • {signal}")
            explanation_parts.append("")
        
        # News Sentiment
        if news_sentiment['news_count'] > 0:
            explanation_parts.append(f"📰 **News Sentiment:**")
            explanation_parts.append(f"  • Overall: {news_sentiment['sentiment'].upper()}")
            explanation_parts.append(f"  • Score: {news_sentiment['score']:.2f}")
            explanation_parts.append(f"  • Articles: {news_sentiment['news_count']}\n")
        
        # Top Contributing Factors
        explanation_parts.append(f"[STAR] **Top Confluence Factors:**")
        sorted_components = sorted(components.items(), key=lambda x: abs(x[1]), reverse=True)
        for factor, score in sorted_components[:5]:
            if abs(score) > 0.1:
                direction = "[OK]" if score > 0 else "[ERROR]"
                explanation_parts.append(f"  {direction} {factor.replace('_', ' ').title()}: {abs(score):.2f}")
        
        return "\n".join(explanation_parts)


# ============================================================================
# MAIN EXECUTION EXAMPLE
# ============================================================================
if __name__ == "__main__":
    """Test the trading engine"""
    
    # Initialize MT5
    if not mt5.initialize():
        print("[ERROR] MT5 initialization failed")
        exit()
    
    print("[OK] MT5 initialized successfully")
    
    # Create engine
    engine = TradingEngine()
    
    try:
        # Test data fetching
        print("\n[ANALYSIS] Fetching market data...")
        df_lazy = engine.get_mt5_data(SYMBOL, TIMEFRAMES['LTF'], 100)
        df_lazy = engine.apply_technical_indicators(df_lazy)
        df = df_lazy.collect()
        print(f"[OK] Fetched {len(df)} bars")
        print(f"Latest close: {df['close'][-1]:.2f}")
        
        # Test news sentiment
        print("\n📰 Fetching news sentiment...")
        news = engine.get_news_sentiment()
        print(f"[OK] Sentiment: {news['sentiment']} (Score: {news['score']:.2f})")
        print(f"   News count: {news['news_count']}")
        
        # Generate technical snapshots
        print("\n[SEARCH] Generating technical analysis...")
        htf_snapshot = engine.generate_technical_snapshot(SYMBOL, TIMEFRAMES['HTF'])
        mtf_snapshot = engine.generate_technical_snapshot(SYMBOL, TIMEFRAMES['MTF'])
        ltf_snapshot = engine.generate_technical_snapshot(SYMBOL, TIMEFRAMES['LTF'])
        
        print(f"[OK] HTF Trend: {htf_snapshot.trend}")
        print(f"[OK] MTF Trend: {mtf_snapshot.trend}")
        print(f"[OK] LTF Trend: {ltf_snapshot.trend}")
        
        # Check confluence
        print("\n⚖️ Analyzing confluence...")
        confluence = engine.check_confluence(htf_snapshot, mtf_snapshot, ltf_snapshot, news)
        
        print(f"\n{'='*60}")
        print(f"[SIGNAL] SIGNAL: {confluence.signal_type}")
        print(f"[ANALYSIS] Confluence Score: {confluence.total_score:.2f}")
        print(f"[PROFIT] Entry: {confluence.entry_price:.2f}")
        print(f"[STOP] Stop Loss: {confluence.stop_loss:.2f}")
        print(f"[SIGNAL] Take Profit: {confluence.take_profit:.2f}")
        print(f"[BUY] Risk:Reward: 1:{confluence.risk_reward:.2f}")
        print(f"{'='*60}")
        
        print(f"\n{confluence.explanation}")
        
    except Exception as e:
        print(f"[ERROR] Error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        mt5.shutdown()
        print("\n[OK] MT5 shutdown complete")