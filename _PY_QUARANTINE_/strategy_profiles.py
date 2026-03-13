# Pre-configured Strategy Profiles

CONSERVATIVE = {
    'name': 'Conservative Strategy',
    'description': 'Low risk, high accuracy',
    'confluence_threshold': 4.0,
    'ai_confidence_threshold': 85.0,
    'min_rr_ratio': 2.5,
    'risk_percent': 0.5,
    'max_daily_trades': 2,
    'timeframes': {
        'HTF': 'D1',
        'MTF': 'H4',
        'LTF': 'H1'
    },
    'confluence_weights': {
        'trend_alignment': 2.0,
        'ema_cross': 1.5,
        'order_block': 2.0,
        'choch': 1.5,
        'liquidity_sweep': 1.5,
        'bos': 1.0,
        'fvg': 1.0,
        'pinbar': 0.8,
        'support_resistance': 1.5,
        'news_sentiment': 0.5,
        'macd_signal': 0.3,
        'rsi_zone': 0.3,
        'sar_flip': 0.3
    }
}

def apply_profile(profile_name):
    """Apply a strategy profile to config"""
    profiles = {
        'conservative': CONSERVATIVE,
        'balanced': BALANCED,
        'aggressive': AGGRESSIVE,
        'scalping': SCALPING
    }
    
    profile = profiles.get(profile_name.lower())
    if not profile:
        print(f"❌ Profile '{profile_name}' not found")
        return False
    
    print(f"✅ Applying profile: {profile['name']}")
    print(f"   Description: {profile['description']}")
    
    # Update config.py
    import config
    
    config.RISK.CONFLUENCE_THRESHOLD = profile['confluence_threshold']
    config.RISK.AI_CONFIDENCE_THRESHOLD = profile['ai_confidence_threshold']
    config.RISK.MIN_RR_RATIO = profile['min_rr_ratio']
    config.RISK.RISK_PERCENT = profile['risk_percent']
    config.RISK.MAX_DAILY_TRADES = profile['max_daily_trades']
    
    # Update weights
    for key, value in profile['confluence_weights'].items():
        if hasattr(config.CONFLUENCE_WEIGHTS, key):
            setattr(config.CONFLUENCE_WEIGHTS, key, value)
    
    print("✅ Profile applied successfully")
    return True