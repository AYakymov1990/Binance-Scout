import pandas as pd

# === Step 3 Enhanced: Indicators + Filters + MACD ===

def add_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal_period: int = 9) -> pd.DataFrame:
    """
    Calculate MACD line and histogram and add 'macd_hist' column.
    """
    exp1 = df['close'].ewm(span=fast, adjust=False).mean()
    exp2 = df['close'].ewm(span=slow, adjust=False).mean()
    macd_line   = exp1 - exp2
    macd_signal = macd_line.ewm(span=signal_period, adjust=False).mean()
    df['macd_hist'] = macd_line - macd_signal
    return df

def generate_signals(df: pd.DataFrame,
                     vol_window: int = 20,
                     vol_mult: float = 1.2,
                     atr_window: int = 20) -> pd.DataFrame:
    """
    Generate trading signals with strict EMA crossover + volume, ATR, and MACD filters.

    Steps:
      1) Add MACD histogram.
      2) Strict EMA9/EMA21 crossover signals.
      3) Volume filter: volume > vol_mult * rolling_mean(volume, vol_window)
      4) ATR filter: atr_14 > rolling_mean(atr_14, atr_window)
      5) MACD filter: macd_hist > 0 for longs, < 0 for shorts.

    Returns a DataFrame with new columns: signal_ema, signal, and helper cols.
    """
    df = df.copy()

    # 1) MACD
    df = add_macd(df)

    # 2) Strict EMA crossover
    prev9  = df['ema_9'].shift(1)
    prev21 = df['ema_21'].shift(1)
    up_cross   = (df['ema_9'] > df['ema_21']) & (prev9 <= prev21)
    down_cross = (df['ema_9'] < df['ema_21']) & (prev9 >= prev21)

    df['signal_ema'] = 0
    df.loc[up_cross,   'signal_ema'] =  1
    df.loc[down_cross, 'signal_ema'] = -1

    # 3) Volume filter
    vol_ma = df['volume'].rolling(window=vol_window).mean()
    df['vol_ok'] = df['volume'] > vol_ma * vol_mult

    # 4) ATR filter
    atr_ma = df['atr_14'].rolling(window=atr_window).mean()
    df['atr_ok'] = df['atr_14'] > atr_ma

    # 5) Combine filters + MACD
    df['signal'] = 0
    # long: EMA cross up AND volume + ATR OK AND MACD bullish
    long_ok  = up_cross & df['vol_ok'] & df['atr_ok'] & (df['macd_hist'] > 0)
    # short: EMA cross down AND volume + ATR OK AND MACD bearish
    short_ok = down_cross & df['vol_ok'] & df['atr_ok'] & (df['macd_hist'] < 0)

    df.loc[long_ok,  'signal'] =  1
    df.loc[short_ok, 'signal'] = -1

    return df
