import pandas as pd

# === Step 2: Technical Indicators ===
# This module provides functions to add common technical indicators to an OHLCV DataFrame.


def add_sma(df: pd.DataFrame, window: int, column: str = 'close') -> pd.Series:
    """
    Simple Moving Average (SMA) over a specified window.
    """
    return df[column].rolling(window=window).mean()


def add_ema(df: pd.DataFrame, span: int, column: str = 'close') -> pd.Series:
    """
    Exponential Moving Average (EMA) with given span.
    """
    return df[column].ewm(span=span, adjust=False).mean()


def add_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Relative Strength Index (RSI) with default period of 14.
    """
    delta = df['close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def add_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Average True Range (ATR) for volatility over a specified period.
    """
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    return atr


def apply_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a set of predefined indicators to the DataFrame:
    - SMA for windows 9, 21, 50
    - EMA for spans 9, 21, 50
    - RSI (14)
    - ATR (14)
    Returns a new DataFrame with added columns.
    """
    df = df.copy()
    # Simple Moving Averages
    df['sma_9'] = add_sma(df, 9)
    df['sma_21'] = add_sma(df, 21)
    df['sma_50'] = add_sma(df, 50)

    # Exponential Moving Averages
    df['ema_9'] = add_ema(df, 9)
    df['ema_21'] = add_ema(df, 21)
    df['ema_50'] = add_ema(df, 50)

    # RSI
    df['rsi_14'] = add_rsi(df, 14)

    # ATR
    df['atr_14'] = add_atr(df, 14)

    return df
