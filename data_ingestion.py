import os
from binance.client import Client
import pandas as pd

# === Step 1: Data Ingestion from Binance ===
# This script fetches historical OHLCV data for a given symbol and interval.

# Load API credentials from environment variables
API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_API_SECRET')

if not API_KEY or not API_SECRET:
    raise RuntimeError("Please set BINANCE_API_KEY and BINANCE_API_SECRET environment variables.")

# Initialize Binance client
client = Client(API_KEY, API_SECRET)

# Parameters: symbol, interval, limit
SYMBOL = 'BTCUSDT'
INTERVAL = Client.KLINE_INTERVAL_1HOUR  # 1h candles
LIMIT = 500  # number of candles to fetch


def fetch_ohlcv(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    """
    Fetches historical candles (kline) data from Binance.
    Returns a DataFrame with open time, open, high, low, close, volume.
    """
    klines = client.get_klines(symbol=symbol, interval=interval, limit=limit)
    df = pd.DataFrame(klines, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'num_trades',
        'taker_buy_base_vol', 'taker_buy_quote_vol', 'ignore'
    ])
    # Convert timestamps and numeric columns
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = df[col].astype(float)
    return df[['open_time', 'open', 'high', 'low', 'close', 'volume']]


if __name__ == '__main__':
    print(f"Fetching last {LIMIT} candles for {SYMBOL} at interval {INTERVAL}...")
    data = fetch_ohlcv(SYMBOL, INTERVAL, LIMIT)
    print(data.head())
    # Save to CSV
    csv_file = f"{SYMBOL}_{INTERVAL}_{LIMIT}.csv"
    data.to_csv(csv_file, index=False)
    print(f"Data saved to {csv_file}")
