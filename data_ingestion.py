import argparse
import pandas as pd
from binance.client import Client
from config import BINANCE_API_KEY as API_KEY, BINANCE_API_SECRET as API_SECRET
from indicators import apply_indicators

# === Step 1 & 2: Data Ingestion and Indicators ===
# This script fetches historical OHLCV data and adds technical indicators.

# Initialize Binance client
if not API_KEY or not API_SECRET:
    raise RuntimeError("Please fill in your Binance API credentials in config.py")

client = Client(API_KEY, API_SECRET)

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
    parser = argparse.ArgumentParser(description='Fetch OHLCV data and add indicators')
    parser.add_argument('--symbol', type=str, default='BTCUSDT',
                        help='Trading symbol (e.g. BTCUSDT)')
    parser.add_argument('--interval', type=str, default=Client.KLINE_INTERVAL_1HOUR,
                        help='Candle interval (e.g. 1m, 5m, 1h)')
    parser.add_argument('--limit', type=int, default=500,
                        help='Number of candles to fetch')
    parser.add_argument('--output', type=str, default=None,
                        help='CSV output filepath')
    args = parser.parse_args()

    symbol = args.symbol
    interval = args.interval
    limit = args.limit

    print(f"Fetching last {limit} candles for {symbol} at interval {interval}...")
    data = fetch_ohlcv(symbol, interval, limit)

    print("Applying technical indicators...")
    data_with_ind = apply_indicators(data)
    print(data_with_ind.head())

    # Determine output filename
    default_name = f"{symbol}_{interval}_{limit}_indicators.csv"
    csv_file = args.output or default_name
    data_with_ind.to_csv(csv_file, index=False)
    print(f"Data with indicators saved to {csv_file}")
