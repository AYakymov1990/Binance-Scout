# backtest.py

import argparse
import pandas as pd
import numpy as np

def backtest(df: pd.DataFrame, sl_atr: float, tp_atr: float, trail_atr: float = 1.0):
    """
    Простейший бэктест с фиксированным и трейлинг-стопом:
     - Входим в лонг/шорт на открытии следующей свечи после сигнала (`signal`).
     - Изначальный SL = entry_price − sign * sl_atr * ATR
     - TP              = entry_price + sign * tp_atr * ATR
     - Трейлинг-SL: для лонга = max(entry SL, max_high_since_entry − trail_atr*ATR)
                    для шорта = min(entry SL, min_low_since_entry + trail_atr*ATR)
     - Выход по SL (фиксированному или трейлинговому) или TP внутри сессии,
       иначе по close последнего бара.
    Возвращает: total trades, win-rate, avg PnL, max drawdown.
    """
    pnls = []
    n = len(df)
    for i in range(n - 1):
        sig = df.loc[i, 'signal']
        if sig == 0:
            continue

        entry_price = df.loc[i+1, 'open']
        atr         = df.loc[i+1, 'atr_14']

        # фиксированный стоп и тейк
        fixed_sl = entry_price - sig * sl_atr * atr
        tp       = entry_price + sig * tp_atr * atr

        # подготовка для трейлинг-стопа
        max_high = entry_price
        min_low  = entry_price
        exit_price = None

        # перебираем последующие бары
        for j in range(i+1, n):
            high = df.loc[j, 'high']
            low  = df.loc[j, 'low']

            # обновляем экстремумы
            if sig == 1:
                max_high = max(max_high, high)
                trail_sl = max(fixed_sl, max_high - trail_atr * atr)
                # проверяем сначала SL, затем TP
                if low <= trail_sl:
                    exit_price = trail_sl
                    break
                if high >= tp:
                    exit_price = tp
                    break
            else:
                min_low = min(min_low, low)
                trail_sl = min(fixed_sl, min_low + trail_atr * atr)
                if high >= trail_sl:
                    exit_price = trail_sl
                    break
                if low <= tp:
                    exit_price = tp
                    break

        # если ни SL, ни TP не сработал — закрываем последним close
        if exit_price is None:
            exit_price = df.loc[n-1, 'close']

        # считаем PnL: для шорта умножаем на -1
        pnl = (exit_price - entry_price) * sig
        pnls.append(pnl)

    # если не было сделок
    if not pnls:
        return 0, np.nan, np.nan, np.nan

    arr   = np.array(pnls)
    total = len(arr)
    wins  = (arr > 0).sum()
    wr    = wins / total
    avg   = arr.mean()
    cum   = arr.cumsum()
    dd    = (cum.max() - cum).max()
    return total, wr, avg, dd

def main():
    parser = argparse.ArgumentParser(
        description="Backtest strategy on CSV with signals and ATR stops"
    )
    parser.add_argument(
        '--data', type=str, required=True,
        help='CSV with columns including open, high, low, close, atr_14, signal'
    )
    parser.add_argument(
        '--sl', type=float, default=1.5,
        help='Коэффициент ATR для первичного стоп-лосса (по умолч. 2.0)'
    )
    parser.add_argument(
        '--tp', type=float, default=0.5,
        help='Коэффициент ATR для тейк-профита (по умолч. 1.0)'
    )
    parser.add_argument(
        '--trail', type=float, default=0.5,
        help='Коэффициент ATR для трейлинг-стопа (по умолч. 1.0)'
    )
    args = parser.parse_args()

    df = pd.read_csv(args.data, parse_dates=['open_time'])
    total, wr, avg, dd = backtest(df, args.sl, args.tp, args.trail)
    print(f"SL={args.sl}⋅ATR  TP={args.tp}⋅ATR  TrailSL={args.trail}⋅ATR → "
          f"Trades: {total}, Win-rate: {wr:.1%}, Avg PnL: {avg:.2f}, Max DD: {dd:.2f}")

if __name__ == '__main__':
    main()
