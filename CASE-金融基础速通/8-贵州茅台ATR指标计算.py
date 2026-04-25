# -*- coding: utf-8 -*-
"""
CASE：贵州茅台ATR指标计算
- ATR(N)：取 N 日内的「真实波幅」的均值，真实波幅 = max(高-低, |高-前收|, |低-前收|)
- 风控：止损常用 2*ATR，跌穿正常波动则趋势坏
- 仓位：波动大的股票仓位要小，波动小的可适当加大

运行前请确保 data/600519_SH_daily.csv 存在。
"""
import os
import pandas as pd
import numpy as np


STOCK_NAME = '贵州茅台'
STOCK_CODE = '600519.SH'
DATA_FILE = os.path.join(os.getcwd(), 'data', '600519_SH_daily.csv')
ATR_PERIOD = 14
LOOKBACK_DAYS = 60


def load_stock_data(data_file):
    if not os.path.exists(data_file):
        print(f"错误：数据文件不存在 {data_file}")
        return None
    df = pd.read_csv(data_file, encoding='utf-8-sig')
    df['date'] = pd.to_datetime(df['date'])
    for col in ['high', 'low', 'close']:
        if col not in df.columns:
            print(f"错误：数据缺少 {col} 列")
            return None
    df = df.sort_values('date').reset_index(drop=True)
    return df


def calc_atr(high, low, close, period=14):
    """真实波幅 TR = max(高-低, |高-前收|, |低-前收|)，ATR = TR 的 N 日均"""
    n = len(close)
    tr = np.zeros(n)
    tr[0] = high[0] - low[0]
    for i in range(1, n):
        tr[i] = max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1])
        )
    atr = pd.Series(tr).rolling(period, min_periods=period).mean().values
    return atr


def run_demo():
    df = load_stock_data(DATA_FILE)
    if df is None:
        return

    df = df.tail(LOOKBACK_DAYS + ATR_PERIOD).reset_index(drop=True)
    high = df['high'].values
    low = df['low'].values
    close = df['close'].values
    dates = df['date'].values

    atr = calc_atr(high, low, close, ATR_PERIOD)
    valid = ~np.isnan(atr)
    atr_valid = atr[valid]
    close_valid = close[valid]
    dates_valid = dates[valid]

    last_atr = atr_valid[-1]
    last_close = close_valid[-1]
    last_date = pd.Timestamp(dates_valid[-1]).strftime('%Y-%m-%d')
    stop_loss_price = last_close - 2 * last_atr

    print(f"ATR 周期：{ATR_PERIOD} 日")
    print(f"最近一日：{last_date}")
    print(f"收盘价：{last_close:.2f} 元")
    print(f"ATR({ATR_PERIOD})：{last_atr:.2f} 元（日均波动约 {last_atr:.2f} 元）")
    print(f"2*ATR 止损距离：{2 * last_atr:.2f} 元")
    print(f"若当日买入，2*ATR 止损价：{stop_loss_price:.2f} 元（跌破则考虑止损）")
    print("-" * 60)
    print("说明：波动大的标的仓位要小；止损常设为 2*ATR，海龟法则核心。")
    print("=" * 60)

    # 最近 5 日 ATR 与对应 2*ATR 止损价
    print("\n最近 5 日 ATR 与 2*ATR 止损价：")
    for i in range(-5, 0):
        d = pd.Timestamp(dates_valid[i]).strftime('%Y-%m-%d')
        c = close_valid[i]
        a = atr_valid[i]
        sl = c - 2 * a
        print(f"  {d}  收盘={c:.2f}  ATR={a:.2f}  2*ATR止损价={sl:.2f}")


if __name__ == '__main__':
    run_demo()
