# -*- coding: utf-8 -*-
"""
CASE：贵州茅台MACD交易信号
- MACD 衡量短期与长期均线的距离，即动量
- 红柱变长：上涨加速；红柱变短（背驰）：涨速放缓，需警惕
- 金叉做多、死叉做空

运行前请确保 data/600519_SH_daily.csv 存在。
"""
import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'SimSun']
plt.rcParams['axes.unicode_minus'] = False

STOCK_NAME = '贵州茅台'
STOCK_CODE = '600519.SH'
DATA_FILE = os.path.join(os.getcwd(), 'data', '600519_SH_daily.csv')
SHORT_PERIOD = 12
LONG_PERIOD = 26
SIGNAL_PERIOD = 9
SHOW_DAYS = 150


def load_stock_data(data_file):
    if not os.path.exists(data_file):
        print(f"错误：数据文件不存在 {data_file}")
        return None
    df = pd.read_csv(data_file, encoding='utf-8-sig')
    df['date'] = pd.to_datetime(df['date'])
    if 'close' not in df.columns:
        return None
    df = df.sort_values('date').reset_index(drop=True)
    return df


def get_MACD(close, short=12, long=26, m=9):
    ema_short = pd.Series(close).ewm(span=short, adjust=False).mean()
    ema_long = pd.Series(close).ewm(span=long, adjust=False).mean()
    dif = ema_short - ema_long
    dea = dif.ewm(span=m, adjust=False).mean()
    macd_bar = (dif - dea) * 2
    return dif.values, dea.values, macd_bar.values


def run_demo():
    df = load_stock_data(DATA_FILE)
    if df is None:
        return

    df = df.tail(SHOW_DAYS + LONG_PERIOD + SIGNAL_PERIOD).reset_index(drop=True)
    close = df['close'].values
    dates = pd.DatetimeIndex(df['date'])

    dif, dea, macd_bar = get_MACD(close, SHORT_PERIOD, LONG_PERIOD, SIGNAL_PERIOD)

    golden = []
    death = []
    for i in range(1, len(dif)):
        if dif[i - 1] <= dea[i - 1] and dif[i] > dea[i]:
            golden.append((i, dates[i], close[i]))
        if dif[i - 1] >= dea[i - 1] and dif[i] < dea[i]:
            death.append((i, dates[i], close[i]))

    # 找一次「红柱缩短」背驰：红柱阶段，前一日柱更长
    divergence_candidates = []
    for i in range(2, len(macd_bar)):
        if macd_bar[i] > 0 and macd_bar[i - 1] > 0 and macd_bar[i] < macd_bar[i - 1]:
            divergence_candidates.append((i, dates[i], close[i], macd_bar[i]))

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    ax1.plot(dates, close, 'b-', linewidth=1.2, label='收盘价')
    for _, dt, pr in golden:
        ax1.scatter([dt], [pr], marker='^', color='red', s=80, zorder=5)
        ax1.annotate('金叉', (dt, pr), textcoords='offset points', xytext=(0, 10), ha='center', fontsize=8, color='red')
    for _, dt, pr in death:
        ax1.scatter([dt], [pr], marker='v', color='green', s=80, zorder=5)
        ax1.annotate('死叉', (dt, pr), textcoords='offset points', xytext=(0, -12), ha='center', fontsize=8, color='green')
    ax1.set_ylabel('价格 (元)')
    ax1.legend(loc='upper left')
    ax1.grid(True, alpha=0.3)
    ax1.set_title('收盘价与买卖点')

    ax2.plot(dates, dif, 'b-', linewidth=1, label='DIF')
    ax2.plot(dates, dea, 'orange', linewidth=1, label='DEA')
    colors = ['red' if v >= 0 else 'green' for v in macd_bar]
    ax2.bar(dates, macd_bar, color=colors, alpha=0.6, width=1.5)
    ax2.axhline(y=0, color='gray', linestyle='--', linewidth=0.8)
    ax2.set_ylabel('MACD')
    ax2.set_xlabel('日期')
    ax2.legend(loc='upper left')
    ax2.grid(True, alpha=0.3)
    ax2.set_title(f'MACD({SHORT_PERIOD},{LONG_PERIOD},{SIGNAL_PERIOD}) 红柱变短=背驰，需警惕', fontsize=11)

    ax2.xaxis.set_major_locator(mdates.MonthLocator())
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.xticks(rotation=45)
    plt.tight_layout()

    out_dir = os.path.join(os.getcwd(), 'outputs')
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, '5-贵州茅台MACD交易信号.png')
    plt.savefig(out_path, dpi=150, bbox_inches='tight', facecolor='white')
    print(f"图表已保存：{out_path}")
    if matplotlib.get_backend().lower() != 'agg':
        plt.show()

    print(f"\n本区间金叉 {len(golden)} 次，死叉 {len(death)} 次")
    print("课程要点：红柱变短为背驰，涨速放缓，量化中常用作风险信号。")


if __name__ == '__main__':
    run_demo()
