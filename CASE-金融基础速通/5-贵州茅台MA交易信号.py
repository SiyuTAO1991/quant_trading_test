# -*- coding: utf-8 -*-
"""
CASE：贵州茅台MA交易信号

- MA 为过去 N 日收盘价的平均，相当于市场成本线（低通滤波）
- 金叉：短周期上穿长周期，短期力量强于长期，趋势启动
- 死叉：短周期下穿长周期，趋势转弱

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
MA_SHORT = 5
MA_LONG = 20
SHOW_DAYS = 120


def load_stock_data(data_file):
    if not os.path.exists(data_file):
        print(f"错误：数据文件不存在 {data_file}")
        return None
    df = pd.read_csv(data_file, encoding='utf-8-sig')
    df['date'] = pd.to_datetime(df['date'])
    if 'close' not in df.columns:
        print("错误：数据缺少 close 列")
        return None
    df = df.sort_values('date').reset_index(drop=True)
    return df


def run_demo():
    df = load_stock_data(DATA_FILE)
    if df is None:
        return

    df = df.tail(SHOW_DAYS + MA_LONG).reset_index(drop=True)
    close = df['close'].values
    dates = pd.DatetimeIndex(df['date'])

    ma5 = pd.Series(close).rolling(MA_SHORT, min_periods=1).mean().values
    ma20 = pd.Series(close).rolling(MA_LONG, min_periods=1).mean().values

    # 金叉：前一日 ma5 <= ma20，当日 ma5 > ma20
    # 死叉：前一日 ma5 >= ma20，当日 ma5 < ma20
    golden = []
    death = []
    for i in range(MA_LONG, len(close)):
        if ma5[i - 1] <= ma20[i - 1] and ma5[i] > ma20[i]:
            golden.append((i, dates[i], close[i]))
        if ma5[i - 1] >= ma20[i - 1] and ma5[i] < ma20[i]:
            death.append((i, dates[i], close[i]))

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(dates, close, 'b-', linewidth=1.2, label='收盘价')
    ax.plot(dates, ma5, 'orange', linewidth=1.2, label=f'MA{MA_SHORT}')
    ax.plot(dates, ma20, 'green', linewidth=1.2, label=f'MA{MA_LONG}')

    for idx, dt, pr in golden:
        ax.scatter([dt], [pr], marker='^', color='red', s=100, zorder=5)
        ax.annotate('金叉', (dt, pr), textcoords='offset points', xytext=(0, 12), ha='center', fontsize=9, color='red')
    for idx, dt, pr in death:
        ax.scatter([dt], [pr], marker='v', color='green', s=100, zorder=5)
        ax.annotate('死叉', (dt, pr), textcoords='offset points', xytext=(0, -15), ha='center', fontsize=9, color='green')

    ax.set_ylabel('价格 (元)', fontsize=11)
    ax.set_xlabel('日期', fontsize=11)
    ax.legend(loc='upper left')
    ax.grid(True, alpha=0.3)
    ax.set_title(f'{STOCK_NAME}({STOCK_CODE}) MA{MA_SHORT}/MA{MA_LONG} 金叉死叉', fontsize=12)
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.xticks(rotation=45)
    plt.tight_layout()

    out_dir = os.path.join(os.getcwd(), 'outputs')
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, '4-贵州茅台MA交易信号.png')
    plt.savefig(out_path, dpi=150, bbox_inches='tight', facecolor='white')
    print(f"图表已保存：{out_path}")
    if matplotlib.get_backend().lower() != 'agg':
        plt.show()

    print(f"\n本区间金叉次数：{len(golden)}，死叉次数：{len(death)}")
    print("说明：金叉偏多、死叉偏空；回踩 MA20 不破可视为支撑。")


if __name__ == '__main__':
    run_demo()
