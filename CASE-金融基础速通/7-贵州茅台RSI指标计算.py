# -*- coding: utf-8 -*-
"""
CASE：贵州茅台RSI指标计算
- RSI 在 [0,100]，超过 80 为超买（太贵），低于 20 为超卖（太便宜）
- 量化解读：均值回归概率高；RSI<20 时反弹概率大，可作网格买入参考
- 本脚本计算 RSI(14)，绘制价格与 RSI，并标注超买/超卖区间

运行前请确保已执行 1-qmt_download_data.py 并存在 data/600519_SH_daily.csv
"""
import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # 无图形界面时直接保存不阻塞；若需弹窗可注释本行
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'SimSun']
plt.rcParams['axes.unicode_minus'] = False

STOCK_NAME = '贵州茅台'
STOCK_CODE = '600519.SH'
DATA_FILE = os.path.join(os.getcwd(), 'data', '600519_SH_daily.csv')
RSI_PERIOD = 14
# 展示区间：最近一段便于观察超买超卖
SHOW_DAYS = 120
OVERBOUGHT = 80
OVERSOLD = 20


def load_stock_data(data_file):
    """从CSV加载日线数据"""
    if not os.path.exists(data_file):
        print(f"错误：数据文件不存在 {data_file}")
        print("请先运行 1-qmt_download_data.py 下载数据")
        return None
    df = pd.read_csv(data_file, encoding='utf-8-sig')
    df['date'] = pd.to_datetime(df['date'])
    if 'close' not in df.columns:
        print("错误：数据缺少 close 列")
        return None
    df = df.sort_values('date').reset_index(drop=True)
    return df


def calc_rsi(close, period=14):
    """
    计算 RSI：RSI = 100 - 100/(1 + RS)，RS = 平均涨幅/平均跌幅
    使用 Wilder 平滑（Wilder's smoothing）
    """
    n = len(close)
    rsi = np.full(n, np.nan)
    if n < period + 1:
        return rsi

    gains = np.zeros(n)
    losses = np.zeros(n)
    for i in range(1, n):
        diff = close[i] - close[i - 1]
        if diff > 0:
            gains[i] = diff
        else:
            losses[i] = -diff

    # 第一段：前 period 日的平均涨跌
    avg_gain = np.mean(gains[1:period + 1])
    avg_loss = np.mean(losses[1:period + 1])

    for i in range(period, n):
        if i > period:
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            rsi[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100.0 - 100.0 / (1.0 + rs)

    return rsi


def run_demo():
    df = load_stock_data(DATA_FILE)
    if df is None:
        return

    df = df.tail(SHOW_DAYS + RSI_PERIOD + 5).reset_index(drop=True)
    close = df['close'].values
    dates = pd.DatetimeIndex(df['date'])

    rsi = calc_rsi(close, RSI_PERIOD)
    # 去掉前面无效的
    valid = ~np.isnan(rsi)
    dates = dates[valid]
    close = close[valid]
    rsi = rsi[valid]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    fig.suptitle(f'{STOCK_NAME}({STOCK_CODE}) RSI({RSI_PERIOD}) 超买超卖示意', fontsize=14, fontweight='bold')

    # 子图1：收盘价
    ax1.plot(dates, close, 'b-', linewidth=1.2, label='收盘价')
    ax1.set_ylabel('价格 (元)', fontsize=11)
    ax1.legend(loc='upper left')
    ax1.grid(True, alpha=0.3)
    ax1.set_title('收盘价', fontsize=11)

    # 子图2：RSI + 超买超卖线
    ax2.plot(dates, rsi, 'purple', linewidth=1.2, label=f'RSI({RSI_PERIOD})')
    ax2.axhline(y=OVERBOUGHT, color='red', linestyle='--', linewidth=1, alpha=0.8, label=f'超买 {OVERBOUGHT}')
    ax2.axhline(y=OVERSOLD, color='green', linestyle='--', linewidth=1, alpha=0.8, label=f'超卖 {OVERSOLD}')
    ax2.axhline(y=50, color='gray', linestyle=':', linewidth=0.8, alpha=0.6)
    ax2.fill_between(dates, OVERBOUGHT, 100, alpha=0.15, color='red')
    ax2.fill_between(dates, 0, OVERSOLD, alpha=0.15, color='green')
    ax2.set_ylim(0, 100)
    ax2.set_ylabel('RSI', fontsize=11)
    ax2.set_xlabel('日期', fontsize=11)
    ax2.legend(loc='upper left')
    ax2.grid(True, alpha=0.3)
    ax2.set_title(f'RSI：>={OVERBOUGHT} 超买（考虑减仓/网格卖），<={OVERSOLD} 超卖（考虑加仓/网格买）', fontsize=11)

    # 标注超买超卖日
    oversold_dates = dates[rsi <= OVERSOLD]
    overbought_dates = dates[rsi >= OVERBOUGHT]
    if len(oversold_dates) > 0:
        ax2.scatter(oversold_dates, rsi[rsi <= OVERSOLD], color='green', s=40, zorder=5, label='超卖日')
    if len(overbought_dates) > 0:
        ax2.scatter(overbought_dates, rsi[rsi >= OVERBOUGHT], color='red', s=40, zorder=5, label='超买日')

    ax2.xaxis.set_major_locator(mdates.MonthLocator())
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.xticks(rotation=45)
    plt.tight_layout()

    # 统计
    print(f"\n本区间内 RSI<={OVERSOLD} 超卖天数: {np.sum(rsi <= OVERSOLD)}")
    print(f"本区间内 RSI>={OVERBOUGHT} 超买天数: {np.sum(rsi >= OVERBOUGHT)}")
    print("课程要点：RSI<20 时反弹概率大，可作为网格策略的买入参考；RSI>80 时可考虑减仓或网格卖出。")

    output_dir = os.path.join(os.getcwd(), 'outputs')
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, '6-贵州茅台RSI指标计算.png')
    plt.savefig(out_path, dpi=150, bbox_inches='tight', facecolor='white')
    print(f"\n图表已保存：{out_path}")
    if matplotlib.get_backend().lower() != 'agg':
        plt.show()


if __name__ == '__main__':
    run_demo()
