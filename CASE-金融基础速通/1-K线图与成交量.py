# -*- coding: utf-8 -*-
"""
《金融基础速通》案例1：K线图与成交量直观理解

对应课程：一、金融基础概念 —— K线图与成交量
目标：看懂开盘/收盘/最高/最低、阳线阴线、影线、量价配合

- 阳线(红)：收盘>开盘，买方赢；实体越长买方越强
- 阴线(绿)：收盘<开盘，卖方赢；实体越长卖方越狠
- 上影线：冲高回落，上方抛压大
- 下影线：杀跌反弹，下方支撑强
- 量价配合：上涨放量有动力，缩量上涨易诱多

运行前请确保已执行 1-qmt_download_data.py 并存在 data/600519_SH_daily.csv
"""
import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # 无图形界面时直接保存不阻塞；若需弹窗可注释本行
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import numpy as np


plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'SimSun']
plt.rcParams['axes.unicode_minus'] = False

STOCK_NAME = '贵州茅台'
STOCK_CODE = '600519.SH'
DATA_FILE = os.path.join(os.getcwd(), 'data', '600519_SH_daily.csv')
# 展示最近一段交易日（便于看清K线形态）
SHOW_DAYS = 60


def load_stock_data(data_file):
    """从CSV加载日线数据，需包含 open, high, low, close, volume"""
    if not os.path.exists(data_file):
        print(f"错误：数据文件不存在 {data_file}")
        print("请先运行 1-qmt_download_data.py 下载数据")
        return None
    df = pd.read_csv(data_file, encoding='utf-8-sig')
    df['date'] = pd.to_datetime(df['date'])
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col not in df.columns:
            print(f"错误：数据缺少列 {col}")
            return None
    df = df.sort_values('date').reset_index(drop=True)
    return df


def plot_candlestick(ax, dates, open_p, high_p, low_p, close_p):
    """
    用矩形和竖线绘制K线（阳线红，阴线绿）
    """
    n = len(dates)
    width = 0.6
    half = width / 2

    for i in range(n):
        o, h, l, c = open_p[i], high_p[i], low_p[i], close_p[i]
        if c >= o:
            color = 'red'
            body_bottom = o
            body_height = c - o
        else:
            color = 'green'
            body_bottom = c
            body_height = o - c

        # 实体：矩形（避免高度为0）
        if body_height < 1e-6:
            body_height = (h - l) * 0.02 if h > l else 0.01
        rect = Rectangle((i - half, body_bottom), width, body_height,
                         facecolor=color, edgecolor=color)
        ax.add_patch(rect)

        # 上影线
        ax.plot([i, i], [max(o, c), h], color=color, linewidth=1)
        # 下影线
        ax.plot([i, i], [min(o, c), l], color=color, linewidth=1)

    ax.set_xlim(-0.5, n - 0.5)
    ax.set_xticks(range(0, n, max(1, n // 10)))
    ax.set_xticklabels([pd.Timestamp(dates[i]).strftime('%m-%d') for i in range(0, n, max(1, n // 10))])
    ax.set_ylabel('价格 (元)', fontsize=11)
    ax.legend([], [], frameon=False)
    ax.grid(True, alpha=0.3)


def run_demo():
    df = load_stock_data(DATA_FILE)
    if df is None:
        return

    # 取最近 SHOW_DAYS 条
    df = df.tail(SHOW_DAYS).reset_index(drop=True)
    dates = df['date'].values
    open_p = df['open'].values
    high_p = df['high'].values
    low_p = df['low'].values
    close_p = df['close'].values
    volume = df['volume'].values

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True,
                                   gridspec_kw={'height_ratios': [3, 1]})
    fig.suptitle(f'{STOCK_NAME}({STOCK_CODE}) K线图与成交量', fontsize=14, fontweight='bold')

    # 子图1：K线
    plot_candlestick(ax1, dates, open_p, high_p, low_p, close_p)
    ax1.set_title('K线：开盘/最高/最低/收盘（红=阳线 绿=阴线）', fontsize=11)

    # 子图2：成交量（红绿与当日涨跌一致）
    colors = ['red' if close_p[i] >= open_p[i] else 'green' for i in range(len(dates))]
    ax2.bar(range(len(dates)), volume / 1e4, color=colors, alpha=0.7, width=0.6)
    ax2.set_ylabel('成交量 (万手)', fontsize=11)
    ax2.set_xlabel('日期', fontsize=11)
    ax2.set_title('成交量：上涨日红柱、下跌日绿柱，量价配合更健康', fontsize=11)
    ax2.grid(True, alpha=0.3)

    plt.xticks(rotation=45)
    plt.tight_layout()

    # 可选：标出量价背离示例（价格创新高但量萎缩）
    # 这里仅做简单提示，不在图上标注具体点，避免逻辑过重
    print("\n提示：若出现「价格创新高、成交量却缩小」的几天，多为量价背离，需警惕见顶。")
    print("可在图中自行观察最近是否出现该现象。")

    output_dir = os.path.join(os.getcwd(), 'outputs')
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, '01_kline_volume_demo.png')
    plt.savefig(out_path, dpi=150, bbox_inches='tight', facecolor='white')
    print(f"\n图表已保存：{out_path}")
    if matplotlib.get_backend().lower() != 'agg':
        plt.show()


if __name__ == '__main__':
    run_demo()
