# -*- coding: utf-8 -*-
"""
第09讲：缠论精华量化 (chan.py 版)
脚本1-chan：K线包含处理与分型识别

对比自研 ChanAnalyzer 与开源 chan.py 在K线合并和分型识别上的差异。
chan.py 的每根合并K线 (klc) 自带 fx 属性，直接标记分型类型。
"""

import os
from datetime import date
from data_loader import load_stock_data
from chan_analyzer import ChanAnalyzer
from chanpy_wrapper import run_chan, draw_chan_chart

STOCK_CODES = [
    '159608.SZ', '159583.SZ', '159560.SZ', '159133.SZ', '159796.SZ',  # ETF
    '603019.SH', '002847.SZ', '603259.SH', '000981.SZ', '002714.SZ', '002115.SZ',
]
START_DATE = '2026-01-01'
END_DATE = date.today().strftime('%Y-%m-%d')


def _setup_matplotlib():
    import matplotlib
    matplotlib.rcParams['font.sans-serif'] = ['PingFang SC', 'SimHei', 'Arial Unicode MS', 'DejaVu Sans']
    matplotlib.rcParams['axes.unicode_minus'] = False


def process_symbol(stock_code):
    import matplotlib.pyplot as plt

    print(f"\n{'=' * 60}")
    print(f"处理 {stock_code} ({START_DATE} ~ {END_DATE})")
    print('=' * 60)

    df = load_stock_data(stock_code, START_DATE, END_DATE)
    print(f"  共 {len(df)} 根K线")

    cp = run_chan(df, symbol=stock_code)
    klc_list = cp['klc_list']
    fractals = cp['fractals']
    top_fx = [f for f in fractals if f['fx'] == 'top']
    bot_fx = [f for f in fractals if f['fx'] == 'bottom']
    print(f"  合并K线: {len(klc_list)} 根 | 顶分型: {len(top_fx)} | 底分型: {len(bot_fx)}")

    analyzer = ChanAnalyzer(df)
    analyzer.analyze()
    our_top = sum(1 for f in analyzer.fractals if f['type'] == 'top')
    our_bot = sum(1 for f in analyzer.fractals if f['type'] == 'bottom')

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(22, 14))

    draw_chan_chart(ax1, df, cp, show_bi=False, show_seg=False,
                    show_zs=False, show_bsp=False, show_fractals=True)
    ax1.set_title(f'{stock_code} | chan.py 分型 | 合并{len(klc_list)}根 | '
                  f'顶{len(top_fx)} 底{len(bot_fx)} 共{len(fractals)}个',
                  fontsize=13, fontweight='bold')
    ax1.set_ylabel('价格')
    ax1.grid(True, alpha=0.3)

    d2x = ChanAnalyzer._draw_candlestick(ax2, df, width_ratio=0.6)
    for f in analyzer.fractals:
        rd = f.get('raw_date', f['date'])
        x = d2x.get(rd)
        if x is None and hasattr(rd, 'date'):
            x = d2x.get(rd.date())
        if x is None:
            continue
        m = 'v' if f['type'] == 'top' else '^'
        c = '#e74c3c' if f['type'] == 'top' else '#2ecc71'
        ax2.scatter(x, f['price'], marker=m, color=c, s=60, zorder=5, alpha=0.6)
    ax2.set_title(f'{stock_code} | ChanAnalyzer 分型 | 合并{len(analyzer.merged_df)}根 | '
                  f'顶{our_top} 底{our_bot} 共{len(analyzer.fractals)}个',
                  fontsize=13, fontweight='bold')
    ax2.set_ylabel('价格')
    ax2.grid(True, alpha=0.3)

    plt.tight_layout(h_pad=3)
    os.makedirs('outputs', exist_ok=True)
    safe_code = stock_code.replace('.', '_')
    out_path = f'outputs/1-chan-分型识别_{safe_code}.png'
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    print(f"  图表已保存: {out_path}")
    plt.close()
    return out_path


def main():
    print("=" * 60)
    print("第09讲 | 脚本1-chan: K线包含处理与分型识别 (chan.py版)")
    print(f"标的数量: {len(STOCK_CODES)} | 区间: {START_DATE} ~ {END_DATE}")
    print("=" * 60)

    _setup_matplotlib()
    saved = []
    for code in STOCK_CODES:
        try:
            saved.append(process_symbol(code))
        except Exception as e:
            print(f"  [失败] {code}: {e}")

    print(f"\n完成! 共生成 {len(saved)} 张图表:")
    for p in saved:
        print(f"  - {p}")


if __name__ == '__main__':
    main()
