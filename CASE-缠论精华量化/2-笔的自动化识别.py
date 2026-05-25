# -*- coding: utf-8 -*-
"""
第09讲：缠论精华量化
脚本2：笔的自动化识别

教学目标:
  - 理解"笔"的定义：连接相邻顶底分型的最小走势单位
  - 掌握笔的三个约束条件
  - 统计笔的属性（长度、幅度、方向分布）

核心概念:
  如果分型是"字母"，那么笔就是"词语"。
  笔是连接相邻顶分型和底分型的线段，构成走势的基本骨架。

  笔的规则:
    1. 顶底分型必须交替出现（顶-底-顶-底...）
    2. 相邻分型之间至少间隔4根合并K线
    3. 上升笔从底分型到顶分型，下降笔从顶分型到底分型
"""

import pandas as pd
import numpy as np
from data_loader import load_stock_data
from chan_analyzer import ChanAnalyzer

# ============================================================
# 参数配置
# ============================================================

STOCK_CODE = '600519.SH'
START_DATE = '2025-06-01'
END_DATE = '2025-12-31'


# ============================================================
# 主逻辑
# ============================================================

def main():
    print("=" * 60)
    print("第09讲 | 脚本2: 笔的自动化识别")
    print("=" * 60)

    # 1. 加载数据并分析
    print(f"\n[1] 加载 {STOCK_CODE} 日线数据...")
    df = load_stock_data(STOCK_CODE, START_DATE, END_DATE)

    analyzer = ChanAnalyzer(df)
    analyzer.analyze()

    # 2. 笔的基本信息
    print(f"\n[2] 笔的识别结果:")
    print(f"    分型数: {len(analyzer.fractals)}")
    print(f"    笔数:   {len(analyzer.bi_list)}")

    up_bis = [b for b in analyzer.bi_list if b['direction'] == 'up']
    down_bis = [b for b in analyzer.bi_list if b['direction'] == 'down']
    print(f"    上升笔: {len(up_bis)}")
    print(f"    下降笔: {len(down_bis)}")

    # 3. 笔的统计分析
    print(f"\n[3] 笔的统计分析:")
    _analyze_bi_stats(analyzer.bi_list, analyzer.merged_df)

    # 4. 笔列表详情
    print(f"\n[4] 笔列表 (最近15笔):")
    print(f"    {'序号':>4} | {'方向':>4} | {'起始日期':>12} | {'结束日期':>12} | "
          f"{'起价':>8} | {'终价':>8} | {'涨跌幅':>8} | {'K线数':>5}")
    print("    " + "-" * 80)

    for i, bi in enumerate(analyzer.bi_list[-15:], start=max(1, len(analyzer.bi_list) - 14)):
        pct = (bi['end_price'] / bi['start_price'] - 1) * 100
        k_count = bi['end_index'] - bi['start_index']
        direction = '上升' if bi['direction'] == 'up' else '下降'
        print(f"    {i:>4} | {direction:>4} | {bi['start_date'].strftime('%Y-%m-%d'):>12} | "
              f"{bi['end_date'].strftime('%Y-%m-%d'):>12} | "
              f"{bi['start_price']:>8.2f} | {bi['end_price']:>8.2f} | "
              f"{pct:>+7.2f}% | {k_count:>5}")

    # 5. 可视化
    print(f"\n[5] 生成笔的可视化图表...")
    analyzer.plot(
        title=f'{STOCK_CODE} 笔的自动化识别',
        save_path='outputs/2-笔的识别.png',
        show_bi=True,
        show_zhongshu=False,
        show_signals=False,
        show_fractals=True,
    )

    print("\n完成!")


def _analyze_bi_stats(bi_list, merged_df):
    """统计笔的属性"""
    if not bi_list:
        print("    暂无笔")
        return

    up_bis = [b for b in bi_list if b['direction'] == 'up']
    down_bis = [b for b in bi_list if b['direction'] == 'down']

    # 上升笔统计
    if up_bis:
        up_amplitudes = [abs(b['end_price'] - b['start_price']) for b in up_bis]
        up_pcts = [(b['end_price'] / b['start_price'] - 1) * 100 for b in up_bis]
        up_lengths = [b['end_index'] - b['start_index'] for b in up_bis]
        print(f"\n    上升笔 ({len(up_bis)} 笔):")
        print(f"      平均幅度:     {np.mean(up_amplitudes):.2f}")
        print(f"      平均涨幅:     {np.mean(up_pcts):+.2f}%")
        print(f"      最大涨幅:     {np.max(up_pcts):+.2f}%")
        print(f"      平均K线数:    {np.mean(up_lengths):.1f}")

    # 下降笔统计
    if down_bis:
        down_amplitudes = [abs(b['end_price'] - b['start_price']) for b in down_bis]
        down_pcts = [(b['end_price'] / b['start_price'] - 1) * 100 for b in down_bis]
        down_lengths = [b['end_index'] - b['start_index'] for b in down_bis]
        print(f"\n    下降笔 ({len(down_bis)} 笔):")
        print(f"      平均幅度:     {np.mean(down_amplitudes):.2f}")
        print(f"      平均跌幅:     {np.mean(down_pcts):+.2f}%")
        print(f"      最大跌幅:     {np.min(down_pcts):+.2f}%")
        print(f"      平均K线数:    {np.mean(down_lengths):.1f}")

    # 整体
    all_amplitudes = [abs(b['end_price'] - b['start_price']) for b in bi_list]
    all_lengths = [b['end_index'] - b['start_index'] for b in bi_list]
    print(f"\n    整体统计:")
    print(f"      总笔数:       {len(bi_list)}")
    print(f"      平均幅度:     {np.mean(all_amplitudes):.2f}")
    print(f"      平均K线数:    {np.mean(all_lengths):.1f}")
    print(f"      上升/下降比:  {len(up_bis)}/{len(down_bis)}")


if __name__ == '__main__':
    main()
