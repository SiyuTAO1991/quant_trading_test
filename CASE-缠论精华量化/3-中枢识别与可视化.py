# -*- coding: utf-8 -*-
"""
第09讲：缠论精华量化
脚本3：中枢识别与可视化

教学目标:
  - 理解"中枢"的定义：至少三段笔构成的价格重叠区间
  - 掌握ZG（中枢高点）和ZD（中枢低点）的计算方法
  - 判断中枢的移动方向（上移/下移/震荡）

核心概念:
  如果分型是"字母"、笔是"词语"，那么中枢就是"句子"。
  中枢代表市场的平衡区域，是多空力量博弈的战场。

  中枢的数学定义:
    至少3笔的价格区间有重叠
    ZG = min(各笔最高价) → 重叠区间的天花板
    ZD = max(各笔最低价) → 重叠区间的地板
    有效条件: ZG > ZD（确实存在重叠）

  中枢方向判断:
    - 中枢上移: 新中枢的ZG/ZD都高于前一个 → 上升趋势
    - 中枢下移: 新中枢的ZG/ZD都低于前一个 → 下跌趋势
    - 中枢重叠: 新旧中枢有交叉 → 震荡走势
"""

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
    print("第09讲 | 脚本3: 中枢识别与可视化")
    print("=" * 60)

    # 1. 加载数据并分析
    print(f"\n[1] 加载 {STOCK_CODE} 日线数据 ({START_DATE} ~ {END_DATE})...")
    df = load_stock_data(STOCK_CODE, START_DATE, END_DATE)
    print(f"    共 {len(df)} 根K线")

    analyzer = ChanAnalyzer(df)
    analyzer.analyze()

    # 2. 中枢基本信息
    print(f"\n[2] 中枢识别结果:")
    print(f"    笔数:   {len(analyzer.bi_list)}")
    print(f"    中枢数: {len(analyzer.zhongshu_list)}")

    if not analyzer.zhongshu_list:
        print("    未识别到中枢，可能数据量不足或区间太短")
        return

    # 3. 中枢详情
    print(f"\n[3] 中枢列表:")
    print(f"    {'序号':>4} | {'起始':>12} | {'结束':>12} | "
          f"{'ZG':>8} | {'ZD':>8} | {'中心':>8} | {'幅度%':>6} | {'笔数':>4}")
    print("    " + "-" * 80)

    for i, zs in enumerate(analyzer.zhongshu_list, 1):
        amplitude = (zs['ZG'] - zs['ZD']) / zs['center'] * 100
        print(f"    {i:>4} | {zs['start_date'].strftime('%Y-%m-%d'):>12} | "
              f"{zs['end_date'].strftime('%Y-%m-%d'):>12} | "
              f"{zs['ZG']:>8.2f} | {zs['ZD']:>8.2f} | {zs['center']:>8.2f} | "
              f"{amplitude:>5.1f}% | {zs['bi_count']:>4}")

    # 4. 中枢方向分析
    print(f"\n[4] 中枢方向分析:")
    _analyze_zhongshu_trend(analyzer.zhongshu_list)

    # 5. 中枢统计
    print(f"\n[5] 中枢统计:")
    amplitudes = [(zs['ZG'] - zs['ZD']) / zs['center'] * 100 for zs in analyzer.zhongshu_list]
    bi_counts = [zs['bi_count'] for zs in analyzer.zhongshu_list]

    print(f"    平均中枢幅度: {np.mean(amplitudes):.2f}%")
    print(f"    最大中枢幅度: {np.max(amplitudes):.2f}%")
    print(f"    最小中枢幅度: {np.min(amplitudes):.2f}%")
    print(f"    平均包含笔数: {np.mean(bi_counts):.1f}")
    print(f"    最大包含笔数: {np.max(bi_counts)}")

    # 6. 可视化
    print(f"\n[6] 生成中枢可视化图表...")
    analyzer.plot(
        title=f'{STOCK_CODE} 中枢识别与可视化',
        save_path='outputs/3-中枢识别.png',
        show_bi=True,
        show_zhongshu=True,
        show_signals=False,
        show_fractals=False,
    )

    print("\n完成!")


def _analyze_zhongshu_trend(zhongshu_list):
    """分析中枢之间的移动方向"""
    if len(zhongshu_list) < 2:
        print("    只有1个中枢，无法判断趋势方向")
        return

    for i in range(1, len(zhongshu_list)):
        prev = zhongshu_list[i - 1]
        curr = zhongshu_list[i]

        if curr['ZG'] > prev['ZG'] and curr['ZD'] > prev['ZD']:
            trend = '上移 (上升趋势)'
        elif curr['ZG'] < prev['ZG'] and curr['ZD'] < prev['ZD']:
            trend = '下移 (下跌趋势)'
        else:
            trend = '重叠 (震荡走势)'

        zg_change = curr['ZG'] - prev['ZG']
        zd_change = curr['ZD'] - prev['ZD']

        print(f"    中枢{i} -> 中枢{i+1}: {trend}")
        print(f"      ZG变化: {zg_change:+.2f}  ZD变化: {zd_change:+.2f}")


if __name__ == '__main__':
    main()
