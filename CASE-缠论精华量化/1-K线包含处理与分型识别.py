# -*- coding: utf-8 -*-
"""
第09讲：缠论精华量化
脚本1：K线包含处理与分型识别

教学目标:
  - 理解K线包含关系及其合并规则（向上合并 / 向下合并）
  - 掌握顶分型和底分型的定义与识别
  - 对比合并前后的分型识别差异

核心概念:
  分型是缠论的最小单位，相当于市场语言的"字母"。
  顶分型 = 中间K线高点最高、低点最高（潜在短期见顶）
  底分型 = 中间K线低点最低、高点最低（潜在短期见底）

  在识别分型之前，必须先处理K线的包含关系，否则会产生大量"伪分型"。
"""

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
    print("第09讲 | 脚本1: K线包含处理与分型识别")
    print("=" * 60)

    # 1. 加载数据
    print(f"\n[1] 加载 {STOCK_CODE} 日线数据 ({START_DATE} ~ {END_DATE})...")
    df = load_stock_data(STOCK_CODE, START_DATE, END_DATE)
    print(f"    共 {len(df)} 根K线")

    # 2. 创建分析器并执行分析
    print("\n[2] 执行缠论分析...")
    analyzer = ChanAnalyzer(df)
    analyzer.analyze()

    # 3. K线合并统计
    print("\n[3] K线合并结果:")
    print(f"    原始K线: {len(df)} 根")
    print(f"    合并后:  {len(analyzer.merged_df)} 根")
    print(f"    被合并:  {len(df) - len(analyzer.merged_df)} 根 "
          f"({(len(df) - len(analyzer.merged_df)) / len(df) * 100:.1f}%)")

    # 4. 演示包含关系处理
    print("\n[4] 包含关系示例:")
    print("    包含关系定义: 一根K线的高低点完全覆盖另一根K线")
    print("    向上合并: 取更高的高点 + 更高的低点 (顺势取高)")
    print("    向下合并: 取更低的高点 + 更低的低点 (顺势取低)")

    _show_merge_examples(df, analyzer.merged_df)

    # 5. 分型统计
    top_fractals = [f for f in analyzer.fractals if f['type'] == 'top']
    bot_fractals = [f for f in analyzer.fractals if f['type'] == 'bottom']

    print(f"\n[5] 分型识别结果:")
    print(f"    顶分型: {len(top_fractals)} 个")
    print(f"    底分型: {len(bot_fractals)} 个")

    if top_fractals:
        print(f"\n    最近5个顶分型:")
        for f in top_fractals[-5:]:
            print(f"      {f['date'].strftime('%Y-%m-%d')} | 价格={f['price']:.2f}")

    if bot_fractals:
        print(f"\n    最近5个底分型:")
        for f in bot_fractals[-5:]:
            print(f"      {f['date'].strftime('%Y-%m-%d')} | 价格={f['price']:.2f}")

    # 6. 对比: 合并前vs合并后的"分型"数量
    raw_top_count, raw_bot_count = _count_raw_fractals(df)
    raw_total = raw_top_count + raw_bot_count
    merged_total = len(top_fractals) + len(bot_fractals)
    print(f"\n[6] 合并前后分型对比:")
    print(f"    原始K线上的分型: 顶{raw_top_count} + 底{raw_bot_count} = {raw_total} 个")
    print(f"    合并K线上的分型: 顶{len(top_fractals)} + 底{len(bot_fractals)} = {merged_total} 个")
    if merged_total > raw_total:
        print(f"    合并后多出 {merged_total - raw_total} 个分型")
        print(f"    (包含关系隐藏了真实的顶底结构，合并后暴露出更多有效分型)")
    else:
        print(f"    合并后减少 {raw_total - merged_total} 个噪声分型")

    # 7. 可视化
    print("\n[7] 生成对比图表...")
    analyzer.plot_compare_merge(save_path='outputs/1-分型识别_合并对比.png')

    print("\n    生成完整分型图...")
    analyzer.plot(
        title=f'{STOCK_CODE} K线合并与分型识别',
        save_path='outputs/1-分型识别_完整图.png',
        show_bi=False,
        show_zhongshu=False,
        show_signals=False,
        show_fractals=True,
    )

    print("\n完成!")


def _show_merge_examples(raw_df, merged_df):
    """展示几个K线合并的具体案例"""
    merged_dates = set(merged_df.index)
    skipped = []

    for i in range(len(raw_df)):
        if raw_df.index[i] not in merged_dates:
            skipped.append(raw_df.index[i])

    if skipped:
        print(f"\n    被合并的K线日期 (前10个):")
        for d in skipped[:10]:
            idx = raw_df.index.get_loc(d)
            row = raw_df.iloc[idx]
            print(f"      {d.strftime('%Y-%m-%d')} | "
                  f"高={row['high']:.2f} 低={row['low']:.2f} "
                  f"(被相邻K线包含)")


def _count_raw_fractals(df):
    """在未合并的原始K线上统计"伪分型"数量"""
    top_count = 0
    bot_count = 0
    for i in range(1, len(df) - 1):
        h_p, h_c, h_n = df['high'].iloc[i-1], df['high'].iloc[i], df['high'].iloc[i+1]
        l_p, l_c, l_n = df['low'].iloc[i-1], df['low'].iloc[i], df['low'].iloc[i+1]
        if h_c > h_p and h_c > h_n and l_c > l_p and l_c > l_n:
            top_count += 1
        elif l_c < l_p and l_c < l_n and h_c < h_p and h_c < h_n:
            bot_count += 1
    return top_count, bot_count


if __name__ == '__main__':
    main()
