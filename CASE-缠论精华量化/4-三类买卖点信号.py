# -*- coding: utf-8 -*-
"""
第09讲：缠论精华量化
脚本4：三类买卖点信号

教学目标:
  - 掌握缠论三类买卖点的精确定义
  - 理解每类买点的实战价值和适用场景
  - 评估信号的后续收益表现

核心概念:
  三类买卖点是缠论最核心的实战输出，构成完整的交易闭环。

  第一类买点（一买）: 趋势背驰点
    - 下跌趋势中，最后一段下跌的力度弱于前一段（MACD面积衡量）
    - 对应"跌不动了"的拐点，风险最大但收益空间最大

  第二类买点（二买）: 确认点
    - 一买后首次回调不破一买低点
    - 对应"回调确认"，安全性高于一买

  第三类买点（三买）: 主升浪启动点 [最具实战价值]
    - 价格突破中枢后回踩不进入中枢
    - 对应"强势确认"，是趋势延续的标志性信号
"""

import pandas as pd
import numpy as np
from data_loader import load_stock_data
from chan_analyzer import ChanAnalyzer

# ============================================================
# 参数配置
# ============================================================

STOCK_CODE = '600519.SH'
START_DATE = '2023-01-01'
END_DATE = '2025-12-31'


# ============================================================
# 主逻辑
# ============================================================

def main():
    print("=" * 60)
    print("第09讲 | 脚本4: 三类买卖点信号")
    print("=" * 60)

    # 1. 加载数据并分析
    print(f"\n[1] 加载 {STOCK_CODE} 日线数据 ({START_DATE} ~ {END_DATE})...")
    df = load_stock_data(STOCK_CODE, START_DATE, END_DATE)
    print(f"    共 {len(df)} 根K线")

    analyzer = ChanAnalyzer(df)
    analyzer.analyze()

    # 2. 分析摘要
    print(f"\n[2] 缠论分析摘要:")
    analyzer.summary()

    # 3. 信号详情与后续收益
    if analyzer.signals:
        print(f"\n[3] 信号后续收益分析:")
        _analyze_signal_returns(df, analyzer.signals)
    else:
        print(f"\n[3] 未检测到任何买卖点信号")
        print("    可能原因: 数据区间内走势结构不典型")
        print("    建议: 尝试更长的时间区间或不同的股票")

    # 4. 三买信号重点分析
    third_buys = [s for s in analyzer.signals if s['type'] == 'third_buy']
    if third_buys:
        print(f"\n[4] 三买信号重点分析 (共{len(third_buys)}个):")
        _analyze_third_buy_detail(df, third_buys)

    # 5. 可视化
    print(f"\n[5] 生成买卖点信号图表...")
    analyzer.plot(
        title=f'{STOCK_CODE} 三类买卖点信号',
        save_path='outputs/4-三类买卖点.png',
        show_bi=True,
        show_zhongshu=True,
        show_signals=True,
        show_fractals=False,
    )

    print("\n完成!")


def _analyze_signal_returns(df, signals):
    """分析每个信号的后续N日收益"""

    sig_names = {
        'first_buy': '一买', 'second_buy': '二买',
        'third_buy': '三买', 'third_sell': '三卖',
    }

    print(f"\n    {'日期':>12} | {'类型':>4} | {'价格':>8} | "
          f"{'5日':>7} | {'10日':>7} | {'20日':>7} | {'判定':>6}")
    print("    " + "-" * 75)

    win_count = 0
    total_count = 0

    for sig in signals:
        date = sig['date']
        try:
            idx = df.index.get_loc(date)
        except KeyError:
            continue

        returns = {}
        for n in [5, 10, 20]:
            if idx + n < len(df):
                future_price = float(df['close'].iloc[idx + n])
                ret = (future_price / sig['price'] - 1) * 100
                returns[n] = ret

        name = sig_names.get(sig['type'], sig['type'])

        r5 = f"{returns.get(5, 0):+6.2f}%" if 5 in returns else '   N/A'
        r10 = f"{returns.get(10, 0):+6.2f}%" if 10 in returns else '   N/A'
        r20 = f"{returns.get(20, 0):+6.2f}%" if 20 in returns else '   N/A'

        if 10 in returns:
            total_count += 1
            if returns[10] > 0:
                win_count += 1
                verdict = '盈利'
            else:
                verdict = '亏损'
        else:
            verdict = '待定'

        print(f"    {date.strftime('%Y-%m-%d'):>12} | {name:>4} | {sig['price']:>8.2f} | "
              f"{r5:>7} | {r10:>7} | {r20:>7} | {verdict:>6}")

    if total_count > 0:
        print(f"\n    10日胜率: {win_count}/{total_count} = {win_count/total_count*100:.1f}%")


def _analyze_third_buy_detail(df, third_buys):
    """三买信号的详细分析"""
    for i, sig in enumerate(third_buys, 1):
        date = sig['date']
        zg = sig.get('zhongshu_zg', 0)
        zd = sig.get('zhongshu_zd', 0)

        try:
            idx = df.index.get_loc(date)
        except KeyError:
            continue

        print(f"\n    三买[{i}] {date.strftime('%Y-%m-%d')}:")
        print(f"      信号价格: {sig['price']:.2f}")
        print(f"      对应中枢: ZG={zg:.2f}, ZD={zd:.2f}")
        if zg > 0:
            distance = (sig['price'] / zg - 1) * 100
            print(f"      距ZG距离: {distance:+.2f}% (回踩幅度)")

        max_return = 0
        for n in [5, 10, 20, 40]:
            if idx + n < len(df):
                future_price = float(df['close'].iloc[idx + n])
                ret = (future_price / sig['price'] - 1) * 100
                max_return = max(max_return, ret)
                print(f"      {n}日后收益: {ret:+.2f}%")

        if max_return > 10:
            print(f"      评价: 强势信号")
        elif max_return > 0:
            print(f"      评价: 有效信号")
        else:
            print(f"      评价: 失败信号")


if __name__ == '__main__':
    main()
