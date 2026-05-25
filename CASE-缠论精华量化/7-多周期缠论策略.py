# -*- coding: utf-8 -*-
"""
第09讲：缠论精华量化
脚本7：多周期缠论策略

教学目标:
  - 理解"区间套"思想：大周期定方向，小周期找入场
  - 周线确定趋势方向（中枢上移 = 上升趋势）
  - 日线在上升趋势中寻找三买入场点
  - 对比单周期 vs 多周期的策略表现

策略逻辑:
  周线分析: 中枢方向判断
    - 最近两个周线中枢上移 → 上升趋势 (trend = 1)
    - 最近两个周线中枢下移 → 下跌趋势 (trend = -1)
    - 其他 → 震荡 (trend = 0)

  日线交易:
    入场: 周线趋势向上(trend=1) 且 日线出现三买信号
    止损: 跌回中枢（收盘价 < ZG）
    止盈: 15% 固定止盈 或 三卖信号离场

  核心优势:
    周线过滤掉了日线在下跌/震荡趋势中的假信号，提升胜率。
"""

import backtrader as bt
import pandas as pd
import numpy as np
from data_loader import (
    load_stock_data, ChanPandasData,
    run_and_report, calc_buy_and_hold,
)
from chan_analyzer import ChanAnalyzer

# ============================================================
# 参数配置
# ============================================================

STOCK_CODE = '600519.SH'
START_DATE = '2023-01-01'
END_DATE = '2025-12-31'


# ============================================================
# 周线趋势计算
# ============================================================

def calc_weekly_trend(df):
    """
    基于周线缠论分析 + 均线辅助 计算趋势方向

    策略:
      优先级1: 周线中枢方向（>=2个中枢时可判断）
      优先级2: 笔的方向 + 价格与中枢位置关系
      优先级3: 周线MA20方向（兜底判断）

    返回: Series, 索引为日期, 值为趋势方向
      1  = 上升趋势
      -1 = 下跌趋势
      0  = 震荡
    """
    weekly_df = df.resample('W').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()

    if len(weekly_df) < 20:
        return pd.Series(0, index=df.index)

    w_analyzer = ChanAnalyzer(weekly_df)
    w_analyzer.analyze()

    print(f"    周线分析: {len(weekly_df)}根周K线, "
          f"{len(w_analyzer.bi_list)}笔, {len(w_analyzer.zhongshu_list)}个中枢")

    weekly_trend = pd.Series(0, index=weekly_df.index)

    # 方法1: 多个中枢时，按中枢方向判断
    if len(w_analyzer.zhongshu_list) >= 2:
        for i in range(1, len(w_analyzer.zhongshu_list)):
            curr = w_analyzer.zhongshu_list[i]
            prev = w_analyzer.zhongshu_list[i - 1]

            if curr['ZG'] > prev['ZG'] and curr['ZD'] > prev['ZD']:
                trend = 1
            elif curr['ZG'] < prev['ZG'] and curr['ZD'] < prev['ZD']:
                trend = -1
            else:
                trend = 0

            mask = weekly_trend.index >= curr['start_date']
            weekly_trend.loc[mask] = trend

    # 方法2: 有中枢但只有1个时，用价格与中枢的位置关系 + 笔方向判断
    if len(w_analyzer.zhongshu_list) >= 1:
        for zs in w_analyzer.zhongshu_list:
            for idx in range(len(weekly_df)):
                date = weekly_df.index[idx]
                if date < zs['start_date']:
                    continue
                close = weekly_df['close'].iloc[idx]
                if weekly_trend.loc[date] != 0:
                    continue
                if close > zs['ZG']:
                    weekly_trend.loc[date] = 1
                elif close < zs['ZD']:
                    weekly_trend.loc[date] = -1

    # 方法3: 兜底 — 用周线MA20判断
    ma20 = weekly_df['close'].rolling(20).mean()
    for idx in range(20, len(weekly_df)):
        date = weekly_df.index[idx]
        if weekly_trend.loc[date] != 0:
            continue
        if weekly_df['close'].iloc[idx] > ma20.iloc[idx]:
            weekly_trend.loc[date] = 1
        elif weekly_df['close'].iloc[idx] < ma20.iloc[idx]:
            weekly_trend.loc[date] = -1

    daily_trend = weekly_trend.reindex(df.index, method='ffill').fillna(0).astype(int)
    return daily_trend


# ============================================================
# 策略定义
# ============================================================

class ChanSinglePeriodStrategy(bt.Strategy):
    """单周期缠论三买策略（对照组）"""

    params = (
        ('take_profit_pct', 0.15),
    )

    def __init__(self):
        self.entry_price = None
        self.stop_price = None
        self.order = None

    def notify_order(self, order):
        if order.status == order.Completed:
            if order.isbuy():
                self.entry_price = order.executed.price
            self.order = None
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.order = None

    def next(self):
        if self.order:
            return

        if not self.position:
            if self.data.chan_signal[0] == 3:
                self.order = self.buy()
                self.stop_price = self.data.chan_zg[0] if self.data.chan_zg[0] > 0 else self.data.close[0] * 0.93
        else:
            current_price = self.data.close[0]
            if self.stop_price and current_price < self.stop_price:
                self.order = self.close()
                return
            if self.entry_price and (current_price / self.entry_price - 1) >= self.p.take_profit_pct:
                self.order = self.close()
                return
            if self.data.chan_signal[0] == -3:
                self.order = self.close()


class ChanMultiPeriodStrategy(bt.Strategy):
    """多周期缠论策略: 周线趋势 + 日线三买"""

    params = (
        ('take_profit_pct', 0.15),
    )

    def __init__(self):
        self.entry_price = None
        self.stop_price = None
        self.order = None

    def notify_order(self, order):
        if order.status == order.Completed:
            if order.isbuy():
                self.entry_price = order.executed.price
            self.order = None
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.order = None

    def next(self):
        if self.order:
            return

        if not self.position:
            weekly_up = self.data.weekly_trend[0] == 1
            daily_third_buy = self.data.chan_signal[0] == 3

            if weekly_up and daily_third_buy:
                self.order = self.buy()
                self.stop_price = self.data.chan_zg[0] if self.data.chan_zg[0] > 0 else self.data.close[0] * 0.93
        else:
            current_price = self.data.close[0]

            if self.stop_price and current_price < self.stop_price:
                self.order = self.close()
                return

            if self.entry_price and (current_price / self.entry_price - 1) >= self.p.take_profit_pct:
                self.order = self.close()
                return

            if self.data.chan_signal[0] == -3:
                self.order = self.close()
                return

            if self.data.weekly_trend[0] == -1:
                self.order = self.close()


# ============================================================
# 主逻辑
# ============================================================

def main():
    print("=" * 60)
    print("第09讲 | 脚本7: 多周期缠论策略")
    print("=" * 60)

    # 1. 加载数据
    print(f"\n[1] 加载 {STOCK_CODE} 日线数据 ({START_DATE} ~ {END_DATE})...")
    df = load_stock_data(STOCK_CODE, START_DATE, END_DATE)
    print(f"    共 {len(df)} 根K线")

    # 2. 日线缠论分析
    print("\n[2] 日线缠论分析...")
    daily_analyzer = ChanAnalyzer(df)
    daily_analyzer.analyze()

    signal_df = daily_analyzer.get_signal_df()
    third_buy_count = (signal_df['chan_signal'] == 3).sum()
    print(f"    日线三买信号: {third_buy_count} 个")

    # 3. 周线趋势分析
    print("\n[3] 周线趋势分析...")
    weekly_trend = calc_weekly_trend(df)
    signal_df['weekly_trend'] = weekly_trend

    up_days = (weekly_trend == 1).sum()
    down_days = (weekly_trend == -1).sum()
    flat_days = (weekly_trend == 0).sum()
    print(f"    趋势分布: 上升={up_days}天, 下跌={down_days}天, 震荡={flat_days}天")

    # 周线趋势下的三买信号过滤
    filtered_count = ((signal_df['chan_signal'] == 3) & (signal_df['weekly_trend'] == 1)).sum()
    print(f"    周线上升+日线三买: {filtered_count} 个 (过滤前: {third_buy_count} 个)")

    if third_buy_count == 0:
        print("    没有三买信号，无法回测。")
        return

    # 4. 回测: 单周期策略
    print(f"\n[4] 回测: 单周期缠论三买策略")
    result_single = run_and_report(
        ChanSinglePeriodStrategy,
        stock_code=STOCK_CODE,
        label='单周期三买',
        plot=True,
        df=signal_df,
        data_class=ChanPandasData,
    )

    # 5. 回测: 多周期策略
    print(f"\n[5] 回测: 多周期缠论策略 (周线+日线)")
    result_multi = run_and_report(
        ChanMultiPeriodStrategy,
        stock_code=STOCK_CODE,
        label='多周期三买',
        plot=True,
        df=signal_df,
        data_class=ChanPandasData,
    )

    # 6. 对比汇总
    bh_return = calc_buy_and_hold(STOCK_CODE, START_DATE, END_DATE)

    print(f"\n{'=' * 60}")
    print(f"[6] 策略对比汇总")
    print(f"{'=' * 60}")
    print(f"\n    {'指标':>12} | {'单周期':>12} | {'多周期':>12} | {'买入持有':>12}")
    print(f"    {'-'*60}")

    metrics = [
        ('总收益', 'total_return', '%', 100),
        ('年化收益', 'annual_return', '%', 100),
        ('最大回撤', 'max_drawdown', '%', 100),
        ('夏普比率', 'sharpe_ratio', '', 1),
        ('胜率', 'win_rate', '%', 100),
        ('盈亏比', 'profit_loss_ratio', '', 1),
        ('交易次数', 'total_trades', '', 1),
    ]

    for label, key, unit, mult in metrics:
        v1 = result_single.get(key, 0) * mult
        v2 = result_multi.get(key, 0) * mult
        fmt = f"+.2f" if key in ['total_return', 'annual_return'] else '.2f'
        print(f"    {label:>12} | {v1:{fmt}}{unit:>2} | {v2:{fmt}}{unit:>2} |", end='')
        if key == 'total_return' and bh_return is not None:
            print(f" {bh_return*100:+.2f}%")
        else:
            print(f" {'':>12}")

    print(f"\n    多周期策略优势:")
    print(f"      - 周线趋势过滤: 只在上升趋势中做多，避免逆势交易")
    print(f"      - 信号质量提升: 从 {third_buy_count} 个三买过滤到 {filtered_count} 个")
    print(f"      - 周线转空自动离场: 趋势反转时及时止损")

    print("\n完成!")


if __name__ == '__main__':
    main()
