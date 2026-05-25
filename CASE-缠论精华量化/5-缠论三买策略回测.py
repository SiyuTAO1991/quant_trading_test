# -*- coding: utf-8 -*-
"""
第09讲：缠论精华量化
脚本5：缠论三买策略回测

教学目标:
  - 将缠论第三类买点信号接入 Backtrader 进行策略回测
  - 设计合理的止损止盈规则
  - 与买入持有进行对比

策略逻辑:
  入场: 缠论三买信号触发（突破中枢后回踩不入中枢）
  止损: 价格跌回中枢（收盘价 < ZG）
  止盈: 固定比例止盈（默认15%），或出现三卖信号离场

数据流:
  1. 加载原始数据 → ChanAnalyzer 分析 → 生成信号列
  2. 信号列合并到 DataFrame → ChanPandasData 送入 Backtrader
  3. 策略在 next() 中读取信号线执行交易
"""

import backtrader as bt
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
# 策略定义
# ============================================================

class ChanThirdBuyStrategy(bt.Strategy):
    """缠论第三类买点策略"""

    params = (
        ('take_profit_pct', 0.15),
        ('use_chan_stop', True),
    )

    def __init__(self):
        self.entry_price = None
        self.stop_price = None
        self.order = None

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                self.entry_price = order.executed.price
            self.order = None
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.order = None

    def next(self):
        if self.order:
            return

        if not self.position:
            # 入场条件: 三买信号 (chan_signal == 3)
            if self.data.chan_signal[0] == 3:
                self.order = self.buy()
                zg_val = self.data.chan_zg[0]
                if self.p.use_chan_stop and zg_val > 0:
                    self.stop_price = zg_val
                else:
                    self.stop_price = self.data.close[0] * 0.93
        else:
            current_price = self.data.close[0]

            # 止损: 跌回中枢（收盘价 < ZG）
            if self.stop_price and current_price < self.stop_price:
                self.order = self.close()
                return

            # 止盈: 达到目标涨幅
            if self.entry_price:
                profit_pct = (current_price / self.entry_price) - 1
                if profit_pct >= self.p.take_profit_pct:
                    self.order = self.close()
                    return

            # 三卖信号离场 (chan_signal == -3)
            if self.data.chan_signal[0] == -3:
                self.order = self.close()
                return


# ============================================================
# 主逻辑
# ============================================================

def main():
    print("=" * 60)
    print("第09讲 | 脚本5: 缠论三买策略回测")
    print("=" * 60)

    # 1. 加载数据
    print(f"\n[1] 加载 {STOCK_CODE} 日线数据...")
    df = load_stock_data(STOCK_CODE, START_DATE, END_DATE)
    print(f"    共 {len(df)} 根K线")

    # 2. 缠论分析 + 生成信号列
    print("\n[2] 执行缠论分析...")
    analyzer = ChanAnalyzer(df)
    analyzer.analyze()
    analyzer.summary()

    signal_df = analyzer.get_signal_df()
    third_buy_count = (signal_df['chan_signal'] == 3).sum()
    third_sell_count = (signal_df['chan_signal'] == -3).sum()
    print(f"\n    信号统计: 三买={third_buy_count}, 三卖={third_sell_count}")

    if third_buy_count == 0:
        print("    没有三买信号，无法回测。请尝试更长的时间区间或其他股票。")
        return

    # 3. 运行回测
    print(f"\n[3] 运行回测...")
    result = run_and_report(
        ChanThirdBuyStrategy,
        stock_code=STOCK_CODE,
        label='缠论三买策略',
        plot=True,
        df=signal_df,
        data_class=ChanPandasData,
    )

    # 4. 对比买入持有
    bh_return = calc_buy_and_hold(STOCK_CODE, START_DATE, END_DATE)
    print(f"\n[4] 策略 vs 买入持有:")
    print(f"    策略收益:   {result['total_return']*100:+.2f}%")
    if bh_return is not None:
        print(f"    买入持有:   {bh_return*100:+.2f}%")
        excess = result['total_return'] - bh_return
        print(f"    超额收益:   {excess*100:+.2f}%")

    # 5. 交易明细
    trades = result.get('trades', [])
    if trades:
        print(f"\n[5] 交易明细 ({len(trades)} 笔):")
        print(f"    {'日期':>12} | {'操作':>4} | {'价格':>8} | {'数量':>6}")
        print("    " + "-" * 45)
        for t in trades:
            print(f"    {t['date']} | {t['type']:>4} | {t['price']:>8.2f} | {t['size']:>6}")

    print("\n完成!")


if __name__ == '__main__':
    main()
