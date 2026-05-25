# -*- coding: utf-8 -*-
"""
第09讲：缠论精华量化
脚本6：缠论 + 量价增强策略

教学目标:
  - 在缠论三买的基础上，改进出场逻辑提升策略表现
  - 对比"固定止盈止损" vs "动态跟踪止损"的效果
  - 理解出场管理对策略收益的关键影响

策略对比:
  基础策略: 三买入场，ZG止损 + 固定15%止盈
  增强策略: 三买入场，阶梯式跟踪止损 + ATR动态止盈

  增强思路:
    - 入场后初始止损同样设在ZG
    - 盈利>5%时止损移至成本价（保本止损）
    - 盈利>10%时止损移至盈利5%位置（锁定利润）
    - 止盈不设固定比例，改用ATR动态判断（波动收窄时离场）
    - 遇到三卖信号也离场
"""

import backtrader as bt
import numpy as np
import talib
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

class ChanBasicStrategy(bt.Strategy):
    """基础缠论三买策略: 固定止盈止损"""

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


class ChanTrailingStopStrategy(bt.Strategy):
    """增强策略: 阶梯式跟踪止损 + ATR动态管理"""

    params = (
        ('atr_period', 14),
        ('atr_exit_mult', 2.5),
        ('breakeven_pct', 0.05),
        ('lock_profit_pct', 0.10),
        ('lock_amount_pct', 0.05),
    )

    def __init__(self):
        self.entry_price = None
        self.stop_price = None
        self.highest_since_entry = None
        self.order = None

    def notify_order(self, order):
        if order.status == order.Completed:
            if order.isbuy():
                self.entry_price = order.executed.price
                self.highest_since_entry = order.executed.price
            self.order = None
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.order = None

    def _calc_atr(self):
        """计算ATR"""
        size = min(len(self.data), self.p.atr_period + 5)
        if size < self.p.atr_period:
            return None
        high_arr = np.array([self.data.high[-i] for i in range(size, 0, -1)], dtype=float)
        low_arr = np.array([self.data.low[-i] for i in range(size, 0, -1)], dtype=float)
        close_arr = np.array([self.data.close[-i] for i in range(size, 0, -1)], dtype=float)
        atr = talib.ATR(high_arr, low_arr, close_arr, timeperiod=self.p.atr_period)
        if atr is None or np.isnan(atr[-1]):
            return None
        return float(atr[-1])

    def next(self):
        if self.order:
            return

        if not self.position:
            if self.data.chan_signal[0] == 3:
                self.order = self.buy()
                zg_val = self.data.chan_zg[0]
                self.stop_price = zg_val if zg_val > 0 else self.data.close[0] * 0.93
        else:
            current_price = self.data.close[0]

            if current_price > self.highest_since_entry:
                self.highest_since_entry = current_price

            profit_pct = (current_price / self.entry_price) - 1 if self.entry_price else 0

            # 阶梯式止损调整
            if profit_pct >= self.p.lock_profit_pct:
                new_stop = self.entry_price * (1 + self.p.lock_amount_pct)
                self.stop_price = max(self.stop_price or 0, new_stop)
            elif profit_pct >= self.p.breakeven_pct:
                new_stop = self.entry_price
                self.stop_price = max(self.stop_price or 0, new_stop)

            # ATR跟踪止损: 最高价 - ATR倍数
            atr = self._calc_atr()
            if atr and self.highest_since_entry:
                atr_stop = self.highest_since_entry - atr * self.p.atr_exit_mult
                if atr_stop > (self.stop_price or 0):
                    self.stop_price = atr_stop

            # 止损触发
            if self.stop_price and current_price < self.stop_price:
                self.order = self.close()
                return

            # 三卖信号离场
            if self.data.chan_signal[0] == -3:
                self.order = self.close()


# ============================================================
# 主逻辑
# ============================================================

def main():
    print("=" * 60)
    print("第09讲 | 脚本6: 缠论 + 量价增强策略")
    print("=" * 60)

    # 1. 加载数据
    print(f"\n[1] 加载 {STOCK_CODE} 日线数据...")
    df = load_stock_data(STOCK_CODE, START_DATE, END_DATE)
    print(f"    共 {len(df)} 根K线")

    # 2. 缠论分析
    print("\n[2] 执行缠论分析...")
    analyzer = ChanAnalyzer(df)
    analyzer.analyze()

    signal_df = analyzer.get_signal_df()
    third_buy_count = (signal_df['chan_signal'] == 3).sum()
    print(f"    三买信号: {third_buy_count} 个")

    if third_buy_count == 0:
        print("    没有三买信号，无法回测。")
        return

    # 3. 回测: 基础策略（固定止盈止损）
    print(f"\n[3] 回测: 基础策略 (ZG止损 + 15%止盈)")
    result_basic = run_and_report(
        ChanBasicStrategy,
        stock_code=STOCK_CODE,
        label='基础-固定止盈',
        plot=True,
        df=signal_df,
        data_class=ChanPandasData,
    )

    # 4. 回测: 增强策略（跟踪止损）
    print(f"\n[4] 回测: 增强策略 (阶梯止损 + ATR跟踪)")
    result_enhanced = run_and_report(
        ChanTrailingStopStrategy,
        stock_code=STOCK_CODE,
        label='增强-跟踪止损',
        plot=True,
        df=signal_df,
        data_class=ChanPandasData,
    )

    # 5. 对比汇总
    bh_return = calc_buy_and_hold(STOCK_CODE, START_DATE, END_DATE)

    print(f"\n{'=' * 60}")
    print(f"[5] 策略对比汇总")
    print(f"{'=' * 60}")
    print(f"\n    {'指标':>12} | {'基础策略':>12} | {'增强策略':>12} | {'买入持有':>12}")
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
        v1 = result_basic.get(key, 0) * mult
        v2 = result_enhanced.get(key, 0) * mult
        fmt = f"+.2f" if key in ['total_return', 'annual_return'] else '.2f'
        print(f"    {label:>12} | {v1:{fmt}}{unit:>2} | {v2:{fmt}}{unit:>2} |", end='')
        if key == 'total_return' and bh_return is not None:
            print(f" {bh_return*100:+.2f}%")
        else:
            print(f" {'':>12}")

    print(f"\n    增强策略改进点:")
    print(f"      - 盈利>5%: 止损移至成本价（保本止损）")
    print(f"      - 盈利>10%: 止损锁定5%利润")
    print(f"      - ATR跟踪: 最高价 - 2.5倍ATR 作为动态止损")
    print(f"      - 三卖信号也会触发离场")

    print("\n完成!")


if __name__ == '__main__':
    main()
