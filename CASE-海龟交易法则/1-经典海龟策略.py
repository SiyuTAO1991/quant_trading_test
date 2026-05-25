# -*- coding: utf-8 -*-
"""
经典海龟交易策略 - ATR仓位管理 + 金字塔加仓

核心理念:
  海龟交易法则的本质不是"如何发现趋势", 而是"如何科学管理风险"
  其他趋势策略关注: "什么时候买"
  海龟策略关注:      "买多少"

四大组件:
  1. 唐奇安通道 - 突破信号 (20日最高价入场, 10日最低价出场)
  2. ATR(N值)   - 波动率度量 (20日均值)
  3. ATR仓位    - 风险恒定的仓位管理 (核心创新)
  4. 金字塔加仓 - 趋势确认后逐步增加头寸 (最多4个单位, 每0.5N加仓)

本案例:
  Part 1: SimpleTurtle vs FullTurtle 对比 (展示ATR仓位管理的价值)
  Part 2: 多标的横向对比 (展示趋势跟踪策略对市场环境的依赖)

运行: python 1-经典海龟策略.py
"""
import numpy as np
import backtrader as bt
from data_loader import run_and_report, calc_buy_and_hold


# ============================================================
# 简单海龟策略 - 仅信号, 固定仓位 (对照组)
# ============================================================

class SimpleTurtleStrategy(bt.Strategy):
    """
    简单版海龟: 只有唐奇安通道突破信号, 没有ATR仓位管理
    用于和完整海龟对比, 展示仓位管理的价值
    """
    params = (
        ('entry_period', 20),
        ('exit_period', 10),
    )

    def __init__(self):
        self.entry_high = bt.ind.Highest(self.data.high, period=self.p.entry_period)
        self.exit_low = bt.ind.Lowest(self.data.low, period=self.p.exit_period)

    def next(self):
        if not self.position:
            if self.data.close[0] > self.entry_high[-1]:
                self.buy()
        else:
            if self.data.close[0] < self.exit_low[-1]:
                self.close()


# ============================================================
# 完整海龟策略 - ATR仓位 + 金字塔加仓 + 2N止损
# ============================================================

class TurtleStrategy(bt.Strategy):
    """
    完整海龟交易策略

    仓位公式:
      单位大小 = (账户资金 * 风险比例) / ATR
      含义: 价格波动1个ATR时, 账户恰好变动 risk_pct

    加仓规则:
      - 最多持有 max_units 个单位
      - 每上涨 add_n 个ATR, 加一个单位
      - 每次加仓后, 止损线上移至 最新入场价 - stop_n * ATR

    止损规则:
      - 价格跌破止损线 -> 全部平仓

    出场规则:
      - 价格跌破 exit_period 日最低价 -> 全部平仓
    """
    params = (
        ('entry_period', 20),   # 入场通道周期
        ('exit_period', 10),    # 出场通道周期
        ('atr_period', 20),     # ATR周期
        ('risk_pct', 0.01),     # 单笔风险比例(1%)
        ('max_units', 4),       # 最大持仓单位数
        ('add_n', 0.5),         # 每上涨0.5个ATR加仓
        ('stop_n', 2.0),        # 2个ATR止损
    )

    def __init__(self):
        self.entry_high = bt.ind.Highest(self.data.high, period=self.p.entry_period)
        self.exit_low = bt.ind.Lowest(self.data.low, period=self.p.exit_period)
        self.atr = bt.ind.ATR(period=self.p.atr_period)
        self.units = 0
        self.entry_prices = []
        self.stop_price = 0.0
        self.last_add_price = 0.0
        self.order = None

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status == order.Completed:
            if order.isbuy():
                fp = order.executed.price
                self.entry_prices.append(fp)
                self.units = len(self.entry_prices)
                self.stop_price = fp - self.p.stop_n * self.atr[0]
                self.last_add_price = fp
            elif order.issell():
                self.units = 0
                self.entry_prices = []
                self.stop_price = 0.0
                self.last_add_price = 0.0
        self.order = None

    def _calc_unit_size(self):
        """
        ATR仓位公式:
          单位大小 = (账户总值 * 风险比例) / ATR
          取整到100股(A股1手)
        """
        portfolio_value = self.broker.getvalue()
        atr_val = self.atr[0]
        if atr_val <= 0:
            return 0
        unit_size = (portfolio_value * self.p.risk_pct) / atr_val
        unit_size = int(unit_size // 100) * 100
        return max(unit_size, 100)

    def next(self):
        if self.order:
            return
        atr_val = self.atr[0]
        if np.isnan(atr_val) or atr_val <= 0:
            return
        close = self.data.close[0]

        if not self.position:
            if close > self.entry_high[-1]:
                size = self._calc_unit_size()
                if size > 0:
                    self.order = self.buy(size=size)
        else:
            if close < self.stop_price:
                self.order = self.close()
                return
            if close < self.exit_low[-1]:
                self.order = self.close()
                return
            if self.units < self.p.max_units:
                if close >= self.last_add_price + self.p.add_n * atr_val:
                    size = self._calc_unit_size()
                    cash = self.broker.getcash()
                    if size > 0 and cash > close * size * 1.01:
                        self.order = self.buy(size=size)


# ============================================================
# 主程序
# ============================================================

def _print_three_strategy_table(bh_ret, r_simple, r_full):
    """打印 买入持有 vs 简单海龟 vs 完整海龟 的对比表格"""
    bh_val = (bh_ret or 0) * 100
    print(f"  {'指标':<12} {'买入持有':>12} {'简单海龟':>12} {'完整海龟':>12}")
    print(f"  {'-' * 52}")
    print(f"  {'总收益':<12} {bh_val:>+11.2f}% {r_simple['total_return']*100:>+11.2f}% {r_full['total_return']*100:>+11.2f}%")
    print(f"  {'最大回撤':<12} {'--':>12} {r_simple['max_drawdown']*100:>11.2f}% {r_full['max_drawdown']*100:>11.2f}%")
    print(f"  {'夏普比率':<12} {'--':>12} {r_simple['sharpe_ratio']:>12.2f} {r_full['sharpe_ratio']:>12.2f}")
    print(f"  {'交易次数':<12} {'--':>12} {r_simple['total_trades']:>12d} {r_full['total_trades']:>12d}")
    print(f"  {'胜率':<12} {'--':>12} {r_simple['win_rate']*100:>11.1f}% {r_full['win_rate']*100:>11.1f}%")
    print(f"  {'盈亏比':<12} {'--':>12} {r_simple['profit_loss_ratio']:>12.2f} {r_full['profit_loss_ratio']:>12.2f}")


if __name__ == '__main__':
    start_date = '2024-01-01'
    end_date = '2025-12-31'

    print("=" * 70)
    print("海龟交易法则 - 经典策略实战")
    print("=" * 70)
    print("\n海龟四大组件:")
    print("  1. 唐奇安通道: 突破20日最高价入场, 跌破10日最低价出场")
    print("  2. ATR(N值):   衡量市场波动幅度")
    print("  3. ATR仓位:    单位大小 = (账户资金 * 1%) / ATR")
    print("  4. 金字塔加仓: 最多4个单位, 每上涨0.5N加仓, 2N止损")

    # ================================================================
    # Part 1: 三策略对比 - 买入持有 vs 简单海龟 vs 完整海龟
    # ================================================================
    demo_stock = '510300.SH'
    print(f"\n{'=' * 70}")
    print(f"Part 1: 买入持有 vs 简单海龟 vs 完整海龟 ({demo_stock} 沪深300ETF)")
    print(f"{'=' * 70}")

    print(f"\n[简单海龟] 仅唐奇安通道信号, 固定仓位95%:")
    r_simple = run_and_report(
        SimpleTurtleStrategy, demo_stock, start_date, end_date,
        label='简单海龟', plot=True, use_sizer=True,
    )

    print(f"\n[完整海龟] ATR仓位管理 + 金字塔加仓 + 2N止损:")
    r_full = run_and_report(
        TurtleStrategy, demo_stock, start_date, end_date,
        label='完整海龟', plot=True, use_sizer=False,
    )

    bh = calc_buy_and_hold(demo_stock, start_date, end_date)

    print(f"\n{'  三策略对比 ':=^60}")
    _print_three_strategy_table(bh, r_simple, r_full)

    # ================================================================
    # Part 2: 多标的横向对比 (展示趋势跟踪策略对市场环境的依赖)
    # ================================================================
    print(f"\n{'=' * 70}")
    print("Part 2: 三策略在不同标的上的表现")
    print(f"{'=' * 70}")
    print("  趋势跟踪策略的核心前提: 市场存在趋势")
    print("  下面对比: 下跌股 vs 上涨ETF, 看三种策略的差异\n")

    stocks = [
        ('600519.SH', '贵州茅台'),
        ('510300.SH', '沪深300ETF'),
        ('159941.SZ', '纳指ETF'),
    ]

    results = {}
    for code, name in stocks:
        try:
            print(f"\n--- {name}({code}) ---")
            r_s = run_and_report(
                SimpleTurtleStrategy, code, start_date, end_date,
                label=f'{name}-简单海龟', plot=True, use_sizer=True,
            )
            r_f = run_and_report(
                TurtleStrategy, code, start_date, end_date,
                label=f'{name}-完整海龟', plot=True, use_sizer=False,
            )
            bh_ret = calc_buy_and_hold(code, start_date, end_date)
            results[code] = {
                'name': name, 'simple': r_s, 'full': r_f, 'bh': bh_ret,
            }
        except ValueError as e:
            print(f"  {name}({code}): 跳过 - {e}")

    # ---- 每个标的的三策略对比 ----
    for code, data in results.items():
        print(f"\n{'=' * 70}")
        print(f"{data['name']}({code}) 三策略对比")
        print(f"{'=' * 70}")
        _print_three_strategy_table(data['bh'], data['simple'], data['full'])

    # ---- 多标的汇总表: 按策略分组 ----
    if results:
        print(f"\n{'=' * 70}")
        print("多标的汇总对比")
        print(f"{'=' * 70}")

        names = [data['name'] for data in results.values()]
        col_width = 12
        header = f"  {'指标':<16}"
        for n in names:
            header += f" {n:>{col_width}}"
        sep_len = 16 + (col_width + 1) * len(names)

        for strategy_label, key in [('买入持有', 'bh'), ('简单海龟', 'simple'), ('完整海龟', 'full')]:
            print(f"\n  [{strategy_label}]")
            print(f"  {header.strip()}")
            print(f"  {'-' * sep_len}")

            if key == 'bh':
                row = f"  {'总收益':<16}"
                for data in results.values():
                    bh_val = (data['bh'] or 0) * 100
                    row += f" {bh_val:>+{col_width-1}.1f}%"
                print(row)
            else:
                rows_cfg = [
                    ('总收益',   lambda r: f"{r['total_return']*100:>+{col_width-1}.1f}%"),
                    ('最大回撤', lambda r: f"{r['max_drawdown']*100:>{col_width-1}.1f}%"),
                    ('夏普比率', lambda r: f"{r['sharpe_ratio']:>{col_width}.2f}"),
                    ('交易次数', lambda r: f"{r['total_trades']:>{col_width}d}"),
                    ('胜率',     lambda r: f"{r['win_rate']*100:>{col_width-1}.1f}%"),
                    ('盈亏比',   lambda r: f"{r['profit_loss_ratio']:>{col_width}.2f}"),
                ]
                for label, fmt_fn in rows_cfg:
                    row = f"  {label:<16}"
                    for data in results.values():
                        row += f" {fmt_fn(data[key])}"
                    print(row)

    print("\n关键发现:")
    print("  - 海龟策略在有明确趋势的市场中表现优异 (纳指ETF 夏普最高)")
    print("  - 在下跌/震荡市场中(茅台), 趋势跟踪策略会频繁假突破而亏损")
    print("  - 简单海龟(95%仓位)收益更高, 但回撤也更大 -- 高仓位是双刃剑")
    print("  - 完整海龟通过ATR仓位管理控制风险, 回撤更小, 但牺牲了收益弹性")
    print("  - 核心启示: 趋势策略不是万能的, 选择合适的市场比优化参数更重要")
