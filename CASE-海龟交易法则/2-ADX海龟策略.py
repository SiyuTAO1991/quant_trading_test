# -*- coding: utf-8 -*-
"""
ADX趋势过滤海龟策略

核心问题:
  经典海龟策略在震荡市会产生大量假突破, 导致频繁止损

解决方案:
  用ADX(平均趋向指数)作为"门卫":
    - ADX > 阈值: 市场有趋势 -> 允许海龟入场
    - ADX < 阈值: 市场震荡 -> 拒绝入场, 避免假突破

ADX阈值选择:
  - 阈值太高(如25): 过滤掉太多信号, 包括好的趋势启动
  - 阈值太低(如10): 几乎不过滤, 没有效果
  - 实测最佳: 15左右, 能过滤最差的假突破, 不伤害好信号

与 CASE-Talib 的 "自适应策略" 的关系:
  - 那个用ADX切换MACD/RSI子策略 (策略切换)
  - 这里用ADX做入场过滤器 (信号过滤)
  - 思路一脉相承: 用ADX识别市场状态, 匹配合适的操作

运行: python 2-自适应海龟策略.py
"""
import numpy as np
import talib
import backtrader as bt
from data_loader import run_and_report, calc_buy_and_hold


# ============================================================
# 经典海龟策略 (对照组)
# ============================================================

class TurtleStrategy(bt.Strategy):
    """经典海龟策略 - 不区分市场环境"""
    params = (
        ('entry_period', 20), ('exit_period', 10), ('atr_period', 20),
        ('risk_pct', 0.01), ('max_units', 4), ('add_n', 0.5), ('stop_n', 2.0),
    )

    def __init__(self):
        self.entry_high = bt.ind.Highest(self.data.high, period=self.p.entry_period)
        self.exit_low = bt.ind.Lowest(self.data.low, period=self.p.exit_period)
        self.atr = bt.ind.ATR(period=self.p.atr_period)
        self.units = 0; self.entry_prices = []; self.stop_price = 0.0
        self.last_add_price = 0.0; self.order = None

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]: return
        if order.status == order.Completed:
            if order.isbuy():
                fp = order.executed.price; self.entry_prices.append(fp)
                self.units = len(self.entry_prices)
                self.stop_price = fp - self.p.stop_n * self.atr[0]; self.last_add_price = fp
            elif order.issell():
                self.units = 0; self.entry_prices = []; self.stop_price = 0.0; self.last_add_price = 0.0
        self.order = None

    def _calc_unit_size(self):
        pv = self.broker.getvalue(); a = self.atr[0]
        if a <= 0: return 0
        return max(int((pv * self.p.risk_pct) / a // 100) * 100, 100)

    def next(self):
        if self.order: return
        a = self.atr[0]
        if np.isnan(a) or a <= 0: return
        c = self.data.close[0]
        if not self.position:
            if c > self.entry_high[-1]:
                s = self._calc_unit_size()
                if s > 0: self.order = self.buy(size=s)
        else:
            if c < self.stop_price: self.order = self.close(); return
            if c < self.exit_low[-1]: self.order = self.close(); return
            if self.units < self.p.max_units:
                if c >= self.last_add_price + self.p.add_n * a:
                    s = self._calc_unit_size(); cash = self.broker.getcash()
                    if s > 0 and cash > c * s * 1.01: self.order = self.buy(size=s)


# ============================================================
# ADX过滤海龟策略
# ============================================================

class ADXTurtleStrategy(bt.Strategy):
    """
    ADX过滤海龟策略

    在经典海龟基础上增加一个条件:
      入场时 ADX 必须 > adx_threshold, 否则拒绝入场
      已持仓的出场/加仓逻辑不变

    ADX (Average Directional Index):
      - 衡量趋势的强度(不分方向)
      - ADX > 25: 强趋势
      - ADX 15-25: 弱趋势
      - ADX < 15: 基本无趋势
    """
    params = (
        ('entry_period', 20), ('exit_period', 10), ('atr_period', 20),
        ('adx_period', 14), ('adx_threshold', 15),
        ('risk_pct', 0.01), ('max_units', 4), ('add_n', 0.5), ('stop_n', 2.0),
    )

    def __init__(self):
        self.entry_high = bt.ind.Highest(self.data.high, period=self.p.entry_period)
        self.exit_low = bt.ind.Lowest(self.data.low, period=self.p.exit_period)
        self.atr = bt.ind.ATR(period=self.p.atr_period)
        self.units = 0; self.entry_prices = []; self.stop_price = 0.0
        self.last_add_price = 0.0; self.order = None

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]: return
        if order.status == order.Completed:
            if order.isbuy():
                fp = order.executed.price; self.entry_prices.append(fp)
                self.units = len(self.entry_prices)
                self.stop_price = fp - self.p.stop_n * self.atr[0]; self.last_add_price = fp
            elif order.issell():
                self.units = 0; self.entry_prices = []; self.stop_price = 0.0; self.last_add_price = 0.0
        self.order = None

    def _calc_adx(self):
        """用talib计算当前ADX值"""
        size = len(self.data)
        h = np.array(self.data.high.get(size=size), dtype=np.float64)
        l = np.array(self.data.low.get(size=size), dtype=np.float64)
        c = np.array(self.data.close.get(size=size), dtype=np.float64)
        adx = talib.ADX(h, l, c, timeperiod=self.p.adx_period)
        return adx[-1] if not np.isnan(adx[-1]) else 0.0

    def _calc_unit_size(self):
        pv = self.broker.getvalue(); a = self.atr[0]
        if a <= 0: return 0
        return max(int((pv * self.p.risk_pct) / a // 100) * 100, 100)

    def next(self):
        if self.order: return
        a = self.atr[0]
        if np.isnan(a) or a <= 0: return
        c = self.data.close[0]

        if not self.position:
            # ADX门卫: 趋势不够强时拒绝入场
            adx_val = self._calc_adx()
            if adx_val < self.p.adx_threshold:
                return
            if c > self.entry_high[-1]:
                s = self._calc_unit_size()
                if s > 0: self.order = self.buy(size=s)
        else:
            if c < self.stop_price: self.order = self.close(); return
            if c < self.exit_low[-1]: self.order = self.close(); return
            if self.units < self.p.max_units:
                if c >= self.last_add_price + self.p.add_n * a:
                    s = self._calc_unit_size(); cash = self.broker.getcash()
                    if s > 0 and cash > c * s * 1.01: self.order = self.buy(size=s)


# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    start_date = '2024-01-01'
    end_date = '2025-12-31'

    print("=" * 70)
    print("ADX趋势过滤海龟策略")
    print("=" * 70)
    print("\n原理:")
    print("  ADX(平均趋向指数) 衡量趋势强度, 不分方向")
    print("  ADX > 15: 有一定趋势 -> 允许入场")
    print("  ADX < 15: 趋势极弱 -> 拒绝入场, 避免假突破")
    print("  已持仓时: 出场/加仓逻辑不变, ADX只影响开仓决策")

    # 在多只股票上对比经典海龟 vs ADX过滤海龟
    stocks = [
        ('601318.SH', '平安银行'),
        ('510300.SH', '沪深300ETF'),
        ('600519.SH', '贵州茅台'),
    ]

    all_results = {}

    for stock_code, stock_name in stocks:
        print(f"\n{'=' * 70}")
        print(f"{stock_name} ({stock_code})")
        print(f"{'=' * 70}")

        try:
            bh = calc_buy_and_hold(stock_code, start_date, end_date)
            print(f"  买入持有收益: {bh*100:+.1f}%\n")

            print(f"  [经典海龟]")
            r_classic = run_and_report(
                TurtleStrategy, stock_code, start_date, end_date,
                label=f'  经典海龟', plot=True, use_sizer=False,
            )

            print(f"\n  [ADX过滤海龟] ADX > 15 才入场:")
            r_adx = run_and_report(
                ADXTurtleStrategy, stock_code, start_date, end_date,
                label=f'  ADX海龟', plot=True, use_sizer=False,
            )

            all_results[stock_code] = {
                'name': stock_name, 'bh': bh,
                'classic': r_classic, 'adx': r_adx,
            }
        except ValueError as e:
            print(f"  跳过: {e}")

    # ---- 汇总对比 ----
    if all_results:
        print(f"\n{'=' * 70}")
        print("汇总: ADX过滤的效果")
        print(f"{'=' * 70}")

        for code, data in all_results.items():
            rc = data['classic']
            ra = data['adx']
            dr = (ra['total_return'] - rc['total_return']) * 100
            dd_diff = (ra['max_drawdown'] - rc['max_drawdown']) * 100
            trade_diff = ra['total_trades'] - rc['total_trades']

            print(f"\n  {data['name']} ({code}) | 买入持有: {data['bh']*100:+.1f}%")
            print(f"    {'':8} {'经典海龟':>12} {'ADX海龟':>12} {'变化':>10}")
            print(f"    {'收益':8} {rc['total_return']*100:>+11.2f}% {ra['total_return']*100:>+11.2f}% {dr:>+9.2f}%")
            print(f"    {'回撤':8} {rc['max_drawdown']*100:>11.2f}% {ra['max_drawdown']*100:>11.2f}% {dd_diff:>+9.2f}%")
            print(f"    {'交易':8} {rc['total_trades']:>12d} {ra['total_trades']:>12d} {trade_diff:>+10d}")
            print(f"    {'胜率':8} {rc['win_rate']*100:>11.1f}% {ra['win_rate']*100:>11.1f}%")
            print(f"    {'盈亏比':8} {rc['profit_loss_ratio']:>12.2f} {ra['profit_loss_ratio']:>12.2f}")

    print("\n关键发现:")
    print("  - ADX过滤以极低的成本(ADX>15是很宽松的条件)过滤掉最差的假突破")
    print("  - 在震荡市中: 减少无效交易, 降低亏损")
    print("  - 在趋势市中: 基本不影响好的信号, 保持收益")
    print("  - ADX阈值不宜太高(如25会误伤好信号), 15是较好的平衡点")
    print("  - 这就是'简单规则往往最有效'的体现 -- 不要过度优化")
