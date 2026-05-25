# -*- coding: utf-8 -*-
"""
多周期融合海龟策略 - 周线定方向, 日线找入场

核心思想:
  "大周期过滤, 小周期执行"
  - 周线级别判断大趋势方向
  - 日线级别执行海龟突破信号
  - 只在大趋势向上时做多, 大趋势向下时空仓

周线过滤规则:
  - 周线收盘 > 周线通道上轨 -> 大趋势向上 (允许做多)
  - 周线收盘 < 周线通道下轨 -> 大趋势向下 (禁止做多, 已持仓则平仓)
  - 其他情况(中性) -> 允许做多 (不能太严格, 否则错过太多机会)

Backtrader多周期实现:
  - data0: 日线数据 (adddata)
  - data1: 周线数据 (resampledata)

运行: python 3-多周期海龟策略.py
"""
import numpy as np
import backtrader as bt
from data_loader import load_stock_data, _wrap_strategy, _calc_metrics, plot_backtest, calc_buy_and_hold
from db_config import INITIAL_CASH, COMMISSION


# ============================================================
# 单周期海龟 (仅日线, 对照组)
# ============================================================

class SingleTFTurtle(bt.Strategy):
    """单周期海龟策略"""
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
# 多周期海龟 (周线过滤 + 日线入场)
# ============================================================

class MultiTFTurtle(bt.Strategy):
    """
    多周期海龟策略

    数据:
      self.data0 = 日线数据
      self.data1 = 周线数据

    周线过滤:
      - 只在周线趋势非"下跌"时允许日线开仓
      - 周线趋势"下跌"时: 不开新仓, 已持仓则平仓
      - 这是一个宽松的过滤器: 只挡住最差的情况
    """
    params = (
        ('daily_entry', 20), ('daily_exit', 10),
        ('weekly_period', 8),       # 周线通道周期 (8周)
        ('atr_period', 20),
        ('risk_pct', 0.01), ('max_units', 4), ('add_n', 0.5), ('stop_n', 2.0),
    )

    def __init__(self):
        self.daily_entry_high = bt.ind.Highest(self.data0.high, period=self.p.daily_entry)
        self.daily_exit_low = bt.ind.Lowest(self.data0.low, period=self.p.daily_exit)
        self.daily_atr = bt.ind.ATR(self.data0, period=self.p.atr_period)
        self.weekly_high = bt.ind.Highest(self.data1.high, period=self.p.weekly_period)
        self.weekly_low = bt.ind.Lowest(self.data1.low, period=self.p.weekly_period)
        self.units = 0; self.entry_prices = []; self.stop_price = 0.0
        self.last_add_price = 0.0; self.order = None

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]: return
        if order.status == order.Completed:
            if order.isbuy():
                fp = order.executed.price; self.entry_prices.append(fp)
                self.units = len(self.entry_prices)
                self.stop_price = fp - self.p.stop_n * self.daily_atr[0]; self.last_add_price = fp
            elif order.issell():
                self.units = 0; self.entry_prices = []; self.stop_price = 0.0; self.last_add_price = 0.0
        self.order = None

    def _calc_unit_size(self):
        pv = self.broker.getvalue(); a = self.daily_atr[0]
        if a <= 0: return 0
        return max(int((pv * self.p.risk_pct) / a // 100) * 100, 100)

    def _get_weekly_trend(self):
        """判断周线大趋势: up / down / neutral"""
        try:
            wc = self.data1.close[0]
            wh = self.weekly_high[-1]
            wl = self.weekly_low[-1]
            if np.isnan(wh) or np.isnan(wl): return 'neutral'
            if wc > wh: return 'up'
            if wc < wl: return 'down'
            return 'neutral'
        except Exception:
            return 'neutral'

    def next(self):
        if self.order: return
        a = self.daily_atr[0]
        if np.isnan(a) or a <= 0: return
        c = self.data0.close[0]
        weekly_trend = self._get_weekly_trend()

        if not self.position:
            # 周线趋势向下时不开仓, 其他情况(up/neutral)都允许
            if weekly_trend == 'down':
                return
            if c > self.daily_entry_high[-1]:
                s = self._calc_unit_size()
                if s > 0: self.order = self.buy(size=s)
        else:
            if c < self.stop_price: self.order = self.close(); return
            if c < self.daily_exit_low[-1]: self.order = self.close(); return
            # 周线转为下跌 -> 平仓 (趋势逆转保护)
            if weekly_trend == 'down':
                self.order = self.close(); return
            if self.units < self.p.max_units:
                if c >= self.last_add_price + self.p.add_n * a:
                    s = self._calc_unit_size(); cash = self.broker.getcash()
                    if s > 0 and cash > c * s * 1.01: self.order = self.buy(size=s)


# ============================================================
# 多周期回测引擎
# ============================================================

def run_multi_tf_backtest(strategy_class, stock_code, start_date, end_date,
                          label='', plot=False, **kwargs):
    """
    多周期回测: 日线 + 周线

    Backtrader多数据源:
      1. 创建两个 PandasData 对象 (相同底层数据)
      2. 第一个用 adddata (日线 data0)
      3. 第二个用 resampledata 重采样为周线 (data1)
    """
    df = load_stock_data(stock_code, start_date, end_date)
    wrapped = _wrap_strategy(strategy_class)

    cerebro = bt.Cerebro()
    cerebro.addstrategy(wrapped, **kwargs)

    data_daily = bt.feeds.PandasData(dataname=df)
    cerebro.adddata(data_daily)
    data_weekly = bt.feeds.PandasData(dataname=df)
    cerebro.resampledata(data_weekly, timeframe=bt.TimeFrame.Weeks)

    cerebro.broker.setcash(INITIAL_CASH)
    cerebro.broker.setcommission(commission=COMMISSION)
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.02)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')

    if label:
        print(f"{label} | {stock_code} | {df.index[0].strftime('%Y-%m-%d')} ~ "
              f"{df.index[-1].strftime('%Y-%m-%d')} | {len(df)}个交易日")

    results = cerebro.run()
    strat = results[0]
    m = _calc_metrics(cerebro, strat, df)

    print(f"  总收益: {m['total_return']*100:+.2f}% | 年化: {m['annual_return']*100:+.2f}% | "
          f"最大回撤: {m['max_drawdown']*100:.2f}% | 夏普: {m['sharpe_ratio']:.2f} | "
          f"卡玛: {m['calmar_ratio']:.2f}")
    print(f"  交易: {m['total_trades']}次 | 胜率: {m['win_rate']*100:.1f}% | "
          f"盈亏比: {m['profit_loss_ratio']:.2f} | 利润因子: {m['profit_factor']:.2f} | "
          f"最大连亏: {m['max_consecutive_losses']}次")

    result = {**m, 'df': df, 'trades': strat._trade_log, 'nav': strat._nav_log}
    if plot:
        plot_backtest(result, stock_code, label or strategy_class.__name__)
    return result


# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    stock_code = '510300.SH'
    start_date = '2024-01-01'
    end_date = '2025-12-31'

    print("=" * 70)
    print("多周期融合海龟策略: 周线定方向 + 日线找入场")
    print("=" * 70)
    print("\n多周期思想:")
    print("  周线通道(8周): 判断大趋势方向")
    print("  日线通道(20日): 寻找入场时机")
    print("  规则: 只有周线趋势向下时才禁止做多, 其他情况正常交易")
    print("  效果: 避免在大趋势下跌时逆势做多")
    print("\nBacktrader 多周期实现:")
    print("  data0 = 日线数据 (adddata)")
    print("  data1 = 周线数据 (resampledata 重采样)")

    bh = calc_buy_and_hold(stock_code, start_date, end_date)

    try:
        from data_loader import run_and_report

        print(f"\n{'-' * 70}")
        print(f"[单周期] 仅日线海龟 | 买入持有: {bh*100:+.1f}%")
        print(f"{'-' * 70}")
        r_single = run_and_report(
            SingleTFTurtle, stock_code, start_date, end_date,
            label='单周期海龟', plot=True, use_sizer=False,
        )

        print(f"\n{'-' * 70}")
        print("[多周期] 周线过滤(只挡下跌) + 日线入场")
        print(f"{'-' * 70}")
        r_multi = run_multi_tf_backtest(
            MultiTFTurtle, stock_code, start_date, end_date,
            label='多周期海龟', plot=True,
        )

        # 对比
        print(f"\n{'=' * 70}")
        print("对比总结")
        print(f"{'=' * 70}")
        print(f"  {'指标':<12} {'单周期':>14} {'多周期':>14}")
        print(f"  {'-' * 42}")
        print(f"  {'买入持有':<12} {bh*100:>+13.1f}% {bh*100:>+13.1f}%")
        print(f"  {'海龟收益':<12} {r_single['total_return']*100:>+13.2f}% {r_multi['total_return']*100:>+13.2f}%")
        print(f"  {'最大回撤':<12} {r_single['max_drawdown']*100:>13.2f}% {r_multi['max_drawdown']*100:>13.2f}%")
        print(f"  {'夏普比率':<12} {r_single['sharpe_ratio']:>14.2f} {r_multi['sharpe_ratio']:>14.2f}")
        print(f"  {'交易次数':<12} {r_single['total_trades']:>14d} {r_multi['total_trades']:>14d}")
        print(f"  {'胜率':<12} {r_single['win_rate']*100:>13.1f}% {r_multi['win_rate']*100:>13.1f}%")
        print(f"  {'盈亏比':<12} {r_single['profit_loss_ratio']:>14.2f} {r_multi['profit_loss_ratio']:>14.2f}")

        print("\n关键发现:")
        print("  - 周线过滤只挡住'大趋势明确向下'的情况, 不过度限制")
        print("  - 减少了逆势交易, 但保留了趋势启动时的入场机会")
        print("  - 多周期适合中长线交易, ETF/指数类标的效果较好")
        print("  - Backtrader的resampledata是实现多周期的关键API")

    except ValueError as e:
        print(f"\n错误: {e}")
        print("提示: 如果没有 510300.SH 数据, 可以改为 600519.SH 或其他有数据的标的")
