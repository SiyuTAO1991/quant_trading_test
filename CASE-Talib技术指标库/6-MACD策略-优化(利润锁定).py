# -*- coding: utf-8 -*-
"""
MACD策略-优化(利润锁定): 用TA-Lib计算MACD + 利润锁定出场

标准版(Backtrader课程): bt.indicators.MACD, 金叉买 死叉卖
优化版(本课): talib.MACD计算指标, 增加利润锁定出场

TA-Lib用法:
  macd, signal, hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)

优化逻辑:
  MACD死叉信号滞后, 等到死叉时利润往往已经回吐很多
  利润锁定: 盈利>5%后, 从高点回撤>3%就出场, 锁住大部分利润

运行: python 6-MACD策略-优化(利润锁定).py
"""
import numpy as np
import talib
import backtrader as bt
from data_loader import run_and_report


class MACDStandard(bt.Strategy):
    """标准版: bt.indicators.MACD (Backtrader课程已学)"""
    params = (('fast', 12), ('slow', 26), ('signal', 9))

    def __init__(self):
        self.macd = bt.indicators.MACD(self.data.close,
            period_me1=self.p.fast, period_me2=self.p.slow, period_signal=self.p.signal)
        self.cross = bt.indicators.CrossOver(self.macd.macd, self.macd.signal)

    def next(self):
        if not self.position:
            if self.cross[0] > 0:
                self.buy()
        elif self.cross[0] < 0:
            self.close()


class MACDProfitLock(bt.Strategy):
    """优化版: talib.MACD + 利润锁定出场

    改用talib计算MACD:
      close_array = np.array(self.data.close.get(...))
      macd, signal, hist = talib.MACD(close_array)

    利润锁定:
      盈利超过profit_trigger% -> 开始监控回撤
      从最高点回落trail_pct% -> 出场锁住利润
    """
    params = (('fast', 12), ('slow', 26), ('signal', 9),
              ('profit_trigger', 5.0), ('trail_pct', 3.0))

    def __init__(self):
        self._entry_price = 0.0
        self._peak_price = 0.0

    def _calc_macd(self):
        """用talib计算MACD"""
        size = len(self.data)
        close = np.array(self.data.close.get(size=size), dtype=np.float64)
        macd, signal, hist = talib.MACD(close,
            fastperiod=self.p.fast, slowperiod=self.p.slow, signalperiod=self.p.signal)
        return macd, signal

    def _is_golden_cross(self, macd, signal):
        if len(macd) < 2 or np.isnan(macd[-1]) or np.isnan(macd[-2]):
            return False
        return macd[-2] <= signal[-2] and macd[-1] > signal[-1]

    def _is_dead_cross(self, macd, signal):
        if len(macd) < 2 or np.isnan(macd[-1]) or np.isnan(macd[-2]):
            return False
        return macd[-2] >= signal[-2] and macd[-1] < signal[-1]

    def next(self):
        if len(self.data) < self.p.slow + self.p.signal:
            return

        macd, signal = self._calc_macd()

        if not self.position:
            if self._is_golden_cross(macd, signal):
                self.buy()
                self._entry_price = self.data.close[0]
                self._peak_price = self.data.close[0]
        else:
            self._peak_price = max(self._peak_price, self.data.close[0])
            gain = (self._peak_price - self._entry_price) / self._entry_price * 100
            drop = (self._peak_price - self.data.close[0]) / self._peak_price * 100

            if gain >= self.p.profit_trigger and drop >= self.p.trail_pct:
                self.close()
            elif self._is_dead_cross(macd, signal):
                self.close()


if __name__ == '__main__':
    stocks = [
        ('600519.SH', '贵州茅台'),
        ('688981.SH', '中芯国际'),
        ('000001.SZ', '平安银行'),
        ('513100.SH', '纳指ETF'),
    ]

    print("=" * 70)
    print("MACD策略-优化(利润锁定) (talib.MACD + 利润锁定出场)")
    print("=" * 70)
    print("\n标准版: MACD金叉买入, 死叉卖出")
    print("优化版: 同样金叉买入, 盈利>5%后从高点回撤>3%出场锁利")
    print("  死叉信号滞后, 利润锁定让盈利交易更早兑现利润\n")

    all_std, all_opt = [], []
    for code, name in stocks:
        print(f"\n--- {name} ({code}) ---")
        print("[标准版]")
        r1 = run_and_report(MACDStandard, code, '2025-01-01', '2025-12-31',
                            label=f'{name} MACD标准', plot=True)
        print("[优化版]")
        r2 = run_and_report(MACDProfitLock, code, '2025-01-01', '2025-12-31',
                            label=f'{name} MACD利润锁定', plot=True)
        all_std.append(r1)
        all_opt.append(r2)

        diff_ret = r2['total_return'] - r1['total_return']
        diff_dd = r2['max_drawdown'] - r1['max_drawdown']
        tags = []
        if abs(diff_ret) > 0.005: tags.append(f"收益{diff_ret*100:+.1f}%")
        if abs(diff_dd) > 0.005: tags.append(f"回撤{diff_dd*100:+.1f}%")
        if tags:
            print(f"  -> 变化: {', '.join(tags)}")

    print(f"\n{'='*70}")
    print("平均对比")
    print(f"{'='*70}")
    avg = lambda lst, k: np.mean([r[k] for r in lst])
    print(f"  标准版: 平均收益 {avg(all_std,'total_return')*100:+.2f}%  平均回撤 {avg(all_std,'max_drawdown')*100:.2f}%")
    print(f"  优化版: 平均收益 {avg(all_opt,'total_return')*100:+.2f}%  平均回撤 {avg(all_opt,'max_drawdown')*100:.2f}%")
