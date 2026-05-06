# -*- coding: utf-8 -*-
"""
RSI策略-优化: 用TA-Lib计算RSI + 穿越确认入场

标准版(Backtrader课程): bt.indicators.RSI, RSI<30买 RSI>70卖
优化版(本课): talib.RSI计算, 等RSI从低位回升穿过30线再买(确认反弹)

TA-Lib用法:
  rsi = talib.RSI(close, timeperiod=14)
  返回numpy数组, rsi[i]为第i天的RSI值

优化逻辑:
  标准版RSI<30立即买 -> 可能接飞刀(RSI从30继续跌到15)
  优化版等RSI从<30回升到>=30 -> 确认止跌反弹才入场

运行: python 4-RSI策略-优化(穿越确认).py
"""
import numpy as np
import talib
import backtrader as bt
from data_loader import run_and_report


class RSIStandard(bt.Strategy):
    """标准版: bt.indicators.RSI (Backtrader课程已学)"""
    params = (('period', 14), ('oversold', 30), ('overbought', 70))

    def __init__(self):
        self.rsi = bt.indicators.RSI(self.data.close, period=self.p.period)

    def next(self):
        if not self.position:
            if self.rsi[0] < self.p.oversold:
                self.buy()
        elif self.rsi[0] > self.p.overbought:
            self.close()


class RSIOptimized(bt.Strategy):
    """优化版: talib.RSI + 穿越确认

    改用talib计算RSI:
      close_array = np.array(self.data.close.get(...))
      rsi = talib.RSI(close_array, timeperiod=14)

    穿越确认:
      rsi[-2] < 30 且 rsi[-1] >= 30 -> RSI从超卖区回升, 确认买入
      比标准版的 rsi < 30 立即买 更安全
    """
    params = (('period', 14), ('oversold', 30), ('overbought', 70))

    def _calc_rsi(self):
        """用talib计算RSI"""
        size = len(self.data)
        close = np.array(self.data.close.get(size=size), dtype=np.float64)
        return talib.RSI(close, timeperiod=self.p.period)

    def next(self):
        if len(self.data) < self.p.period + 2:
            return

        rsi = self._calc_rsi()

        if not self.position:
            if rsi[-2] < self.p.oversold and rsi[-1] >= self.p.oversold:
                self.buy()
        else:
            if rsi[-1] > self.p.overbought:
                self.close()


if __name__ == '__main__':
    stocks = [
        ('600519.SH', '贵州茅台'),
        ('688981.SH', '中芯国际'),
        ('000001.SZ', '平安银行'),
        ('513100.SH', '纳指ETF'),
    ]

    print("=" * 70)
    print("RSI策略-优化 (talib.RSI + 穿越确认入场)")
    print("=" * 70)
    print("\n标准版: RSI<30立即买入, RSI>70卖出")
    print("优化版: 等RSI从低位回升穿过30线再买, 确认止跌反弹")
    print("  减少'接飞刀'风险, 降低回撤, 提高夏普比率\n")

    for code, name in stocks:
        print(f"\n--- {name} ({code}) ---")
        print("[标准版]")
        r1 = run_and_report(RSIStandard, code, '2025-01-01', '2025-12-31',
                            label=f'{name} RSI标准', plot=True)
        print("[优化版]")
        r2 = run_and_report(RSIOptimized, code, '2025-01-01', '2025-12-31',
                            label=f'{name} RSI优化', plot=True)

        diff_ret = r2['total_return'] - r1['total_return']
        diff_dd = r2['max_drawdown'] - r1['max_drawdown']
        tags = []
        if abs(diff_ret) > 0.005: tags.append(f"收益{diff_ret*100:+.1f}%")
        if abs(diff_dd) > 0.005: tags.append(f"回撤{diff_dd*100:+.1f}%")
        sh1 = r1.get('sharpe_ratio', 0)
        sh2 = r2.get('sharpe_ratio', 0)
        if sh2 > sh1 + 0.05: tags.append("夏普提升")
        if tags:
            print(f"  -> 变化: {', '.join(tags)}")
