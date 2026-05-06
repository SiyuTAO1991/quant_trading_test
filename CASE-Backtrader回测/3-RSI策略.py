# -*- coding: utf-8 -*-
"""
RSI策略 - 超买超卖

类别: 超买超卖
逻辑: RSI < 30(超卖) -> 买入; RSI > 70(超买) -> 卖出
参数: RSI周期14, 超卖线30, 超买线70

运行: python 3-RSI策略.py
"""
import backtrader as bt
from data_loader import run_and_report


class RSIStrategy(bt.Strategy):
    params = (('period', 14), ('oversold', 30), ('overbought', 70))

    def __init__(self):
        self.rsi = bt.indicators.RSI(self.data.close, period=self.p.period)

    def next(self):
        if not self.position:
            if self.rsi < self.p.oversold:
                self.buy()
        elif self.rsi > self.p.overbought:
            self.close()


if __name__ == '__main__':
    run_and_report(RSIStrategy, '600519.SH', '2025-01-01', '2025-12-31', label='RSI策略', plot=True)
