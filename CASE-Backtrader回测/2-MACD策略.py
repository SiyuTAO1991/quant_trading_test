# -*- coding: utf-8 -*-
"""
MACD策略 - 趋势跟踪

类别: 趋势跟踪
逻辑: DIF上穿DEA(金叉) -> 买入; DIF下穿DEA(死叉) -> 卖出
参数: 短周期12, 长周期26, 信号线9

运行: python 2-MACD策略.py
"""
import backtrader as bt
from data_loader import run_and_report


class MACDStrategy(bt.Strategy):
    params = (('short', 12), ('long', 26), ('signal', 9))

    def __init__(self):
        self.macd = bt.indicators.MACD(
            self.data.close,
            period_me1=self.p.short,
            period_me2=self.p.long,
            period_signal=self.p.signal)
        self.crossover = bt.indicators.CrossOver(self.macd.macd, self.macd.signal)

    def next(self):
        if not self.position:
            if self.crossover > 0:
                self.buy()
        elif self.crossover < 0:
            self.close()


if __name__ == '__main__':
    run_and_report(MACDStrategy, '600519.SH', '2025-01-01', '2025-12-31', label='MACD策略', plot=True)
