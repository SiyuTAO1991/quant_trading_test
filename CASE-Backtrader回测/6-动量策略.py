# -*- coding: utf-8 -*-
"""
动量策略 - 动量因子

类别: 动量因子
逻辑: N日涨幅 > 5% -> 买入; N日跌幅 > 5% -> 卖出
参数: 观察周期20日, 阈值5%

运行: python 7-动量策略.py
"""
import backtrader as bt
from data_loader import run_and_report


class MomentumStrategy(bt.Strategy):
    params = (('period', 20), ('threshold', 5.0))

    def __init__(self):
        self.roc = bt.indicators.ROC100(self.data.close, period=self.p.period)

    def next(self):
        if not self.position:
            if self.roc[0] > self.p.threshold:
                self.buy()
        elif self.roc[0] < -self.p.threshold:
            self.close()


if __name__ == '__main__':
    run_and_report(MomentumStrategy, '600519.SH', '2025-01-01', '2025-12-31', label='动量策略', plot=True)
