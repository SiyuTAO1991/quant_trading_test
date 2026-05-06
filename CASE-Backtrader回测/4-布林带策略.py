# -*- coding: utf-8 -*-
"""
布林带策略 - 波动率

类别: 波动率
逻辑: 价格触及下轨(均线-2倍标准差) -> 买入; 触及上轨 -> 卖出
参数: 周期20, 标准差倍数2.0

运行: python 5-布林带策略.py
"""
import backtrader as bt
from data_loader import run_and_report


class BollingerBandStrategy(bt.Strategy):
    params = (('period', 20), ('devfactor', 2.0))

    def __init__(self):
        self.boll = bt.indicators.BollingerBands(
            self.data.close, period=self.p.period, devfactor=self.p.devfactor)

    def next(self):
        if not self.position:
            if self.data.close[0] < self.boll.bot[0]:
                self.buy()
        elif self.data.close[0] > self.boll.top[0]:
            self.close()


if __name__ == '__main__':
    run_and_report(BollingerBandStrategy, '600519.SH', '2025-01-01', '2025-12-31', label='布林带策略', plot=True)
