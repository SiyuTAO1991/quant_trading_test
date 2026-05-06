# -*- coding: utf-8 -*-
"""
乖离率策略 - 均值回归

类别: 均值回归
逻辑: 乖离率 < -6%(超跌) -> 买入; 乖离率 > 6%(超涨) -> 卖出
参数: 均线周期20, 买入阈值-6%, 卖出阈值6%

运行: python 6-乖离率策略.py
"""
import backtrader as bt
from data_loader import run_and_report


class BIASStrategy(bt.Strategy):
    params = (('period', 20), ('buy_threshold', -6.0), ('sell_threshold', 3.0))

    def __init__(self):
        self.sma = bt.indicators.SMA(self.data.close, period=self.p.period)

    def next(self):
        bias = (self.data.close[0] - self.sma[0]) / self.sma[0] * 100
        if not self.position:
            if bias < self.p.buy_threshold:
                self.buy()
        elif bias > self.p.sell_threshold:
            self.close()


if __name__ == '__main__':
    run_and_report(BIASStrategy, '600519.SH', '2025-01-01', '2025-12-31', label='乖离率策略', plot=True)
