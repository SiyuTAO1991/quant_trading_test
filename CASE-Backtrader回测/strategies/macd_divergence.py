# -*- coding: utf-8 -*-
"""
MACD底背离策略 - 自定义策略示例

使用方法:
  1. 在 wucai_trade/strategies/ 目录下创建 .py 文件
  2. 定义 STRATEGY_META 字典（策略元信息）
  3. 定义 Strategy 类，继承 backtrader.Strategy
  4. 系统会自动加载并注册到策略列表

策略逻辑:
  价格创N日新低但MACD柱未创新低（底背离），说明下跌动能衰竭，买入
  MACD死叉时卖出
"""
import backtrader as bt

# 策略元信息（必须定义）
STRATEGY_META = {
    'name': 'MACD底背离',
    'category': 'custom',
    'desc': '价格创新低但MACD未创新低时买入，捕捉趋势反转的经典背离策略',
    'params': {'lookback': 30, 'fast': 12, 'slow': 26, 'signal': 9},
    'params_desc': '观察周期30日, MACD参数(12,26,9)',
    'logic': '价格创N日新低且MACD未创新低(底背离) -> 买入; MACD死叉 -> 卖出',
}


class Strategy(bt.Strategy):
    """自定义策略类，必须命名为 Strategy"""
    params = (
        ('lookback', 30),
        ('fast', 12),
        ('slow', 26),
        ('signal', 9),
    )

    def __init__(self):
        self.macd = bt.indicators.MACD(
            self.data.close,
            period_me1=self.p.fast,
            period_me2=self.p.slow,
            period_signal=self.p.signal)
        self.price_lowest = bt.indicators.Lowest(
            self.data.low, period=self.p.lookback)
        self.macd_lowest = bt.indicators.Lowest(
            self.macd.macd, period=self.p.lookback)

    def next(self):
        if not self.position:
            # 底背离: 价格在N日最低点附近, 但MACD高于N日最低值
            at_price_low = self.data.low[0] <= self.price_lowest[0] * 1.01
            macd_higher = self.macd.macd[0] > self.macd_lowest[0] * 0.8
            golden_cross = self.macd.macd[0] > self.macd.signal[0]
            if at_price_low and macd_higher and golden_cross:
                self.buy()
        else:
            # MACD死叉卖出
            if (self.macd.macd[0] < self.macd.signal[0] and
                    self.macd.macd[-1] >= self.macd.signal[-1]):
                self.close()
