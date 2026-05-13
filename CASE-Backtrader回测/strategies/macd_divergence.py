# -*- coding: utf-8 -*-
"""
MACD金叉死叉策略（MA30过滤+止损） - 自定义策略示例

使用方法:
  1. 在 wucai_trade/strategies/ 目录下创建 .py 文件
  2. 定义 STRATEGY_META 字典（策略元信息）
  3. 定义 Strategy 类，继承 backtrader.Strategy
  4. 系统会自动加载并注册到策略列表

策略逻辑:
  价格在MA30之上且MACD金叉时买入
  MACD死叉时卖出
  从持仓最高点下跌超过5%时无条件止损
"""
import backtrader as bt

# 策略元信息（必须定义）
STRATEGY_META = {
    'name': 'MACD金叉死叉(MA30+止损)',
    'category': 'custom',
    'desc': '价格在MA30之上金叉买入，死叉卖出，持仓最高点下跌5%止损',
    'params': {'fast': 12, 'slow': 26, 'signal': 9, 'ma_period': 30, 'stop_loss_pct': 0.05},
    'params_desc': 'MACD(12,26,9), MA30, 止损5%',
    'logic': '价格在MA30之上+MACD金叉 -> 买入; MACD死叉/止损 -> 卖出',
}


class Strategy(bt.Strategy):
    """自定义策略类，必须命名为 Strategy"""
    params = (
        ('fast', 12),
        ('slow', 26),
        ('signal', 9),
        ('ma_period', 30),
        ('stop_loss_pct', 0.05),
    )

    def __init__(self):
        self.macd = bt.indicators.MACD(
            self.data.close,
            period_me1=self.p.fast,
            period_me2=self.p.slow,
            period_signal=self.p.signal)
        self.ma = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.p.ma_period)
        self.highest_price = None  # 持仓期间的最高价

    def next(self):
        # 如果有持仓
        if self.position:
            # 更新持仓期间的最高价
            if self.highest_price is None or self.data.close[0] > self.highest_price:
                self.highest_price = self.data.close[0]
            
            # 检查止损：从最高价下跌超过5%
            if self.highest_price and self.data.close[0] < self.highest_price * (1 - self.p.stop_loss_pct):
                self.close()
                self.highest_price = None
                return
            
            # MACD死叉卖出
            if (self.macd.macd[0] < self.macd.signal[0] and
                    self.macd.macd[-1] >= self.macd.signal[-1]):
                self.close()
                self.highest_price = None
        
        # 如果没有持仓
        else:
            # 价格在MA30之上且MACD金叉时买入
            if self.data.close[0] > self.ma[0]:
                if (self.macd.macd[0] > self.macd.signal[0] and
                        self.macd.macd[-1] <= self.macd.signal[-1]):
                    self.buy()
                    self.highest_price = self.data.close[0]
