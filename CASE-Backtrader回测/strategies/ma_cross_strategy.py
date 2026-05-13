# -*- coding: utf-8 -*-
"""
K线-均线分批交叉策略 - 自定义策略示例

使用方法:
  1. 在 wucai_trade/strategies/ 目录下创建 .py 文件
  2. 定义 STRATEGY_META 字典（策略元信息）
  3. 定义 Strategy 类，继承 backtrader.Strategy
  4. 系统会自动加载并注册到策略列表

策略逻辑:
  K线上穿MA5时，买入1/3仓位
  K线上穿MA10时，买入1/3仓位
  K线上穿MA20时，买入1/3仓位
  K线下穿MA5时，卖出1/3仓位
  K线下穿MA10时，卖出1/3仓位
  K线下穿MA20时，清仓
  分批建仓和平仓，跟踪趋势变化
"""
import backtrader as bt

# 策略元信息（必须定义）
STRATEGY_META = {
    'name': 'K线-均线分批策略',
    'category': 'custom',
    'desc': 'K线分别上穿MA5/MA10/MA20时分批买入，下穿时卖出，下穿MA20清仓',
    'params': {},
    'params_desc': 'MA5, MA10, MA20',
    'logic': 'K线上穿MA5/MA10/MA20 -> 分别买入1/3; 下穿MA5/MA10 -> 分别卖出1/3; 下穿MA20 -> 清仓',
}


class Strategy(bt.Strategy):
    """自定义策略类，必须命名为 Strategy"""
    
    def __init__(self):
        self.ma5 = bt.indicators.SimpleMovingAverage(self.data.close, period=5)
        self.ma10 = bt.indicators.SimpleMovingAverage(self.data.close, period=10)
        self.ma20 = bt.indicators.SimpleMovingAverage(self.data.close, period=20)
        
        # 跟踪每个信号是否已执行
        self.signal_5_buy = False   # K线上穿MA5是否已买入
        self.signal_10_buy = False  # K线上穿MA10是否已买入
        self.signal_20_buy = False  # K线上穿MA20是否已买入
        
        # 记录初始资金
        self.initial_value = None
    
    def start(self):
        # 记录初始资金
        self.initial_value = self.broker.getvalue()
    
    def next(self):
        position_size = self.position.size
        
        # 计算每1/3仓位对应的股数（基于初始价值）
        one_third_value = self.initial_value / 3
        price = self.data.close[0]
        one_third_size = int(one_third_value / price / 100) * 100  # 取整到100股
        
        # 检查买入信号（K线指收盘价）
        if not self.signal_5_buy and (self.data.close[0] > self.ma5[0] and self.data.close[-1] <= self.ma5[-1]):
            # K线上穿MA5，买入1/3
            if one_third_size > 0:
                self.buy(size=one_third_size)
                self.signal_5_buy = True
        
        if not self.signal_10_buy and (self.data.close[0] > self.ma10[0] and self.data.close[-1] <= self.ma10[-1]):
            # K线上穿MA10，买入1/3
            if one_third_size > 0:
                self.buy(size=one_third_size)
                self.signal_10_buy = True
        
        if not self.signal_20_buy and (self.data.close[0] > self.ma20[0] and self.data.close[-1] <= self.ma20[-1]):
            # K线上穿MA20，买入1/3
            if one_third_size > 0:
                self.buy(size=one_third_size)
                self.signal_20_buy = True
        
        # 检查卖出信号
        if self.signal_5_buy and (self.data.close[0] < self.ma5[0] and self.data.close[-1] >= self.ma5[-1]):
            # K线下穿MA5，卖出1/3
            sell_size = min(position_size, one_third_size)
            if sell_size > 0:
                self.sell(size=sell_size)
                self.signal_5_buy = False
        
        if self.signal_10_buy and (self.data.close[0] < self.ma10[0] and self.data.close[-1] >= self.ma10[-1]):
            # K线下穿MA10，卖出1/3
            sell_size = min(position_size, one_third_size)
            if sell_size > 0:
                self.sell(size=sell_size)
                self.signal_10_buy = False
        
        if self.signal_20_buy and (self.data.close[0] < self.ma20[0] and self.data.close[-1] >= self.ma20[-1]):
            # K线下穿MA20，清仓
            if position_size > 0:
                self.close()
                self.signal_5_buy = False
                self.signal_10_buy = False
                self.signal_20_buy = False
