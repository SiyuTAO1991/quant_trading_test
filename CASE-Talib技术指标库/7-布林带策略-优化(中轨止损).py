# -*- coding: utf-8 -*-
"""
布林带策略-优化: 用TA-Lib计算布林带 + 中轨止损

标准版(Backtrader课程): bt.indicators.BollingerBands, 下轨买 上轨卖
优化版(本课): talib.BBANDS计算, 增加中轨止损(反弹失败出场)

TA-Lib用法:
  upper, middle, lower = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2)
  等价于 bt.indicators.BollingerBands, 但talib返回numpy数组

优化逻辑:
  标准版: 买入下轨后一直持有到上轨, 中间反弹失败也不出场
  优化版: 价格回升到中轨后又跌破 = 反弹失败, 及时出场避免更大亏损

实验结果:
  4只股票平均收益 +27.77% -> +30.44%, 回撤 15.33% -> 10.33%
  纳指ETF: +11.34% -> +20.59%, 回撤 22.63% -> 13.36%

运行: python 7-布林带策略-优化(中轨止损).py
"""
import numpy as np
import talib
import backtrader as bt
from data_loader import run_and_report


class BollingerStandard(bt.Strategy):
    """标准版: bt.indicators.BollingerBands (Backtrader课程已学)"""
    params = (('period', 20), ('dev', 2.0))

    def __init__(self):
        self.bb = bt.indicators.BollingerBands(self.data.close,
            period=self.p.period, devfactor=self.p.dev)

    def next(self):
        if not self.position:
            if self.data.close[0] <= self.bb.bot[0]:
                self.buy()
        elif self.data.close[0] >= self.bb.top[0]:
            self.close()


class BollingerOptimized(bt.Strategy):
    """优化版: talib.BBANDS + 中轨止损

    改用talib计算布林带:
      close_arr = np.array(self.data.close.get(...))
      upper, middle, lower = talib.BBANDS(close_arr, timeperiod=20, nbdevup=2, nbdevdn=2)

    中轨止损:
      买入下轨后, 价格回升到中轨以上 -> 标记"反弹有效"
      之后如果又跌破中轨 -> 反弹失败, 出场
      好处: 反弹成功继续持有到上轨; 反弹失败及时止损
    """
    params = (('period', 20), ('dev', 2.0))

    def __init__(self):
        self._bounced = False

    def _calc_bbands(self):
        """用talib计算布林带"""
        size = len(self.data)
        close = np.array(self.data.close.get(size=size), dtype=np.float64)
        upper, middle, lower = talib.BBANDS(close,
            timeperiod=self.p.period, nbdevup=self.p.dev, nbdevdn=self.p.dev)
        return upper, middle, lower

    def next(self):
        if len(self.data) < self.p.period + 1:
            return

        upper, middle, lower = self._calc_bbands()

        if not self.position:
            if self.data.close[0] <= lower[-1]:
                self.buy()
                self._bounced = False
        else:
            if self.data.close[0] > middle[-1]:
                self._bounced = True

            if self._bounced and self.data.close[0] < middle[-1]:
                self.close()
            elif self.data.close[0] >= upper[-1]:
                self.close()


if __name__ == '__main__':
    stocks = [
        ('600519.SH', '贵州茅台'),
        ('688981.SH', '中芯国际'),
        ('000001.SZ', '平安银行'),
        ('513100.SH', '纳指ETF'),
    ]

    print("=" * 70)
    print("布林带策略-优化 (talib.BBANDS + 中轨止损)")
    print("=" * 70)
    print("\n标准版: 价格触下轨买入, 触上轨卖出")
    print("优化版: 增加中轨止损, 价格反弹到中轨后又跌破 = 反弹失败出场")
    print("  反弹成功: 继续持有到上轨止盈")
    print("  反弹失败: 及时出场, 避免二次下跌的损失\n")

    for code, name in stocks:
        print(f"\n--- {name} ({code}) ---")
        print("[标准版]")
        r1 = run_and_report(BollingerStandard, code, '2025-01-01', '2025-12-31',
                            label=f'{name} 布林带标准', plot=True)
        print("[优化版]")
        r2 = run_and_report(BollingerOptimized, code, '2025-01-01', '2025-12-31',
                            label=f'{name} 布林带优化', plot=True)

        diff_ret = r2['total_return'] - r1['total_return']
        diff_dd = r2['max_drawdown'] - r1['max_drawdown']
        tags = []
        if abs(diff_ret) > 0.005: tags.append(f"收益{diff_ret*100:+.1f}%")
        if abs(diff_dd) > 0.005: tags.append(f"回撤{diff_dd*100:+.1f}%")
        if tags:
            print(f"  -> 变化: {', '.join(tags)}")
