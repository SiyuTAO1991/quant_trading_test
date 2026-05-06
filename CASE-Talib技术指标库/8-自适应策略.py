# -*- coding: utf-8 -*-
"""
自适应市场状态策略 - ADX判断趋势/震荡, 自动切换子策略

核心问题: 趋势策略(如MACD)在震荡市频繁假信号, 震荡策略(如RSI)在趋势市过早离场
解决方案: 用ADX自动判断市场状态, 匹配最合适的子策略

  ADX > 25: 趋势市 -> MACD趋势跟踪
  ADX < 20: 震荡市 -> RSI均值回归
  20-25:    过渡区 -> 观望不操作

使用TA-Lib计算: ADX, MACD, RSI, ATR

运行: python 8-自适应策略.py
"""
import numpy as np
import talib
import backtrader as bt
from data_loader import run_and_report


class AdaptiveStrategy(bt.Strategy):
    params = (
        ('adx_period', 14),
        ('adx_trend', 25), ('adx_range', 20),
        ('atr_period', 14), ('atr_mult', 2.0),
        ('macd_fast', 12), ('macd_slow', 26), ('macd_signal', 9),
        ('rsi_period', 14),
    )

    def __init__(self):
        self._stop_price = 0.0
        self._mode = None

    def _calc_indicators(self):
        """用talib计算ADX、MACD、RSI、ATR"""
        size = len(self.data)
        high = np.array(self.data.high.get(size=size), dtype=np.float64)
        low = np.array(self.data.low.get(size=size), dtype=np.float64)
        close = np.array(self.data.close.get(size=size), dtype=np.float64)

        adx = talib.ADX(high, low, close, timeperiod=self.p.adx_period)
        macd, signal, _ = talib.MACD(close,
            fastperiod=self.p.macd_fast, slowperiod=self.p.macd_slow, signalperiod=self.p.macd_signal)
        rsi = talib.RSI(close, timeperiod=self.p.rsi_period)
        atr = talib.ATR(high, low, close, timeperiod=self.p.atr_period)

        return adx, macd, signal, rsi, atr

    def _is_golden_cross(self, macd, signal):
        """MACD金叉"""
        if len(macd) < 2 or np.isnan(macd[-1]) or np.isnan(macd[-2]):
            return False
        return macd[-2] <= signal[-2] and macd[-1] > signal[-1]

    def _is_dead_cross(self, macd, signal):
        """MACD死叉"""
        if len(macd) < 2 or np.isnan(macd[-1]) or np.isnan(macd[-2]):
            return False
        return macd[-2] >= signal[-2] and macd[-1] < signal[-1]

    def next(self):
        if len(self.data) < self.p.macd_slow + self.p.macd_signal:
            return

        adx, macd, signal, rsi, atr = self._calc_indicators()

        # 判断市场状态
        adx_val = adx[-1]
        if np.isnan(adx_val):
            return
        if adx_val > self.p.adx_trend:
            regime = 'trend'
        elif adx_val < self.p.adx_range:
            regime = 'range'
        else:
            regime = 'neutral'

        atr_val = atr[-1]
        if np.isnan(atr_val):
            atr_val = 0.0

        if not self.position:
            if regime == 'trend' and self._is_golden_cross(macd, signal):
                self.buy()
                self._stop_price = self.data.close[0] - self.p.atr_mult * atr_val
                self._mode = 'trend'

            elif regime == 'range' and not np.isnan(rsi[-1]) and rsi[-1] < 30:
                self.buy()
                self._stop_price = self.data.close[0] - self.p.atr_mult * atr_val
                self._mode = 'range'
        else:
            new_stop = self.data.close[0] - self.p.atr_mult * atr_val
            self._stop_price = max(self._stop_price, new_stop)
            stop_hit = self.data.close[0] < self._stop_price

            if self._mode == 'trend':
                if self._is_dead_cross(macd, signal) or stop_hit:
                    self.close()
                    self._mode = None
            elif self._mode == 'range':
                if (not np.isnan(rsi[-1]) and rsi[-1] > 70) or stop_hit:
                    self.close()
                    self._mode = None


# 对照: 纯MACD策略
class PureMACDStrategy(bt.Strategy):
    params = (('fast', 12), ('slow', 26), ('signal', 9))

    def __init__(self):
        pass

    def _calc_macd(self):
        """用talib计算MACD"""
        size = len(self.data)
        close = np.array(self.data.close.get(size=size), dtype=np.float64)
        macd, signal, _ = talib.MACD(close,
            fastperiod=self.p.fast, slowperiod=self.p.slow, signalperiod=self.p.signal)
        return macd, signal

    def _is_golden_cross(self, macd, signal):
        if len(macd) < 2 or np.isnan(macd[-1]) or np.isnan(macd[-2]):
            return False
        return macd[-2] <= signal[-2] and macd[-1] > signal[-1]

    def _is_dead_cross(self, macd, signal):
        if len(macd) < 2 or np.isnan(macd[-1]) or np.isnan(macd[-2]):
            return False
        return macd[-2] >= signal[-2] and macd[-1] < signal[-1]

    def next(self):
        if len(self.data) < self.p.slow + self.p.signal:
            return

        macd, signal = self._calc_macd()

        if not self.position:
            if self._is_golden_cross(macd, signal):
                self.buy()
        elif self._is_dead_cross(macd, signal):
            self.close()


if __name__ == '__main__':
    stock = '600519.SH'
    start = '2024-01-01'
    end = '2025-12-31'

    print("=" * 60)
    print("自适应策略 vs 纯MACD策略")
    print("=" * 60)

    print("\nADX市场状态判断:")
    print("  ADX > 25: 趋势市(用MACD跟踪趋势)")
    print("  ADX < 20: 震荡市(用RSI抄底逃顶)")
    print("  20-25:    过渡区(观望)\n")

    print("[纯MACD] 不分市场环境:")
    run_and_report(PureMACDStrategy, stock, start, end, label='纯MACD', plot=True)

    print("\n[自适应] ADX状态切换:")
    run_and_report(AdaptiveStrategy, stock, start, end, label='自适应策略', plot=True)
