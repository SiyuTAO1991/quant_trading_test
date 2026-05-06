# -*- coding: utf-8 -*-
"""
MACD策略-优化: 用TA-Lib计算MACD + 成交量确认入场

标准版(Backtrader课程): bt.indicators.MACD, 金叉买 死叉卖
优化版(本课): talib.MACD + talib.SMA计算指标, 增加成交量确认

TA-Lib用法:
  macd, signal, hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
  vol_ma = talib.SMA(volume, timeperiod=20)

优化逻辑:
  缩量金叉缺乏资金推动, 容易成为假信号
  成交量确认: 金叉时成交量需高于近20日均量, 过滤无效信号

运行: python 5-MACD策略-优化(成交量确认).py
"""
import numpy as np
import talib
import backtrader as bt
from data_loader import run_and_report


class MACDStandard(bt.Strategy):
    """标准版: bt.indicators.MACD (Backtrader课程已学)"""
    params = (('fast', 12), ('slow', 26), ('signal', 9))

    def __init__(self):
        self.macd = bt.indicators.MACD(self.data.close,
            period_me1=self.p.fast, period_me2=self.p.slow, period_signal=self.p.signal)
        self.cross = bt.indicators.CrossOver(self.macd.macd, self.macd.signal)

    def next(self):
        if not self.position:
            if self.cross[0] > 0:
                self.buy()
        elif self.cross[0] < 0:
            self.close()


class MACDOptimized(bt.Strategy):
    """优化版: talib.MACD + 成交量确认入场

    改用talib计算MACD和成交量均线:
      macd, signal, hist = talib.MACD(close_array)
      vol_ma = talib.SMA(volume_array, timeperiod=20)

    成交量确认:
      金叉时要求当日成交量 > vol_mult * 20日均量
      过滤掉缩量金叉(缺乏资金推动的假信号)
    """
    params = (('fast', 12), ('slow', 26), ('signal', 9),
              ('vol_period', 20), ('vol_mult', 0.9))

    def _calc(self):
        """用talib计算MACD和成交量均线"""
        size = len(self.data)
        close = np.array(self.data.close.get(size=size), dtype=np.float64)
        volume = np.array(self.data.volume.get(size=size), dtype=np.float64)
        macd, signal, hist = talib.MACD(close,
            fastperiod=self.p.fast, slowperiod=self.p.slow, signalperiod=self.p.signal)
        vol_ma = talib.SMA(volume, timeperiod=self.p.vol_period)
        return macd, signal, volume, vol_ma

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

        macd, signal, volume, vol_ma = self._calc()

        if not self.position:
            if self._is_golden_cross(macd, signal):
                vol_ok = not np.isnan(vol_ma[-1]) and volume[-1] > vol_ma[-1] * self.p.vol_mult
                if vol_ok:
                    self.buy()
        else:
            if self._is_dead_cross(macd, signal):
                self.close()


if __name__ == '__main__':
    stocks = [
        ('600519.SH', '贵州茅台'),
        ('688981.SH', '中芯国际'),
        ('000001.SZ', '平安银行'),
        ('513100.SH', '纳指ETF'),
    ]

    print("=" * 70)
    print("MACD策略-优化 (talib.MACD + 成交量确认入场)")
    print("=" * 70)
    print("\n标准版: MACD金叉买入, 死叉卖出")
    print("优化版: 金叉 + 成交量>0.9倍20日均量时买入, 过滤缩量假信号")
    print("  talib.MACD 计算MACD指标, talib.SMA 计算成交量均线\n")

    all_std, all_opt = [], []
    for code, name in stocks:
        print(f"\n--- {name} ({code}) ---")
        print("[标准版]")
        r1 = run_and_report(MACDStandard, code, '2025-01-01', '2025-12-31',
                            label=f'{name} MACD标准', plot=True)
        print("[优化版]")
        r2 = run_and_report(MACDOptimized, code, '2025-01-01', '2025-12-31',
                            label=f'{name} MACD优化', plot=True)
        all_std.append(r1)
        all_opt.append(r2)

        diff_ret = r2['total_return'] - r1['total_return']
        diff_dd = r2['max_drawdown'] - r1['max_drawdown']
        tags = []
        if abs(diff_ret) > 0.005: tags.append(f"收益{diff_ret*100:+.1f}%")
        if abs(diff_dd) > 0.005: tags.append(f"回撤{diff_dd*100:+.1f}%")
        if tags:
            print(f"  -> 变化: {', '.join(tags)}")

    print(f"\n{'='*70}")
    print("平均对比")
    print(f"{'='*70}")
    avg = lambda lst, k: np.mean([r[k] for r in lst])
    print(f"  标准版: 平均收益 {avg(all_std,'total_return')*100:+.2f}%  平均回撤 {avg(all_std,'max_drawdown')*100:.2f}%")
    print(f"  优化版: 平均收益 {avg(all_opt,'total_return')*100:+.2f}%  平均回撤 {avg(all_opt,'max_drawdown')*100:.2f}%")
