# -*- coding: utf-8 -*-
"""
K线形态识别 - TA-Lib独有能力

TA-Lib内置61种K线形态识别函数(CDL_*), 这是Backtrader没有的。
本脚本扫描茅台的K线数据, 找出所有出现过的形态信号。

运行: python 3-K线形态识别.py
"""
import numpy as np
import talib
from data_loader import load_stock_data

print("=" * 60)
print("K线形态识别 - 扫描茅台(600519.SH)")
print("=" * 60)

df = load_stock_data('600519.SH', '2024-01-01', '2025-12-31')
o = df['open'].values.astype(np.float64)
h = df['high'].values.astype(np.float64)
l = df['low'].values.astype(np.float64)
c = df['close'].values.astype(np.float64)

print(f"数据范围: {df.index[0].strftime('%Y-%m-%d')} ~ {df.index[-1].strftime('%Y-%m-%d')}")
print(f"交易日数: {len(df)}")

# ============================================================
# 1. 所有CDL函数列表（共61种）
# ============================================================
cdl_funcs = [f for f in talib.get_functions() if f.startswith('CDL')]
print(f"\nTA-Lib K线形态函数: {len(cdl_funcs)} 种")
print("完整列表:")
for i, f in enumerate(cdl_funcs):
    print(f"  {i+1:2d}. {f}")

# ============================================================
# 2. 逐个扫描, 统计出现次数
# ============================================================
print(f"\n{'-'*60}")
print("扫描结果:")
print(f"{'-'*60}")

# 形态中文名映射
PATTERN_NAMES = {
    'CDLHAMMER': '锤子线', 'CDLINVERTEDHAMMER': '倒锤子线',
    'CDLENGULFING': '吞没形态', 'CDLHARAMI': '孕线',
    'CDLMORNINGSTAR': '早晨之星', 'CDLEVENINGSTAR': '黄昏之星',
    'CDLDOJI': '十字星', 'CDLDRAGONFLYDOJI': '蜻蜓十字',
    'CDLGRAVESTONEDOJI': '墓碑十字', 'CDLHANGINGMAN': '吊人线',
    'CDLSHOOTINGSTAR': '射击之星', 'CDLDARKCLOUDCOVER': '乌云盖顶',
    'CDLPIERCING': '曙光初现', 'CDL3WHITESOLDIERS': '三白兵',
    'CDL3BLACKCROWS': '三黑鸦', 'CDLSPINNINGTOP': '纺锤线',
    'CDLMARUBOZU': '光头光脚', 'CDLKICKING': '反冲形态',
    'CDLBELTHOLD': '捉腰带线', 'CDLCLOSINGMARUBOZU': '收盘光头',
    'CDL3INSIDE': '三内部', 'CDL3OUTSIDE': '三外部',
    'CDLABANDONEDBABY': '弃婴', 'CDLADVANCEBLOCK': '前进受阻',
    'CDLCOUNTERATTACK': '反击线', 'CDLGAPSIDESIDEWHITE': '并列阳线',
    'CDLHIGHWAVE': '长脚十字', 'CDLLONGLINE': '长实体',
    'CDLSHORTLINE': '短实体', 'CDLSTALLEDPATTERN': '停顿形态',
}

bullish_signals = []
bearish_signals = []

for func_name in cdl_funcs:
    func = getattr(talib, func_name)
    result = func(o, h, l, c)

    bullish_count = np.sum(result > 0)
    bearish_count = np.sum(result < 0)

    cn_name = PATTERN_NAMES.get(func_name, func_name)

    if bullish_count > 0:
        # 找最近一次出现的日期
        last_idx = np.where(result > 0)[0][-1]
        last_date = df.index[last_idx].strftime('%Y-%m-%d')
        bullish_signals.append((cn_name, func_name, bullish_count, last_date))

    if bearish_count > 0:
        last_idx = np.where(result < 0)[0][-1]
        last_date = df.index[last_idx].strftime('%Y-%m-%d')
        bearish_signals.append((cn_name, func_name, bearish_count, last_date))

# 看涨形态
print(f"\n  看涨形态 (共 {len(bullish_signals)} 种出现过):")
for cn, en, count, date in sorted(bullish_signals, key=lambda x: -x[2]):
    print(f"    {cn:12s} ({en:25s}) 出现 {count:3d} 次, 最近: {date}")

# 看跌形态
print(f"\n  看跌形态 (共 {len(bearish_signals)} 种出现过):")
for cn, en, count, date in sorted(bearish_signals, key=lambda x: -x[2]):
    print(f"    {cn:12s} ({en:25s}) 出现 {count:3d} 次, 最近: {date}")

# ============================================================
# 3. 重点形态解读
# ============================================================
print(f"\n{'='*60}")
print("重点形态解读")
print("=" * 60)

key_patterns = [
    ('锤子线(Hammer)', '下跌趋势末端, 下影线长(>=实体2倍), 上影线短或无\n'
     '    含义: 空方力量衰竭, 可能反转向上'),
    ('吞没形态(Engulfing)', '当前K线实体完全包住前一根\n'
     '    看涨吞没: 阳包阴, 底部反转信号\n'
     '    看跌吞没: 阴包阳, 顶部反转信号'),
    ('早晨之星(Morning Star)', '三根K线组合: 大阴线 + 十字星 + 大阳线\n'
     '    含义: 底部反转的强信号'),
    ('十字星(Doji)', '开盘价=收盘价(或极接近), 说明多空平衡\n'
     '    含义: 趋势可能即将改变'),
    ('射击之星(Shooting Star)', '上升趋势末端, 上影线长, 实体小\n'
     '    含义: 多方力量衰竭, 可能反转向下'),
]

for name, desc in key_patterns:
    print(f"\n  {name}")
    print(f"    {desc}")

print(f"\n{'='*60}")
print("K线形态是TA-Lib的独有优势, 可用于:")
print("  1. 策略入场点精确化 (趋势+形态共振)")
print("  2. 选股扫描 (批量扫描出现特定形态的股票)")
print("  3. 特征工程 (作为机器学习模型的输入特征)")
print("=" * 60)
