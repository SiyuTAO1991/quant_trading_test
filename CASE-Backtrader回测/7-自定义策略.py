# -*- coding: utf-8 -*-
"""
自定义策略开发与加载

1-7号脚本的策略是"写死在脚本里"的，直接运行即可。
本脚本演示的是"插件机制"：
  - 把策略文件丢到 strategies/ 目录
  - 系统自动发现并加载，不用改主程序代码
  - 这是线上 Zoe 回测系统使用的架构

自定义策略规范（3步）：
  1. 在 strategies/ 目录下创建 .py 文件
  2. 定义 STRATEGY_META 字典
  3. 定义 Strategy 类，继承 bt.Strategy

示例: strategies/macd_divergence.py (MACD底背离策略)

运行: python 9-自定义策略.py
"""
import backtrader as bt
import sys
import importlib.util
from pathlib import Path
from data_loader import run_and_report

STRATEGY_DIR = Path(__file__).parent / 'strategies'


def load_custom_strategies():
    """扫描 strategies/ 目录，动态加载所有自定义策略"""
    strategies = {}
    if not STRATEGY_DIR.exists():
        print(f"策略目录不存在: {STRATEGY_DIR}")
        return strategies

    for py_file in sorted(STRATEGY_DIR.glob('*.py')):
        if py_file.name.startswith('_'):
            continue
        key = py_file.stem
        module_name = f'custom_{key}'
        spec = importlib.util.spec_from_file_location(module_name, py_file)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = mod
        spec.loader.exec_module(mod)

        meta = getattr(mod, 'STRATEGY_META', None)
        cls = getattr(mod, 'Strategy', None)
        if not meta or not cls:
            print(f"跳过 {py_file.name}: 缺少 STRATEGY_META 或 Strategy 类")
            continue
        strategies[key] = {'meta': meta, 'class': cls}
        print(f"加载: {key} ({meta.get('name', '')})")
    return strategies


def show_template():
    """打印自定义策略模板"""
    print("""
# ============================================================
# 自定义策略模板 - 保存到 strategies/ 目录即可自动加载
# ============================================================
import backtrader as bt

STRATEGY_META = {
    'name': '策略中文名',
    'category': 'custom',
    'params': {'period': 20},
    'logic': '买入条件 -> 买入; 卖出条件 -> 卖出',
}

class Strategy(bt.Strategy):
    params = (('period', 20),)

    def __init__(self):
        self.sma = bt.indicators.SMA(self.data.close, period=self.p.period)

    def next(self):
        if not self.position:
            if self.data.close[0] > self.sma[0]:
                self.buy()
        else:
            if self.data.close[0] < self.sma[0]:
                self.close()
""")


if __name__ == '__main__':
    print("=" * 60)
    print("自定义策略模板")
    print("=" * 60)
    show_template()

    strategies = load_custom_strategies()
    if not strategies:
        print("没有找到自定义策略，请在 strategies/ 目录下创建策略文件")
    else:
        for key, info in strategies.items():
            meta = info['meta']
            print(f"\n策略: {meta.get('name', key)}")
            print(f"逻辑: {meta.get('logic', '')}")
            run_and_report(info['class'], '600519.SH', '2024-01-01', '2025-12-31',
                          label=meta.get('name', key), plot=True)
