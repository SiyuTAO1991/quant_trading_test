#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略推荐脚本：根据 ADX 判断股票更适合趋势交易还是震荡交易
ADX > 25: 趋势交易; 15 < ADX <= 25: 灰色地带; ADX <= 15: 震荡交易
"""

import sys
import json
import argparse
import numpy as np
import pandas as pd
import talib

# ADX 分档阈值
ADX_TREND = 25   # 大于此值为趋势
ADX_RANGE = 15   # 小于等于此值为震荡，介于两者之间为灰色地带


def load_from_csv(csv_path):
    """从 CSV 加载 OHLC 数据，需含 open/high/low/close 列"""
    df = pd.read_csv(csv_path)
    for col in ['open', 'high', 'low', 'close']:
        if col not in df.columns:
            raise ValueError(f"CSV 需含 {col} 列")
    close = df['close'].astype(float).values
    high = df['high'].astype(float).values
    low = df['low'].astype(float).values
    return close, high, low, df


def load_from_stock(stock_code, count=200):
    """从 MiniQMT 获取 K 线数据"""
    try:
        from xtquant import xtdata
        xtdata.connect()
        xtdata.download_history_data(stock_code, '1d', start_time='20230101', incrementally=True)
        data = xtdata.get_market_data_ex(
            field_list=['open', 'high', 'low', 'close', 'volume'],
            stock_list=[stock_code],
            period='1d',
            count=count
        )
        if not data or stock_code not in data:
            raise ValueError(f"未获取到 {stock_code} 数据")
        df = data[stock_code]
        close = df['close'].values.astype(float)
        high = df['high'].values.astype(float)
        low = df['low'].values.astype(float)
        return close, high, low, df
    except ImportError:
        raise ValueError("使用 --stock 需安装 xtquant")


def classify_strategy(adx_value):
    """
    根据 ADX 值返回策略类型
    - trend: 趋势交易 (ADX > 25)
    - gray: 灰色地带 (15 < ADX <= 25)
    - range: 震荡交易 (ADX <= 15)
    """
    if adx_value is None or (isinstance(adx_value, float) and np.isnan(adx_value)):
        return "unknown", "未知（数据不足或 ADX 无效）"
    adx = float(adx_value)
    if adx > ADX_TREND:
        return "trend", "趋势交易"
    if adx > ADX_RANGE:
        return "gray", "灰色地带"
    return "range", "震荡交易"


def main():
    parser = argparse.ArgumentParser(description="根据 ADX 推荐趋势/震荡策略")
    parser.add_argument("stock_or_csv", nargs="?", help="股票代码或 CSV 路径")
    parser.add_argument("--stock", "-s", help="股票代码，与 stock_or_csv 二选一")
    parser.add_argument("--period", "-p", type=int, default=14, help="ADX 周期，默认 14")
    parser.add_argument("--count", "-n", type=int, default=100, help="K 线条数（仅 --stock 时有效），默认 100")
    args = parser.parse_args()

    stock_code = None
    if args.stock:
        stock_code = args.stock
        close, high, low, df = load_from_stock(stock_code, args.count)
    elif args.stock_or_csv:
        arg = args.stock_or_csv
        if arg.endswith(".csv") or "/" in arg or "\\" in arg:
            close, high, low, df = load_from_csv(arg)
        else:
            stock_code = arg
            close, high, low, df = load_from_stock(arg, args.count)
    else:
        print(json.dumps({"error": "请提供股票代码或 CSV 路径"}, ensure_ascii=False))
        sys.exit(1)

    period = args.period
    adx = talib.ADX(high, low, close, timeperiod=period)
    # 取最后一个有效值
    valid_adx = adx[~np.isnan(adx)]
    if len(valid_adx) == 0:
        print(json.dumps({
            "error": "ADX 计算无有效值，请确保 K 线数量足够（至少 period*2 以上）",
            "period": period,
            "bar_count": len(close)
        }, ensure_ascii=False))
        sys.exit(1)
    adx_current = round(float(valid_adx[-1]), 4)
    strategy_type, strategy_label = classify_strategy(adx_current)

    result = {
        "stock_code": stock_code,
        "adx_period": period,
        "adx_current": adx_current,
        "strategy_type": strategy_type,
        "strategy_label": strategy_label,
        "bar_count": len(close),
        "message": f"ADX={adx_current}，建议：{strategy_label}"
    }
    if stock_code is None:
        result.pop("stock_code", None)
        result["source"] = "csv"
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
