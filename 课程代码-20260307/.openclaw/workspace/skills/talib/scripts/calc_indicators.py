#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TA-Lib 技术指标计算脚本
支持从 CSV 或股票代码（需 xtquant）计算指标
"""

import sys
import json
import argparse
import numpy as np
import pandas as pd
import talib


def load_from_csv(csv_path):
    """从 CSV 加载 OHLCV 数据，需含 open/high/low/close/volume 列"""
    df = pd.read_csv(csv_path)
    for col in ['open', 'high', 'low', 'close']:
        if col not in df.columns:
            raise ValueError(f"CSV 需含 {col} 列")
    close = df['close'].astype(float).values
    high = df['high'].astype(float).values if 'high' in df.columns else close * 1.01
    low = df['low'].astype(float).values if 'low' in df.columns else close * 0.99
    open_ = df['open'].astype(float).values if 'open' in df.columns else close
    return close, high, low, open_, df


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
        open_ = df['open'].values.astype(float)
        return close, high, low, open_, df
    except ImportError:
        raise ValueError("使用 --stock 需安装 xtquant")


def calc_sma(close, period=10):
    out = talib.SMA(close, timeperiod=period)
    return {"sma": out.tolist(), "period": period}


def calc_ema(close, period=12):
    out = talib.EMA(close, timeperiod=period)
    return {"ema": out.tolist(), "period": period}


def calc_macd(close, fast=12, slow=26, signal=9):
    macd, sig, hist = talib.MACD(close, fastperiod=fast, slowperiod=slow, signalperiod=signal)
    return {"macd": macd.tolist(), "signal": sig.tolist(), "histogram": hist.tolist()}


def calc_rsi(close, period=14):
    out = talib.RSI(close, timeperiod=period)
    return {"rsi": out.tolist(), "period": period}


def calc_atr(high, low, close, period=14):
    out = talib.ATR(high, low, close, timeperiod=period)
    return {"atr": out.tolist(), "period": period}


def calc_bbands(close, period=20, nbdevup=2, nbdevdn=2):
    upper, mid, lower = talib.BBANDS(close, timeperiod=period, nbdevup=nbdevup, nbdevdn=nbdevdn)
    return {"upper": upper.tolist(), "middle": mid.tolist(), "lower": lower.tolist(), "period": period}


INDICATORS = {
    "sma": (calc_sma, ["close"], {"period": 10}),
    "ema": (calc_ema, ["close"], {"period": 12}),
    "macd": (calc_macd, ["close"], {"fast": 12, "slow": 26, "signal": 9}),
    "rsi": (calc_rsi, ["close"], {"period": 14}),
    "atr": (calc_atr, ["high", "low", "close"], {"period": 14}),
    "bbands": (calc_bbands, ["close"], {"period": 20, "nbdevup": 2, "nbdevdn": 2}),
}


def main():
    parser = argparse.ArgumentParser(description="TA-Lib 指标计算")
    parser.add_argument("indicator", help="指标名: sma,ema,macd,rsi,atr,bbands")
    parser.add_argument("csv_path", nargs="?", help="CSV 路径，含 open/high/low/close")
    parser.add_argument("--stock", "-s", help="股票代码，需 xtquant，与 csv_path 二选一")
    parser.add_argument("--period", "-p", type=int, help="周期参数")
    parser.add_argument("--count", "-n", type=int, default=200, help="获取 K 线条数")
    args = parser.parse_args()

    if args.stock:
        close, high, low, open_, df = load_from_stock(args.stock, args.count)
    elif args.csv_path:
        close, high, low, open_, df = load_from_csv(args.csv_path)
    else:
        print(json.dumps({"error": "请提供 csv_path 或 --stock 股票代码"}, ensure_ascii=False))
        sys.exit(1)

    indicator = args.indicator.lower()
    if indicator not in INDICATORS:
        print(json.dumps({"error": f"未知指标 {indicator}，支持: {list(INDICATORS.keys())}"}, ensure_ascii=False))
        sys.exit(1)

    func, needs, defaults = INDICATORS[indicator]
    params = defaults.copy()
    if args.period:
        params["period"] = args.period

    data = {"close": close, "high": high, "low": low, "open": open_}
    kwargs = {k: data.get(k, close) for k in needs}
    kwargs.update({k: v for k, v in params.items() if k in ["period", "fast", "slow", "signal", "nbdevup", "nbdevdn"]})

    result = func(**kwargs)
    result["length"] = len(close)
    last_vals = {}
    for k, v in result.items():
        if isinstance(v, list) and len(v) > 0:
            last = v[-1]
            last_vals[k] = round(float(last), 4) if last is not None and not np.isnan(last) else None
    result["last_values"] = last_vals
    for k in list(result.keys()):
        if isinstance(result[k], list) and len(result[k]) > 30:
            result[k] = result[k][-30:]
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
