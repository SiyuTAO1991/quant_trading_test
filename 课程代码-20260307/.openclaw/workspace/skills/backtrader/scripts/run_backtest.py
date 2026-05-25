#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backtrader 回测脚本
从 MySQL trade_stock_daily 加载数据，运行策略，输出绩效 JSON
"""

import os
import sys
import json
import argparse
from pathlib import Path

import pandas as pd
import pymysql
import backtrader as bt
from dotenv import dotenv_values


def load_env():
    """加载 .env 配置，按优先级查找：BACKTEST_CONFIG_DIR > 脚本所在技能目录 > 当前目录"""
    candidates = []
    config_dir = os.environ.get("BACKTEST_CONFIG_DIR")
    if config_dir:
        candidates.append(Path(config_dir) / ".env")
    # 脚本所在目录的上一层（skills/backtrader/）
    script_dir = Path(__file__).resolve().parent.parent
    candidates.append(script_dir / ".env")
    candidates.append(Path.cwd() / ".env")
    for env_path in candidates:
        if env_path.exists():
            return dotenv_values(env_path)
    return {}


def get_db_config():
    env = load_env()
    return {
        "host": env.get("WUCAI_SQL_HOST", "localhost"),
        "user": env.get("WUCAI_SQL_USERNAME", "root"),
        "password": env.get("WUCAI_SQL_PASSWORD", ""),
        "database": env.get("WUCAI_SQL_DB", "wucai_trade"),
        "port": int(env.get("WUCAI_SQL_PORT", "3306")),
        "charset": "utf8mb4",
    }


def load_stock_data(stock_code, start_date=None, end_date=None):
    """从 MySQL 加载 K 线数据"""
    cfg = get_db_config()
    conditions = ["stock_code = %s"]
    params = [stock_code]
    if start_date:
        conditions.append("trade_date >= %s")
        params.append(start_date)
    if end_date:
        conditions.append("trade_date <= %s")
        params.append(end_date)
    sql = f"""
        SELECT trade_date, open_price, high_price, low_price, close_price, volume
        FROM trade_stock_daily
        WHERE {' AND '.join(conditions)}
        ORDER BY trade_date ASC
    """
    conn = pymysql.connect(**cfg)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    if not rows:
        raise ValueError(f"没有找到 {stock_code} 的数据")
    df = pd.DataFrame(rows)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df.set_index("trade_date", inplace=True)
    df.columns = ["open", "high", "low", "close", "volume"]
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    valid = (df["open"] > 0) & (df["high"] > 0) & (df["low"] > 0) & (df["close"] > 0)
    df = df.loc[valid]
    if df.empty:
        raise ValueError(f"{stock_code} 无有效数据")
    return df


# ========== 策略 ==========

class DoubleMAStrategy(bt.Strategy):
    params = (("fast", 10), ("slow", 30))

    def __init__(self):
        self.ma_fast = bt.indicators.SMA(self.data.close, period=self.p.fast)
        self.ma_slow = bt.indicators.SMA(self.data.close, period=self.p.slow)
        self.crossover = bt.indicators.CrossOver(self.ma_fast, self.ma_slow)

    def next(self):
        if not self.position:
            if self.crossover > 0:
                self.buy()
        elif self.crossover < 0:
            self.close()


class MACDStrategy(bt.Strategy):
    def __init__(self):
        self.macd = bt.indicators.MACD(self.data.close, period_me1=12, period_me2=26, period_signal=9)
        self.crossover = bt.indicators.CrossOver(self.macd.macd, self.macd.signal)

    def next(self):
        if not self.position:
            if self.crossover > 0:
                self.buy()
        elif self.crossover < 0:
            self.close()


class RSIStrategy(bt.Strategy):
    params = (("period", 14), ("upper", 70), ("lower", 30))

    def __init__(self):
        self.rsi = bt.indicators.RSI(self.data.close, period=self.p.period)

    def next(self):
        if not self.position:
            if self.rsi < self.p.lower:
                self.buy()
        elif self.rsi > self.p.upper:
            self.close()


class BBandsStrategy(bt.Strategy):
    params = (("period", 20), ("devfactor", 2.0))

    def __init__(self):
        self.bbands = bt.indicators.BollingerBands(self.data.close, period=self.p.period, devfactor=self.p.devfactor)

    def next(self):
        if not self.position:
            if self.data.close < self.bbands.bot:
                self.buy()
        elif self.data.close > self.bbands.top:
            self.close()


class MomentumStrategy(bt.Strategy):
    params = (("period", 10),)

    def __init__(self):
        self.mom = bt.indicators.Momentum(self.data.close, period=self.p.period)

    def next(self):
        if not self.position:
            if self.mom > 0:
                self.buy()
        elif self.mom < 0:
            self.close()


STRATEGIES = {
    "double_ma": DoubleMAStrategy,
    "macd": MACDStrategy,
    "rsi": RSIStrategy,
    "bbands": BBandsStrategy,
    "momentum": MomentumStrategy,
}


def run_backtest(stock_code, start_date, end_date, strategy_name):
    env = load_env()
    initial_cash = int(env.get("BACKTEST_INITIAL_CASH", "1000000"))
    commission = float(env.get("BACKTEST_COMMISSION", "0.0002"))
    position_pct = int(env.get("BACKTEST_POSITION_PCT", "95"))

    df = load_stock_data(stock_code, start_date, end_date)
    strat_cls = STRATEGIES.get(strategy_name, DoubleMAStrategy)

    cerebro = bt.Cerebro()
    cerebro.addstrategy(strat_cls)
    cerebro.adddata(bt.feeds.PandasData(dataname=df))
    cerebro.broker.setcash(initial_cash)
    cerebro.broker.setcommission(commission=commission)
    cerebro.addsizer(bt.sizers.PercentSizer, percents=position_pct)
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe", riskfreerate=0.02)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="trades")

    results = cerebro.run()
    strat = results[0]
    final_value = cerebro.broker.getvalue()
    total_return = (final_value - initial_cash) / initial_cash

    sharpe = strat.analyzers.sharpe.get_analysis().get("sharperatio", 0) or 0
    dd = strat.analyzers.drawdown.get_analysis()
    max_dd = dd.get("max", {}).get("drawdown", 0) / 100
    ta = strat.analyzers.trades.get_analysis()
    total_trades = ta.get("total", {}).get("total", 0)
    won = ta.get("won", {}).get("total", 0)
    win_rate = won / total_trades if total_trades > 0 else 0

    years = len(df) / 252
    annual_return = (1 + total_return) ** (1 / years) - 1 if years > 0 and total_return > -1 else total_return

    return {
        "stock_code": stock_code,
        "strategy": strategy_name,
        "start_date": start_date,
        "end_date": end_date,
        "trading_days": len(df),
        "initial_cash": initial_cash,
        "final_value": round(final_value, 2),
        "total_return": round(total_return, 4),
        "annual_return": round(annual_return, 4),
        "max_drawdown": round(max_dd, 4),
        "sharpe_ratio": round(sharpe, 4),
        "total_trades": total_trades,
        "win_rate": round(win_rate, 4),
    }


def main():
    parser = argparse.ArgumentParser(description="Backtrader 回测")
    parser.add_argument("stock_code", help="股票代码")
    parser.add_argument("start_date", nargs="?", default="2024-01-01", help="开始日期")
    parser.add_argument("end_date", nargs="?", default="2025-12-31", help="结束日期")
    parser.add_argument("strategy", nargs="?", default="double_ma",
                        choices=list(STRATEGIES.keys()), help="策略名")
    args = parser.parse_args()

    try:
        result = run_backtest(args.stock_code, args.start_date, args.end_date, args.strategy)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as e:
        print(json.dumps({"error": str(e)}, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    main()
