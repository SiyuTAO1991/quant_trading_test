# -*- coding: utf-8 -*-
"""
双均线策略 - 趋势跟踪入门

类别: 趋势跟踪
逻辑: 快线(10日均线)上穿慢线(30日均线) -> 买入; 快线下穿慢线 -> 卖出
核心: 理解 Cerebro 大脑 + 跑通第一个策略

运行: python 1-双均线策略.py
"""
import backtrader as bt
from data_loader import load_stock_data, run_and_report, INITIAL_CASH, COMMISSION, POSITION_PCT


class DoubleMAStrategy(bt.Strategy):
    """双均线金叉/死叉策略"""
    params = (('fast', 10), ('slow', 30))

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


if __name__ == '__main__':
    stock_code = '600519.SH'
    start_date = '2025-01-01'
    end_date = '2025-12-31'

    # 1. 从MySQL加载数据
    df = load_stock_data(stock_code, start_date, end_date)
    print(f"股票: {stock_code}")
    print(f"数据: {df.index[0].strftime('%Y-%m-%d')} ~ {df.index[-1].strftime('%Y-%m-%d')}  共{len(df)}个交易日")

    # 2. 创建 Cerebro 大脑 - Backtrader的核心引擎
    cerebro = bt.Cerebro()

    # 3. 添加策略
    cerebro.addstrategy(DoubleMAStrategy)

    # 4. 添加数据（将DataFrame转为Backtrader的数据格式）
    cerebro.adddata(bt.feeds.PandasData(dataname=df))

    # 5. 设置回测参数（从.env读取）
    cerebro.broker.setcash(INITIAL_CASH)                          # 初始资金
    cerebro.broker.setcommission(commission=COMMISSION)           # 手续费
    cerebro.addsizer(bt.sizers.PercentSizer, percents=POSITION_PCT)  # 仓位比例

    # 6. 添加分析器（用于计算绩效指标）
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.02)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')

    # 7. 运行回测
    results = cerebro.run()
    strat = results[0]

    # 8. 输出结果
    final_value = cerebro.broker.getvalue()
    total_return = (final_value - INITIAL_CASH) / INITIAL_CASH

    sharpe = strat.analyzers.sharpe.get_analysis()
    sharpe_ratio = sharpe.get('sharperatio', 0) or 0

    dd = strat.analyzers.drawdown.get_analysis()
    max_drawdown = dd.get('max', {}).get('drawdown', 0) / 100

    ta = strat.analyzers.trades.get_analysis()
    trade_count = ta.get('total', {}).get('total', 0)
    won = ta.get('won', {}).get('total', 0)
    win_rate = won / trade_count if trade_count > 0 else 0

    print(f"\n{'='*50}")
    print(f"初始资金:  {INITIAL_CASH:>14,.2f}")
    print(f"手续费:    {COMMISSION*10000:>11.1f} (万分之)")
    print(f"仓位比例:  {POSITION_PCT:>13d}%")
    print(f"最终资金:  {final_value:>14,.2f}")
    print(f"总收益率:  {total_return*100:>13.2f}%")
    print(f"最大回撤:  {max_drawdown*100:>13.2f}%")
    print(f"夏普比率:  {sharpe_ratio:>14.4f}")
    print(f"交易次数:  {trade_count:>14d}")
    print(f"胜率:      {win_rate*100:>13.2f}%")
    print(f"{'='*50}")

    # 上面是展开写的教学版本，实际使用时可以一行搞定（同时生成图表）:
    # run_and_report(DoubleMAStrategy, '600519.SH', '2024-01-01', '2025-12-31', label='双均线策略', plot=True)
