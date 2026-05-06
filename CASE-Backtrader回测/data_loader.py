# -*- coding: utf-8 -*-
"""
数据加载与回测工具模块

功能:
  - 从MySQL读取K线数据 (trade_stock_daily)
  - 统一配置Cerebro引擎 (初始资金/手续费/仓位 来自 .env)
  - 自动记录交易和净值 (包装任意策略类)
  - 计算完整绩效指标
  - 生成可视化图表 (K线+买卖点, 净值曲线, 回撤曲线)
"""
import pandas as pd
import numpy as np
import backtrader as bt
import os
from db_config import execute_query, INITIAL_CASH, COMMISSION, POSITION_PCT


# ============================================================
# 数据加载
# ============================================================

def load_stock_data(stock_code, start_date=None, end_date=None):
    """
    从MySQL加载日K线数据

    参数:
        stock_code: 股票代码，如 '600519.SH'
        start_date: 开始日期，如 '2024-01-01'
        end_date:   结束日期，如 '2025-12-31'

    返回:
        pandas DataFrame，索引为日期，列为 open/high/low/close/volume
    """
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
    rows = execute_query(sql, params)
    if not rows:
        raise ValueError(f"没有找到 {stock_code} 的数据，请检查数据库或先运行数据采集")

    df = pd.DataFrame(rows)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df.set_index('trade_date', inplace=True)
    df.columns = ['open', 'high', 'low', 'close', 'volume']
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    # 过滤无效价格(负价/零价多见于后复权异常)，避免策略信号和收益计算错误
    valid_mask = (df['open'] > 0) & (df['high'] > 0) & (df['low'] > 0) & (df['close'] > 0)
    df = df.loc[valid_mask]
    if df.empty:
        raise ValueError(f"{stock_code} 过滤无效价格后无有效数据，请检查数据源")
    return df


# ============================================================
# 策略包装器 - 自动记录交易和净值
# ============================================================

def _wrap_strategy(strategy_class):
    """
    包装任意策略类，自动记录:
      - _trade_log: 买卖交易记录
      - _nav_log: 每日净值
    不影响原策略逻辑
    """
    class WrappedStrategy(strategy_class):
        def __init__(self):
            super().__init__()
            self._trade_log = []
            self._nav_log = []

        def notify_order(self, order):
            if order.status == order.Completed:
                self._trade_log.append({
                    'date': self.data.datetime.date(0),
                    'type': 'BUY' if order.isbuy() else 'SELL',
                    'price': round(order.executed.price, 2),
                    'size': abs(int(order.executed.size)),
                })
            if hasattr(super(), 'notify_order'):
                super().notify_order(order)

        def next(self):
            self._nav_log.append({
                'date': self.data.datetime.date(0),
                'nav': self.broker.getvalue(),
            })
            super().next()

    WrappedStrategy.__name__ = strategy_class.__name__
    WrappedStrategy.__qualname__ = strategy_class.__qualname__
    WrappedStrategy.__module__ = strategy_class.__module__
    return WrappedStrategy


# ============================================================
# Cerebro 配置
# ============================================================

def setup_cerebro(strategy_class, stock_code, start_date=None, end_date=None, **strategy_kwargs):
    """
    创建并配置好 Cerebro 引擎

    返回: (cerebro, df)
    """
    df = load_stock_data(stock_code, start_date, end_date)

    cerebro = bt.Cerebro()
    cerebro.addstrategy(strategy_class, **strategy_kwargs)
    cerebro.adddata(bt.feeds.PandasData(dataname=df))
    cerebro.broker.setcash(INITIAL_CASH)
    cerebro.broker.setcommission(commission=COMMISSION)
    cerebro.addsizer(bt.sizers.PercentSizer, percents=POSITION_PCT)

    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.02)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')

    return cerebro, df


# ============================================================
# 绩效计算
# ============================================================

def _calc_metrics(cerebro, strat, df):
    """从Backtrader结果中提取完整绩效指标"""
    final_value = cerebro.broker.getvalue()
    total_return = (final_value - INITIAL_CASH) / INITIAL_CASH

    # 年化收益
    trading_days = len(df)
    years = trading_days / 252
    if years > 0 and total_return > -1:
        annual_return = (1 + total_return) ** (1 / years) - 1
    else:
        annual_return = total_return

    # 夏普比率
    sharpe_ratio = strat.analyzers.sharpe.get_analysis().get('sharperatio', 0) or 0

    # 最大回撤: 优先用净值序列计算(避免Backtrader在异常情况下返回>100%的值)
    max_drawdown = 0.0
    max_dd_len = 0
    nav_log = getattr(strat, '_nav_log', [])
    if nav_log:
        navs = [x['nav'] for x in nav_log]
        peak = navs[0]
        dd_len = 0
        for v in navs:
            if v > peak:
                peak = v
                dd_len = 0
            else:
                dd_len += 1
                if peak > 0 and v > 0:
                    dd_pct = (peak - v) / peak
                    max_drawdown = max(max_drawdown, min(dd_pct, 1.0))  # 长仓回撤不超过100%
                max_dd_len = max(max_dd_len, dd_len)
    if not nav_log:
        dd = strat.analyzers.drawdown.get_analysis()
        bt_dd = dd.get('max', {}).get('drawdown', 0) / 100
        max_drawdown = min(bt_dd, 1.0)  # 长仓策略回撤不超过100%，异常值截断
        max_dd_len = dd.get('max', {}).get('len', 0)

    # 卡玛比率 = 年化收益 / 最大回撤
    calmar_ratio = annual_return / max_drawdown if max_drawdown > 0 else 0

    # 交易统计
    ta = strat.analyzers.trades.get_analysis()
    total_trades = ta.get('total', {}).get('total', 0)
    won_trades = ta.get('won', {}).get('total', 0)
    lost_trades = ta.get('lost', {}).get('total', 0)
    win_rate = won_trades / total_trades if total_trades > 0 else 0

    # 盈亏比
    avg_win = ta.get('won', {}).get('pnl', {}).get('average', 0) or 0
    avg_loss = ta.get('lost', {}).get('pnl', {}).get('average', 0) or 0
    profit_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0

    # 利润因子 = 总盈利 / |总亏损|
    gross_profit = ta.get('won', {}).get('pnl', {}).get('total', 0) or 0
    gross_loss = ta.get('lost', {}).get('pnl', {}).get('total', 0) or 0
    profit_factor = abs(gross_profit / gross_loss) if gross_loss != 0 else 0

    # 最大连续亏损次数
    max_consecutive_losses = _calc_max_consecutive_losses(ta)

    # 期望值
    expected_value = win_rate * avg_win + (1 - win_rate) * avg_loss if total_trades > 0 else 0

    # 买入持有基准收益(用于对比策略是否跑赢简单持有)
    # 使用有效正价格，避免后复权等导致的首尾负价
    valid_close = df['close'][df['close'] > 0]
    if len(valid_close) >= 2:
        close_start = float(valid_close.iloc[0])
        close_end = float(valid_close.iloc[-1])
        benchmark_return = (close_end / close_start - 1) if close_start > 0 else 0
    else:
        benchmark_return = 0

    return {
        'final_value': round(final_value, 2),
        'total_return': total_return,
        'annual_return': annual_return,
        'max_drawdown': max_drawdown,
        'max_dd_len': max_dd_len,
        'sharpe_ratio': round(sharpe_ratio, 4),
        'calmar_ratio': round(calmar_ratio, 4),
        'total_trades': total_trades,
        'won_trades': won_trades,
        'lost_trades': lost_trades,
        'win_rate': win_rate,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'profit_loss_ratio': round(profit_loss_ratio, 2),
        'profit_factor': round(profit_factor, 2),
        'max_consecutive_losses': max_consecutive_losses,
        'expected_value': round(expected_value, 2),
        'years': round(years, 2),
        'trading_days': trading_days,
        'benchmark_return': benchmark_return,
    }


def _calc_max_consecutive_losses(ta):
    """从TradeAnalyzer提取最大连续亏损"""
    streak = ta.get('streak', {})
    lost_streak = streak.get('lost', {})
    return lost_streak.get('longest', 0) if lost_streak else 0


# ============================================================
# 运行回测 + 输出报告
# ============================================================

def run_and_report(strategy_class, stock_code, start_date=None, end_date=None,
                   label='', plot=False, **strategy_kwargs):
    """
    运行回测并打印绩效报告

    参数:
        strategy_class: 策略类
        stock_code: 股票代码
        start_date/end_date: 数据范围
        label: 显示名称
        plot: 是否输出可视化图表到 outputs/ 目录
        **strategy_kwargs: 传给策略的额外参数

    返回:
        dict 包含绩效指标和回测数据
    """
    wrapped = _wrap_strategy(strategy_class)
    cerebro, df = setup_cerebro(wrapped, stock_code, start_date, end_date, **strategy_kwargs)

    if label:
        print(f"{label} | {stock_code} | {df.index[0].strftime('%Y-%m-%d')} ~ "
              f"{df.index[-1].strftime('%Y-%m-%d')} | {len(df)}个交易日")

    results = cerebro.run()
    strat = results[0]
    m = _calc_metrics(cerebro, strat, df)

    # 打印绩效
    print(f"  总收益: {m['total_return']*100:+.2f}% | 年化: {m['annual_return']*100:+.2f}% | "
          f"最大回撤: {m['max_drawdown']*100:.2f}% | 夏普: {m['sharpe_ratio']:.2f} | "
          f"卡玛: {m['calmar_ratio']:.2f}")
    print(f"  交易: {m['total_trades']}次 | 胜率: {m['win_rate']*100:.1f}% | "
          f"盈亏比: {m['profit_loss_ratio']:.2f} | 利润因子: {m['profit_factor']:.2f} | "
          f"最大连亏: {m['max_consecutive_losses']}次")
    print(f"  [基准] 买入持有: {m['benchmark_return']*100:+.2f}%")

    result = {**m, 'df': df, 'trades': strat._trade_log, 'nav': strat._nav_log}

    if plot:
        chart_name = label or strategy_class.__name__
        plot_backtest(result, stock_code, chart_name)

    return result


# ============================================================
# 可视化图表
# ============================================================

def plot_backtest(result, stock_code='', title=''):
    """
    绘制回测结果图表:
      上图: K线(收盘价) + 买卖点标记
      下图: 净值曲线 + 回撤填充

    参数:
        result: run_and_report 的返回值
        stock_code: 股票代码（用于标题）
        title: 图表标题
    """
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.rcParams['font.sans-serif'] = ['SimHei']
    matplotlib.rcParams['axes.unicode_minus'] = False

    os.makedirs('outputs', exist_ok=True)

    df = result['df']
    trades = result.get('trades', [])
    nav_data = result.get('nav', [])

    if not nav_data:
        print("没有净值数据，跳过绘图")
        return

    # 构建净值DataFrame
    nav_df = pd.DataFrame(nav_data)
    nav_df['date'] = pd.to_datetime(nav_df['date'])
    nav_df.set_index('date', inplace=True)
    nav_df['nav_pct'] = nav_df['nav'] / INITIAL_CASH
    nav_df['peak'] = nav_df['nav'].cummax()
    nav_df['drawdown'] = (nav_df['nav'] - nav_df['peak']) / nav_df['peak'] * 100

    # 买入持有基准
    close_start = float(df['close'].iloc[0])
    benchmark = df['close'] / close_start

    # 分离买卖点
    buy_dates = [t['date'] for t in trades if t['type'] == 'BUY']
    buy_prices = [t['price'] for t in trades if t['type'] == 'BUY']
    sell_dates = [t['date'] for t in trades if t['type'] == 'SELL']
    sell_prices = [t['price'] for t in trades if t['type'] == 'SELL']

    m = result
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 12),
                                         gridspec_kw={'height_ratios': [3, 2, 1]})

    # ---- 上图: K线(收盘价) + 买卖点 ----
    ax1.plot(df.index, df['close'], 'gray', linewidth=1, alpha=0.8, label='收盘价')
    if buy_dates:
        ax1.scatter(buy_dates, buy_prices, color='#e74c3c', marker='^', s=80,
                    zorder=5, label=f'买入({len(buy_dates)}次)')
    if sell_dates:
        ax1.scatter(sell_dates, sell_prices, color='#2ecc71', marker='v', s=80,
                    zorder=5, label=f'卖出({len(sell_dates)}次)')
    ax1.set_ylabel('价格')
    ax1.set_title(f'{title}  {stock_code}', fontsize=14, fontweight='bold')
    ax1.legend(loc='upper left', fontsize=9)
    ax1.grid(True, alpha=0.3)

    # 右侧绩效指标
    info_text = (
        f"Return:    {m['total_return']*100:+.2f}%\n"
        f"Benchmark: {m.get('benchmark_return', 0)*100:+.2f}%\n"
        f"Annual:    {m['annual_return']*100:+.2f}%\n"
        f"MaxDD:     {m['max_drawdown']*100:.2f}%\n"
        f"Sharpe:    {m['sharpe_ratio']:.2f}\n"
        f"Calmar:    {m['calmar_ratio']:.2f}\n"
        f"WinRate:   {m['win_rate']*100:.1f}%\n"
        f"P/L Ratio: {m['profit_loss_ratio']:.2f}\n"
        f"ProfitF:   {m['profit_factor']:.2f}"
    )
    ax1.text(0.98, 0.97, info_text, transform=ax1.transAxes,
             fontsize=9, verticalalignment='top', horizontalalignment='right',
             bbox=dict(boxstyle='round,pad=0.5', facecolor='wheat', alpha=0.8),
             family='monospace')

    # ---- 中图: 净值曲线 vs 基准 ----
    ax2.plot(nav_df.index, nav_df['nav_pct'], '#2980b9', linewidth=1.5, label='策略净值')
    ax2.plot(benchmark.index, benchmark, 'gray', linewidth=1, alpha=0.6, label='买入持有')
    ax2.axhline(y=1.0, color='red', linestyle='--', alpha=0.3)
    ax2.set_ylabel('净值 (初始=1.0)')
    ax2.legend(loc='upper left', fontsize=9)
    ax2.grid(True, alpha=0.3)

    # ---- 下图: 回撤曲线 ----
    ax3.fill_between(nav_df.index, nav_df['drawdown'], 0, color='#e74c3c', alpha=0.4)
    ax3.plot(nav_df.index, nav_df['drawdown'], '#c0392b', linewidth=0.8)
    ax3.set_ylabel('回撤(%)')
    ax3.set_xlabel('日期')
    ax3.grid(True, alpha=0.3)

    plt.tight_layout()

    safe_name = title.replace(' ', '_').replace('/', '_')
    plot_file = os.path.join('outputs', f'{safe_name}.png')
    plt.savefig(plot_file, dpi=150, bbox_inches='tight')
    print(f"  图表已保存: {plot_file}")
    plt.close()
