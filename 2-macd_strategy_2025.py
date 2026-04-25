# -*- coding: utf-8 -*-
"""
MACD交易策略回测 - 使用本地CSV数据
针对贵州茅台股票（600519.SH）
回测区间：2025年1月1日到12月31日
全仓买入和全仓卖出策略
初始资金：100万

策略逻辑：
- 当MACD的DIF线上穿DEA线（金叉）时，满仓买入
- 当MACD的DIF线下穿DEA线（死叉）时，清仓卖出

注意：运行此脚本前，请先运行 6a-qmt_download_data.py 下载数据
      或者手动准备CSV数据文件
"""
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'SimSun']
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题


# 策略参数配置
STOCK_CODE = '688256.SH'  # 寒武纪股票代码
STOCK_NAME = '寒武纪'
SHORT_PERIOD = 12       # MACD快线周期
LONG_PERIOD = 26        # MACD慢线周期
SIGNAL_PERIOD = 9       # MACD信号线周期
START_DATE = '2025-01-01'  # 回测开始日期
END_DATE = '2025-12-31'    # 回测结束日期
INIT_CASH = 1000000.0       # 初始资金（100万）
LOT_SIZE = 100             # 最小交易单位（一手=100股）
COMMISSION_RATE = 0.0003    # 手续费率（万分之三，买入和卖出都收取）

# 数据文件路径（相对于当前目录）
DATA_FILE = os.path.join(os.getcwd(), 'data', '688256_SH_daily.csv')


def load_stock_data(data_file):
    """
    从CSV文件加载股票数据
    参数:
        data_file: CSV文件路径
    返回:
        DataFrame: 包含日期和收盘价的数据框
    """
    if not os.path.exists(data_file):
        print(f"错误：数据文件不存在：{data_file}")
        print("请先运行 1-tushare_download_data.py 下载数据")
        return None
    
    try:
        df = pd.read_csv(data_file, encoding='utf-8-sig')
        
        # 转换日期格式
        df['date'] = pd.to_datetime(df['date'])
        
        # 确保有收盘价列
        if 'close' not in df.columns:
            print("错误：数据文件中没有 close 列")
            return None
        
        # 按日期排序
        df = df.sort_values('date').reset_index(drop=True)
        
        return df
        
    except Exception as e:
        print(f"加载数据文件时发生错误：{e}")
        return None


def get_MACD(close, short=12, long=26, m=9):
    """
    计算MACD指标
    参数:
        close: 收盘价序列
        short: 快线周期，默认12
        long: 慢线周期，默认26
        m: 信号线周期，默认9
    返回:
        dif: DIF线（快线）
        dea: DEA线（信号线）
        macd_bar: MACD柱状图（DIF-DEA）*2
    """
    # 计算EMA
    ema_short = pd.Series(close).ewm(span=short, adjust=False).mean()
    ema_long = pd.Series(close).ewm(span=long, adjust=False).mean()
    
    # DIF = EMA(12) - EMA(26)
    dif = ema_short - ema_long
    
    # DEA = EMA(DIF, 9)
    dea = dif.ewm(span=m, adjust=False).mean()
    
    # MACD柱 = (DIF - DEA) * 2
    macd_bar = (dif - dea) * 2
    
    return dif.values, dea.values, macd_bar.values


def calculate_max_drawdown(nav_series):
    """
    计算最大回撤
    参数:
        nav_series: 净值序列（pandas Series）
    返回:
        max_drawdown: 最大回撤值（负数）
    """
    # 计算累计最大值
    running_max = nav_series.cummax()
    
    # 计算回撤 = (当前净值 / 历史最高净值) - 1
    drawdown = (nav_series / running_max) - 1.0
    
    # 最大回撤是最小的回撤值（负数）
    max_drawdown = drawdown.min()
    
    return max_drawdown


def calculate_total_return(nav_series):
    """
    计算总收益率
    参数:
        nav_series: 净值序列（pandas Series）
    返回:
        total_return: 总收益率
    """
    if len(nav_series) < 2:
        return 0.0
    
    # 总收益率 = (期末净值 / 期初净值) - 1
    total_return = (nav_series.iloc[-1] / nav_series.iloc[0]) - 1.0
    
    return total_return


def plot_strategy_results(date_index, close_prices, nav_series, return_series, 
                          dif, dea, macd_bar, trades, stock_name, stock_code):
    """
    绘制策略回测结果图表
    包含：股价走势与买卖点、MACD指标、资金曲线
    参数:
        date_index: 日期索引
        close_prices: 收盘价序列
        nav_series: 净值序列
        return_series: 收益率序列
        dif: DIF线
        dea: DEA线
        macd_bar: MACD柱
        trades: 交易记录列表
        stock_name: 股票名称
        stock_code: 股票代码
    """
    # 创建图表，3行1列
    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
    fig.suptitle(f'{stock_name}({stock_code}) MACD策略回测 - 2025年', fontsize=16, fontweight='bold')
    
    # 提取买入和卖出点
    buy_dates = []
    buy_prices = []
    sell_dates = []
    sell_prices = []
    
    for trade in trades:
        if trade['action'] == '买入':
            buy_dates.append(trade['date'])
            buy_prices.append(trade['price'])
        elif trade['action'] == '卖出':
            sell_dates.append(trade['date'])
            sell_prices.append(trade['price'])
    
    # ===== 子图1：股价走势和买卖点 =====
    ax1 = axes[0]
    ax1.plot(date_index, close_prices, 'b-', linewidth=1.5, label='收盘价')
    
    # 标记买入点（红色向上三角形）
    if buy_dates:
        ax1.scatter(buy_dates, buy_prices, marker='^', color='red', s=150, 
                   zorder=5, label='买入点', edgecolors='darkred', linewidths=1)
        # 添加买入标注
        for i, (date, price) in enumerate(zip(buy_dates, buy_prices)):
            ax1.annotate(f'买{i+1}', (date, price), textcoords="offset points", 
                        xytext=(0, 15), ha='center', fontsize=9, color='red', fontweight='bold')
    
    # 标记卖出点（绿色向下三角形）
    if sell_dates:
        ax1.scatter(sell_dates, sell_prices, marker='v', color='green', s=150, 
                   zorder=5, label='卖出点', edgecolors='darkgreen', linewidths=1)
        # 添加卖出标注
        for i, (date, price) in enumerate(zip(sell_dates, sell_prices)):
            ax1.annotate(f'卖{i+1}', (date, price), textcoords="offset points", 
                        xytext=(0, -20), ha='center', fontsize=9, color='green', fontweight='bold')
    
    ax1.set_ylabel('股价 (元)', fontsize=12)
    ax1.legend(loc='upper left', fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.set_title('股价走势与买卖点', fontsize=12)
    
    # 设置Y轴范围，留出一些空间显示标注
    price_min = min(close_prices) * 0.95
    price_max = max(close_prices) * 1.05
    ax1.set_ylim(price_min, price_max)
    
    # ===== 子图2：MACD指标 =====
    ax2 = axes[1]
    
    # 绘制DIF和DEA线
    ax2.plot(date_index, dif, 'b-', linewidth=1.2, label='DIF')
    ax2.plot(date_index, dea, 'orange', linewidth=1.2, label='DEA')
    
    # 绘制MACD柱状图
    colors = ['red' if val >= 0 else 'green' for val in macd_bar]
    ax2.bar(date_index, macd_bar, color=colors, alpha=0.6, width=1.5, label='MACD柱')
    
    ax2.axhline(y=0, color='gray', linestyle='--', linewidth=1, alpha=0.7)
    ax2.set_ylabel('MACD', fontsize=12)
    ax2.legend(loc='upper left', fontsize=10)
    ax2.grid(True, alpha=0.3)
    ax2.set_title(f'MACD指标 (快线={SHORT_PERIOD}, 慢线={LONG_PERIOD}, 信号线={SIGNAL_PERIOD})', fontsize=12)
    
    # 标记金叉和死叉
    for trade in trades:
        trade_date = trade['date']
        if trade_date in date_index:
            if trade['action'] == '买入':
                ax2.axvline(x=trade_date, color='red', linestyle='--', alpha=0.5, linewidth=1)
            else:
                ax2.axvline(x=trade_date, color='green', linestyle='--', alpha=0.5, linewidth=1)
    
    # ===== 子图3：资金曲线 =====
    ax3 = axes[2]
    
    # 绘制资金曲线（净值）
    nav_values = nav_series.values / 10000  # 转换为万元，便于显示
    ax3.plot(date_index, nav_values, 'purple', linewidth=1.5, label='资金曲线')
    ax3.axhline(y=INIT_CASH / 10000, color='gray', linestyle='--', linewidth=1, alpha=0.7, label='初始资金')
    
    # 填充盈利区域（浅绿色）和亏损区域（浅红色）
    init_nav = INIT_CASH / 10000
    ax3.fill_between(date_index, nav_values, init_nav, 
                     where=(nav_values >= init_nav), color='lightgreen', alpha=0.3)
    ax3.fill_between(date_index, nav_values, init_nav, 
                     where=(nav_values < init_nav), color='lightcoral', alpha=0.3)
    
    # 标记买卖点对应的资金位置
    for trade in trades:
        trade_date = trade['date']
        if trade_date in date_index:
            idx = date_index.get_loc(trade_date)
            nav_val = nav_values[idx]
            color = 'red' if trade['action'] == '买入' else 'green'
            marker = '^' if trade['action'] == '买入' else 'v'
            ax3.scatter([trade_date], [nav_val], marker=marker, color=color, s=80, zorder=5)
    
    ax3.set_ylabel('资金 (万元)', fontsize=12)
    ax3.set_xlabel('日期', fontsize=12)
    ax3.legend(loc='upper left', fontsize=10)
    ax3.grid(True, alpha=0.3)
    ax3.set_title('资金曲线', fontsize=12)
    
    # 添加最终资金和收益率标注
    final_nav = nav_values[-1]
    final_return = (nav_series.iloc[-1] / INIT_CASH - 1) * 100
    ax3.annotate(f'最终资金: {final_nav:.2f}万 ({final_return:+.2f}%)', 
                 xy=(date_index[-1], final_nav),
                 xytext=(-150, 20), textcoords='offset points',
                 fontsize=11, fontweight='bold',
                 arrowprops=dict(arrowstyle='->', color='purple'),
                 color='purple')
    
    # 格式化X轴日期
    ax3.xaxis.set_major_locator(mdates.MonthLocator())
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.xticks(rotation=45)
    
    # 调整布局
    plt.tight_layout()
    plt.subplots_adjust(top=0.93)
    
    # 保存图表
    output_dir = os.path.join(os.getcwd(), 'outputs')
    os.makedirs(output_dir, exist_ok=True)
    chart_file = os.path.join(output_dir, 'macd_strategy_2025_chart.png')
    plt.savefig(chart_file, dpi=150, bbox_inches='tight', facecolor='white')
    print(f"\n策略图表已保存至：{chart_file}")
    
    # 显示图表
    plt.show()
    
    return chart_file


def macd_strategy_backtest(data_file=None):
    """
    MACD策略回测主函数
    策略逻辑：
    1. 当DIF线上穿DEA线（金叉）时，满仓买入
    2. 当DIF线下穿DEA线（死叉）时，清仓卖出
    
    参数:
        data_file: 数据文件路径，默认使用 DATA_FILE
    """
    if data_file is None:
        data_file = DATA_FILE
    
    print(f"开始回测：{STOCK_NAME}({STOCK_CODE}) - MACD策略")
    print(f"回测区间：{START_DATE} 至 {END_DATE}")
    print(f"初始资金：{INIT_CASH:,.0f} 元")
    print(f"数据文件：{data_file}")
    print("-" * 60)
    
    try:
        # 步骤1：加载数据
        print("步骤1：加载历史数据")
        df = load_stock_data(data_file)
        
        if df is None:
            return None
        
        print(f"成功加载 {len(df)} 条历史数据")
        print(f"数据日期范围：{df['date'].iloc[0].strftime('%Y-%m-%d')} 至 {df['date'].iloc[-1].strftime('%Y-%m-%d')}")
        
        # 创建时间序列
        close_series_all = pd.Series(df['close'].values, index=df['date']).sort_index()
        
        if len(close_series_all) < LONG_PERIOD + SIGNAL_PERIOD + 2:
            print(f"错误：历史数据不足，只有{len(close_series_all)}条，计算MACD需要至少{LONG_PERIOD + SIGNAL_PERIOD + 2}条")
            return None
        
        # 步骤2：计算MACD指标（使用全部历史数据，确保有足够的历史数据）
        print(f"\n步骤2：计算MACD指标")
        print(f"使用全部 {len(close_series_all)} 条历史数据计算MACD指标")
        dif_all, dea_all, macd_bar_all = get_MACD(close_series_all.values, SHORT_PERIOD, LONG_PERIOD, SIGNAL_PERIOD)
        
        # 创建MACD指标的Series，索引与close_series_all对齐
        dif_series = pd.Series(dif_all, index=close_series_all.index)
        dea_series = pd.Series(dea_all, index=close_series_all.index)
        macd_bar_series = pd.Series(macd_bar_all, index=close_series_all.index)
        
        # 过滤回测区间的数据
        start_date = pd.Timestamp(START_DATE)
        end_date = pd.Timestamp(END_DATE)
        mask = (close_series_all.index >= start_date) & (close_series_all.index <= end_date)
        close_prices = close_series_all[mask].values
        date_index = close_series_all[mask].index
        
        # 同样过滤MACD指标数据
        dif = dif_series[mask].values
        dea = dea_series[mask].values
        macd_bar = macd_bar_series[mask].values
        
        if len(close_prices) == 0:
            print(f"回测区间 {START_DATE} 至 {END_DATE} 内没有交易日数据")
            print("提示：请检查数据文件是否包含该时间段的数据")
            return None
        
        print(f"回测区间内共有 {len(close_prices)} 个交易日")
        
        # 初始化仓位和净值
        position = np.zeros(len(close_prices), dtype=float)  # 仓位比例：0表示空仓，1表示满仓
        nav = np.zeros(len(close_prices), dtype=float)       # 净值序列
        nav[0] = INIT_CASH
        
        # 实际交易状态
        cash = INIT_CASH      # 当前现金
        shares = 0            # 当前持有股数（必须是100的整数倍）
        
        # 记录交易信号
        trades = []
        
        # 逐日回测
        print(f"\n步骤3：执行回测")
        print(f"交易参数：最小交易单位={LOT_SIZE}股，手续费率={COMMISSION_RATE*10000:.2f}")
        for i in range(1, len(close_prices)):
            current_price = close_prices[i]
            
            # 判断MACD金叉和死叉
            # 金叉：DIF上穿DEA
            golden_cross = (dif[i-1] <= dea[i-1]) and (dif[i] > dea[i])
            # 死叉：DIF下穿DEA
            death_cross = (dif[i-1] >= dea[i-1]) and (dif[i] < dea[i])
            
            # 根据信号调整仓位
            if golden_cross and shares == 0:  # 金叉，满仓买入
                # 计算能买多少手（向下取整到100的整数倍）
                # 考虑手续费：可用资金 = cash / (1 + COMMISSION_RATE)
                available_cash = cash / (1 + COMMISSION_RATE)
                max_shares = int(available_cash / current_price / LOT_SIZE) * LOT_SIZE
                
                if max_shares >= LOT_SIZE:  # 至少能买1手
                    # 计算实际买入金额（含手续费）
                    buy_amount = max_shares * current_price
                    commission = buy_amount * COMMISSION_RATE
                    total_cost = buy_amount + commission
                    
                    if total_cost <= cash:
                        shares = max_shares
                        cash -= total_cost
                        
                        # 记录交易
                        current_nav = cash + shares * current_price
                        position_pct = (shares * current_price) / current_nav if current_nav > 0 else 0.0
                        trades.append({
                            'date': date_index[i],
                            'action': '买入',
                            'price': current_price,
                            'shares': shares,
                            'amount': buy_amount,
                            'commission': commission,
                            'cash': cash,
                            'position': position_pct
                        })
            
            elif death_cross and shares > 0:  # 死叉，清仓卖出
                # 计算卖出金额（含手续费）
                sell_amount = shares * current_price
                commission = sell_amount * COMMISSION_RATE
                net_proceeds = sell_amount - commission
                
                # 记录交易
                trades.append({
                    'date': date_index[i],
                    'action': '卖出',
                    'price': current_price,
                    'shares': shares,
                    'amount': sell_amount,
                    'commission': commission,
                    'cash': cash + net_proceeds,
                    'position': 0.0
                })
                
                cash += net_proceeds
                shares = 0
            
            # 计算当日净值（现金 + 持仓市值）
            nav[i] = cash + shares * current_price
            # 计算仓位比例
            position[i] = (shares * current_price) / nav[i] if nav[i] > 0 else 0.0
        
        # 转换为pandas Series便于计算
        nav_series = pd.Series(nav, index=date_index)
        
        # 计算收益率序列（相对于初始资金）
        return_series = (nav_series / INIT_CASH) - 1.0
        
        # 计算总收益率和最大回撤
        total_return = calculate_total_return(nav_series)
        max_drawdown = calculate_max_drawdown(nav_series)
        
        # 输出结果
        print("\n" + "=" * 60)
        print("回测结果")
        print("=" * 60)
        print(f"股票代码：{STOCK_CODE} ({STOCK_NAME})")
        print(f"回测区间：{START_DATE} 至 {END_DATE}")
        print(f"初始资金：{INIT_CASH:,.2f} 元")
        print(f"期末净值：{nav_series.iloc[-1]:,.2f} 元")
        print(f"总收益率：{total_return:.4%}")
        print(f"最大回撤：{max_drawdown:.4%}")
        print(f"交易次数：{len(trades)} 次")
        print(f"期末持仓：{shares} 股" if shares > 0 else "期末持仓：空仓")
        print(f"期末现金：{cash:,.2f} 元")
        print("=" * 60)
        
        # 显示交易记录
        if len(trades) > 0:
            print("\n交易记录：")
            total_commission = 0.0
            for trade in trades:
                total_commission += trade.get('commission', 0.0)
                shares_str = f"{trade['shares']}股" if 'shares' in trade else "N/A"
                amount_str = f"金额: {trade.get('amount', 0):,.2f}" if 'amount' in trade else ""
                commission_str = f"手续费: {trade.get('commission', 0):,.2f}" if 'commission' in trade else ""
                position_str = f"仓位: {trade['position']:.1%}" if 'position' in trade else ""
                print(f"  {trade['date'].strftime('%Y-%m-%d')} | {trade['action']:4s} | "
                      f"价格: {trade['price']:.2f} | {shares_str} | {amount_str} | "
                      f"{commission_str} | {position_str}")
            print(f"\n累计手续费：{total_commission:,.2f} 元")
        else:
            print("\n交易记录：无交易")
        
        # 保存结果到CSV文件
        output_dir = os.path.join(os.getcwd(), 'outputs')
        os.makedirs(output_dir, exist_ok=True)
        
        # 保存净值曲线
        nav_df = pd.DataFrame({
            'date': nav_series.index,
            'nav': nav_series.values,
            'return': return_series.values,
            'position': position
        })
        nav_file = os.path.join(output_dir, 'macd_strategy_2025_nav.csv')
        nav_df.to_csv(nav_file, index=False, encoding='utf-8-sig')
        print(f"\n净值曲线已保存至：{nav_file}")
        
        # 保存交易记录
        if len(trades) > 0:
            trades_df = pd.DataFrame(trades)
            trades_file = os.path.join(output_dir, 'macd_strategy_2025_trades.csv')
            trades_df.to_csv(trades_file, index=False, encoding='utf-8-sig')
            print(f"交易记录已保存至：{trades_file}")
        
        # 保存汇总报告
        summary_file = os.path.join(output_dir, 'macd_strategy_2025_summary.txt')
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("MACD策略回测报告\n")
            f.write("=" * 60 + "\n")
            f.write(f"股票代码：{STOCK_CODE} ({STOCK_NAME})\n")
            f.write(f"回测区间：{START_DATE} 至 {END_DATE}\n")
            f.write(f"初始资金：{INIT_CASH:,.2f} 元\n")
            f.write(f"期末净值：{nav_series.iloc[-1]:,.2f} 元\n")
            f.write(f"总收益率：{total_return:.4%}\n")
            f.write(f"最大回撤：{max_drawdown:.4%}\n")
            f.write(f"交易次数：{len(trades)} 次\n")
            f.write(f"期末持仓：{shares} 股\n" if shares > 0 else "期末持仓：空仓\n")
            f.write(f"期末现金：{cash:,.2f} 元\n")
            f.write(f"MACD参数：快线={SHORT_PERIOD}, 慢线={LONG_PERIOD}, 信号线={SIGNAL_PERIOD}\n")
            f.write(f"交易参数：最小交易单位={LOT_SIZE}股，手续费率={COMMISSION_RATE*10000:.2f}\n")
        print(f"汇总报告已保存至：{summary_file}")
        
        # 绘制策略图表
        print("\n步骤4：绘制策略图表")
        plot_strategy_results(
            date_index=date_index,
            close_prices=close_prices,
            nav_series=nav_series,
            return_series=return_series,
            dif=dif,
            dea=dea,
            macd_bar=macd_bar,
            trades=trades,
            stock_name=STOCK_NAME,
            stock_code=STOCK_CODE
        )
        
        return {
            'total_return': total_return,
            'max_drawdown': max_drawdown,
            'final_nav': nav_series.iloc[-1],
            'trades_count': len(trades),
            'trades': trades
        }
        
    except Exception as e:
        print(f"回测过程中发生错误：{e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    # 运行回测
    result = macd_strategy_backtest()
    
    if result:
        print("\n回测完成!")
        print(f"\n关键指标：")
        print(f"  总收益率：{result['total_return']:.4%}")
        print(f"  最大回撤：{result['max_drawdown']:.4%}")
        print(f"  交易次数：{result['trades_count']} 次")
    else:
        print("\n回测失败，请检查错误信息。")
