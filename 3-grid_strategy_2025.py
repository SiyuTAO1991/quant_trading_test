# -*- coding: utf-8 -*-
"""
网格交易策略回测 - 使用本地CSV数据
针对贵州茅台股票（600519.SH）
回测区间：2025年1月1日到12月31日

策略逻辑（等差网格）：
- 震荡区间：1300-1700，中心位置：1500
- 每下跌50元，买入100股（1450买、1400买、1350买、1300买，低于1300不买）
- 每上涨50元，卖出100股（1550卖、1600卖、1650卖、1700卖，高于1700不卖）
- 初始100万现金，无持仓

网格交易原理：
- 在预设的价格区间内设置多个网格线
- 价格触及下方网格时买入，触及上方网格时卖出
- 通过高抛低吸赚取震荡区间内的差价

注意：运行此脚本前，请确保 data/600519_SH_daily.csv 文件存在
"""
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'SimSun']
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题


# =============================================================================
# 策略参数配置
# =============================================================================
STOCK_CODE = '601088.SH'  # 中国神华股票代码
STOCK_NAME = '中国神华'

# 网格参数
CENTER_PRICE = 45         # 中心价格
GRID_SHARES = 1000       # 每次交易股数

# 买入网格价位列表（从高到低，价格下穿时触发买入）
BUY_GRID_PRICES = [44, 43, 42, 41]

# 卖出网格价位列表（从低到高，价格上穿时触发卖出）
SELL_GRID_PRICES = [46, 47, 48, 49]

# 网格范围（用于绘图显示）
LOWER_LIMIT = 41         # 最低买入价格
UPPER_LIMIT = 49         # 最高卖出价格

# 初始持仓
INIT_SHARES = 0           # 初始持股数量（无持仓）
INIT_CASH = 1000000.0     # 初始现金（100万）

# 回测区间
START_DATE = '2025-01-01'  # 回测开始日期
END_DATE = '2025-12-31'    # 回测结束日期

# 交易成本
COMMISSION_RATE = 0.0003   # 手续费率（万分之三，买入和卖出都收取）

# 数据文件路径
DATA_FILE = os.path.join(os.getcwd(), 'data', '002714_SZ_daily.csv')


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
        print("请先运行 1-qmt_download_data.py 下载数据")
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


def calculate_max_drawdown(nav_series):
    """
    计算最大回撤
    参数:
        nav_series: 净值序列（pandas Series）
    返回:
        max_drawdown: 最大回撤值（负数）
    """
    running_max = nav_series.cummax()
    drawdown = (nav_series / running_max) - 1.0
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
    total_return = (nav_series.iloc[-1] / nav_series.iloc[0]) - 1.0
    return total_return


class GridStrategy:
    """
    网格交易策略类
    
    核心逻辑（正确的网格交易）：
    - 每个买入网格触发后，必须有对应的卖出才能恢复
    - 使用"持仓层级"概念：每买入一次层级+1，每卖出一次层级-1
    - 当前层级决定了哪些网格可以触发
    
    例如：买入网格[1450, 1400, 1350, 1300]，卖出网格[1550, 1600, 1650, 1700]
    - 层级0：可触发1450买入，不能卖出（无持仓）
    - 层级1：可触发1400买入，可触发1550卖出
    - 层级2：可触发1350买入，可触发1600卖出
    - 层级3：可触发1300买入，可触发1650卖出
    - 层级4：不能再买，可触发1700卖出
    """
    def __init__(self, center_price, grid_shares, 
                 buy_grid_prices, sell_grid_prices,
                 init_cash, init_shares, commission_rate):
        """
        初始化网格策略
        参数:
            center_price: 中心价格
            grid_shares: 每次交易股数
            buy_grid_prices: 买入网格价格列表（从高到低排列）
            sell_grid_prices: 卖出网格价格列表（从低到高排列）
            init_cash: 初始现金
            init_shares: 初始持股
            commission_rate: 手续费率
        """
        self.center_price = center_price
        self.grid_shares = grid_shares
        self.commission_rate = commission_rate
        
        # 账户状态
        self.cash = init_cash
        self.shares = init_shares
        
        # 确保买入网格从高到低排列，卖出网格从低到高排列
        self.buy_grid_prices = sorted(buy_grid_prices, reverse=True)   # [1450, 1400, 1350, 1300]
        self.sell_grid_prices = sorted(sell_grid_prices)               # [1550, 1600, 1650, 1700]
        
        # 当前持仓层级（决定可以触发哪个网格）
        # 层级0表示空仓，层级n表示已经触发了n个买入网格
        self.position_level = init_shares // grid_shares if grid_shares > 0 else 0
        
        # 记录上一次的价格位置（用于判断穿越方向）
        self.last_price = None
        
        # 交易记录
        self.trades = []
        
        print(f"\n网格策略初始化完成：")
        print(f"  中心价格：{center_price}")
        print(f"  每次交易：{grid_shares}股")
        print(f"  买入网格：{self.buy_grid_prices}（从高到低）")
        print(f"  卖出网格：{self.sell_grid_prices}（从低到高）")
        print(f"  初始现金：{init_cash:,.2f}元")
        print(f"  初始持股：{init_shares}股")
        print(f"  初始层级：{self.position_level}")
    
    def get_nav(self, current_price):
        """计算当前净值"""
        return self.cash + self.shares * current_price
    
    def execute(self, date, current_price, prev_price=None):
        """
        执行网格交易逻辑（正确版本）
        
        核心原理：使用"层级"概念控制网格触发
        - 层级n时，只能触发第n+1个买入网格（更低价格）
        - 层级n时，只能触发第n个卖出网格（更高价格）
        - 买入后层级+1，卖出后层级-1
        
        这样确保：必须先卖出，对应的买入网格才能再次触发
        
        参数:
            date: 当前日期
            current_price: 当前价格
            prev_price: 前一日价格（用于判断穿越方向）
        返回:
            trade: 交易记录，如果没有交易则返回None
        """
        if prev_price is None:
            return None
        
        trade = None
        
        # ========== 检查买入网格 ==========
        # 当前层级决定可以触发哪个买入网格
        # 层级0 → 可触发buy_grid_prices[0]（最高的买入网格，如1450）
        # 层级1 → 可触发buy_grid_prices[1]（如1400）
        # 层级n → 可触发buy_grid_prices[n]
        
        if self.position_level < len(self.buy_grid_prices):
            target_buy_price = self.buy_grid_prices[self.position_level]
            
            # 价格从上向下穿越目标买入网格
            if prev_price > target_buy_price >= current_price:
                # 成交价使用网格价格（限价单逻辑，适用于流动性好的股票如茅台）
                exec_price = target_buy_price
                
                # 检查现金是否足够
                buy_amount = self.grid_shares * exec_price
                commission = buy_amount * self.commission_rate
                total_cost = buy_amount + commission
                
                if self.cash >= total_cost:
                    # 执行买入
                    self.cash -= total_cost
                    self.shares += self.grid_shares
                    self.position_level += 1  # 层级+1
                    
                    trade = {
                        'date': date,
                        'action': '买入',
                        'grid_price': target_buy_price,
                        'exec_price': exec_price,
                        'shares': self.grid_shares,
                        'amount': buy_amount,
                        'commission': commission,
                        'cash': self.cash,
                        'total_shares': self.shares,
                        'position_level': self.position_level,
                        'nav': self.get_nav(current_price)
                    }
                    self.trades.append(trade)
        
        # ========== 检查卖出网格 ==========
        # 当前层级决定可以触发哪个卖出网格
        # 层级1 → 可触发sell_grid_prices[0]（最低的卖出网格，如1550）
        # 层级2 → 可触发sell_grid_prices[1]（如1600）
        # 层级n → 可触发sell_grid_prices[n-1]
        
        if trade is None and self.position_level > 0:
            sell_index = self.position_level - 1
            if sell_index < len(self.sell_grid_prices):
                target_sell_price = self.sell_grid_prices[sell_index]
                
                # 价格从下向上穿越目标卖出网格
                if prev_price < target_sell_price <= current_price:
                    # 检查股票是否足够
                    if self.shares >= self.grid_shares:
                        # 成交价使用网格价格（限价单逻辑，适用于流动性好的股票如茅台）
                        exec_price = target_sell_price
                        
                        # 执行卖出
                        sell_amount = self.grid_shares * exec_price
                        commission = sell_amount * self.commission_rate
                        net_proceeds = sell_amount - commission
                        
                        self.cash += net_proceeds
                        self.shares -= self.grid_shares
                        self.position_level -= 1  # 层级-1
                        
                        trade = {
                            'date': date,
                            'action': '卖出',
                            'grid_price': target_sell_price,
                            'exec_price': exec_price,
                            'shares': self.grid_shares,
                            'amount': sell_amount,
                            'commission': commission,
                            'cash': self.cash,
                            'total_shares': self.shares,
                            'position_level': self.position_level,
                            'nav': self.get_nav(current_price)
                        }
                        self.trades.append(trade)
        
        return trade


def plot_grid_strategy_results(date_index, close_prices, nav_series, 
                                trades, stock_name, stock_code,
                                center_price, buy_grid_prices, sell_grid_prices,
                                lower_limit, upper_limit):
    """
    绘制网格策略回测结果图表
    参数:
        date_index: 日期索引
        close_prices: 收盘价序列
        nav_series: 净值序列
        trades: 交易记录列表
        stock_name: 股票名称
        stock_code: 股票代码
        center_price: 中心价格
        buy_grid_prices: 买入网格价格列表
        sell_grid_prices: 卖出网格价格列表
        lower_limit: 最低价格
        upper_limit: 最高价格
    """
    # 创建图表，2行1列
    fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=True)
    fig.suptitle(f'{stock_name}({stock_code}) 网格交易策略回测 - 2025年', fontsize=16, fontweight='bold')
    
    # 提取买入和卖出点
    buy_dates = []
    buy_prices = []
    sell_dates = []
    sell_prices = []
    
    for trade in trades:
        if trade['action'] == '买入':
            buy_dates.append(trade['date'])
            buy_prices.append(trade['exec_price'])
        elif trade['action'] == '卖出':
            sell_dates.append(trade['date'])
            sell_prices.append(trade['exec_price'])
    
    # ===== 子图1：股价走势、网格线和买卖点 =====
    ax1 = axes[0]
    ax1.plot(date_index, close_prices, 'b-', linewidth=1.5, label='收盘价')
    
    # 绘制网格线
    # 中心线
    ax1.axhline(y=center_price, color='gray', linestyle='-', linewidth=2, alpha=0.8, label=f'中心线 {center_price}')
    
    # 买入网格线（绿色虚线）
    for price in buy_grid_prices:
        ax1.axhline(y=price, color='green', linestyle='--', linewidth=1, alpha=0.6)
        ax1.text(date_index[0], price, f' 买入 {int(price)}', va='center', fontsize=9, color='green')
    
    # 卖出网格线（红色虚线）
    for price in sell_grid_prices:
        ax1.axhline(y=price, color='red', linestyle='--', linewidth=1, alpha=0.6)
        ax1.text(date_index[0], price, f' 卖出 {int(price)}', va='center', fontsize=9, color='red')
    
    # 标记买入点（绿色向上三角形）
    if buy_dates:
        ax1.scatter(buy_dates, buy_prices, marker='^', color='green', s=120, 
                   zorder=5, label='买入点', edgecolors='darkgreen', linewidths=1)
    
    # 标记卖出点（红色向下三角形）
    if sell_dates:
        ax1.scatter(sell_dates, sell_prices, marker='v', color='red', s=120, 
                   zorder=5, label='卖出点', edgecolors='darkred', linewidths=1)
    
    ax1.set_ylabel('股价 (元)', fontsize=12)
    ax1.legend(loc='upper right', fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.set_title('股价走势与网格交易点', fontsize=12)
    
    # 设置Y轴范围
    price_min = min(min(close_prices), lower_limit) * 0.95
    price_max = max(max(close_prices), upper_limit) * 1.05
    ax1.set_ylim(price_min, price_max)
    
    # ===== 子图2：资金曲线 =====
    ax2 = axes[1]
    
    # 绘制资金曲线（净值）
    nav_values = nav_series.values / 10000  # 转换为万元
    init_nav = nav_series.iloc[0] / 10000
    ax2.plot(date_index, nav_values, 'purple', linewidth=1.5, label='资金曲线')
    ax2.axhline(y=init_nav, color='gray', linestyle='--', linewidth=1, alpha=0.7, label='初始资金')
    
    # 填充盈利区域（浅绿色）和亏损区域（浅红色）
    ax2.fill_between(date_index, nav_values, init_nav, 
                     where=(nav_values >= init_nav), color='lightgreen', alpha=0.3)
    ax2.fill_between(date_index, nav_values, init_nav, 
                     where=(nav_values < init_nav), color='lightcoral', alpha=0.3)
    
    # 标记交易点
    for trade in trades:
        trade_date = trade['date']
        if trade_date in date_index:
            idx = date_index.get_loc(trade_date)
            nav_val = nav_values[idx]
            color = 'green' if trade['action'] == '买入' else 'red'
            marker = '^' if trade['action'] == '买入' else 'v'
            ax2.scatter([trade_date], [nav_val], marker=marker, color=color, s=60, zorder=5)
    
    ax2.set_ylabel('资金 (万元)', fontsize=12)
    ax2.set_xlabel('日期', fontsize=12)
    ax2.legend(loc='upper left', fontsize=10)
    ax2.grid(True, alpha=0.3)
    ax2.set_title('资金曲线', fontsize=12)
    
    # 添加最终资金标注
    final_nav = nav_values[-1]
    final_return = (nav_series.iloc[-1] / nav_series.iloc[0] - 1) * 100
    ax2.annotate(f'最终资金: {final_nav:.2f}万 ({final_return:+.2f}%)', 
                 xy=(date_index[-1], final_nav),
                 xytext=(-150, 20), textcoords='offset points',
                 fontsize=11, fontweight='bold',
                 arrowprops=dict(arrowstyle='->', color='purple'),
                 color='purple')
    
    # 格式化X轴日期
    ax2.xaxis.set_major_locator(mdates.MonthLocator())
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.xticks(rotation=45)
    
    # 调整布局
    plt.tight_layout()
    plt.subplots_adjust(top=0.93)
    
    # 保存图表
    output_dir = os.path.join(os.getcwd(), 'outputs')
    os.makedirs(output_dir, exist_ok=True)
    chart_file = os.path.join(output_dir, 'grid_strategy_2025_chart.png')
    plt.savefig(chart_file, dpi=150, bbox_inches='tight', facecolor='white')
    print(f"\n策略图表已保存至：{chart_file}")
    
    # 显示图表
    plt.show()
    
    return chart_file


def grid_strategy_backtest(data_file=None):
    """
    网格策略回测主函数
    """
    if data_file is None:
        data_file = DATA_FILE
    
    print("=" * 70)
    print("网格交易策略回测")
    print("=" * 70)
    print(f"股票：{STOCK_NAME}({STOCK_CODE})")
    print(f"回测区间：{START_DATE} 至 {END_DATE}")
    print(f"数据文件：{data_file}")
    print("-" * 70)
    
    try:
        # 步骤1：加载数据
        print("\n步骤1：加载历史数据")
        df = load_stock_data(data_file)
        
        if df is None:
            return None
        
        print(f"成功加载 {len(df)} 条历史数据")
        print(f"数据日期范围：{df['date'].iloc[0].strftime('%Y-%m-%d')} 至 {df['date'].iloc[-1].strftime('%Y-%m-%d')}")
        
        # 过滤回测区间的数据
        start_date = pd.Timestamp(START_DATE)
        end_date = pd.Timestamp(END_DATE)
        mask = (df['date'] >= start_date) & (df['date'] <= end_date)
        df_backtest = df[mask].reset_index(drop=True)
        
        if len(df_backtest) == 0:
            print(f"回测区间 {START_DATE} 至 {END_DATE} 内没有交易日数据")
            return None
        
        print(f"回测区间内共有 {len(df_backtest)} 个交易日")
        
        # 步骤2：初始化网格策略
        print("\n步骤2：初始化网格策略")
        strategy = GridStrategy(
            center_price=CENTER_PRICE,
            grid_shares=GRID_SHARES,
            buy_grid_prices=BUY_GRID_PRICES,
            sell_grid_prices=SELL_GRID_PRICES,
            init_cash=INIT_CASH,
            init_shares=INIT_SHARES,
            commission_rate=COMMISSION_RATE
        )
        
        # 步骤3：执行回测
        print("\n步骤3：执行回测")
        close_prices = df_backtest['close'].values
        dates = df_backtest['date'].values
        
        # 净值序列
        nav_list = []
        
        # 首日净值（使用首日收盘价计算）
        first_price = close_prices[0]
        nav_list.append(strategy.get_nav(first_price))
        
        # 逐日回测
        for i in range(1, len(close_prices)):
            current_price = close_prices[i]
            prev_price = close_prices[i-1]
            current_date = pd.Timestamp(dates[i])
            
            # 执行网格交易
            trade = strategy.execute(current_date, current_price, prev_price)
            
            # 记录当日净值
            nav_list.append(strategy.get_nav(current_price))
        
        # 创建净值序列
        date_index = pd.DatetimeIndex(dates)
        nav_series = pd.Series(nav_list, index=date_index)
        
        # 计算收益指标
        total_return = calculate_total_return(nav_series)
        max_drawdown = calculate_max_drawdown(nav_series)
        
        # 计算交易统计
        trades = strategy.trades
        buy_trades = [t for t in trades if t['action'] == '买入']
        sell_trades = [t for t in trades if t['action'] == '卖出']
        total_commission = sum(t['commission'] for t in trades)
        
        # 计算买卖差价收益
        if buy_trades and sell_trades:
            avg_buy_price = sum(t['exec_price'] for t in buy_trades) / len(buy_trades)
            avg_sell_price = sum(t['exec_price'] for t in sell_trades) / len(sell_trades)
        else:
            avg_buy_price = 0
            avg_sell_price = 0
        
        # 输出结果
        print("\n" + "=" * 70)
        print("回测结果")
        print("=" * 70)
        print(f"股票代码：{STOCK_CODE} ({STOCK_NAME})")
        print(f"回测区间：{START_DATE} 至 {END_DATE}")
        print(f"网格参数：中心{CENTER_PRICE}，每次{GRID_SHARES}股")
        print(f"买入网格：{BUY_GRID_PRICES}")
        print(f"卖出网格：{SELL_GRID_PRICES}")
        print("-" * 70)
        print(f"初始资金：{nav_series.iloc[0]:,.2f} 元")
        print(f"期末资金：{nav_series.iloc[-1]:,.2f} 元")
        print(f"总收益率：{total_return:.4%}")
        print(f"最大回撤：{max_drawdown:.4%}")
        print("-" * 70)
        print(f"总交易次数：{len(trades)} 次")
        print(f"  买入次数：{len(buy_trades)} 次")
        print(f"  卖出次数：{len(sell_trades)} 次")
        print(f"累计手续费：{total_commission:,.2f} 元")
        if avg_buy_price > 0:
            print(f"平均买入价：{avg_buy_price:.2f} 元")
        if avg_sell_price > 0:
            print(f"平均卖出价：{avg_sell_price:.2f} 元")
        print("-" * 70)
        print(f"期末持股：{strategy.shares} 股")
        print(f"期末现金：{strategy.cash:,.2f} 元")
        print("=" * 70)
        
        # 显示交易记录
        if len(trades) > 0:
            print("\n交易记录：")
            print("-" * 110)
            print(f"{'日期':<12} {'操作':<6} {'网格价':<10} {'成交价':<10} {'股数':<8} {'金额':<14} {'手续费':<10} {'持股':<8} {'层级':<6} {'净值':<14}")
            print("-" * 110)
            for trade in trades:
                level = trade.get('position_level', '-')
                print(f"{trade['date'].strftime('%Y-%m-%d'):<12} "
                      f"{trade['action']:<6} "
                      f"{trade['grid_price']:<10.2f} "
                      f"{trade['exec_price']:<10.2f} "
                      f"{trade['shares']:<8} "
                      f"{trade['amount']:<14,.2f} "
                      f"{trade['commission']:<10,.2f} "
                      f"{trade['total_shares']:<8} "
                      f"{str(level):<6} "
                      f"{trade['nav']:<14,.2f}")
            print("-" * 110)
        else:
            print("\n交易记录：无交易（价格未触及任何网格线）")
        
        # 保存结果
        output_dir = os.path.join(os.getcwd(), 'outputs')
        os.makedirs(output_dir, exist_ok=True)
        
        # 保存净值曲线
        nav_df = pd.DataFrame({
            'date': nav_series.index,
            'nav': nav_series.values,
            'return': (nav_series / nav_series.iloc[0]) - 1
        })
        nav_file = os.path.join(output_dir, 'grid_strategy_2025_nav.csv')
        nav_df.to_csv(nav_file, index=False, encoding='utf-8-sig')
        print(f"\n净值曲线已保存至：{nav_file}")
        
        # 保存交易记录
        if len(trades) > 0:
            trades_df = pd.DataFrame(trades)
            trades_file = os.path.join(output_dir, 'grid_strategy_2025_trades.csv')
            trades_df.to_csv(trades_file, index=False, encoding='utf-8-sig')
            print(f"交易记录已保存至：{trades_file}")
        
        # 保存汇总报告
        summary_file = os.path.join(output_dir, 'grid_strategy_2025_summary.txt')
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("网格交易策略回测报告\n")
            f.write("=" * 60 + "\n")
            f.write(f"股票代码：{STOCK_CODE} ({STOCK_NAME})\n")
            f.write(f"回测区间：{START_DATE} 至 {END_DATE}\n")
            f.write(f"网格参数：中心{CENTER_PRICE}，每次{GRID_SHARES}股\n")
            f.write(f"买入网格：{BUY_GRID_PRICES}\n")
            f.write(f"卖出网格：{SELL_GRID_PRICES}\n")
            f.write("-" * 60 + "\n")
            f.write(f"初始资金：{nav_series.iloc[0]:,.2f} 元\n")
            f.write(f"期末资金：{nav_series.iloc[-1]:,.2f} 元\n")
            f.write(f"总收益率：{total_return:.4%}\n")
            f.write(f"最大回撤：{max_drawdown:.4%}\n")
            f.write("-" * 60 + "\n")
            f.write(f"总交易次数：{len(trades)} 次\n")
            f.write(f"  买入次数：{len(buy_trades)} 次\n")
            f.write(f"  卖出次数：{len(sell_trades)} 次\n")
            f.write(f"累计手续费：{total_commission:,.2f} 元\n")
            f.write("-" * 60 + "\n")
            f.write(f"期末持股：{strategy.shares} 股\n")
            f.write(f"期末现金：{strategy.cash:,.2f} 元\n")
        print(f"汇总报告已保存至：{summary_file}")
        
        # 绘制策略图表
        print("\n步骤4：绘制策略图表")
        plot_grid_strategy_results(
            date_index=date_index,
            close_prices=close_prices,
            nav_series=nav_series,
            trades=trades,
            stock_name=STOCK_NAME,
            stock_code=STOCK_CODE,
            center_price=CENTER_PRICE,
            buy_grid_prices=BUY_GRID_PRICES,
            sell_grid_prices=SELL_GRID_PRICES,
            lower_limit=LOWER_LIMIT,
            upper_limit=UPPER_LIMIT
        )
        
        return {
            'total_return': total_return,
            'max_drawdown': max_drawdown,
            'init_nav': nav_series.iloc[0],
            'final_nav': nav_series.iloc[-1],
            'trades_count': len(trades),
            'buy_count': len(buy_trades),
            'sell_count': len(sell_trades),
            'total_commission': total_commission,
            'final_shares': strategy.shares,
            'final_cash': strategy.cash,
            'trades': trades
        }
        
    except Exception as e:
        print(f"回测过程中发生错误：{e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    # 运行回测
    result = grid_strategy_backtest()
    
    if result:
        print("\n" + "=" * 70)
        print("回测完成!")
        print("=" * 70)
        print(f"\n关键指标：")
        print(f"  总收益率：{result['total_return']:.4%}")
        print(f"  最大回撤：{result['max_drawdown']:.4%}")
        print(f"  交易次数：{result['trades_count']} 次（买{result['buy_count']}，卖{result['sell_count']}）")
        print(f"  累计手续费：{result['total_commission']:,.2f} 元")
    else:
        print("\n回测失败，请检查错误信息。")
