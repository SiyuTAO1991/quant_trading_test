# -*- coding: utf-8 -*-
"""
网格交易引擎

提供两种网格引擎:
  1. GridEngine     - 固定价格区间网格 (脚本1使用)
  2. ChanGridEngine - 缠论中枢网格 (脚本2使用, 支持中枢切换)

网格交易逻辑:
  将价格区间 [lower, upper] 等分为 N 个格子
  价格每下穿一个格子 → 买入1份
  价格每上穿一个格子 → 卖出之前在该格买的1份
  每笔网格交易赚取固定差价 = grid_size

  例: 区间 [100, 110], 5格, grid_size=2
  格子: [100, 102, 104, 106, 108, 110]
  价格从106跌到102: 在104买入, 在102买入
  价格从102涨到108: 在104卖出(赚2), 在106卖出(赚2)
"""


class GridEngine:
    """
    固定网格引擎

    属性:
        upper/lower: 网格上下界
        num_grids: 格子数
        grid_size: 每格间距 = (upper - lower) / num_grids
        levels: 各格价位列表 [lower, lower+gs, ..., upper]
        position_at: 每格是否持有仓位
    """

    def __init__(self, upper, lower, num_grids, total_capital):
        """
        参数:
            upper: 网格上界
            lower: 网格下界
            num_grids: 格子数量
            total_capital: 分配给网格的总资金 (用于计算每格买入股数)
        """
        self.upper = upper
        self.lower = lower
        self.num_grids = num_grids
        self.grid_size = (upper - lower) / num_grids
        self.levels = [lower + i * self.grid_size for i in range(num_grids + 1)]
        self.total_capital = total_capital
        self.capital_per_grid = total_capital / num_grids

        # 每格持仓状态: position_at[i] 表示在第i格(价格=levels[i])买入的股数
        self.position_at = [0] * (num_grids + 1)
        self.prev_cell = None

        # 统计
        self.buy_count = 0
        self.sell_count = 0
        self.total_profit = 0.0
        self.max_layers = 0

    def get_cell(self, price):
        """
        获取价格所在的格子索引

        返回:
            -1: 低于网格下界
            num_grids: 高于网格上界
            0 ~ num_grids-1: 正常格子
        """
        if price < self.lower:
            return -1
        if price >= self.upper:
            return self.num_grids
        return int((price - self.lower) / self.grid_size)

    def calc_shares(self, price):
        """计算每格买入股数 (取整到100股)"""
        if price <= 0:
            return 0
        shares = self.capital_per_grid / price
        shares = int(shares // 100) * 100
        return max(shares, 100)

    def current_layers(self):
        """当前持仓层数"""
        return sum(1 for s in self.position_at if s > 0)

    def update(self, price):
        """
        输入当前价格, 返回交易信号列表

        返回:
            list of dict: [{'action': 'BUY'/'SELL', 'price': float,
                           'size': int, 'grid_level': int}, ...]
        """
        curr_cell = self.get_cell(price)
        signals = []

        if self.prev_cell is None:
            self.prev_cell = curr_cell
            return signals

        prev_cell = self.prev_cell

        if curr_cell < prev_cell:
            # 价格下跌, 穿越格子 → 买入
            # 从上往下遍历进入的每个格子: prev_cell-1, prev_cell-2, ..., curr_cell
            for cell in range(prev_cell - 1, curr_cell - 1, -1):
                if 0 <= cell < self.num_grids and self.position_at[cell] == 0:
                    size = self.calc_shares(self.levels[cell])
                    if size > 0:
                        signals.append({
                            'action': 'BUY',
                            'price': self.levels[cell],
                            'size': size,
                            'grid_level': cell,
                        })
                        self.position_at[cell] = size
                        self.buy_count += 1

        elif curr_cell > prev_cell:
            # 价格上涨, 穿越格子 → 卖出持仓
            # 从下往上遍历离开的每个格子: prev_cell, prev_cell+1, ..., curr_cell-1
            for cell in range(prev_cell, curr_cell):
                if 0 <= cell < self.num_grids and self.position_at[cell] > 0:
                    size = self.position_at[cell]
                    sell_price = self.levels[cell + 1]
                    profit = (sell_price - self.levels[cell]) * size
                    signals.append({
                        'action': 'SELL',
                        'price': sell_price,
                        'size': size,
                        'grid_level': cell,
                        'profit': round(profit, 2),
                    })
                    self.position_at[cell] = 0
                    self.sell_count += 1
                    self.total_profit += profit

        layers = self.current_layers()
        self.max_layers = max(self.max_layers, layers)
        self.prev_cell = curr_cell
        return signals

    def is_out_of_range(self, price):
        """价格是否超出网格范围"""
        return price < self.lower or price >= self.upper

    def get_stats(self):
        """获取网格统计信息"""
        return {
            'buy_count': self.buy_count,
            'sell_count': self.sell_count,
            'total_profit': round(self.total_profit, 2),
            'max_layers': self.max_layers,
            'current_layers': self.current_layers(),
            'grid_utilization': f"{self.sell_count}/{self.buy_count}" if self.buy_count > 0 else "0/0",
        }

    def summary(self):
        """打印网格摘要"""
        s = self.get_stats()
        print(f"  网格参数: [{self.lower:.2f} ~ {self.upper:.2f}], "
              f"{self.num_grids}格, 间距={self.grid_size:.2f}")
        print(f"  网格交易: 买入{s['buy_count']}次, 卖出{s['sell_count']}次, "
              f"利用率={s['grid_utilization']}")
        print(f"  网格利润: {s['total_profit']:.2f}, "
              f"最大持仓层数: {s['max_layers']}")


class ChanGridEngine(GridEngine):
    """
    缠论中枢网格引擎

    以缠论中枢的 ZG/ZD 作为网格边界:
      - ZG = 网格上界 (中枢上沿)
      - ZD = 网格下界 (中枢下沿)
      - 在中枢内等距切格做网格交易
      - 价格突破中枢 → 停止网格

    支持中枢切换: 旧中枢结束后, 新中枢形成时自动重建网格
    """

    def __init__(self, zg, zd, num_grids=6, total_capital=0):
        super().__init__(upper=zg, lower=zd, num_grids=num_grids,
                         total_capital=total_capital)
        self.zg = zg
        self.zd = zd
        self.active = True
        self.switch_count = 0

    def is_in_zhongshu(self, price):
        """价格是否在中枢区间内"""
        return self.zd <= price <= self.zg

    def is_breakout_up(self, price):
        """价格是否向上突破中枢"""
        return price > self.zg

    def is_breakdown(self, price):
        """价格是否向下跌破中枢"""
        return price < self.zd

    def switch_zhongshu(self, new_zg, new_zd):
        """
        切换到新中枢, 重建网格

        旧网格未平仓位不会自动清仓, 需要策略层面处理
        """
        self.zg = new_zg
        self.zd = new_zd
        self.upper = new_zg
        self.lower = new_zd
        self.grid_size = (new_zg - new_zd) / self.num_grids
        self.levels = [new_zd + i * self.grid_size for i in range(self.num_grids + 1)]
        self.position_at = [0] * (self.num_grids + 1)
        self.prev_cell = None
        self.active = True
        self.switch_count += 1

    def deactivate(self):
        """停用网格 (价格突破中枢时调用)"""
        self.active = False

    def update(self, price):
        """在活跃状态下执行网格逻辑"""
        if not self.active:
            return []
        return super().update(price)

    def summary(self):
        """打印中枢网格摘要"""
        s = self.get_stats()
        print(f"  中枢网格: ZG={self.zg:.2f}, ZD={self.zd:.2f}, "
              f"{self.num_grids}格, 间距={self.grid_size:.2f}")
        print(f"  中枢切换: {self.switch_count}次, "
              f"当前状态: {'活跃' if self.active else '停用'}")
        print(f"  网格交易: 买入{s['buy_count']}次, 卖出{s['sell_count']}次")
        print(f"  网格利润: {s['total_profit']:.2f}, "
              f"最大持仓层数: {s['max_layers']}")
