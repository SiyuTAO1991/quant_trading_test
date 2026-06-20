# -*- coding: utf-8 -*-
"""
策略进化引擎

功能:
  1. Individual       - 策略参数个体(基因编码)
  2. Population       - 种群管理
  3. evolve()         - 遗传算法进化主循环
  4. pareto_front()   - Pareto前沿(多目标优化)
  5. run_backtest_fitness() - 适应度评估(包装Backtrader回测)

遗传算法流程:
  初始化种群 -> 评估适应度 -> 选择(锦标赛) -> 交叉 -> 变异 -> 精英保留 -> 下一代
"""
import numpy as np
import random
import copy
import backtrader as bt
from db_config import INITIAL_CASH, COMMISSION


# ============================================================
# 个体: 策略参数的基因编码
# ============================================================

class Individual:
    """
    策略参数个体

    属性:
        genes: dict, 参数名 -> 参数值
        fitness: dict, 适应度指标 (如 {'sharpe': 1.5, 'max_dd': 0.15})
        param_space: dict, 参数搜索空间 (参数名 -> (min, max, type))
    """

    def __init__(self, param_space, genes=None):
        self.param_space = param_space
        if genes is not None:
            self.genes = genes.copy()
        else:
            self.genes = self._random_init()
        self.fitness = {}

    def _random_init(self):
        """随机初始化基因"""
        genes = {}
        for name, (lo, hi, ptype) in self.param_space.items():
            if ptype == 'int':
                genes[name] = random.randint(int(lo), int(hi))
            elif ptype == 'float':
                genes[name] = random.uniform(lo, hi)
            else:
                genes[name] = random.uniform(lo, hi)
        return genes

    def mutate(self, mutation_rate=0.2, mutation_strength=0.3):
        """
        基因变异

        参数:
            mutation_rate: 每个基因的变异概率
            mutation_strength: 变异幅度(相对于搜索范围的比例)
        """
        for name, (lo, hi, ptype) in self.param_space.items():
            if random.random() < mutation_rate:
                range_size = hi - lo
                delta = random.gauss(0, mutation_strength * range_size)
                new_val = self.genes[name] + delta
                new_val = max(lo, min(hi, new_val))
                if ptype == 'int':
                    new_val = int(round(new_val))
                self.genes[name] = new_val

    def __repr__(self):
        fitness_str = ', '.join(f'{k}={v:.4f}' for k, v in self.fitness.items())
        params_str = ', '.join(f'{k}={v}' for k, v in self.genes.items())
        return f"Individual({params_str} | {fitness_str})"


def crossover(parent1, parent2):
    """
    均匀交叉: 每个基因随机从父方或母方继承

    返回: 两个子代Individual
    """
    child1_genes = {}
    child2_genes = {}
    for name in parent1.param_space:
        if random.random() < 0.5:
            child1_genes[name] = parent1.genes[name]
            child2_genes[name] = parent2.genes[name]
        else:
            child1_genes[name] = parent2.genes[name]
            child2_genes[name] = parent1.genes[name]
    return (Individual(parent1.param_space, child1_genes),
            Individual(parent1.param_space, child2_genes))


def tournament_select(population, k=3):
    """锦标赛选择: 随机取k个个体, 返回适应度最高的"""
    candidates = random.sample(population, min(k, len(population)))
    return max(candidates, key=lambda ind: ind.fitness.get('sharpe', -999))


# ============================================================
# 种群与进化
# ============================================================

class Population:
    """种群管理"""

    def __init__(self, param_space, size=30):
        self.param_space = param_space
        self.individuals = [Individual(param_space) for _ in range(size)]
        self.generation = 0
        self.history = []

    def evaluate(self, fitness_fn):
        """评估所有个体的适应度"""
        for ind in self.individuals:
            if not ind.fitness:
                ind.fitness = fitness_fn(ind.genes)

    def evolve_one_generation(self, fitness_fn, elite_count=3,
                               mutation_rate=0.2, mutation_strength=0.3):
        """
        进化一代

        步骤: 精英保留 -> 锦标赛选择 -> 交叉 -> 变异 -> 评估
        """
        self.evaluate(fitness_fn)

        sorted_pop = sorted(self.individuals,
                            key=lambda ind: ind.fitness.get('sharpe', -999),
                            reverse=True)

        best = sorted_pop[0]
        avg_sharpe = np.mean([ind.fitness.get('sharpe', 0) for ind in self.individuals])
        self.history.append({
            'generation': self.generation,
            'best_sharpe': best.fitness.get('sharpe', 0),
            'avg_sharpe': avg_sharpe,
            'best_genes': best.genes.copy(),
        })

        elites = [copy.deepcopy(ind) for ind in sorted_pop[:elite_count]]

        new_pop = list(elites)
        target_size = len(self.individuals)

        while len(new_pop) < target_size:
            p1 = tournament_select(sorted_pop)
            p2 = tournament_select(sorted_pop)
            c1, c2 = crossover(p1, p2)
            c1.mutate(mutation_rate, mutation_strength)
            c2.mutate(mutation_rate, mutation_strength)
            new_pop.extend([c1, c2])

        self.individuals = new_pop[:target_size]

        for ind in self.individuals[elite_count:]:
            ind.fitness = fitness_fn(ind.genes)

        self.generation += 1

    def best(self):
        """返回当前最优个体"""
        return max(self.individuals,
                   key=lambda ind: ind.fitness.get('sharpe', -999))


def evolve(param_space, fitness_fn, pop_size=30, generations=50,
           elite_count=3, mutation_rate=0.2, verbose=True):
    """
    遗传算法进化主循环

    参数:
        param_space: dict, 参数搜索空间
        fitness_fn: callable, 输入genes字典 -> 返回fitness字典
        pop_size: 种群大小
        generations: 进化代数
        elite_count: 精英保留数量
        verbose: 是否打印进度

    返回:
        Population 对象(含进化历史)
    """
    pop = Population(param_space, pop_size)

    for gen in range(generations):
        pop.evolve_one_generation(fitness_fn, elite_count, mutation_rate)
        if verbose:
            best = pop.best()
            print(f"  第{gen+1}代 | 最优夏普: {best.fitness.get('sharpe', 0):.4f} | "
                  f"平均夏普: {pop.history[-1]['avg_sharpe']:.4f}")

    return pop


# ============================================================
# Pareto前沿 (多目标优化)
# ============================================================

def dominates(ind1, ind2, objectives):
    """
    判断ind1是否Pareto支配ind2

    支配条件: ind1在所有目标上不劣于ind2, 且在至少一个目标上严格优于ind2
    """
    better_or_equal = True
    strictly_better = False

    for obj_name, direction in objectives:
        v1 = ind1.fitness.get(obj_name, 0)
        v2 = ind2.fitness.get(obj_name, 0)

        if direction == 'max':
            if v1 < v2:
                better_or_equal = False
            if v1 > v2:
                strictly_better = True
        else:
            if v1 > v2:
                better_or_equal = False
            if v1 < v2:
                strictly_better = True

    return better_or_equal and strictly_better


def pareto_front(population_list, objectives):
    """
    计算Pareto前沿

    参数:
        population_list: Individual列表
        objectives: [(目标名, 方向), ...], 如 [('annual_return', 'max'), ('max_dd', 'min')]

    返回:
        Pareto前沿上的Individual列表
    """
    front = []
    for ind in population_list:
        is_dominated = False
        for other in population_list:
            if other is ind:
                continue
            if dominates(other, ind, objectives):
                is_dominated = True
                break
        if not is_dominated:
            front.append(ind)
    return front


# ============================================================
# Backtrader适应度评估
# ============================================================

def run_backtest_fitness(strategy_class, params, df,
                         initial_cash=None, commission=None):
    """
    运行Backtrader回测并返回适应度指标

    参数:
        strategy_class: Backtrader策略类
        params: dict, 策略参数
        df: DataFrame, OHLCV数据
        initial_cash: 初始资金
        commission: 手续费率

    返回:
        dict, 适应度指标 {sharpe, annual_return, max_dd, total_trades, ...}
    """
    if initial_cash is None:
        initial_cash = INITIAL_CASH
    if commission is None:
        commission = COMMISSION

    try:
        cerebro = bt.Cerebro()
        cerebro.addstrategy(strategy_class, **params)
        cerebro.adddata(bt.feeds.PandasData(dataname=df))
        cerebro.broker.setcash(initial_cash)
        cerebro.broker.setcommission(commission=commission)
        cerebro.addsizer(bt.sizers.PercentSizer, percents=95)
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.02)
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')

        results = cerebro.run()
        strat = results[0]

        final_value = cerebro.broker.getvalue()
        total_return = (final_value - initial_cash) / initial_cash

        trading_days = len(df)
        years = trading_days / 252
        if years > 0 and total_return > -1:
            annual_return = (1 + total_return) ** (1 / years) - 1
        else:
            annual_return = total_return

        sharpe = strat.analyzers.sharpe.get_analysis().get('sharperatio', 0) or 0

        dd = strat.analyzers.drawdown.get_analysis()
        max_dd = dd.get('max', {}).get('drawdown', 0) / 100

        ta = strat.analyzers.trades.get_analysis()
        total_trades = ta.get('total', {}).get('total', 0)
        won = ta.get('won', {}).get('total', 0)
        win_rate = won / total_trades if total_trades > 0 else 0

        return {
            'sharpe': sharpe,
            'annual_return': annual_return,
            'max_dd': max_dd,
            'total_return': total_return,
            'total_trades': total_trades,
            'win_rate': win_rate,
        }

    except Exception as e:
        return {
            'sharpe': -999,
            'annual_return': -1,
            'max_dd': 1.0,
            'total_return': -1,
            'total_trades': 0,
            'win_rate': 0,
        }
