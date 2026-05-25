# -*- coding: utf-8 -*-
"""
多因子引擎

提供因子计算、因子打分、股票池筛选的核心模块

功能:
  1. calc_all_factors()   - 对单只股票计算全部技术因子
  2. score_stocks()       - 对一组股票计算综合因子得分
  3. select_top_stocks()  - 按得分排名选出Top-N股票
  4. 因子说明与权重配置
"""
import numpy as np
import pandas as pd
import talib


# ============================================================
# 因子定义与权重
# ============================================================

FACTOR_CONFIG = {
    'momentum_20d': {
        'name': '20日动量',
        'direction': 1,    # 正向: 值越大越好
        'weight': 0.20,
        'desc': 'ROC(20), 反映短期价格趋势',
    },
    'momentum_60d': {
        'name': '60日动量',
        'direction': 1,
        'weight': 0.15,
        'desc': 'ROC(60), 反映中期价格趋势',
    },
    'volatility': {
        'name': '波动率',
        'direction': -1,   # 反向: 值越小越好 (低波动优先)
        'weight': 0.15,
        'desc': 'ATR(14)/Close, 归一化波动率',
    },
    'rsi_14': {
        'name': 'RSI(14)',
        'direction': -1,   # 反向: RSI越低越可能反弹
        'weight': 0.10,
        'desc': 'RSI(14), 超卖区间更优',
    },
    'adx_14': {
        'name': 'ADX(14)',
        'direction': 1,    # 正向: 趋势越强越好 (用于趋势策略)
        'weight': 0.10,
        'desc': 'ADX(14), 趋势强度指标',
    },
    'turnover_ratio': {
        'name': '换手率指标',
        'direction': 1,
        'weight': 0.10,
        'desc': '当日量/20日均量, 量能放大信号',
    },
    'price_position': {
        'name': '价格位置',
        'direction': -1,   # 反向: 价格在区间越低越好 (捡便宜)
        'weight': 0.10,
        'desc': '当前价在60日区间内的位置 (0~1)',
    },
    'macd_signal': {
        'name': 'MACD信号',
        'direction': 1,
        'weight': 0.10,
        'desc': 'MACD柱状图 > 0 为正',
    },
}


# ============================================================
# 因子计算
# ============================================================

def calc_all_factors(df):
    """
    对单只股票的DataFrame计算全部技术因子

    参数:
        df: DataFrame, 含 open/high/low/close/volume, DatetimeIndex

    返回:
        dict {factor_name: float}
    """
    if len(df) < 60:
        return None

    h = df['high'].values.astype(np.float64)
    l = df['low'].values.astype(np.float64)
    c = df['close'].values.astype(np.float64)
    o = df['open'].values.astype(np.float64)
    v = df['volume'].values.astype(np.float64)

    if c[-1] <= 0 or np.isnan(c[-1]):
        return None

    try:
        roc_20 = talib.ROC(c, timeperiod=20)
        roc_60 = talib.ROC(c, timeperiod=60)
        atr = talib.ATR(h, l, c, timeperiod=14)
        rsi = talib.RSI(c, timeperiod=14)
        adx = talib.ADX(h, l, c, timeperiod=14)
        vol_ma = talib.SMA(v, timeperiod=20)
        macd_line, macd_signal, macd_hist = talib.MACD(c)

        # 60日最高/最低价
        high_60 = np.nanmax(h[-60:])
        low_60 = np.nanmin(l[-60:])
        price_range = high_60 - low_60

        vol_ma_val = vol_ma[-1] if not np.isnan(vol_ma[-1]) and vol_ma[-1] > 0 else 1

        factors = {
            'momentum_20d': float(roc_20[-1]) if not np.isnan(roc_20[-1]) else 0,
            'momentum_60d': float(roc_60[-1]) if not np.isnan(roc_60[-1]) else 0,
            'volatility': float(atr[-1] / c[-1]) if not np.isnan(atr[-1]) and c[-1] > 0 else 0,
            'rsi_14': float(rsi[-1]) if not np.isnan(rsi[-1]) else 50,
            'adx_14': float(adx[-1]) if not np.isnan(adx[-1]) else 0,
            'turnover_ratio': float(v[-1] / vol_ma_val) if vol_ma_val > 0 else 1,
            'price_position': float((c[-1] - low_60) / price_range) if price_range > 0 else 0.5,
            'macd_signal': float(macd_hist[-1]) if not np.isnan(macd_hist[-1]) else 0,
            'close': float(c[-1]),
        }
        return factors
    except Exception:
        return None


def batch_calc_factors(all_data, calc_date=None):
    """
    批量计算所有股票的因子

    参数:
        all_data: dict {code: DataFrame}
        calc_date: 截止日期 (None=用所有数据)

    返回:
        DataFrame, 索引为stock_code, 列为因子
    """
    factor_dict = {}
    for code, df in all_data.items():
        if calc_date is not None:
            df = df[df.index <= calc_date]
        f = calc_all_factors(df)
        if f is not None:
            factor_dict[code] = f

    return pd.DataFrame(factor_dict).T


# ============================================================
# 因子打分
# ============================================================

def score_stocks(factor_df, factor_config=None):
    """
    对股票池进行多因子打分

    打分逻辑:
      1. 对每个因子进行横截面排名 (0~1)
      2. 反向因子取 1-rank
      3. 按权重加权求和

    参数:
        factor_df: DataFrame, batch_calc_factors() 的输出
        factor_config: 因子配置字典 (默认用 FACTOR_CONFIG)

    返回:
        DataFrame, 增加 'score' 列, 按得分降序
    """
    config = factor_config or FACTOR_CONFIG
    result = factor_df.copy()
    result['score'] = 0.0

    for fname, cfg in config.items():
        if fname not in result.columns:
            continue

        # 横截面排名归一化 (0~1)
        rank = result[fname].rank(pct=True)

        # 反向因子翻转
        if cfg['direction'] < 0:
            rank = 1 - rank

        result[f'{fname}_rank'] = rank
        result['score'] += rank * cfg['weight']

    # 按得分降序排列
    result = result.sort_values('score', ascending=False)
    return result


def select_top_stocks(factor_df, top_n=10, factor_config=None):
    """
    选出得分最高的 Top-N 股票

    返回:
        (scored_df, top_codes)
    """
    scored = score_stocks(factor_df, factor_config)
    top_codes = scored.head(top_n).index.tolist()
    return scored, top_codes


def print_factor_report(scored_df, top_n=10, title=''):
    """
    打印因子打分报告
    """
    if title:
        print(f"\n  {title}")

    top = scored_df.head(top_n)
    print(f"\n  {'排名':>4} {'代码':<12} {'得分':>8} {'20D动量':>10} {'波动率':>10} "
          f"{'RSI':>8} {'价格位置':>10}")
    print(f"  {'-' * 66}")

    for i, (code, row) in enumerate(top.iterrows()):
        m20 = row.get('momentum_20d', 0)
        vol = row.get('volatility', 0)
        rsi = row.get('rsi_14', 0)
        pp = row.get('price_position', 0)
        print(f"  {i+1:>4} {code:<12} {row['score']:>8.4f} {m20:>+9.2f}% "
              f"{vol:>10.4f} {rsi:>8.1f} {pp:>10.3f}")

    bottom = scored_df.tail(5)
    print(f"\n  ... 得分最低 5 只:")
    for i, (code, row) in enumerate(bottom.iterrows()):
        rank_pos = len(scored_df) - len(bottom) + i + 1
        print(f"  {rank_pos:>4} {code:<12} {row['score']:>8.4f}")
