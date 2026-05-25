# -*- coding: utf-8 -*-
"""
多因子评价框架 - 因子有效性检验

核心问题:
  之前的策略都是"给什么标的用什么标的", 如何科学地从全市场选出值得交易的品种?
  因子(Factor) = 能预测未来收益的指标

本脚本:
  1. 用 TA-Lib 计算技术因子 (动量/波动率/RSI/ADX)
  2. 从 trade_stock_financial 读取基本面因子 (PE/ROE)
  3. IC (信息系数) 分析 - 因子值与未来收益的相关性
  4. 分层回测 - 按因子值分5组, 看各组收益差异

运行: python 4-多因子评价框架.py
"""
import numpy as np
import pandas as pd
import talib
import time
import os
from db_config import execute_query


# ============================================================
# 批量数据加载
# ============================================================

def batch_load_daily(start_date, end_date, min_bars=120):
    """
    批量加载全市场日K线数据

    返回:
        dict {stock_code: DataFrame}
    """
    sql = """
        SELECT stock_code, trade_date, open_price, high_price, low_price,
               close_price, volume
        FROM trade_stock_daily
        WHERE trade_date >= %s AND trade_date <= %s
        ORDER BY stock_code, trade_date ASC
    """
    rows = execute_query(sql, [start_date, end_date])
    if not rows:
        return {}

    df_all = pd.DataFrame(rows)
    df_all['trade_date'] = pd.to_datetime(df_all['trade_date'])
    for col in ['open_price', 'high_price', 'low_price', 'close_price', 'volume']:
        df_all[col] = pd.to_numeric(df_all[col], errors='coerce')

    result = {}
    for code, group in df_all.groupby('stock_code'):
        sub = group.set_index('trade_date').sort_index()
        sub = sub[['open_price', 'high_price', 'low_price', 'close_price', 'volume']]
        sub.columns = ['open', 'high', 'low', 'close', 'volume']
        if len(sub) >= min_bars:
            result[code] = sub
    return result


def load_financial_data(report_date_min=None):
    """
    加载财务数据 (PE/ROE等)

    返回:
        DataFrame, 索引为 (stock_code, report_date)
    """
    sql = """
        SELECT stock_code, report_date, eps, roe, gross_margin,
               debt_ratio, net_profit, revenue, total_assets
        FROM trade_stock_financial
        WHERE 1=1
    """
    params = []
    if report_date_min:
        sql += " AND report_date >= %s"
        params.append(report_date_min)
    sql += " ORDER BY stock_code, report_date DESC"

    rows = execute_query(sql, params)
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df['report_date'] = pd.to_datetime(df['report_date'])
    for col in ['eps', 'roe', 'gross_margin', 'debt_ratio', 'net_profit', 'revenue', 'total_assets']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


# ============================================================
# 因子计算
# ============================================================

def calc_technical_factors(all_data, calc_date_idx=-1):
    """
    用 TA-Lib 批量计算技术因子

    因子列表:
        momentum_20d: 20日动量 (ROC)
        momentum_60d: 60日动量
        volatility:   ATR/Close 归一化波动率
        rsi_14:       RSI(14)
        adx_14:       ADX(14) 趋势强度
        vol_ratio:    当日成交量/20日均量

    参数:
        all_data: dict {code: DataFrame}
        calc_date_idx: 取哪一天的因子值 (-1=最后一天)

    返回:
        DataFrame, 索引为stock_code, 列为各因子
    """
    factors = {}

    for code, df in all_data.items():
        if len(df) < 60:
            continue

        h = df['high'].values.astype(np.float64)
        l = df['low'].values.astype(np.float64)
        c = df['close'].values.astype(np.float64)
        v = df['volume'].values.astype(np.float64)

        try:
            roc_20 = talib.ROC(c, timeperiod=20)
            roc_60 = talib.ROC(c, timeperiod=60)
            atr = talib.ATR(h, l, c, timeperiod=14)
            rsi = talib.RSI(c, timeperiod=14)
            adx = talib.ADX(h, l, c, timeperiod=14)
            vol_ma = talib.SMA(v, timeperiod=20)

            idx = calc_date_idx
            if np.isnan(c[idx]) or c[idx] <= 0:
                continue

            atr_val = atr[idx] if not np.isnan(atr[idx]) else 0
            vol_ma_val = vol_ma[idx] if not np.isnan(vol_ma[idx]) and vol_ma[idx] > 0 else 1

            factors[code] = {
                'momentum_20d': float(roc_20[idx]) if not np.isnan(roc_20[idx]) else 0,
                'momentum_60d': float(roc_60[idx]) if not np.isnan(roc_60[idx]) else 0,
                'volatility': float(atr_val / c[idx]) if c[idx] > 0 else 0,
                'rsi_14': float(rsi[idx]) if not np.isnan(rsi[idx]) else 50,
                'adx_14': float(adx[idx]) if not np.isnan(adx[idx]) else 0,
                'vol_ratio': float(v[idx] / vol_ma_val),
                'close': float(c[idx]),
            }
        except Exception:
            continue

    return pd.DataFrame(factors).T


def calc_forward_returns(all_data, holding_days=20):
    """
    计算未来N日收益率 (用于评价因子有效性)

    参数:
        holding_days: 持有天数

    返回:
        dict {date: DataFrame}, 每个日期对应各股票的未来收益
    """
    date_returns = {}

    # 取所有日期的并集
    all_dates = set()
    for df in all_data.values():
        all_dates.update(df.index.tolist())
    all_dates = sorted(all_dates)

    # 每月末计算一次
    monthly_dates = []
    for i, d in enumerate(all_dates):
        if i == len(all_dates) - 1:
            continue
        if i + 1 < len(all_dates) and all_dates[i + 1].month != d.month:
            monthly_dates.append(d)

    for calc_date in monthly_dates:
        returns = {}
        for code, df in all_data.items():
            if calc_date not in df.index:
                continue
            idx = df.index.get_loc(calc_date)
            if idx + holding_days >= len(df):
                continue
            c_now = df['close'].iloc[idx]
            c_future = df['close'].iloc[idx + holding_days]
            if c_now > 0:
                returns[code] = float(c_future / c_now - 1)
        if len(returns) >= 10:
            date_returns[calc_date] = returns

    return date_returns


# ============================================================
# IC 分析
# ============================================================

def calc_ic_series(all_data, factor_name, holding_days=20):
    """
    计算单因子的月度IC序列

    IC = rank_corr(因子值, 未来收益)
    ICIR = mean(IC) / std(IC)

    返回:
        (ic_series: Series, icir: float)
    """
    from scipy.stats import spearmanr

    # 取所有日期
    all_dates = set()
    for df in all_data.values():
        all_dates.update(df.index.tolist())
    all_dates = sorted(all_dates)

    monthly_dates = []
    for i, d in enumerate(all_dates):
        if i + 1 < len(all_dates) and all_dates[i + 1].month != d.month:
            monthly_dates.append(d)

    ic_values = {}

    for calc_date in monthly_dates:
        factor_vals = {}
        return_vals = {}

        for code, df in all_data.items():
            if calc_date not in df.index:
                continue
            idx = df.index.get_loc(calc_date)
            if idx < 60 or idx + holding_days >= len(df):
                continue

            h = df['high'].values[:idx + 1].astype(np.float64)
            l = df['low'].values[:idx + 1].astype(np.float64)
            c = df['close'].values[:idx + 1].astype(np.float64)
            v = df['volume'].values[:idx + 1].astype(np.float64)

            try:
                if factor_name == 'momentum_20d':
                    vals = talib.ROC(c, timeperiod=20)
                elif factor_name == 'momentum_60d':
                    vals = talib.ROC(c, timeperiod=60)
                elif factor_name == 'volatility':
                    atr = talib.ATR(h, l, c, timeperiod=14)
                    vals = atr / c
                elif factor_name == 'rsi_14':
                    vals = talib.RSI(c, timeperiod=14)
                elif factor_name == 'adx_14':
                    vals = talib.ADX(h, l, c, timeperiod=14)
                else:
                    continue

                fv = vals[-1]
                if np.isnan(fv):
                    continue
                factor_vals[code] = float(fv)

                c_full = df['close'].values.astype(float)
                ret = c_full[idx + holding_days] / c_full[idx] - 1
                return_vals[code] = float(ret)
            except Exception:
                continue

        common = set(factor_vals.keys()) & set(return_vals.keys())
        if len(common) < 10:
            continue

        f_arr = [factor_vals[c] for c in common]
        r_arr = [return_vals[c] for c in common]
        ic, _ = spearmanr(f_arr, r_arr)
        if not np.isnan(ic):
            ic_values[calc_date] = ic

    ic_series = pd.Series(ic_values)
    icir = ic_series.mean() / ic_series.std() if len(ic_series) > 1 and ic_series.std() > 0 else 0

    return ic_series, icir


# ============================================================
# 分层回测
# ============================================================

def quintile_backtest(all_data, factor_name, num_groups=5, holding_days=20):
    """
    分层回测: 按因子值分N组, 看各组平均收益

    返回:
        DataFrame, 列为各组(Q1~Q5), 行为各期, 值为组平均收益
    """
    all_dates = set()
    for df in all_data.values():
        all_dates.update(df.index.tolist())
    all_dates = sorted(all_dates)

    monthly_dates = []
    for i, d in enumerate(all_dates):
        if i + 1 < len(all_dates) and all_dates[i + 1].month != d.month:
            monthly_dates.append(d)

    group_returns = {f'Q{i+1}': [] for i in range(num_groups)}
    dates_used = []

    for calc_date in monthly_dates:
        records = []
        for code, df in all_data.items():
            if calc_date not in df.index:
                continue
            idx = df.index.get_loc(calc_date)
            if idx < 60 or idx + holding_days >= len(df):
                continue

            c = df['close'].values[:idx + 1].astype(np.float64)
            h = df['high'].values[:idx + 1].astype(np.float64)
            l = df['low'].values[:idx + 1].astype(np.float64)

            try:
                if factor_name == 'momentum_20d':
                    fv = float(talib.ROC(c, timeperiod=20)[-1])
                elif factor_name == 'volatility':
                    atr_v = talib.ATR(h, l, c, timeperiod=14)
                    fv = float(atr_v[-1] / c[-1]) if c[-1] > 0 else 0
                elif factor_name == 'rsi_14':
                    fv = float(talib.RSI(c, timeperiod=14)[-1])
                else:
                    continue

                if np.isnan(fv):
                    continue

                c_full = df['close'].values.astype(float)
                ret = c_full[idx + holding_days] / c_full[idx] - 1
                records.append({'code': code, 'factor': fv, 'return': float(ret)})
            except Exception:
                continue

        if len(records) < num_groups * 3:
            continue

        rec_df = pd.DataFrame(records).sort_values('factor')
        group_size = len(rec_df) // num_groups

        for g in range(num_groups):
            start_idx = g * group_size
            end_idx = (g + 1) * group_size if g < num_groups - 1 else len(rec_df)
            avg_ret = rec_df.iloc[start_idx:end_idx]['return'].mean()
            group_returns[f'Q{g+1}'].append(avg_ret)

        dates_used.append(calc_date)

    return pd.DataFrame(group_returns, index=dates_used)


# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    start_date = '2024-01-01'
    end_date = '2025-12-31'

    print("=" * 70)
    print("多因子评价框架 - 因子有效性检验")
    print("=" * 70)

    # ---- 1. 加载数据 ----
    print(f"\n[1] 批量加载K线数据 ({start_date} ~ {end_date})...")
    t0 = time.time()
    all_data = batch_load_daily(start_date, end_date, min_bars=60)
    print(f"    加载完成: {len(all_data)} 只标的, 耗时 {time.time()-t0:.1f}s")

    if len(all_data) < 20:
        print("    标的数量不足, 请确保数据库中有足够的股票数据")
        exit()

    # ---- 2. 截面因子计算 ----
    print(f"\n[2] 计算技术因子 (TA-Lib)...")
    factor_df = calc_technical_factors(all_data)
    print(f"    {len(factor_df)} 只标的的因子已计算")
    print(f"\n    因子描述统计:")
    for col in ['momentum_20d', 'momentum_60d', 'volatility', 'rsi_14', 'adx_14']:
        if col in factor_df.columns:
            vals = factor_df[col].dropna()
            print(f"    {col:<16} mean={vals.mean():>8.2f}  std={vals.std():>8.2f}  "
                  f"min={vals.min():>8.2f}  max={vals.max():>8.2f}")

    # ---- 3. IC分析 ----
    print(f"\n[3] 因子IC分析 (Rank IC, 未来20日收益)...")
    factors_to_test = ['momentum_20d', 'volatility', 'rsi_14']
    ic_results = {}

    for fname in factors_to_test:
        print(f"\n    --- {fname} ---")
        ic_series, icir = calc_ic_series(all_data, fname, holding_days=20)
        ic_results[fname] = {'ic_series': ic_series, 'icir': icir}

        if len(ic_series) > 0:
            print(f"    IC均值:  {ic_series.mean():+.4f}")
            print(f"    IC标准差: {ic_series.std():.4f}")
            print(f"    ICIR:    {icir:+.4f}")
            print(f"    IC>0比例: {(ic_series > 0).mean()*100:.1f}%")
            print(f"    IC期数:  {len(ic_series)}")

            # IC判定
            abs_ic = abs(ic_series.mean())
            if abs_ic > 0.05:
                strength = "强" if abs_ic > 0.1 else "中等"
                direction = "正向" if ic_series.mean() > 0 else "反向"
                print(f"    判定: {strength}因子, {direction}")
            else:
                print(f"    判定: 因子效果较弱")
        else:
            print(f"    样本不足, 无法计算IC")

    # ---- 4. 分层回测 ----
    print(f"\n[4] 分层回测 (5分位组, 20日持有)")
    for fname in factors_to_test:
        print(f"\n    --- {fname} ---")
        qr = quintile_backtest(all_data, fname, num_groups=5, holding_days=20)
        if len(qr) > 0:
            avg_returns = qr.mean()
            print(f"    {'分组':<6} {'平均月收益':>12} {'累计收益':>12}")
            print(f"    {'-' * 34}")
            for q in ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']:
                cum = (1 + qr[q]).prod() - 1
                print(f"    {q:<6} {avg_returns[q]*100:>+11.2f}% {cum*100:>+11.2f}%")

            spread = avg_returns['Q5'] - avg_returns['Q1']
            print(f"    Q5-Q1价差: {spread*100:+.2f}%/期")
            if abs(spread) > 0.01:
                print(f"    判定: 因子有{'正向' if spread > 0 else '反向'}区分能力")
            else:
                print(f"    判定: 因子区分能力不明显")
        else:
            print(f"    样本不足")

    # ---- 5. IC排名 ----
    print(f"\n{'=' * 70}")
    print("因子有效性排名 (按|ICIR|降序)")
    print(f"{'=' * 70}")
    ranking = []
    for fname, data in ic_results.items():
        ranking.append((fname, data['icir'], data['ic_series'].mean() if len(data['ic_series']) > 0 else 0))
    ranking.sort(key=lambda x: abs(x[1]), reverse=True)

    print(f"  {'因子':<20} {'ICIR':>10} {'IC均值':>10} {'判定':>10}")
    print(f"  {'-' * 54}")
    for fname, icir, ic_mean in ranking:
        if abs(icir) > 0.5:
            grade = '优秀'
        elif abs(icir) > 0.3:
            grade = '良好'
        elif abs(icir) > 0.1:
            grade = '一般'
        else:
            grade = '较弱'
        print(f"  {fname:<20} {icir:>+10.4f} {ic_mean:>+10.4f} {grade:>10}")

    print("\n因子说明:")
    print("  IC (信息系数) = 因子值与未来收益的Spearman相关系数")
    print("  ICIR = IC均值 / IC标准差, 衡量因子有效性的稳定性")
    print("  ICIR > 0.5: 优秀因子, 可直接用于选股")
    print("  ICIR 0.3~0.5: 良好因子, 适合多因子组合")
    print("  分层回测: Q1=因子最小组, Q5=因子最大组, Q5-Q1越大越好")
