# -*- coding: utf-8 -*-
"""
因子分析: 因子分类枚举、calc_features、可选基本面、单因子 RankIC。

运行: python 1-贵州茅台因子分析.py
"""

import pandas as pd
import numpy as np
from scipy.stats import spearmanr

from data_loader import load_stock_data, load_financial_data
from feature_engine import (
    FACTOR_TAXONOMY,
    calc_features,
    calc_fundamental_features,
    get_all_feature_cols,
)

STOCK_CODES = [
    '159608.SZ', '159583.SZ', '159560.SZ', '159133.SZ', '159796.SZ',  # ETF
    '603019.SH', '002847.SZ', '603259.SH', '000981.SZ', '002714.SZ', '002115.SZ',
]
START_DATE = '2025-01-01'
END_DATE = None


def calc_rank_ic(factor_values, forward_returns):
    """单因子 RankIC: Spearman(因子, 未来收益)。有效样本少于 30 返回 nan。"""
    valid = pd.DataFrame({
        'factor': factor_values,
        'fwd_ret': forward_returns
    }).dropna()
    if len(valid) < 30:
        return np.nan
    ic, _ = spearmanr(valid['factor'], valid['fwd_ret'])
    return ic


def print_taxonomy():
    print('\n[1] FACTOR_TAXONOMY')
    total_features = 0
    for cat_key, cat_info in FACTOR_TAXONOMY.items():
        n = len(cat_info['features'])
        total_features += n
        feat_str = ', '.join(cat_info['features'][:5])
        if n > 5:
            feat_str += f' ... (共{n}个)'
        print(f"  {cat_info['name']} ({cat_key}): {n} 个 | {feat_str}")
    print(f'  技术因子合计: {total_features} | 课件中另含 4 个基本面定义')


def analyze_stock(stock_code, start_date, end_date, fin_df=None):
    end_label = end_date or '最新'
    print(f'\n{"=" * 72}')
    print(f'分析 {stock_code} | {start_date} ~ {end_label}')
    print('=' * 72)

    df = load_stock_data(stock_code, start_date, end_date)
    print(f'  交易日: {len(df)} | {df.index[0].strftime("%Y-%m-%d")} ~ {df.index[-1].strftime("%Y-%m-%d")}')
    print(f'  close: {df["close"].min():.2f} ~ {df["close"].max():.2f}')

    df = calc_features(df)
    tech_cols = get_all_feature_cols()
    available_tech = [c for c in tech_cols if c in df.columns]
    print(f'  技术因子列数: {len(available_tech)}')

    df['fwd_ret_1d'] = df['close'].pct_change(1).shift(-1)

    fundamental_cols = []
    if fin_df is not None and not fin_df.empty:
        try:
            fund_features = calc_fundamental_features(df, fin_df, stock_code)
            for col in fund_features.columns:
                df[col] = fund_features[col]
                if df[col].notna().sum() > 0:
                    fundamental_cols.append(col)
            if fundamental_cols:
                print(f'  合并基本面列: {fundamental_cols}')
            else:
                print('  无可用基本面数据')
        except Exception as e:
            print(f'  基本面合并异常: {e}')

    all_factor_cols = available_tech + fundamental_cols
    ic_results = []
    for col in all_factor_cols:
        ic_val = calc_rank_ic(df[col], df['fwd_ret_1d'])
        if np.isnan(ic_val):
            continue
        cat_name = '基本面'
        for _, cat_info in FACTOR_TAXONOMY.items():
            if col in cat_info['features']:
                cat_name = cat_info['name']
                break
        ic_results.append({
            'stock_code': stock_code,
            'factor': col,
            'category': cat_name,
            'RankIC': round(ic_val, 4),
            '|IC|': round(abs(ic_val), 4),
        })

    ic_df = pd.DataFrame(ic_results).sort_values('|IC|', ascending=False)
    ic_df = ic_df.reset_index(drop=True)
    ic_df.index = ic_df.index + 1
    ic_df.index.name = '排名'

    print(f'\n  完成检验因子数: {len(ic_df)}')
    top_n = min(10, len(ic_df))
    if top_n:
        print(f'\n  TOP {top_n} by |IC|')
        print(ic_df.head(top_n).to_string())

    if len(ic_df):
        strong = ic_df[ic_df['|IC|'] >= 0.05]
        effective = ic_df[(ic_df['|IC|'] >= 0.03) & (ic_df['|IC|'] < 0.05)]
        weak = ic_df[(ic_df['|IC|'] >= 0.02) & (ic_df['|IC|'] < 0.03)]
        ineffective = ic_df[ic_df['|IC|'] < 0.02]
        print(f'\n  分档: >=0.05={len(strong)} | [0.03,0.05)={len(effective)} | '
              f'[0.02,0.03)={len(weak)} | <0.02={len(ineffective)}')

        best = ic_df.iloc[0]
        print(f'  最强因子: {best["factor"]} ({best["category"]}) |IC|={best["|IC|"]:.4f}')

    return ic_df


if __name__ == '__main__':
    print_taxonomy()

    print('\n[2] 批量因子分析')
    print(f'  标的: {len(STOCK_CODES)} 只 | 区间: {START_DATE} ~ {END_DATE or "最新"}')

    fin_df = None
    try:
        fin_df = load_financial_data(report_date_min='2022-01-01')
        if fin_df.empty:
            print('  财务表为空, 跳过基本面')
            fin_df = None
        else:
            print(f'  财务记录: {len(fin_df)} | 股票数: {fin_df["stock_code"].nunique()}')
    except Exception as e:
        print(f'  财务数据加载异常: {e}')

    print('\n[3] 行业哑变量示例 get_dummies')
    industry_demo = pd.DataFrame({
        'stock_code': ['600519.SH', '000858.SZ', '601318.SH', '600036.SH', '000001.SZ'],
        'stock_name': ['贵州茅台', '五粮液', '中国平安', '招商银行', '平安银行'],
        'industry': ['食品饮料', '食品饮料', '非银金融', '银行', '银行'],
    })
    industry_dummies = pd.get_dummies(industry_demo['industry'], prefix='ind')
    demo_out = pd.concat([industry_demo[['stock_code', 'stock_name']], industry_dummies], axis=1)
    print(demo_out.to_string(index=False))
    print(f'  哑变量列数: {industry_dummies.shape[1]}')

    all_results = []
    failed = []
    for code in STOCK_CODES:
        try:
            ic_df = analyze_stock(code, START_DATE, END_DATE, fin_df=fin_df)
            if not ic_df.empty:
                all_results.append(ic_df)
        except Exception as e:
            print(f'\n  [失败] {code}: {e}')
            failed.append((code, str(e)))

    print(f'\n{"=" * 72}')
    print('[4] 汇总')
    print('=' * 72)

    if all_results:
        summary_df = pd.concat(all_results, ignore_index=True)
        stock_summary = summary_df.groupby('stock_code').agg(
            factor_cnt=('factor', 'count'),
            avg_ic=('|IC|', 'mean'),
            max_ic=('|IC|', 'max'),
            best_factor=('factor', lambda x: summary_df.loc[x.index, 'factor'].iloc[
                summary_df.loc[x.index, '|IC|'].values.argmax()
            ]),
        ).sort_values('max_ic', ascending=False)
        stock_summary['avg_ic'] = stock_summary['avg_ic'].round(4)
        stock_summary['max_ic'] = stock_summary['max_ic'].round(4)
        print('\n各标的最强因子汇总:')
        print(stock_summary.to_string())

        global_top = summary_df.sort_values('|IC|', ascending=False).head(15)
        global_top = global_top.reset_index(drop=True)
        global_top.index = global_top.index + 1
        print('\n全市场 TOP 15 因子 (跨标的):')
        print(global_top[['stock_code', 'factor', 'category', 'RankIC', '|IC|']].to_string())

    if failed:
        print(f'\n失败 {len(failed)} 只:')
        for code, err in failed:
            print(f'  {code}: {err}')

    print('\n完成!')
