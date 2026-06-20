# -*- coding: utf-8 -*-
"""
2-工业级特征工程.py

内容:
  1. 批量加载多只股票日K线 (batch_load_daily)
  2. 计算50+技术特征 (calc_features)
  3. 加载财务数据, 补充基本面因子 (calc_fundamental_features)
  4. 构造行业因子 (one-hot编码)
  5. 华泰标准预处理流水线: MAD去极值 + Z-score标准化
  6. 行业市值中性化 (neutralize)
  7. 特征相关性分析, 冗余特征识别

运行:
  python 2-工业级特征工程.py
"""

import numpy as np
import pandas as pd
from data_loader import load_stock_data, load_financial_data
from feature_engine import (
    calc_features, calc_fundamental_features,
    preprocess_features, neutralize,
    get_all_feature_cols, FACTOR_TAXONOMY,
)


# ============================================================
# 配置
# ============================================================

STOCK_POOL = [
    '600519.SH',   # 贵州茅台
    '688981.SH',   # 中芯国际
    '000001.SZ',   # 平安银行
    '159941.SZ',   # 纳指ETF
    '300750.SZ',   # 宁德时代
]

START_DATE = '2023-01-01'
END_DATE = '2025-12-31'

INDUSTRY_MAP = {
    '600519.SH': '食品饮料',
    '688981.SH': '半导体',
    '000001.SZ': '银行',
    '159941.SZ': '指数基金',
    '300750.SZ': '新能源',
}


def print_section(title):
    """打印分节标题"""
    width = 60
    print('\n' + '=' * width)
    print(f'  {title}')
    print('=' * width)


# ============================================================
# 第1步: 批量加载数据
# ============================================================

def step1_load_data():
    """逐只加载目标股票的日K线数据"""
    print_section('第1步: 加载目标股票日K线')

    target_stocks = {}
    for code in STOCK_POOL:
        try:
            df = load_stock_data(code, START_DATE, END_DATE)
            if len(df) >= 120:
                target_stocks[code] = df
        except Exception as e:
            print(f'  [跳过] {code}: {e}')

    print(f'成功加载 {len(target_stocks)} 只股票')
    for code, df in target_stocks.items():
        name = INDUSTRY_MAP.get(code, '未知')
        print(f'  {code} ({name}): {len(df)} 个交易日, '
              f'{df.index[0].strftime("%Y-%m-%d")} ~ {df.index[-1].strftime("%Y-%m-%d")}')

    if len(target_stocks) == 0:
        print('[警告] 未加载到任何目标股票, 请检查数据库数据')

    return target_stocks


# ============================================================
# 第2步: 计算50+技术特征
# ============================================================

def step2_calc_technical_features(stock_data):
    """对每只股票计算50+技术特征"""
    print_section('第2步: 计算50+技术特征 (calc_features)')

    featured_data = {}
    for code, df in stock_data.items():
        df_feat = calc_features(df)
        featured_data[code] = df_feat

    all_feature_cols = get_all_feature_cols()
    sample_code = list(featured_data.keys())[0]
    sample_df = featured_data[sample_code]
    available_features = [c for c in all_feature_cols if c in sample_df.columns]

    print(f'因子分类体系 ({len(FACTOR_TAXONOMY)} 大类):')
    total_count = 0
    for cat_key, cat_info in FACTOR_TAXONOMY.items():
        n = len(cat_info['features'])
        total_count += n
        print(f'  {cat_info["name"]} ({cat_key}): {n} 个因子')
        features_str = ', '.join(cat_info['features'][:4])
        if n > 4:
            features_str += f' ... 共{n}个'
        print(f'    -> {features_str}')

    print(f'\n因子总数: {total_count} 个, 实际可用: {len(available_features)} 个')

    print(f'\n以 {sample_code} 为例, 最近5行部分特征:')
    show_cols = ['close', 'ret_1d', 'momentum_20d', 'rsi_14', 'macd_hist', 'ma20_bias']
    show_cols = [c for c in show_cols if c in sample_df.columns]
    print(sample_df[show_cols].tail(5).to_string())

    return featured_data


# ============================================================
# 第3步: 加载财务数据, 补充基本面因子
# ============================================================

def step3_add_fundamental(featured_data):
    """加载财务数据, 为每只股票添加基本面因子"""
    print_section('第3步: 加载财务数据, 补充基本面因子')

    fin_df = load_financial_data(report_date_min='2022-01-01')
    if fin_df.empty:
        print('[警告] 财务数据为空, 跳过基本面因子计算')
        return featured_data

    print(f'加载财务数据: {len(fin_df)} 条记录, '
          f'覆盖 {fin_df["stock_code"].nunique()} 只股票')

    fundamental_cols = ['pe_ratio', 'roe_factor', 'gross_margin_factor', 'debt_ratio_factor']

    for code, df in featured_data.items():
        fund_df = calc_fundamental_features(df, fin_df, code)
        for col in fundamental_cols:
            if col in fund_df.columns:
                featured_data[code][col] = fund_df[col]

    sample_code = list(featured_data.keys())[0]
    sample_df = featured_data[sample_code]
    avail_fund = [c for c in fundamental_cols if c in sample_df.columns]
    print(f'\n基本面因子: {avail_fund}')
    if avail_fund:
        print(f'以 {sample_code} 为例, 最近5行:')
        print(sample_df[avail_fund].tail(5).to_string())

    return featured_data


# ============================================================
# 第4步: 构造行业因子 (one-hot编码)
# ============================================================

def step4_industry_factors(featured_data):
    """构造行业哑变量 (one-hot编码)"""
    print_section('第4步: 构造行业因子 (one-hot编码)')

    print('行业映射:')
    for code, industry in INDUSTRY_MAP.items():
        print(f'  {code} -> {industry}')

    industries = sorted(set(INDUSTRY_MAP.values()))
    print(f'\n行业列表: {industries}')

    all_frames = []
    for code, df in featured_data.items():
        tmp = df.copy()
        tmp['stock_code'] = code
        tmp['trade_date'] = tmp.index
        industry = INDUSTRY_MAP.get(code, '其他')
        for ind in industries:
            tmp[f'ind_{ind}'] = 1.0 if ind == industry else 0.0
        all_frames.append(tmp)

    merged = pd.concat(all_frames, ignore_index=True)
    ind_cols = [f'ind_{ind}' for ind in industries]

    print(f'\n合并后数据: {merged.shape[0]} 行 x {merged.shape[1]} 列')
    print(f'行业哑变量列: {ind_cols}')

    print('\n行业分布:')
    for ind in industries:
        col = f'ind_{ind}'
        count = int(merged[col].sum())
        print(f'  {ind}: {count} 条记录')

    return merged, ind_cols, industries


# ============================================================
# 第5步: 华泰标准预处理流水线
# ============================================================

def step5_preprocess(merged):
    """MAD去极值 + Z-score标准化"""
    print_section('第5步: 华泰标准预处理 (MAD去极值 + Z-score标准化)')

    feature_cols = get_all_feature_cols()
    feature_cols = [c for c in feature_cols if c in merged.columns]

    demo_factors = ['momentum_20d', 'rsi_14', 'macd_hist']
    demo_factors = [f for f in demo_factors if f in merged.columns]

    print('--- 去极值前的分布统计 ---')
    for col in demo_factors:
        s = merged[col].dropna()
        print(f'  {col:20s}: min={s.min():10.4f}  max={s.max():10.4f}  '
              f'mean={s.mean():10.4f}  std={s.std():10.4f}')

    preprocessed = merged.copy()
    for code in merged['stock_code'].unique():
        mask = preprocessed['stock_code'] == code
        stock_df = preprocessed.loc[mask].copy()

        stock_df_processed = preprocess_features(stock_df, feature_cols=feature_cols, method='mad')

        for col in feature_cols:
            preprocessed.loc[mask, col] = stock_df_processed[col].values

    print('\n--- 去极值+标准化后的分布统计 ---')
    for col in demo_factors:
        s = preprocessed[col].dropna()
        print(f'  {col:20s}: min={s.min():10.4f}  max={s.max():10.4f}  '
              f'mean={s.mean():10.4f}  std={s.std():10.4f}')

    print('\n预处理效果:')
    print('  - 极端值被MAD方法截断 (中位数 +/- 5*1.4826*MAD)')
    print('  - 标准化后均值接近0, 标准差接近1')
    print('  - 不同因子量纲统一, 可直接输入模型')

    return preprocessed, feature_cols


# ============================================================
# 第6步: 行业市值中性化
# ============================================================

def step6_neutralize(preprocessed, ind_cols):
    """行业市值中性化: 对动量因子做回归取残差"""
    print_section('第6步: 行业市值中性化')

    print('原理: factor = beta_industry * industry + beta_mktcap * ln(mktcap) + residual')
    print('残差residual即为中性化后的因子值, 消除了行业和市值的影响\n')

    preprocessed['mktcap_proxy'] = (
        preprocessed['close'] * preprocessed['volume']
    ).rolling(20, min_periods=1).mean()
    preprocessed['mktcap_log'] = np.log(preprocessed['mktcap_proxy'].clip(lower=1))

    industry_dummies = preprocessed[ind_cols]
    mktcap_log = preprocessed['mktcap_log']

    target_factor = 'momentum_20d'
    if target_factor not in preprocessed.columns:
        print(f'[警告] {target_factor} 不在数据中, 跳过中性化演示')
        return preprocessed

    factor_before = preprocessed[target_factor].copy()

    factor_neutralized = neutralize(
        factor_series=preprocessed[target_factor],
        industry_dummies=industry_dummies,
        mktcap_log=mktcap_log,
    )
    preprocessed[f'{target_factor}_neutral'] = factor_neutralized

    print(f'因子: {target_factor}')
    print('\n--- 中性化前 ---')
    before_stats = factor_before.dropna()
    print(f'  mean={before_stats.mean():.6f}  std={before_stats.std():.6f}  '
          f'min={before_stats.min():.6f}  max={before_stats.max():.6f}')

    print('\n--- 中性化后 ---')
    after_stats = factor_neutralized.dropna()
    print(f'  mean={after_stats.mean():.6f}  std={after_stats.std():.6f}  '
          f'min={after_stats.min():.6f}  max={after_stats.max():.6f}')

    print('\n各行业因子均值对比:')
    print(f'  {"行业":<10s} {"中性化前":>12s} {"中性化后":>12s}')
    print(f'  {"-"*10} {"-"*12} {"-"*12}')
    for col in ind_cols:
        ind_name = col.replace('ind_', '')
        mask = preprocessed[col] == 1.0
        if mask.sum() == 0:
            continue
        mean_before = factor_before.loc[mask].mean()
        mean_after = factor_neutralized.loc[mask].mean()
        print(f'  {ind_name:<10s} {mean_before:>12.6f} {mean_after:>12.6f}')

    print('\n中性化效果: 消除行业间的因子均值差异, 使因子反映个股相对行业的超额信息')

    return preprocessed


# ============================================================
# 第7步: 特征相关性分析
# ============================================================

def step7_correlation_analysis(preprocessed, feature_cols):
    """计算特征间相关系数, 识别冗余特征"""
    print_section('第7步: 特征相关性分析')

    avail_cols = [c for c in feature_cols if c in preprocessed.columns]
    corr_matrix = preprocessed[avail_cols].corr()

    print(f'计算 {len(avail_cols)} 个特征的相关系数矩阵: {corr_matrix.shape}')

    threshold = 0.8
    high_corr_pairs = []
    for i in range(len(avail_cols)):
        for j in range(i + 1, len(avail_cols)):
            corr_val = corr_matrix.iloc[i, j]
            if abs(corr_val) > threshold:
                high_corr_pairs.append((avail_cols[i], avail_cols[j], corr_val))

    high_corr_pairs.sort(key=lambda x: abs(x[2]), reverse=True)

    print(f'\n高相关特征对 (|corr| > {threshold}): 共 {len(high_corr_pairs)} 对')
    print(f'  {"特征A":<25s} {"特征B":<25s} {"相关系数":>10s}')
    print(f'  {"-"*25} {"-"*25} {"-"*10}')

    show_limit = 20
    for feat_a, feat_b, corr_val in high_corr_pairs[:show_limit]:
        print(f'  {feat_a:<25s} {feat_b:<25s} {corr_val:>10.4f}')
    if len(high_corr_pairs) > show_limit:
        print(f'  ... 共 {len(high_corr_pairs)} 对, 仅展示前 {show_limit} 对')

    redundant_features = set()
    for feat_a, feat_b, _ in high_corr_pairs:
        redundant_features.add(feat_b)

    print(f'\n冗余特征建议 (可考虑剔除): {len(redundant_features)} 个')
    if redundant_features:
        for feat in sorted(redundant_features):
            print(f'  - {feat}')

    print(f'\n保留后特征数: {len(avail_cols) - len(redundant_features)} 个 '
          f'(原 {len(avail_cols)} 个)')

    return corr_matrix, high_corr_pairs


# ============================================================
# 教学总结
# ============================================================

def summary():
    """打印特征工程流水线总结"""

    print('特征工程流水线:')
    print('  原始OHLCV -> 50+因子 -> 去极值 -> 中性化 -> 标准化 -> 建模')
    print()
    print('各环节要点:')
    print('  1. 原始OHLCV: 从数据库批量加载日K线数据')
    print('  2. 50+因子:    calc_features() 计算价量/动量/波动率/技术/均线/交互 6大类因子')
    print('  3. 基本面因子: calc_fundamental_features() 补充PE/ROE/毛利率等')
    print('  4. 行业因子:   构造行业哑变量 (one-hot), 用于后续中性化')
    print('  5. MAD去极值:  中位数 +/- 5*1.4826*MAD 截断, 消除极端离群值')
    print('  6. 中性化:     回归法消除行业和市值对因子的影响')
    print('  7. Z-score:    标准化到均值0/标准差1, 统一量纲')
    print('  8. 相关性分析: 识别冗余特征, 降低多重共线性')


# ============================================================
# 主流程
# ============================================================

def main():
    # 第1步: 加载数据
    stock_data = step1_load_data()
    if not stock_data:
        print('没有加载到数据, 程序退出')
        return

    # 第2步: 计算技术特征
    featured_data = step2_calc_technical_features(stock_data)

    # 第3步: 加载财务数据, 补充基本面因子
    featured_data = step3_add_fundamental(featured_data)

    # 第4步: 构造行业因子
    merged, ind_cols, industries = step4_industry_factors(featured_data)

    # 第5步: 华泰预处理流水线
    preprocessed, feature_cols = step5_preprocess(merged)

    # 第6步: 行业市值中性化
    preprocessed = step6_neutralize(preprocessed, ind_cols)

    # 第7步: 特征相关性分析
    step7_correlation_analysis(preprocessed, feature_cols)

    # 教学总结
    summary()


if __name__ == '__main__':
    main()
