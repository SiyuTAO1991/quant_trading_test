# -*- coding: utf-8 -*-
"""
MASTER数据与因子 - 数据探索与预处理对比实战

本脚本从MySQL加载真实A股数据, 计算因子后对比两种主流预处理方法:
  1. RobustZScoreNorm (MASTER论文使用): 中位数 + MAD + clip[-3,3]
  2. MAD + Z-Score (L11华泰标准): MAD去极值 + 均值/标准差标准化

这是建模前的必要工作: 了解因子分布、处理异常值、发现冗余因子。
自然衔接脚本4的XGBoost截面预测。

MASTER论文: Li et al., "MASTER: Market-Guided Stock Transformer (AAAI 2024)"
"""

import os
import time
import numpy as np
import pandas as pd

from data_loader import load_stock_data
from feature_engine import calc_features, get_all_feature_cols, preprocess_features

# ============================================================
# 配置
# ============================================================

START_DATE = '2023-01-01'
END_DATE = '2025-12-31'

# 选取10只代表性股票做EDA(快速, 覆盖主要行业)
EDA_STOCKS = [
    '600519.SH',  # 贵州茅台 - 消费
    '601318.SH',  # 中国平安 - 金融
    '000333.SZ',  # 美的集团 - 制造
    '300750.SZ',  # 宁德时代 - 新能源
    '002594.SZ',  # 比亚迪   - 汽车
    '000858.SZ',  # 五粮液   - 消费
    '600036.SH',  # 招商银行 - 银行
    '600276.SH',  # 恒瑞医药 - 医药
    '002415.SZ',  # 海康威视 - 科技
    '601012.SH',  # 隆基绿能 - 新能源
]

MASTER_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "MASTER-master")
MARKET_INFO_PATH = os.path.join(MASTER_DIR, "data", "csi_market_information.csv")


# ============================================================
# 第一部分: 加载真实A股数据并计算因子
# ============================================================

def load_and_compute():
    """从MySQL加载10只股票, 计算50+技术因子"""

    print("=" * 80)
    print("第一部分: 加载A股数据并计算因子")
    print("=" * 80)
    print(f"  股票池: {len(EDA_STOCKS)} 只(覆盖消费/金融/制造/新能源/医药/科技)")
    print(f"  日期范围: {START_DATE} ~ {END_DATE}")

    t0 = time.time()
    all_frames = []
    loaded = 0

    for code in EDA_STOCKS:
        try:
            df = load_stock_data(code, START_DATE, END_DATE)
            if len(df) < 200:
                continue
            feat_df = calc_features(df)
            feat_df['stock_code'] = code
            feat_df['trade_date'] = feat_df.index
            all_frames.append(feat_df)
            loaded += 1
        except Exception as e:
            print(f"  [跳过] {code}: {e}")

    elapsed = time.time() - t0
    print(f"\n  加载成功: {loaded}/{len(EDA_STOCKS)} 只, 耗时: {elapsed:.1f}s")

    if loaded < 3:
        print("  [错误] 有效股票不足, 无法继续分析")
        return None, None

    panel = pd.concat(all_frames, ignore_index=True)
    feature_cols = get_all_feature_cols()
    feature_cols = [c for c in feature_cols if c in panel.columns]

    print(f"  面板大小: {len(panel):,} 行 x {len(feature_cols)} 因子")

    return panel, feature_cols


# ============================================================
# 第二部分: 因子分布诊断
# ============================================================

def diagnose_factor_distribution(panel, feature_cols):
    """分析原始因子的分布特征: 偏度、峰度、异常值率"""

    print("\n" + "=" * 80)
    print("第二部分: 因子分布诊断 (建模前必须了解的)")
    print("=" * 80)

    stats = []
    for col in feature_cols:
        series = panel[col].dropna()
        if len(series) < 100:
            continue

        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr = q3 - q1
        outlier_rate = ((series < q1 - 3 * iqr) | (series > q3 + 3 * iqr)).mean()

        stats.append({
            'factor': col,
            'mean': series.mean(),
            'std': series.std(),
            'skew': series.skew(),
            'kurtosis': series.kurtosis(),
            'outlier_pct': outlier_rate * 100,
            'nan_pct': panel[col].isna().mean() * 100,
        })

    stats_df = pd.DataFrame(stats)

    # 异常值率最高的因子
    top_outlier = stats_df.nlargest(5, 'outlier_pct')
    print("\n  异常值率最高的5个因子 (IQR 3倍标准):")
    print(f"  {'因子':<25s} {'偏度':>8s} {'峰度':>8s} {'异常值%':>8s}")
    print(f"  {'-'*25} {'-'*8} {'-'*8} {'-'*8}")
    for _, row in top_outlier.iterrows():
        print(f"  {row['factor']:<25s} {row['skew']:>8.2f} {row['kurtosis']:>8.1f} {row['outlier_pct']:>7.2f}%")

    # 偏度最大的因子
    top_skew = stats_df.nlargest(5, 'skew')
    print(f"\n  正偏最严重的5个因子 (右尾厚):")
    print(f"  {'因子':<25s} {'偏度':>8s} {'峰度':>8s}")
    print(f"  {'-'*25} {'-'*8} {'-'*8}")
    for _, row in top_skew.iterrows():
        print(f"  {row['factor']:<25s} {row['skew']:>8.2f} {row['kurtosis']:>8.1f}")

    # 汇总
    avg_outlier = stats_df['outlier_pct'].mean()
    high_skew_count = (stats_df['skew'].abs() > 1).sum()
    high_kurtosis_count = (stats_df['kurtosis'] > 5).sum()

    print(f"\n  汇总:")
    print(f"    平均异常值率: {avg_outlier:.2f}%")
    print(f"    高偏度因子(|skew|>1): {high_skew_count}/{len(stats_df)}")
    print(f"    高峰度因子(kurtosis>5): {high_kurtosis_count}/{len(stats_df)}")
    print(f"    --> 说明原始因子普遍存在厚尾分布, 需要去极值处理")

    return stats_df


# ============================================================
# 第三部分: 两种预处理方法对比
# ============================================================

def robust_zscore_norm(series, clip_range=3.0):
    """
    MASTER论文的RobustZScoreNorm

    步骤:
      1. 计算中位数(median)和MAD(median absolute deviation)
      2. 鲁棒标准差 = MAD * 1.4826  (正态分布下MAD与标准差的换算)
      3. 标准化: (x - median) / robust_std
      4. 裁剪到 [-clip_range, clip_range]

    优势: 中位数和MAD对异常值免疫(高breakdown point)
    """
    median = np.nanmedian(series)
    mad = np.nanmedian(np.abs(series - median))
    robust_std = mad * 1.4826

    if robust_std < 1e-10:
        return np.zeros_like(series)

    normalized = (series - median) / robust_std
    return np.clip(normalized, -clip_range, clip_range)


def compare_preprocessing(panel, feature_cols):
    """对比RobustZScoreNorm (MASTER) vs MAD+Z-Score (L11)"""

    print("\n" + "=" * 80)
    print("第三部分: 预处理方法对比")
    print("  方法A: RobustZScoreNorm (MASTER论文) - 中位数 + MAD + clip[-3,3]")
    print("  方法B: MAD + Z-Score (L11华泰标准) - MAD去极值 + mean/std标准化")
    print("=" * 80)

    # 选5个有代表性的因子做对比
    demo_factors = [f for f in ['rsi_14', 'momentum_20d', 'hist_vol_20d',
                                'vol_ratio_5d', 'adx_14'] if f in feature_cols]

    if not demo_factors:
        demo_factors = feature_cols[:5]

    # 方法A: RobustZScoreNorm
    robust_panel = panel.copy()
    for col in feature_cols:
        vals = robust_panel[col].values.astype(float)
        robust_panel[col] = robust_zscore_norm(vals)

    # 方法B: L11的MAD+Z-Score
    mad_panel = preprocess_features(panel, feature_cols, method='mad')

    # 对比结果
    print(f"\n  {'因子':<25s} | {'原始范围':>20s} | {'RobustZScore范围':>20s} | {'MAD+ZScore范围':>20s}")
    print(f"  {'-'*25}-+-{'-'*20}-+-{'-'*20}-+-{'-'*20}")

    for col in demo_factors:
        raw_vals = panel[col].dropna().values
        r_vals = robust_panel[col].dropna().values
        m_vals = mad_panel[col].dropna().values

        raw_range = f"[{np.min(raw_vals):>7.2f}, {np.max(raw_vals):>7.2f}]"
        r_range = f"[{np.min(r_vals):>7.2f}, {np.max(r_vals):>7.2f}]"
        m_range = f"[{np.min(m_vals):>7.2f}, {np.max(m_vals):>7.2f}]"

        print(f"  {col:<25s} | {raw_range:>20s} | {r_range:>20s} | {m_range:>20s}")

    # 详细对比一个高异常值因子
    test_col = demo_factors[0]
    raw = panel[test_col].dropna().values
    r_out = robust_panel[test_col].dropna().values
    m_out = mad_panel[test_col].dropna().values

    print(f"\n  详细对比 ({test_col}):")
    print(f"    原始数据:  均值={np.mean(raw):.4f}  标准差={np.std(raw):.4f}  偏度={pd.Series(raw).skew():.3f}")
    print(f"    RobustZ:   均值={np.mean(r_out):.4f}  标准差={np.std(r_out):.4f}  偏度={pd.Series(r_out).skew():.3f}")
    print(f"    MAD+ZScore: 均值={np.mean(m_out):.4f}  标准差={np.std(m_out):.4f}  偏度={pd.Series(m_out).skew():.3f}")

    # 被裁剪/去极值影响的比例
    robust_clipped = (np.abs(r_out) >= 2.99).mean() * 100
    print(f"\n    RobustZ clip到[-3,3]被裁比例: {robust_clipped:.2f}%")

    print(f"\n  核心差异:")
    print(f"    RobustZScoreNorm: 输出严格限制在[-3,3], 对极端值硬截断")
    print(f"                     适合深度学习(梯度稳定, 激活函数不饱和)")
    print(f"    MAD+Z-Score:     先去极值再标准化, 输出范围取决于数据")
    print(f"                     适合树模型(不依赖数值范围, 只看排序)")


# ============================================================
# 第四部分: 因子相关性分析
# ============================================================

def analyze_factor_correlation(panel, feature_cols):
    """分析因子间相关性, 找出高度冗余的因子对"""

    print("\n" + "=" * 80)
    print("第四部分: 因子相关性分析 (发现冗余因子)")
    print("=" * 80)

    valid_panel = panel[feature_cols].dropna()
    if len(valid_panel) < 100:
        print("  有效数据不足, 跳过相关性分析")
        return

    corr_matrix = valid_panel.corr()

    # 提取上三角的高相关对
    high_corr_pairs = []
    for i in range(len(feature_cols)):
        for j in range(i + 1, len(feature_cols)):
            r = corr_matrix.iloc[i, j]
            if abs(r) > 0.8:
                high_corr_pairs.append((feature_cols[i], feature_cols[j], r))

    high_corr_pairs.sort(key=lambda x: abs(x[2]), reverse=True)

    print(f"\n  总因子数: {len(feature_cols)}")
    print(f"  高相关对(|r|>0.8): {len(high_corr_pairs)} 对")

    if high_corr_pairs:
        print(f"\n  Top 10 高相关因子对:")
        print(f"  {'因子A':<25s}  {'因子B':<25s}  {'相关系数':>8s}")
        print(f"  {'-'*25}  {'-'*25}  {'-'*8}")
        for fa, fb, r in high_corr_pairs[:10]:
            print(f"  {fa:<25s}  {fb:<25s}  {r:>8.3f}")

        print(f"\n  实践建议:")
        print(f"    - 相关性>0.9的因子对可以考虑只保留其中一个")
        print(f"    - 树模型(XGBoost)对共线性不敏感, 影响不大")
        print(f"    - 线性模型/神经网络对共线性敏感, 需要去冗余")
    else:
        print(f"\n  未发现高相关因子对, 因子体系正交性较好")


# ============================================================
# 第五部分: MASTER市场信息数据对比 (如有)
# ============================================================

def analyze_master_csv():
    """加载MASTER的63维市场信息CSV, 与我们的因子做维度对比"""

    print("\n" + "=" * 80)
    print("第五部分: MASTER市场信息数据 (63维)")
    print("=" * 80)

    if not os.path.exists(MARKET_INFO_PATH):
        print(f"  [跳过] 未找到MASTER CSV: {MARKET_INFO_PATH}")
        print(f"  提示: 这是论文附带的中国A股指数级别数据, 非必需")
        return

    df = pd.read_csv(MARKET_INFO_PATH, header=[0, 1], index_col=0)
    print(f"\n  数据形状: {df.shape[0]} 天 x {df.shape[1]} 维")
    print(f"  时间范围: {df.index[0]} ~ {df.index[-1]}")

    feature_names = [col[1] for col in df.columns]

    index_map = {"SH000300": "沪深300", "SH000905": "中证500", "SH000906": "中证800"}
    print(f"\n  覆盖指数:")
    for code, name in index_map.items():
        count = sum(1 for fn in feature_names if code in fn)
        print(f"    {name}({code}): {count} 维")

    val_min, val_max = df.values.min(), df.values.max()
    print(f"\n  数据范围: [{val_min:.4f}, {val_max:.4f}]")
    if abs(val_min + 3.0) < 0.01 and abs(val_max - 3.0) < 0.01:
        print(f"  --> 范围恰好为[-3, 3], 已经过RobustZScoreNorm+clip处理")

    print(f"\n  对比总结:")
    print(f"    {'维度':<15s} {'MASTER':>15s}  {'我们(L11)':>15s}")
    print(f"    {'-'*15} {'-'*15}  {'-'*15}")
    print(f"    {'因子数量':<15s} {'158(Alpha158)':>15s}  {'52(TA-Lib)':>15s}")
    print(f"    {'市场信息':<15s} {'63维(3指数x21)':>15s}  {'无':>15s}")
    print(f"    {'总特征维度':<15s} {'221':>15s}  {'52':>15s}")
    print(f"    {'预处理方法':<15s} {'RobustZScoreNorm':>15s}  {'MAD+Z-Score':>15s}")
    print(f"    {'数据来源':<15s} {'Qlib框架':>15s}  {'MySQL+TA-Lib':>15s}")


# ============================================================
# 主流程
# ============================================================

if __name__ == "__main__":
    print("MASTER数据与因子 - 数据探索与预处理对比实战")
    print("=" * 80)

    panel, feature_cols = load_and_compute()

    if panel is not None:
        diagnose_factor_distribution(panel, feature_cols)
        compare_preprocessing(panel, feature_cols)
        analyze_factor_correlation(panel, feature_cols)

    analyze_master_csv()

    print(f"\n{'=' * 80}")
    print("[完成] 数据探索结束, 接下来运行 4-XGBoost截面预测.py 进行截面预测")
