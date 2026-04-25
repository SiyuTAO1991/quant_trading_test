# -*- coding: utf-8 -*-
"""
多因子选股 - 筛选2（行业版）
纯行业视角，打分制：每个指标按行业内排名换算为 1~5 分，5 项总分相加筛选。

打分规则：
  - 每个指标在行业内按排名分五档：前 20% -> 5 分，20%~40% -> 4 分，...，后 20% -> 1 分
  - 5 个指标各自得分后相加，总分范围 5~25
  - 筛选：总分 >= SCORE_MIN

输入：data/stock_fina_pool_QMT.csv（由 多因子选股-下载数据.py 生成）
输出：data/stock_fina_selected_QMT_industry.csv、data/industry_viz/*.png

运行：python 多因子选股-筛选2.py
"""
import os
import sys
import pandas as pd

# Windows 控制台 UTF-8 输出
if sys.platform == 'win32' and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# ============================================================
# 配置（可按需调整）
# ============================================================
# 总分阈值，5 项各 1~5 分，总分范围 5~25，默认 18 表示平均每项 3.6 分
SCORE_MIN = 18

ENABLE_VISUALIZATION = True

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
INPUT_FILE = os.path.join(DATA_DIR, "stock_fina_pool_QMT.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "stock_fina_selected_QMT_industry.csv")
VIZ_OUTPUT_DIR = os.path.join(DATA_DIR, "industry_viz")


def add_industry_percentile(df):
    """
    为有 industry 的股票计算各指标在行业内的排名百分比（0~1，越大越好）。
    高越好：roe, netprofit_yoy, grossprofit_margin, ocf_to_revenue
    低越好：debt_to_assets（取 1 - pct 使越大越好）
    """
    if 'industry' not in df.columns or df['industry'].isna().all() or (df['industry'] == '').all():
        return df
    df = df.copy()
    valid = df['industry'].notna() & (df['industry'] != '')
    if not valid.any():
        return df
    for col, higher_better in [
        ('roe', True), ('netprofit_yoy', True), ('grossprofit_margin', True),
        ('ocf_to_revenue', True), ('debt_to_assets', False)
    ]:
        if col not in df.columns:
            continue
        pct_col = f'{col}_industry_pct'
        if higher_better:
            df.loc[valid, pct_col] = df.loc[valid].groupby('industry')[col].rank(pct=True, method='average')
        else:
            df.loc[valid, pct_col] = 1 - df.loc[valid].groupby('industry')[col].rank(pct=True, method='average')
    return df


def add_industry_score(df):
    """
    将 industry_pct（0~1）换算为 1~5 分，并计算总分。
    行业内前 20% -> 5 分，20%~40% -> 4 分，40%~60% -> 3 分，60%~80% -> 2 分，后 20% -> 1 分
    """
    pct_cols = ['roe_industry_pct', 'netprofit_yoy_industry_pct', 'grossprofit_margin_industry_pct',
                'debt_to_assets_industry_pct', 'ocf_to_revenue_industry_pct']

    def pct_to_score(x):
        if pd.isna(x):
            return None
        s = min(5, max(1, int(x * 5) + 1))
        return s

    score_cols = []
    for pct_col in pct_cols:
        if pct_col not in df.columns:
            continue
        score_col = pct_col.replace('_pct', '_score')
        df[score_col] = df[pct_col].apply(pct_to_score)
        score_cols.append(score_col)
    if score_cols:
        df['industry_score'] = df[score_cols].sum(axis=1)
    return df


def industry_distribution_stats(df):
    """计算各行业的指标分布（均值、中位数、样本数）。"""
    if 'industry' not in df.columns or df['industry'].isna().all():
        return None
    valid = df['industry'].notna() & (df['industry'] != '')
    if not valid.any():
        return None
    sub = df.loc[valid]
    metrics = ['roe', 'netprofit_yoy', 'grossprofit_margin', 'debt_to_assets', 'ocf_to_revenue']
    metrics = [m for m in metrics if m in sub.columns]
    if not metrics:
        return None
    agg_dict = {m: ['mean', 'median', 'count'] for m in metrics}
    stats = sub.groupby('industry').agg(agg_dict).round(2)
    return stats


def save_industry_visualization(df, output_dir):
    """按行业生成指标分布可视化图。"""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'SimSun', 'KaiTi']
        plt.rcParams['axes.unicode_minus'] = False
    except ImportError:
        return
    if 'industry' not in df.columns or df['industry'].isna().all():
        return
    valid = df['industry'].notna() & (df['industry'] != '')
    if not valid.any():
        return
    os.makedirs(output_dir, exist_ok=True)
    for col, label in [('roe', 'ROE (%)'), ('grossprofit_margin', '毛利率 (%)'), ('debt_to_assets', '资产负债率 (%)')]:
        if col not in df.columns:
            continue
        sub = df.loc[valid, ['industry', col]].dropna(subset=[col])
        if sub.empty:
            continue
        ind_agg = sub.groupby('industry')[col].agg(['mean', 'median', 'count'])
        ind_agg = ind_agg[ind_agg['count'] >= 3].sort_values('mean', ascending=(col == 'debt_to_assets'))
        if ind_agg.empty:
            continue
        fig, ax = plt.subplots(figsize=(12, max(6, len(ind_agg) * 0.3)))
        ax.barh(range(len(ind_agg)), ind_agg['mean'], label='均值', alpha=0.8)
        ax.set_yticks(range(len(ind_agg)))
        ax.set_yticklabels(ind_agg.index, fontsize=8)
        ax.set_xlabel(label)
        ax.set_title(f'各行业{label}分布（均值）')
        ax.legend()
        plt.tight_layout()
        out_path = os.path.join(output_dir, f'industry_{col}.png')
        plt.savefig(out_path, dpi=100, bbox_inches='tight')
        plt.close()
        print(f"  已保存行业分布图: {out_path}")


def main():
    print("多因子选股 - 筛选2（行业版，打分制）")
    print(f"筛选条件：5 项各 1~5 分（按行业内排名），总分 >= {SCORE_MIN}")
    print("-" * 60)

    if not os.path.exists(INPUT_FILE):
        print(f"错误：未找到输入文件 {INPUT_FILE}")
        print("请先运行：python 多因子选股-下载数据.py")
        return

    df = pd.read_csv(INPUT_FILE, encoding='utf-8-sig')
    print(f"读取 {len(df)} 只股票")

    # 数值化处理（CSV 可能读成字符串）
    for col in ['roe', 'netprofit_yoy', 'grossprofit_margin', 'debt_to_assets', 'ocf_to_revenue']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 行业内排名百分比 -> 1~5 分 -> 总分
    df = add_industry_percentile(df)
    df = add_industry_score(df)

    mask = (df['industry_score'] >= SCORE_MIN) & df['industry_score'].notna()
    print(f"  打分制：5 项总分 >= {SCORE_MIN}（每项 1~5 分，总分 5~25）: 剩余 {mask.sum()} 只")

    selected = df[mask].copy()
    selected = selected.sort_values('industry_score', ascending=False).reset_index(drop=True)

    selected.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print(f"\n筛选完成：共 {len(selected)} 只股票达标")
    print(f"已保存：{OUTPUT_FILE}")

    stats = industry_distribution_stats(df)
    if stats is not None:
        print("\n" + "=" * 60)
        print("各行业指标分布（全市场）：")
        print("=" * 60)
        pd.set_option('display.max_columns', 20)
        pd.set_option('display.width', 200)
        print(stats.to_string())

    if ENABLE_VISUALIZATION:
        print("\n生成行业分布可视化...")
        save_industry_visualization(df, VIZ_OUTPUT_DIR)

    if len(selected) > 0:
        print("\n" + "=" * 60)
        print("达标股票（按总分排序）：")
        print("=" * 60)
        disp_cols = ['stock_code', 'stock_name', 'industry', 'industry_score', 'end_date', 'roe',
                    'netprofit_yoy', 'grossprofit_margin', 'debt_to_assets', 'ocf_to_revenue']
        score_cols = [c for c in selected.columns if c.endswith('_industry_score') and c != 'industry_score']
        disp_cols = [c for c in disp_cols if c in selected.columns] + score_cols
        print(selected[disp_cols].head(20).to_string(index=False))

    return OUTPUT_FILE


if __name__ == "__main__":
    main()
