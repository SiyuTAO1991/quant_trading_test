# -*- coding: utf-8 -*-
"""
XGBoost 涨跌二分类（教学演示）

对贵州茅台、中芯国际分别：拉日K、算技术因子、打标签、滚动训练 XGBoost，输出 AUC 等指标。
运行: python 3-XGBoost涨跌预测.py

可调: START_DATE / END_DATE；LABEL_HORIZON；下方 XGB_STOCKS 字典（改代码即换标的）。
"""

import numpy as np
import pandas as pd
from sklearn.metrics import confusion_matrix

from data_loader import load_stock_data
from feature_engine import calc_features, preprocess_features, get_all_feature_cols
from ml_engine import make_labels, rolling_train_predict, evaluate_classification


# ============================================================
# 配置
# ============================================================

# 教学用标的（需库中有日线，与 1-贵州茅台因子分析.py 一致）
STOCK_CODES = [
    '159608.SZ', '159583.SZ', '159560.SZ', '159133.SZ', '159796.SZ',  # ETF
    '603019.SH', '002847.SZ', '603259.SH', '000981.SZ', '002714.SZ', '002115.SZ',
]
XGB_STOCKS = {code: code for code in STOCK_CODES}

START_DATE = '2025-01-01'
END_DATE = None

# 1=次日涨跌；N=未来第 N 个交易日收盘相对今日涨跌
LABEL_HORIZON = 1


def prepare_stock_features(stock_code, start_date, end_date, min_bars=120):
    """日K -> 技术因子 -> MAD 去极值与 Z-score。"""
    df = load_stock_data(stock_code, start_date, end_date)
    if len(df) < min_bars:
        raise ValueError(f'{stock_code} 有效交易日 {len(df)} < {min_bars}')
    df = calc_features(df)
    feature_cols = [c for c in get_all_feature_cols() if c in df.columns]
    df = preprocess_features(df, feature_cols)
    return df, feature_cols


# ============================================================
# 单只股票: 特征工程 + 滚动预测 + 评估
# ============================================================

def run_single_stock(stock_code, stock_name):
    """对单只股票执行完整的 XGBoost 滚动预测流程"""

    print(f"\n{'='*60}")
    print(f"  {stock_name} ({stock_code})")
    print(f"{'='*60}")

    # --- 加载数据 + 特征 ---
    print(f"\n[1] 加载数据: {START_DATE} ~ {END_DATE or '最新'}")
    df, feature_cols = prepare_stock_features(stock_code, START_DATE, END_DATE)
    print(f"    共 {len(df)} 个交易日, 价格区间: {df['close'].min():.2f} ~ {df['close'].max():.2f}")

    print("[2] 计算技术特征 + 预处理（已完成）")
    print(f"    特征数量: {len(feature_cols)}")

    # --- 构建标签 ---
    print(f"[3] 构建标签: 未来第{LABEL_HORIZON}日涨=1, 跌=0 (相对今日收盘)")
    df['label'] = make_labels(df, horizon=LABEL_HORIZON, method='binary')
    df.dropna(subset=['label'], inplace=True)
    label_dist = df['label'].value_counts().sort_index()
    print(f"    标签分布: 跌(0)={label_dist.get(0,0)}, 涨(1)={label_dist.get(1,0)}, "
          f"涨占比={label_dist.get(1,0)/len(df)*100:.1f}%")

    # --- 滚动预测 ---
    print("[4] XGBoost 滚动预测 (train_days=120, retrain_interval=20)")
    df_reset = df.reset_index()
    pred_df = rolling_train_predict(
        df_reset, feature_cols, label_col='label',
        model_type='xgboost', train_days=120, retrain_interval=20,
        verbose=True,
    )
    print(f"    预测样本数: {len(pred_df)}")

    # --- 整体评估 ---
    print("\n[5] 整体评估指标")
    metrics = evaluate_classification(
        pred_df['y_true'].values,
        pred_df['y_pred'].values,
        pred_df['y_prob'].values,
    )
    print(f"    AUC:       {metrics['auc']:.4f}")
    print(f"    Accuracy:  {metrics['accuracy']:.4f}")
    print(f"    Precision: {metrics['precision']:.4f}")
    print(f"    Recall:    {metrics['recall']:.4f}")
    print(f"    F1:        {metrics['f1']:.4f}")

    # --- 混淆矩阵 ---
    print("\n[6] 混淆矩阵")
    cm = confusion_matrix(pred_df['y_true'], pred_df['y_pred'])
    print(f"              预测跌  预测涨")
    print(f"    实际跌    {cm[0,0]:>5d}   {cm[0,1]:>5d}")
    print(f"    实际涨    {cm[1,0]:>5d}   {cm[1,1]:>5d}")

    # --- 按月统计准确率 ---
    print("\n[7] 按月准确率变化")
    pred_df['date'] = pd.to_datetime(pred_df['date'])
    pred_df['month'] = pred_df['date'].dt.to_period('M')
    monthly = pred_df.groupby('month').apply(
        lambda g: pd.Series({
            'accuracy': (g['y_true'] == g['y_pred']).mean(),
            'samples': len(g),
            'auc': evaluate_classification(
                g['y_true'].values, g['y_pred'].values, g['y_prob'].values
            )['auc'] if len(g['y_true'].unique()) > 1 else float('nan'),
        })
    )
    print(f"    {'月份':<12s} {'样本':>6s} {'Accuracy':>10s} {'AUC':>8s}")
    print(f"    {'-'*38}")
    for idx, row in monthly.iterrows():
        auc_str = f"{row['auc']:.4f}" if not np.isnan(row['auc']) else '  N/A '
        print(f"    {str(idx):<12s} {int(row['samples']):>6d} {row['accuracy']:>10.4f} {auc_str:>8s}")

    return {
        'stock_code': stock_code,
        'stock_name': stock_name,
        'metrics': metrics,
        'monthly': monthly,
        'pred_df': pred_df,
        'n_samples': len(pred_df),
    }


# ============================================================
# 两只股票对比
# ============================================================

def compare_stocks(results_list):
    """对比多只股票的预测结果"""

    print(f"\n{'='*60}")
    print("  XGBoost 预测结果对比")
    print(f"{'='*60}")

    header = f"{'股票':<12s} {'AUC':>8s} {'Accuracy':>10s} {'Precision':>10s} {'Recall':>8s} {'F1':>8s} {'样本数':>8s}"
    print(f"\n    {header}")
    print(f"    {'-'*len(header)}")

    for r in results_list:
        m = r['metrics']
        print(f"    {r['stock_name']:<12s} "
              f"{m['auc']:>8.4f} {m['accuracy']:>10.4f} {m['precision']:>10.4f} "
              f"{m['recall']:>8.4f} {m['f1']:>8.4f} {r['n_samples']:>8d}")

    # 月度准确率波动对比
    print("\n    月度准确率统计:")
    for r in results_list:
        acc_series = r['monthly']['accuracy']
        print(f"    {r['stock_name']}: "
              f"均值={acc_series.mean():.4f}, "
              f"标准差={acc_series.std():.4f}, "
              f"最高={acc_series.max():.4f}, "
              f"最低={acc_series.min():.4f}")


# ============================================================
# 可预测性分析
# ============================================================

def analyze_predictability(results_list):
    """分析不同类型股票的可预测性差异"""

    print(f"\n{'='*60}")
    print("  可预测性分析")
    print(f"{'='*60}")
    print('  (可预测性的分析维度详见课件 Part1)')

    for r in results_list:
        pred_df = r['pred_df']
        acc = r['metrics']['accuracy']
        monthly_std = r['monthly']['accuracy'].std()
        print(f"\n    {r['stock_name']}:")
        print(f"      - 整体准确率: {acc:.4f}")
        print(f"      - 月度准确率波动(std): {monthly_std:.4f}")
        stability = "稳定" if monthly_std < 0.08 else "波动较大"
        print(f"      - 预测稳定性: {stability}")


# ============================================================
# 主程序
# ============================================================

def main():
    print("=" * 60)
    title = "次日涨跌" if LABEL_HORIZON == 1 else f"未来第{LABEL_HORIZON}日涨跌"
    print(f"  XGBoost {title} 二分类预测模型")
    print("=" * 60)
    stocks = XGB_STOCKS
    print(f"  数据区间: {START_DATE} ~ {END_DATE or '最新'}")
    print(f"  标的（{len(stocks)}只）: {', '.join(f'{v}({k})' for k, v in stocks.items())}")
    print(f"  标签定义: 未来第{LABEL_HORIZON}个交易日收盘 > 今日收盘 => 1(涨), 否则 => 0(跌)")
    print(f"  模型: XGBoost | 滚动窗口=120天 | 重训间隔=20天")

    results = []
    for code, name in stocks.items():
        try:
            r = run_single_stock(code, name)
            results.append(r)
        except Exception as e:
            print(f"\n  [错误] {name}({code}) 处理失败: {e}")

    if len(results) < 1:
        print("\n  没有成功完成的标的, 跳过对比分析")
    else:
        compare_stocks(results)
        analyze_predictability(results)

    # --- 最终结论 ---
    if results:
        avg_acc = np.mean([r['metrics']['accuracy'] for r in results])
        print(f"\n{'='*60}")
        print(f"  结论")
        print(f"{'='*60}")
        hz = "次日" if LABEL_HORIZON == 1 else f"未来第{LABEL_HORIZON}日"
        print(f"\n  XGBoost二分类预测模型完成，{hz}方向准确率约{avg_acc*100:.0f}%，")
        print(f"  输出的概率值(0~1)就是'上涨概率因子'。")
        print(f"\n  该因子可作为多因子选股模型的alpha信号之一，")
        print(f"  与基本面因子、动量因子等组合使用，构建综合选股策略。")
    print()


if __name__ == '__main__':
    main()
