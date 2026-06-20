# -*- coding: utf-8 -*-
"""
LightGBM 对比与 Optuna 超参数优化

流程:
  1. 加载茅台(600519.SH) + 中芯国际(688981.SH), 计算特征与标签（与 3-XGBoost涨跌预测.py 一致）
  2. LightGBM 默认参数滚动预测
  3. Optuna 50次搜索最优超参(目标: 最大化 Purged K-Fold AUC)
  4. 最优参数重新滚动预测
  5. Purged K-Fold 交叉验证各折详情
  6. XGBoost vs LightGBM 对比表
"""

import time
import numpy as np
import pandas as pd
import optuna
from sklearn.metrics import confusion_matrix

from data_loader import load_stock_data
from feature_engine import calc_features, preprocess_features, get_all_feature_cols
from ml_engine import (make_labels, rolling_train_predict, evaluate_classification,
                       purged_kfold_cv)

optuna.logging.set_verbosity(optuna.logging.WARNING)


# ============================================================
# 配置
# ============================================================

STOCK_CODES = [
    '159608.SZ', '159583.SZ', '159560.SZ', '159133.SZ', '159796.SZ',  # ETF
    '603019.SH', '002847.SZ', '603259.SH', '000981.SZ', '002714.SZ', '002115.SZ',
]
STOCKS = {code: code for code in STOCK_CODES}

START_DATE = '2025-01-01'
END_DATE = None


# ============================================================
# 数据准备 (与脚本3共用逻辑, 保持一致)
# ============================================================

def prepare_data(stock_code, stock_name):
    """加载数据 -> 特征工程 -> 标签构建, 返回可用于训练的 DataFrame 及特征列"""

    print(f"\n  加载 {stock_name}({stock_code}) ...")
    df = load_stock_data(stock_code, START_DATE, END_DATE)
    print(f"    交易日: {len(df)}, 价格区间: {df['close'].min():.2f} ~ {df['close'].max():.2f}")

    df = calc_features(df)
    feature_cols = get_all_feature_cols()
    feature_cols = [c for c in feature_cols if c in df.columns]
    df = preprocess_features(df, feature_cols)

    df['label'] = make_labels(df, horizon=1, method='binary')
    df.dropna(subset=['label'], inplace=True)

    return df, feature_cols


# ============================================================
# 滚动预测并计时
# ============================================================

def run_rolling(df, feature_cols, model_type, params=None, verbose=True):
    """执行滚动预测并返回结果 + 耗时(秒)"""

    df_reset = df.reset_index()
    t0 = time.time()
    pred_df = rolling_train_predict(
        df_reset, feature_cols, label_col='label',
        model_type=model_type, train_days=120, retrain_interval=20,
        params=params, verbose=verbose,
    )
    elapsed = time.time() - t0

    metrics = evaluate_classification(
        pred_df['y_true'].values,
        pred_df['y_pred'].values,
        pred_df['y_prob'].values,
    )
    return pred_df, metrics, elapsed


# ============================================================
# Optuna 超参数优化
# ============================================================

def optuna_lgb_search(X, y, n_trials=50):
    """
    用 Optuna 搜索 LightGBM 最优超参数
    目标: 最大化 Purged K-Fold 平均 AUC
    """

    def objective(trial):
        params = {
            'num_leaves':        trial.suggest_int('num_leaves', 15, 63),
            'learning_rate':     trial.suggest_float('learning_rate', 0.01, 0.15, log=True),
            'max_depth':         trial.suggest_int('max_depth', 3, 8),
            'min_child_samples': trial.suggest_int('min_child_samples', 10, 50),
            'subsample':         trial.suggest_float('subsample', 0.6, 1.0),
            'colsample_bytree':  trial.suggest_float('colsample_bytree', 0.6, 1.0),
            'reg_alpha':         trial.suggest_float('reg_alpha', 0.0, 1.0),
            'reg_lambda':        trial.suggest_float('reg_lambda', 0.0, 1.0),
            'n_estimators': 200,
            'random_state': 42,
            'verbose': -1,
        }

        fold_results = purged_kfold_cv(X, y, model_type='lightgbm',
                                        n_splits=5, gap=5, params=params)
        if not fold_results:
            return 0.5

        mean_auc = np.mean([f['auc'] for f in fold_results])
        return mean_auc

    study = optuna.create_study(direction='maximize',
                                 sampler=optuna.samplers.TPESampler(seed=42))
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    return study


# ============================================================
# 主程序
# ============================================================

def main():
    print("=" * 70)
    print("  LightGBM 对比实验与 Optuna 超参数优化")
    print("=" * 70)
    print(f"  数据区间: {START_DATE} ~ {END_DATE or '最新'}")
    print(f"  目标股票: {', '.join(f'{v}({k})' for k,v in STOCKS.items())}")

    # ----------------------------------------------------------
    # 第一部分: 数据准备
    # ----------------------------------------------------------
    print(f"\n{'='*70}")
    print("  [第一部分] 数据准备")
    print(f"{'='*70}")

    stock_data = {}
    for code, name in STOCKS.items():
        df, feature_cols = prepare_data(code, name)
        stock_data[code] = {'df': df, 'feature_cols': feature_cols, 'name': name}

    # ----------------------------------------------------------
    # 第二部分: LightGBM 默认参数滚动预测
    # ----------------------------------------------------------
    print(f"\n{'='*70}")
    print("  [第二部分] LightGBM 默认参数 - 滚动预测")
    print(f"{'='*70}")

    lgb_default_results = {}
    for code, info in stock_data.items():
        print(f"\n  --- {info['name']} ({code}) ---")
        pred_df, metrics, elapsed = run_rolling(
            info['df'], info['feature_cols'], model_type='lightgbm')
        lgb_default_results[code] = {
            'pred_df': pred_df, 'metrics': metrics, 'elapsed': elapsed}
        print(f"    AUC={metrics['auc']:.4f}  Acc={metrics['accuracy']:.4f}  "
              f"F1={metrics['f1']:.4f}  耗时={elapsed:.1f}s")

    # ----------------------------------------------------------
    # 第三部分: XGBoost 滚动预测 (用于后续对比)
    # ----------------------------------------------------------
    print(f"\n{'='*70}")
    print("  [第三部分] XGBoost 滚动预测 (对比基准)")
    print(f"{'='*70}")

    xgb_results = {}
    for code, info in stock_data.items():
        print(f"\n  --- {info['name']} ({code}) ---")
        pred_df, metrics, elapsed = run_rolling(
            info['df'], info['feature_cols'], model_type='xgboost')
        xgb_results[code] = {
            'pred_df': pred_df, 'metrics': metrics, 'elapsed': elapsed}
        print(f"    AUC={metrics['auc']:.4f}  Acc={metrics['accuracy']:.4f}  "
              f"F1={metrics['f1']:.4f}  耗时={elapsed:.1f}s")

    # ----------------------------------------------------------
    # 第四部分: Optuna 超参数搜索 (以第一只标的为主)
    # ----------------------------------------------------------
    ref_code = STOCK_CODES[0]
    ref_info = stock_data[ref_code]

    print(f"\n{'='*70}")
    print(f"  [第四部分] Optuna 超参数优化 ({ref_info['name']})")
    print(f"{'='*70}")
    print(f"  搜索空间: num_leaves/learning_rate/max_depth/min_child_samples/")
    print(f"            subsample/colsample_bytree/reg_alpha/reg_lambda")
    print(f"  目标: 最大化 Purged K-Fold AUC (n_splits=5, gap=5)")
    print(f"  试验次数: 50")

    df_ref = ref_info['df']
    fc = ref_info['feature_cols']
    df_clean = df_ref.dropna(subset=fc + ['label'])
    X_all = df_clean[fc].values
    y_all = df_clean['label'].values

    t0 = time.time()
    study = optuna_lgb_search(X_all, y_all, n_trials=50)
    search_time = time.time() - t0

    best = study.best_params
    print(f"\n  搜索完成, 耗时 {search_time:.1f}s")
    print(f"  最优 AUC: {study.best_value:.4f}")
    print(f"  最优参数:")
    for k, v in best.items():
        if isinstance(v, float):
            print(f"    {k:<22s} = {v:.6f}")
        else:
            print(f"    {k:<22s} = {v}")

    # ----------------------------------------------------------
    # 第五部分: 最优参数重新滚动预测
    # ----------------------------------------------------------
    print(f"\n{'='*70}")
    print("  [第五部分] 最优参数 LightGBM - 滚动预测")
    print(f"{'='*70}")

    best_params = {**best, 'n_estimators': 200, 'random_state': 42, 'verbose': -1}

    lgb_tuned_results = {}
    for code, info in stock_data.items():
        print(f"\n  --- {info['name']} ({code}) ---")
        pred_df, metrics, elapsed = run_rolling(
            info['df'], info['feature_cols'],
            model_type='lightgbm', params=best_params)
        lgb_tuned_results[code] = {
            'pred_df': pred_df, 'metrics': metrics, 'elapsed': elapsed}
        print(f"    AUC={metrics['auc']:.4f}  Acc={metrics['accuracy']:.4f}  "
              f"F1={metrics['f1']:.4f}  耗时={elapsed:.1f}s")

    # ----------------------------------------------------------
    # 第六部分: Purged K-Fold 交叉验证详情
    # ----------------------------------------------------------
    print(f"\n{'='*70}")
    print(f"  [第六部分] Purged K-Fold 交叉验证 (n_splits=5, gap=5)")
    print(f"{'='*70}")

    for code, info in stock_data.items():
        df_c = info['df'].dropna(subset=info['feature_cols'] + ['label'])
        X = df_c[info['feature_cols']].values
        y = df_c['label'].values

        print(f"\n  --- {info['name']} ({code}) ---")
        fold_results = purged_kfold_cv(X, y, model_type='lightgbm',
                                        n_splits=5, gap=5, params=best_params)

        print(f"    {'Fold':>6s} {'AUC':>8s} {'Accuracy':>10s} {'Precision':>10s} {'Recall':>8s} {'F1':>8s}")
        print(f"    {'-'*52}")
        for fr in fold_results:
            print(f"    {fr['fold']:>6d} {fr['auc']:>8.4f} {fr['accuracy']:>10.4f} "
                  f"{fr['precision']:>10.4f} {fr['recall']:>8.4f} {fr['f1']:>8.4f}")

        if fold_results:
            avg_auc = np.mean([f['auc'] for f in fold_results])
            avg_acc = np.mean([f['accuracy'] for f in fold_results])
            avg_f1 = np.mean([f['f1'] for f in fold_results])
            print(f"    {'均值':>6s} {avg_auc:>8.4f} {avg_acc:>10.4f} {'':>10s} {'':>8s} {avg_f1:>8.4f}")

    # ----------------------------------------------------------
    # 第七部分: XGBoost vs LightGBM 对比表
    # ----------------------------------------------------------
    print(f"\n{'='*70}")
    print("  [第七部分] XGBoost vs LightGBM 性能对比")
    print(f"{'='*70}")

    print(f"\n  {'股票':<10s} {'模型':<18s} {'AUC':>8s} {'Accuracy':>10s} {'F1':>8s} {'训练时间(s)':>12s}")
    print(f"  {'-'*68}")

    for code, info in stock_data.items():
        name = info['name']
        xm = xgb_results[code]['metrics']
        xt = xgb_results[code]['elapsed']
        print(f"  {name:<10s} {'XGBoost':<18s} "
              f"{xm['auc']:>8.4f} {xm['accuracy']:>10.4f} {xm['f1']:>8.4f} {xt:>12.1f}")

        lm_d = lgb_default_results[code]['metrics']
        lt_d = lgb_default_results[code]['elapsed']
        print(f"  {'':10s} {'LightGBM(默认)':<18s} "
              f"{lm_d['auc']:>8.4f} {lm_d['accuracy']:>10.4f} {lm_d['f1']:>8.4f} {lt_d:>12.1f}")

        lm_t = lgb_tuned_results[code]['metrics']
        lt_t = lgb_tuned_results[code]['elapsed']
        print(f"  {'':10s} {'LightGBM(调优)':<18s} "
              f"{lm_t['auc']:>8.4f} {lm_t['accuracy']:>10.4f} {lm_t['f1']:>8.4f} {lt_t:>12.1f}")
        print()

    # 汇总平均
    avg_xgb_auc = np.mean([xgb_results[c]['metrics']['auc'] for c in STOCKS])
    avg_xgb_time = np.mean([xgb_results[c]['elapsed'] for c in STOCKS])
    avg_lgb_d_auc = np.mean([lgb_default_results[c]['metrics']['auc'] for c in STOCKS])
    avg_lgb_d_time = np.mean([lgb_default_results[c]['elapsed'] for c in STOCKS])
    avg_lgb_t_auc = np.mean([lgb_tuned_results[c]['metrics']['auc'] for c in STOCKS])
    avg_lgb_t_time = np.mean([lgb_tuned_results[c]['elapsed'] for c in STOCKS])

    print(f"  平均 AUC 对比:")
    print(f"    XGBoost:         {avg_xgb_auc:.4f}  (平均耗时 {avg_xgb_time:.1f}s)")
    print(f"    LightGBM(默认):  {avg_lgb_d_auc:.4f}  (平均耗时 {avg_lgb_d_time:.1f}s)")
    print(f"    LightGBM(调优):  {avg_lgb_t_auc:.4f}  (平均耗时 {avg_lgb_t_time:.1f}s)")

    speed_ratio = avg_xgb_time / avg_lgb_d_time if avg_lgb_d_time > 0 else 0
    print(f"\n  训练速度: LightGBM 比 XGBoost 快约 {speed_ratio:.1f} 倍")

    # ----------------------------------------------------------
    # 结论
    # ----------------------------------------------------------
    print(f"\n{'='*70}")
    print("  分析洞察")
    print(f"{'='*70}")
    print(f"\n  Optuna调优后 LightGBM AUC: {avg_lgb_d_auc:.4f} -> {avg_lgb_t_auc:.4f}")
    print(f"  LightGBM 训练速度约为 XGBoost 的 {speed_ratio:.1f} 倍")


if __name__ == '__main__':
    main()
