# -*- coding: utf-8 -*-
"""
MASTER截面预测 - 使用论文预训练模型进行截面预测与IC评估

本脚本:
  1. 加载论文作者提供的预训练MASTER模型权重
  2. 加载论文配套的CSI300/CSI800测试数据(Alpha158因子 + 63维市场信息)
  3. 用预训练模型对测试集进行截面预测
  4. 计算IC/ICIR/RankIC/RankICIR指标, 与论文Table 1对比
  5. 可视化逐日IC分布和预测效果

MASTER论文: Li et al., "MASTER: Market-Guided Stock Transformer" (AAAI 2024)

数据来源: 论文作者提供的opensource数据集
  - 训练集: 2008Q1 ~ 2020Q1
  - 验证集: 2020Q2
  - 测试集: 2020Q3 ~ 2022Q4

依赖: pip install pyqlib (仅用于加载pickle数据格式, 不需要初始化Qlib)
"""

import sys
import os
import pickle
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import spearmanr

# 添加MASTER源码路径
_MASTER_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'MASTER-master')
sys.path.insert(0, _MASTER_DIR)

from master import MASTERModel


# ============================================================
# 配置
# ============================================================

# 数据集: 'csi300' 或 'csi800'
UNIVERSE = 'csi300'

# 数据路径 (opensource数据已复制到 MASTER-master/data/opensource/)
DATA_DIR = os.path.join(_MASTER_DIR, 'data', 'opensource')
MODEL_DIR = os.path.join(_MASTER_DIR, 'model')

# 数据来源标识 (与模型权重文件名对应)
PREFIX = 'opensource'

# MASTER超参数 (与论文一致)
D_FEAT = 158          # Alpha158因子数量
D_MODEL = 256         # Transformer隐藏层维度
T_NHEAD = 4           # TAttention头数 (N1=4)
S_NHEAD = 2           # SAttention头数 (N2=2)
DROPOUT = 0.5
GATE_INPUT_START = 158  # 市场信息在特征中的起始位置
GATE_INPUT_END = 221    # 市场信息在特征中的结束位置 (158+63=221)

# CSI300用beta=5(因子质量高), CSI800用beta=2(噪声大需要更强筛选)
BETA = 5 if UNIVERSE == 'csi300' else 2

# 论文参考值 (Table 1)
PAPER_REFERENCE = {
    'csi300': {'IC': 0.064, 'ICIR': 0.42, 'RankIC': 0.076, 'RankICIR': 0.49},
    'csi800': {'IC': 0.052, 'ICIR': 0.40, 'RankIC': 0.066, 'RankICIR': 0.48},
}


def calc_ic(pred, label):
    """计算IC(Pearson相关)和RankIC(Spearman相关)"""
    df = pd.DataFrame({'pred': pred, 'label': label})
    df = df.dropna()
    if len(df) < 5:
        return np.nan, np.nan
    ic = df['pred'].corr(df['label'])
    ric = df['pred'].corr(df['label'], method='spearman')
    return ic, ric


# ============================================================
# 1. 加载数据
# ============================================================
print("=" * 70)
print(f"  MASTER截面预测 - {UNIVERSE.upper()}")
print(f"  beta={BETA}, d_model={D_MODEL}, T_nhead={T_NHEAD}, S_nhead={S_NHEAD}")
print("=" * 70)

test_path = os.path.join(DATA_DIR, f'{UNIVERSE}_dl_test.pkl')
print(f"\n[1] 加载测试数据: {test_path}")

with open(test_path, 'rb') as f:
    dl_test = pickle.load(f)

test_index = dl_test.get_index()
dates = test_index.get_level_values('datetime')
instruments = test_index.get_level_values('instrument')

print(f"    样本数: {len(dl_test)}")
print(f"    股票数: {instruments.nunique()}")
print(f"    日期范围: {dates.min().strftime('%Y-%m-%d')} -> {dates.max().strftime('%Y-%m-%d')}")
print(f"    交易日数: {dates.nunique()}")
print(f"    每样本维度: {dl_test[0].shape} (T=8天, F=222=158因子+63市场+1标签)")


# ============================================================
# 2. 加载预训练模型
# ============================================================
model_path = os.path.join(MODEL_DIR, f'{UNIVERSE}_{PREFIX}_0.pkl')
print(f"\n[2] 加载预训练模型: {os.path.basename(model_path)}")

model = MASTERModel(
    d_feat=D_FEAT, d_model=D_MODEL,
    t_nhead=T_NHEAD, s_nhead=S_NHEAD,
    T_dropout_rate=DROPOUT, S_dropout_rate=DROPOUT,
    beta=BETA,
    gate_input_start_index=GATE_INPUT_START,
    gate_input_end_index=GATE_INPUT_END,
    n_epochs=1, lr=1e-5, GPU=0, seed=0,
    train_stop_loss_thred=0.95,
    save_path=MODEL_DIR, save_prefix=f'{UNIVERSE}_{PREFIX}'
)
model.load_param(model_path)
print(f"    模型参数量: {sum(p.numel() for p in model.model.parameters()):,}")
print(f"    运行设备: {model.device}")


# ============================================================
# 3. 运行截面预测
# ============================================================
print(f"\n[3] 运行截面预测...")

import torch
from torch.utils.data import DataLoader

# 使用base_model中的DailyBatchSampler逐日预测
from base_model import DailyBatchSamplerRandom, zscore, drop_na

sampler = DailyBatchSamplerRandom(dl_test, shuffle=False)
test_loader = DataLoader(dl_test, sampler=sampler, drop_last=False)

daily_ic_list = []
daily_ric_list = []
daily_dates = []
all_preds = []
all_labels = []

model.model.eval()
with torch.no_grad():
    for batch_idx, data in enumerate(test_loader):
        data = torch.squeeze(data, dim=0)
        feature = data[:, :, 0:-1].to(model.device)
        label = data[:, -1, -1].numpy()

        pred = model.model(feature.float()).detach().cpu().numpy().ravel()

        daily_ic, daily_ric = calc_ic(pred, label)
        daily_ic_list.append(daily_ic)
        daily_ric_list.append(daily_ric)
        all_preds.extend(pred)
        all_labels.extend(label)

        if (batch_idx + 1) % 100 == 0:
            print(f"    已处理 {batch_idx + 1} 个交易日...")

# 获取每日的日期
daily_counts = pd.Series(index=test_index).groupby("datetime").size()
daily_dates = daily_counts.index.tolist()

print(f"    预测完成! 共 {len(daily_dates)} 个交易日")


# ============================================================
# 4. 计算整体指标
# ============================================================
print(f"\n[4] 评估指标计算")

# 过滤NaN
ic_arr = np.array(daily_ic_list)
ric_arr = np.array(daily_ric_list)
valid_ic = ic_arr[~np.isnan(ic_arr)]
valid_ric = ric_arr[~np.isnan(ric_arr)]

metrics = {
    'IC': np.mean(valid_ic),
    'ICIR': np.mean(valid_ic) / np.std(valid_ic),
    'RankIC': np.mean(valid_ric),
    'RankICIR': np.mean(valid_ric) / np.std(valid_ric),
}

ref = PAPER_REFERENCE[UNIVERSE]

print(f"\n{'='*70}")
print(f"  MASTER {UNIVERSE.upper()} 截面预测结果")
print(f"{'='*70}")
print(f"{'指标':<12}{'本次结果':>12}{'论文参考值':>12}{'差异':>12}")
print(f"{'-'*48}")
print(f"{'IC':<12}{metrics['IC']:>12.4f}{ref['IC']:>12.4f}{metrics['IC']-ref['IC']:>+12.4f}")
print(f"{'ICIR':<12}{metrics['ICIR']:>12.4f}{ref['ICIR']:>12.4f}{metrics['ICIR']-ref['ICIR']:>+12.4f}")
print(f"{'RankIC':<12}{metrics['RankIC']:>12.4f}{ref['RankIC']:>12.4f}{metrics['RankIC']-ref['RankIC']:>+12.4f}")
print(f"{'RankICIR':<12}{metrics['RankICIR']:>12.4f}{ref['RankICIR']:>12.4f}{metrics['RankICIR']-ref['RankICIR']:>+12.4f}")
print(f"\n注: 论文Table 1为5次随机种子的平均值, 本次仅用seed=0的单次结果, 存在差异属正常。")


# ============================================================
# 5. IC正比例和统计信息
# ============================================================
print(f"\n[5] IC统计分析")

ic_positive_rate = np.mean(valid_ic > 0) * 100
ric_positive_rate = np.mean(valid_ric > 0) * 100

print(f"    IC > 0 的比例:     {ic_positive_rate:.1f}% ({int(np.sum(valid_ic > 0))}/{len(valid_ic)} 天)")
print(f"    RankIC > 0 的比例: {ric_positive_rate:.1f}% ({int(np.sum(valid_ric > 0))}/{len(valid_ric)} 天)")
print(f"    IC 中位数:         {np.median(valid_ic):.4f}")
print(f"    IC 标准差:         {np.std(valid_ic):.4f}")
print(f"    IC 最大值:         {np.max(valid_ic):.4f}")
print(f"    IC 最小值:         {np.min(valid_ic):.4f}")


# ============================================================
# 6. 可视化
# ============================================================
print(f"\n[6] 生成可视化图表...")

plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False

fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle(f'MASTER {UNIVERSE.upper()} 截面预测评估', fontsize=16, fontweight='bold')

# 6a: 逐日IC时序图
ax = axes[0, 0]
ax.bar(range(len(valid_ic)), valid_ic, color=['#27ae60' if x > 0 else '#e74c3c' for x in valid_ic],
       alpha=0.6, width=1.0)
ax.axhline(y=0, color='black', linewidth=0.5)
ax.axhline(y=metrics['IC'], color='#3498db', linewidth=2, linestyle='--',
           label=f"IC Mean = {metrics['IC']:.4f}")
ax.set_title('逐日IC (Pearson相关)')
ax.set_xlabel('交易日')
ax.set_ylabel('IC')
ax.legend(loc='upper right')

# 6b: 逐日RankIC时序图
ax = axes[0, 1]
ax.bar(range(len(valid_ric)), valid_ric, color=['#27ae60' if x > 0 else '#e74c3c' for x in valid_ric],
       alpha=0.6, width=1.0)
ax.axhline(y=0, color='black', linewidth=0.5)
ax.axhline(y=metrics['RankIC'], color='#3498db', linewidth=2, linestyle='--',
           label=f"RankIC Mean = {metrics['RankIC']:.4f}")
ax.set_title('逐日RankIC (Spearman相关)')
ax.set_xlabel('交易日')
ax.set_ylabel('RankIC')
ax.legend(loc='upper right')

# 6c: IC分布直方图
ax = axes[1, 0]
ax.hist(valid_ic, bins=50, color='#3498db', alpha=0.7, edgecolor='white', label='IC')
ax.hist(valid_ric, bins=50, color='#e67e22', alpha=0.5, edgecolor='white', label='RankIC')
ax.axvline(x=0, color='black', linewidth=1)
ax.axvline(x=metrics['IC'], color='#3498db', linewidth=2, linestyle='--')
ax.axvline(x=metrics['RankIC'], color='#e67e22', linewidth=2, linestyle='--')
ax.set_title('IC / RankIC 分布')
ax.set_xlabel('IC值')
ax.set_ylabel('频数')
ax.legend()

# 6d: 累计IC曲线
ax = axes[1, 1]
cumsum_ic = np.cumsum(valid_ic)
cumsum_ric = np.cumsum(valid_ric)
ax.plot(cumsum_ic, color='#3498db', linewidth=1.5, label='Cumulative IC')
ax.plot(cumsum_ric, color='#e67e22', linewidth=1.5, label='Cumulative RankIC')
ax.set_title('累计IC曲线 (向上倾斜=持续有效)')
ax.set_xlabel('交易日')
ax.set_ylabel('累计IC')
ax.legend()
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(os.path.join(os.path.dirname(os.path.abspath(__file__)),
            f'MASTER_{UNIVERSE}_prediction_results.png'), dpi=150, bbox_inches='tight')
plt.show()


# ============================================================
# 7. 预测结果分析: 样本日期的截面排名
# ============================================================
print(f"\n[7] 截面预测示例 (展示3个样本日期的预测排名)")

predictions_series = pd.Series(np.array(all_preds), index=test_index)
labels_series = pd.Series(np.array(all_labels), index=test_index)

sample_dates = daily_dates[::len(daily_dates) // 3][:3]

for dt in sample_dates:
    day_pred = predictions_series.xs(dt, level='datetime')
    day_label = labels_series.xs(dt, level='datetime')

    day_df = pd.DataFrame({
        'pred_score': day_pred,
        'actual_ret': day_label,
        'pred_rank': day_pred.rank(ascending=False),
        'actual_rank': day_label.rank(ascending=False),
    }).dropna()

    if len(day_df) == 0:
        continue

    day_ic = day_df['pred_score'].corr(day_df['actual_ret'])
    day_ric = day_df['pred_score'].corr(day_df['actual_ret'], method='spearman')

    print(f"\n  日期: {dt.strftime('%Y-%m-%d')} | 股票数: {len(day_df)} | IC={day_ic:.4f} | RankIC={day_ric:.4f}")
    print(f"  预测排名前5 (最看好):")
    top5 = day_df.nsmallest(5, 'pred_rank')
    for stock, row in top5.iterrows():
        print(f"    {stock:<12} 预测排名: {int(row['pred_rank']):>3}  实际排名: {int(row['actual_rank']):>3}")

    print(f"  预测排名后5 (最不看好):")
    bottom5 = day_df.nlargest(5, 'pred_rank')
    for stock, row in bottom5.iterrows():
        print(f"    {stock:<12} 预测排名: {int(row['pred_rank']):>3}  实际排名: {int(row['actual_rank']):>3}")


# ============================================================
# 8. 与XGBoost基线对比总结
# ============================================================
print(f"\n{'='*70}")
print(f"  MASTER vs XGBoost (论文Table 1, {UNIVERSE.upper()})")
print(f"{'='*70}")

xgb_ref = {
    'csi300': {'IC': 0.051, 'ICIR': 0.37, 'RankIC': 0.050, 'RankICIR': 0.36},
    'csi800': {'IC': 0.040, 'ICIR': 0.37, 'RankIC': 0.047, 'RankICIR': 0.42},
}
xgb = xgb_ref[UNIVERSE]

print(f"{'指标':<12}{'MASTER(本次)':>14}{'MASTER(论文)':>14}{'XGBoost(论文)':>14}{'提升':>10}")
print(f"{'-'*64}")
for key, xkey in [('IC', 'IC'), ('ICIR', 'ICIR'), ('RankIC', 'RankIC'), ('RankICIR', 'RankICIR')]:
    m_val = metrics[key]
    m_ref = ref[key]
    x_val = xgb[xkey]
    pct = (m_ref - x_val) / x_val * 100
    print(f"{key:<12}{m_val:>14.4f}{m_ref:>14.4f}{x_val:>14.4f}{pct:>+9.1f}%")

print(f"\n结论: MASTER通过Transformer架构(TAttention+SAttention+Gate)在截面预测上")
print(f"      显著优于传统XGBoost, 验证了深度学习在股票预测中的优势。")
print(f"\n提示: 修改脚本顶部 UNIVERSE='csi800' 可切换到CSI800数据集。")
