# -*- coding: utf-8 -*-
"""
多因子选股 - 筛选1（通用版）
与 多因子选股-筛选.py 相同，5 层漏斗 + 绝对阈值，简单输出达标股票。

筛选条件（5 层）：
  1. ROE >= 15%
  2. 净利润同比 >= 10%
  3. 毛利率 >= 30%
  4. 资产负债率 <= 60%
  5. 经营现金流/营收 >= 10%


  1. ROE >= 10%
  2. 净利润同比 >= 10%
  3. 毛利率 >= 30%
  4. 资产负债率 <= 60%
  5. 经营现金流/营收 >= 10%

输入：data/stock_fina_pool_QMT.csv（由 多因子选股-下载数据.py 生成）
输出：data/stock_fina_selected_QMT.csv

运行：python 多因子选股-筛选1.py
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
# 配置（可按需调整阈值）
# ============================================================
ROE_MIN = 10
NETPROFIT_YOY_MIN = 10
GROSSPROFIT_MARGIN_MIN = 30
DEBT_TO_ASSETS_MAX = 60
OCF_TO_REVENUE_MIN = 10

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
INPUT_FILE = os.path.join(DATA_DIR, "stock_fina_pool_QMT.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "stock_fina_selected_QMT.csv")


def main():
    print("多因子选股 - 筛选1（通用版）")
    print(f"ROE >= {ROE_MIN}%  |  净利润同比 >= {NETPROFIT_YOY_MIN}%  |  毛利率 >= {GROSSPROFIT_MARGIN_MIN}%")
    print(f"资产负债率 <= {DEBT_TO_ASSETS_MAX}%  |  经营现金流/营收 >= {OCF_TO_REVENUE_MIN}%")
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

    # 5 层漏斗筛选
    mask = pd.Series(True, index=df.index)

    if 'roe' in df.columns:
        mask &= (df['roe'] >= ROE_MIN)
        print(f"  第1层 ROE >= {ROE_MIN}%: 剩余 {mask.sum()} 只")
    if 'netprofit_yoy' in df.columns:
        mask &= (df['netprofit_yoy'] >= NETPROFIT_YOY_MIN)
        print(f"  第2层 净利润同比 >= {NETPROFIT_YOY_MIN}%: 剩余 {mask.sum()} 只")
    if 'grossprofit_margin' in df.columns:
        mask &= (df['grossprofit_margin'] >= GROSSPROFIT_MARGIN_MIN)
        print(f"  第3层 毛利率 >= {GROSSPROFIT_MARGIN_MIN}%: 剩余 {mask.sum()} 只")
    if 'debt_to_assets' in df.columns:
        mask &= (df['debt_to_assets'] <= DEBT_TO_ASSETS_MAX)
        print(f"  第4层 资产负债率 <= {DEBT_TO_ASSETS_MAX}%: 剩余 {mask.sum()} 只")
    if 'ocf_to_revenue' in df.columns:
        mask &= (df['ocf_to_revenue'] >= OCF_TO_REVENUE_MIN)
        print(f"  第5层 经营现金流/营收 >= {OCF_TO_REVENUE_MIN}%: 剩余 {mask.sum()} 只")

    selected = df[mask].copy()

    # 按 ROE 降序
    if 'roe' in selected.columns:
        selected = selected.sort_values('roe', ascending=False).reset_index(drop=True)

    selected.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print(f"\n筛选完成：共 {len(selected)} 只股票达标")
    print(f"已保存：{OUTPUT_FILE}")

    if len(selected) > 0:
        print("\n" + "=" * 60)
        print("达标股票（按 ROE 排序）：")
        print("=" * 60)
        disp_cols = ['stock_code', 'stock_name', 'end_date', 'roe', 'netprofit_yoy', 'grossprofit_margin',
                     'debt_to_assets', 'ocf_to_revenue']
        disp_cols = [c for c in disp_cols if c in selected.columns]
        print(selected[disp_cols].head(20).to_string(index=False))

    return OUTPUT_FILE


if __name__ == "__main__":
    main()
