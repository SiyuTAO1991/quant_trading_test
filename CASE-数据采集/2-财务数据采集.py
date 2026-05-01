# -*- coding: utf-8 -*-
"""
财务数据采集 - 使用AkShare下载全量A股财务数据存入MySQL

从AkShare获取三大财务报表（利润表、资产负债表、现金流量表），
提取并计算常用财务指标，写入 trade_stock_financial 表。

功能：
  1. 获取沪深A股全量股票列表
  2. 跳过数据库中已有财务数据的股票（断点续传）
  3. 批量下载（每批20只）
  4. 提取ROE/毛利率/资产负债率等核心指标
  5. 写入MySQL（ON DUPLICATE KEY UPDATE）

模式：
  - TEST_MODE = True  -> 只采集1只股票(贵州茅台)
  - TEST_MODE = False -> 采集沪深A股全量

运行：python 2-财务数据采集.py
环境：pip install akshare pymysql python-dotenv
"""
import sys
import os
import time
from datetime import datetime, date

import pandas as pd
import akshare as ak

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_config import get_connection, execute_query

if sys.platform == 'win32' and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# ============================================================
# 配置
# ============================================================
TEST_MODE = True
# TEST_STOCK 已改为测试多只股票列表
TEST_STOCKS = [
    '600406.SH',
    '513120.SH',
    '515120.SH',
    '159887.SZ',
    '516650.SH',
    '159560.SZ',
    '159583.SZ'
]

SECTOR = '沪深A股'
BATCH_SIZE = 20
DATA_START = '20200101'
DATA_END = date.today().strftime('%Y%m%d')

INSERT_SQL = """
    INSERT INTO trade_stock_financial
    (stock_code, report_date, revenue, net_profit, eps, roe, roa,
     gross_margin, net_margin, debt_ratio, current_ratio,
     operating_cashflow, total_assets, total_equity, total_shares, data_source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    revenue=VALUES(revenue), net_profit=VALUES(net_profit), eps=VALUES(eps),
    roe=VALUES(roe), roa=VALUES(roa), gross_margin=VALUES(gross_margin),
    net_margin=VALUES(net_margin), debt_ratio=VALUES(debt_ratio),
    current_ratio=VALUES(current_ratio), operating_cashflow=VALUES(operating_cashflow),
    total_assets=VALUES(total_assets), total_equity=VALUES(total_equity),
    total_shares=VALUES(total_shares)
"""


# ============================================================
# 工具函数
# ============================================================

def get_existing_stocks():
    """查询数据库中已有财务数据的股票集合"""
    rows = execute_query("SELECT DISTINCT stock_code FROM trade_stock_financial")
    return {r['stock_code'] for r in rows}


def safe_float(val):
    """安全转换为浮点数"""
    if val is None:
        return None
    try:
        if str(val).strip() in ('', '--', 'None', 'nan'):
            return None
        v = float(val)
        return v if v == v else None
    except (ValueError, TypeError):
        return None


def safe_divide(a, b, pct=False):
    """安全除法，b为0时返回None。pct=True时结果乘以100"""
    if a is None or b is None:
        return None
    a, b = float(a), float(b)
    if b == 0:
        return None
    result = a / b
    if pct:
        result *= 100
    return round(result, 4)


def get_col(row, col_names, default=None):
    """
    从行中获取字段值，支持多个候选列名。
    使用 'in' 进行模糊匹配，避免列名包含额外字符时匹配不上。
    """
    for name in col_names:
        if name in row.index:
            val = safe_float(row[name])
            if val is not None:
                return val
        for col in row.index:
            if name in str(col):
                val = safe_float(row[col])
                if val is not None:
                    return val
    return default


def ts_code_to_sina(ts_code):
    """将 Tushare 格式的股票代码转换为新浪格式"""
    if '.' not in ts_code:
        return ts_code
    code, market = ts_code.split('.')
    if market.upper() == 'SH':
        return f'sh{code}'
    elif market.upper() == 'SZ':
        return f'sz{code}'
    return ts_code


# ============================================================
# 核心逻辑
# ============================================================

def extract_from_akshare(stock_code, df_income, df_balance, df_cashflow, df_share):
    """从AkShare财务报表中提取财务指标"""
    records = []
    
    # 获取总股本
    total_shares = None
    if df_share is not None and len(df_share) > 0:
        # 尝试获取最新的总股本
        total_shares = safe_float(df_share.iloc[0].get('总股本'))
        if total_shares is None:
            total_shares = safe_float(df_share.iloc[0].get('流通股'))

    # 标准化日期列并返回按日期索引的数据
    def normalize_and_index(df):
        if df is None or len(df) == 0:
            return {}
        date_col_name = df.columns[0]
        df = df.copy()
        df['_date'] = df[date_col_name].astype(str).str.replace('-', '').str[:8]
        result_map = {}
        for _, row in df.iterrows():
            period = row['_date']
            if period and len(period) == 8 and period.isdigit():
                if period >= DATA_START:
                    result_map[period] = row
        return result_map

    income_map = normalize_and_index(df_income)
    balance_map = normalize_and_index(df_balance)
    cashflow_map = normalize_and_index(df_cashflow) if df_cashflow is not None else {}

    # 取所有报告期的交集
    all_periods = set(income_map.keys()) & set(balance_map.keys())
    if cashflow_map:
        all_periods = all_periods & set(cashflow_map.keys())
    all_periods = sorted(all_periods)

    for period in all_periods:
        inc = income_map.get(period)
        bal = balance_map.get(period)
        cf = cashflow_map.get(period) if cashflow_map else None

        if inc is None or bal is None:
            continue

        # --- 从利润表提取 ---
        revenue = get_col(inc, ['营业收入', '一、营业收入', '一、营业总收入', '营业总收入'])
        operating_cost = get_col(inc, ['营业成本', '二、营业总成本', '营业总成本'])
        net_profit = get_col(inc, ['净利润', '五、净利润', '四、净利润'])
        net_profit_parent = get_col(inc, ['归属于母公司所有者的净利润', '归属于母公司股东的净利润'])
        operating_profit = get_col(inc, ['营业利润', '三、营业利润'])
        eps = get_col(inc, ['基本每股收益', '（一）基本每股收益'])

        if net_profit is None:
            net_profit = net_profit_parent

        # --- 从资产负债表提取 ---
        total_assets = get_col(bal, ['资产总计', '资产合计'])
        total_liab = get_col(bal, ['负债合计', '负债总计'])
        total_equity = get_col(bal, ['所有者权益合计', '所有者权益（或股东权益）合计',
                                      '股东权益合计', '归属于母公司股东权益合计'])
        current_assets = get_col(bal, ['流动资产合计'])
        current_liab = get_col(bal, ['流动负债合计'])
        inventory = get_col(bal, ['存货'])

        # --- 从现金流量表提取 ---
        operating_cashflow = None
        if cf is not None:
            operating_cashflow = get_col(cf, ['经营活动产生的现金流量净额'])

        # --- 计算财务指标 ---
        grossprofit_margin = None
        if revenue and operating_cost and revenue > 0:
            grossprofit_margin = (revenue - operating_cost) / revenue * 100

        netprofit_margin = safe_divide(net_profit, revenue, True)
        roe = safe_divide(net_profit, total_equity, True)
        roa = safe_divide(net_profit, total_assets, True)
        debt_to_assets = safe_divide(total_liab, total_assets, True)
        current_ratio = safe_divide(current_assets, current_liab)

        records.append({
            'report_date': period,
            'revenue': revenue,
            'net_profit': net_profit,
            'eps': eps,
            'roe': roe,
            'roa': roa,
            'gross_margin': grossprofit_margin,
            'net_margin': netprofit_margin,
            'debt_ratio': debt_to_assets,
            'current_ratio': current_ratio,
            'operating_cashflow': operating_cashflow,
            'total_assets': total_assets,
            'total_equity': total_equity,
            'total_shares': total_shares,
        })

    return records


def process_batch(batch_codes):
    """批量下载 + 解析 + 写DB，返回 (写入总行数, 成功股票数)"""
    conn = get_connection()
    cursor = conn.cursor()
    batch_rows = 0
    batch_ok = 0

    for code in batch_codes:
        try:
            sina_code = ts_code_to_sina(code)
            
            df_income = None
            df_balance = None
            df_cashflow = None
            df_share = None

            try:
                df_income = ak.stock_financial_report_sina(stock=sina_code, symbol="利润表")
            except Exception:
                pass

            try:
                df_balance = ak.stock_financial_report_sina(stock=sina_code, symbol="资产负债表")
            except Exception:
                pass

            try:
                df_cashflow = ak.stock_financial_report_sina(stock=sina_code, symbol="现金流量表")
            except Exception:
                pass

            if df_income is None or df_balance is None:
                batch_ok += 1
                time.sleep(0.5)
                continue

            records = extract_from_akshare(code, df_income, df_balance, df_cashflow, df_share)

            if records:
                rows = []
                for rec in records:
                    p = rec['report_date']
                    report_date = f"{p[:4]}-{p[4:6]}-{p[6:8]}"
                    rows.append((
                        code, report_date,
                        rec['revenue'], rec['net_profit'], rec['eps'],
                        rec['roe'], rec['roa'], rec['gross_margin'], rec['net_margin'],
                        rec['debt_ratio'], rec['current_ratio'],
                        rec['operating_cashflow'], rec['total_assets'], rec['total_equity'],
                        rec['total_shares'],
                        'akshare'
                    ))
                cursor.executemany(INSERT_SQL, rows)
                batch_rows += len(rows)
            batch_ok += 1

            time.sleep(0.5)

        except Exception as e:
            print(f"\n  警告: {code} 下载失败: {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()
    return batch_rows, batch_ok


# ============================================================
# 主流程
# ============================================================

def main():
    print("=" * 60)
    print("财务数据采集 (AkShare -> MySQL)")
    if TEST_MODE:
        print("[测试模式] 只采集贵州茅台")
    else:
        print(f"[全量模式] 采集{SECTOR}, 每批{BATCH_SIZE}只")
    print("=" * 60)

    print("\n初始化...")
    print("  初始化成功")

    # 获取股票列表
    if TEST_MODE:
        all_codes = TEST_STOCKS
        print(f"\n[测试模式] 采集 {len(all_codes)} 只股票: {all_codes}")
    else:
        print(f"\n获取 {SECTOR} 股票列表...")
        try:
            stock_list = ak.stock_info_a_code_name()
            all_codes = []
            for _, row in stock_list.iterrows():
                code = row['code']
                if code.startswith('6'):
                    all_codes.append(f"{code}.SH")
                else:
                    all_codes.append(f"{code}.SZ")
            print(f"  共 {len(all_codes)} 只股票")
        except Exception as e:
            print(f"  获取股票列表失败: {e}")
            return

    # 查询已采集的股票，跳过已有数据的
    print("查询数据库已有数据...")
    existing = get_existing_stocks()
    pending = [c for c in all_codes if c not in existing]

    print(f"  已采集: {len(existing)} 只, 待采集: {len(pending)} 只")

    if not pending:
        print("\n全部已采集完成，无需下载")
        _print_summary()
        return

    # 分批处理
    batches = [pending[i:i + BATCH_SIZE] for i in range(0, len(pending), BATCH_SIZE)]
    total_batches = len(batches)
    total_pending = len(pending)

    print(f"\n开始批量下载（共 {total_batches} 批, 每批最多 {BATCH_SIZE} 只）...")

    total_rows = 0
    total_ok = 0
    total_done_stocks = 0
    start_time = time.time()

    for i, batch in enumerate(batches):
        sys.stdout.write(
            f"\r  批次 {i + 1}/{total_batches} 下载中... "
            f"({total_done_stocks}/{total_pending})    "
        )
        sys.stdout.flush()

        batch_rows, batch_ok = process_batch(batch)
        total_rows += batch_rows
        total_ok += batch_ok
        total_done_stocks += len(batch)

        elapsed = time.time() - start_time
        speed = total_done_stocks / elapsed if elapsed > 0 else 0
        eta = (total_pending - total_done_stocks) / speed if speed > 0 else 0

        sys.stdout.write(
            f"\r  批次 {i + 1}/{total_batches} 完成 | "
            f"进度 {total_done_stocks}/{total_pending} ({total_done_stocks * 100 / total_pending:.1f}%) | "
            f"{speed:.1f} 只/秒 | 剩余约 {eta:.0f}秒 | "
            f"写入 {total_rows:,} 条    "
        )
        sys.stdout.flush()

    print()

    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print(f"财务数据采集完成! 耗时 {elapsed:.1f} 秒")
    print(f"  本次处理: {total_ok}/{total_pending} 只股票")
    print(f"  总写入: {total_rows:,} 条记录")

    _print_summary()


def _print_summary():
    summary = execute_query("""
        SELECT COUNT(DISTINCT stock_code) as stock_cnt,
               COUNT(*) as row_cnt,
               MIN(report_date) as min_date, MAX(report_date) as max_date
        FROM trade_stock_financial
    """)
    if summary:
        row = summary[0]
        print(f"\n数据库 trade_stock_financial 概况:")
        print(f"  {row['stock_cnt']} 只股票, {row['row_cnt']:,} 条记录")
        print(f"  日期范围: {row['min_date']} ~ {row['max_date']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
