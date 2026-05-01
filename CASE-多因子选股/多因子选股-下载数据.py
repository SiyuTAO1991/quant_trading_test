# -*- coding: utf-8 -*-
"""
多因子选股 - 数据下载脚本（AkShare 版）
通过 AkShare 下载指定板块内所有股票的财务数据，提取指标并汇总保存，供筛选脚本使用。
同时获取股票名称，一并写入输出文件。

输出文件：data/stock_fina_pool_QMT.csv（每只股票最新一期财务指标 + 净利润同比 + 名称 + 行业）

运行：python 多因子选股-下载数据.py
"""
import sys
import os
import time
import traceback
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import akshare as ak
import tushare as ts

# Windows 控制台 UTF-8 输出
if sys.platform == 'win32' and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# ============================================================
# 配置
# ============================================================
NUM_WORKERS = 8
DATA_START = '20240101'
DATA_END = date.today().strftime('%Y%m%d')

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "stock_fina_pool_QMT.csv")

# 从 .env 读取 Tushare token
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
from dotenv import dotenv_values
_env = dotenv_values(_env_path)
TUSHARE_TOKEN = _env.get('TUSHARE_TOKEN', '')
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()


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
    """安全除法，b 为 0 时返回 None。"""
    if a is None or b is None or pd.isna(a) or pd.isna(b):
        return None
    try:
        a, b = float(a), float(b)
        if b == 0:
            return None
        result = a / b
        if pct:
            result *= 100
        return round(result, 4)
    except (ValueError, TypeError):
        return None


def get_col(row, col_names, default=None):
    """从行中获取字段值，支持多个候选列名"""
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


def extract_financial_from_akshare(stock_code):
    """从 AkShare 获取单只股票的财务数据并提取指标"""
    records = []
    try:
        sina_code = ts_code_to_sina(stock_code)
        
        df_income = None
        df_balance = None
        df_cashflow = None
        
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
            return records

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
            eps = get_col(inc, ['基本每股收益', '（一）基本每股收益'])

            if net_profit is None:
                net_profit = net_profit_parent

            # --- 从资产负债表提取 ---
            total_assets = get_col(bal, ['资产总计', '总资产'])
            total_liab = get_col(bal, ['负债合计', '总负债'])
            total_equity = get_col(bal, ['所有者权益合计', '所有者权益或股东权益合计', '股东权益合计'])
            current_assets = get_col(bal, ['流动资产合计'])
            current_liab = get_col(bal, ['流动负债合计'])
            inventory = get_col(bal, ['存货'])

            # --- 从现金流量表提取 ---
            operating_cashflow = None
            if cf is not None:
                operating_cashflow = get_col(cf, ['经营活动产生的现金流量净额', '经营活动现金流量净额'])

            # --- 计算指标 ---
            grossprofit_margin = None
            if revenue is not None and operating_cost is not None and revenue > 0:
                grossprofit_margin = safe_divide(revenue - operating_cost, revenue, pct=True)

            netprofit_margin = safe_divide(net_profit, revenue, pct=True)

            roe = safe_divide(net_profit, total_equity, pct=True)
            roa = safe_divide(net_profit, total_assets, pct=True)
            debt_to_assets = safe_divide(total_liab, total_assets, pct=True)
            current_ratio = safe_divide(current_assets, current_liab)
            
            quick_ratio = None
            if current_assets is not None and inventory is not None and current_liab is not None:
                quick_ratio = safe_divide(current_assets - inventory, current_liab)

            assets_turn = safe_divide(revenue, total_assets)
            ocf_to_revenue = safe_divide(operating_cashflow, revenue, pct=True)
            ocf_to_profit = safe_divide(operating_cashflow, net_profit)

            record = {
                'end_date': period,
                'eps': eps,
                'bps': None,
                'ocfps': None,
                'roe': roe,
                'roa': roa,
                'grossprofit_margin': grossprofit_margin,
                'netprofit_margin': netprofit_margin,
                'debt_to_assets': debt_to_assets,
                'current_ratio': current_ratio,
                'quick_ratio': quick_ratio,
                'assets_turn': assets_turn,
                'operating_cashflow': operating_cashflow,
                'ocf_to_revenue': ocf_to_revenue,
                'ocf_to_profit': ocf_to_profit,
                'revenue': revenue,
                'net_profit': net_profit,
                'total_assets': total_assets,
                'total_equity': total_equity,
            }
            records.append(record)

    except Exception as e:
        pass
    
    return records


def calc_netprofit_yoy(records):
    """用年报数据计算最新一期净利润同比增长率"""
    if not records:
        return None
    annual = [r for r in records if str(r.get('end_date', '')).endswith('1231')]
    if len(annual) < 2:
        return None
    annual = sorted(annual, key=lambda x: x['end_date'])
    profits = []
    for r in annual:
        p = r.get('net_profit')
        if p is not None and not pd.isna(p):
            try:
                profits.append(float(p))
            except Exception:
                pass
    if len(profits) < 2:
        return None
    s = pd.Series(profits)
    yoy = s.pct_change().iloc[-1] * 100
    return round(yoy, 2)


def get_stock_list():
    """获取沪深A股股票列表（过滤ETF和基金）"""
    stock_list = []
    stock_name_map = {}
    stock_industry_map = {}
    
    try:
        # 使用 AkShare 获取股票列表
        df_stock = ak.stock_info_a_code_name()
        for _, row in df_stock.iterrows():
            code = row['code']
            name = row['name']
            
            # 过滤规则：
            # - 保留：6开头（沪市主板）、0开头（深市主板、中小板）、3开头（创业板）
            # - 排除：5开头（ETF）、1开头（基金、ETF）、8开头（北交所）
            if code.startswith('6') or code.startswith('0') or code.startswith('3'):
                if code.startswith('6'):
                    ts_code = f"{code}.SH"
                else:
                    ts_code = f"{code}.SZ"
                stock_list.append(ts_code)
                stock_name_map[ts_code] = name
    except Exception as e:
        print(f"获取股票列表异常: {e}")
    
    return stock_list, stock_name_map, stock_industry_map


def download_one_stock(stock_code, name_map=None, industry_map=None):
    """下载单只股票的财务数据并提取最新一期指标"""
    records = extract_financial_from_akshare(stock_code)
    
    if not records:
        return None
        
    records = sorted(records, key=lambda x: x['end_date'])
    latest = records[-1].copy()
    latest['netprofit_yoy'] = calc_netprofit_yoy(records)
    latest['stock_code'] = stock_code
    latest['stock_name'] = name_map.get(stock_code, '') if name_map else ''
    latest['industry'] = ''
    
    return latest


def main():
    print("=" * 60)
    print("多因子选股 - 财务数据下载（AkShare）")
    print(f"日期：{DATA_START} ~ {DATA_END}")
    print("=" * 60)

    try:
        print("\n获取股票列表...")
        stock_list, stock_name_map, stock_industry_map = get_stock_list()
        if not stock_list:
            print("错误：未获取到股票列表")
            return None

        # 下载所有股票
        # stock_list = stock_list[:100]
        total = len(stock_list)
        print(f"共 {total} 只股票待下载")

        pool = []
        failed = []
        start_time = time.time()

        print(f"\n并行下载（{NUM_WORKERS} 线程）...")
        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = {executor.submit(download_one_stock, code, stock_name_map, stock_industry_map): code for code in stock_list}
            done = 0
            for future in as_completed(futures):
                code, row = futures[future], future.result()
                if row:
                    pool.append(row)
                else:
                    failed.append(code)
                done += 1
                elapsed = time.time() - start_time
                pct = done * 100 / total
                speed = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / speed if speed > 0 else 0
                sys.stdout.write(f"\r获取财务 {done}/{total} ({pct:.1f}%) | {speed:.1f} 只/秒 | 剩余约 {eta:.0f} 秒 | 成功 {len(pool)} 只    ")
                sys.stdout.flush()
        elapsed = time.time() - start_time
        print(f"\n完成，耗时 {elapsed:.1f} 秒")

        if not pool:
            print("错误：未成功提取任何股票数据")
            return None

        df = pd.DataFrame(pool)
        cols_order = ['stock_code', 'stock_name', 'industry', 'end_date', 'roe', 'netprofit_yoy',
                     'grossprofit_margin', 'debt_to_assets', 'current_ratio', 'operating_cashflow',
                     'ocf_to_revenue', 'ocf_to_profit', 'net_profit', 'revenue', 'eps', 'bps',
                     'roa', 'netprofit_margin', 'quick_ratio', 'assets_turn', 'total_assets', 'total_equity']
        for c in cols_order:
            if c not in df.columns:
                df[c] = None
        df = df[[c for c in cols_order if c in df.columns]]

        os.makedirs(DATA_DIR, exist_ok=True)
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        print(f"\n成功 {len(pool)} 只，失败 {len(failed)} 只")
        print(f"已保存：{OUTPUT_FILE}")

        if failed:
            print(f"失败列表（前 10 个）：{failed[:10]}")

        return OUTPUT_FILE

    except Exception as e:
        print(f"错误：{e}")
        traceback.print_exc()
        return None


if __name__ == "__main__":
    result = main()
    if result:
        print("\n下载完成")
    else:
        print("\n下载失败")

