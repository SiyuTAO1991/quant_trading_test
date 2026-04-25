# -*- coding: utf-8 -*-
"""
多因子选股 - 数据下载脚本（QMT 版）
通过 QMT 下载指定板块内所有股票的财务数据，提取指标并汇总保存，供筛选脚本使用。
同时获取股票名称、申万一级行业，一并写入输出文件。

输出文件：data/stock_fina_pool_QMT.csv（每只股票最新一期财务指标 + 净利润同比 + 名称 + 行业）

运行：python 多因子选股-下载数据.py
环境：需安装 QMT 并配置好 xtquant，运行前需启动 miniQMT
"""
import os
import sys
import time
import traceback
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from xtquant import xtdata

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
# 股票池：沪深A股（全量）
SECTOR = '沪深A股'
DATA_START = '20150101'
DATA_END = date.today().strftime('%Y%m%d')   # 自动使用今天日期

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "stock_fina_pool_QMT.csv")

TABLE_LIST = ['Balance', 'Income', 'CashFlow', 'PershareIndex', 'Capital']


def normalize_timetag(ts_val):
    """将 xtquant 的 m_timetag 转换为 YYYYMMDD 字符串。"""
    if ts_val is None:
        return None
    s = str(ts_val).strip()
    if len(s) == 8 and s.isdigit():
        return s
    try:
        v = float(s)
        if v == 0:
            return None
        if v > 1e12:
            v = v / 1000
        return datetime.fromtimestamp(v).strftime('%Y%m%d')
    except (OSError, ValueError, TypeError):
        return None


def get_field(record, field_names, default=None):
    """从记录中获取字段值，支持多候选字段名。"""
    for name in field_names:
        val = record.get(name)
        if val is not None:
            return val
    return default


def safe_divide(a, b, pct=False):
    """安全除法，b 为 0 时返回 None。"""
    if a is None or b is None:
        return None
    a, b = float(a), float(b)
    if b == 0:
        return None
    result = a / b
    if pct:
        result *= 100
    return round(result, 4)


def build_period_map(data_list):
    """将 xtquant 返回的财务数据转换为 {报告期: 记录} 映射。"""
    period_map = {}
    if isinstance(data_list, pd.DataFrame):
        for _, row in data_list.iterrows():
            period_date = normalize_timetag(row.get('m_timetag'))
            if period_date:
                period_map[period_date] = row.to_dict()
    elif isinstance(data_list, list):
        for rec in data_list:
            if isinstance(rec, dict):
                period_date = normalize_timetag(rec.get('m_timetag'))
                if period_date:
                    period_map[period_date] = rec
    return period_map


def extract_all_periods(data, stock_code):
    """
    从 xtquant 返回的财务数据中提取该股票所有报告期的综合财务指标。
    data: xtdata.get_financial_data 返回的 {stock: {table: data}}
    返回: 按报告期排列的记录列表
    """
    stock_data = data.get(stock_code, {})
    if not stock_data:
        return []

    pershare_map = build_period_map(stock_data.get('PershareIndex', []))
    balance_map = build_period_map(stock_data.get('Balance', []))
    income_map = build_period_map(stock_data.get('Income', []))
    cashflow_map = build_period_map(stock_data.get('CashFlow', []))
    capital_map = build_period_map(stock_data.get('Capital', []))

    all_periods = sorted(set(
        list(pershare_map.keys()) +
        list(balance_map.keys()) +
        list(income_map.keys()) +
        list(cashflow_map.keys())
    ))

    records = []
    for period in all_periods:
        ps = pershare_map.get(period, {})
        bal = balance_map.get(period, {})
        inc = income_map.get(period, {})
        cf = cashflow_map.get(period, {})
        cap = capital_map.get(period, {})

        eps = get_field(ps, ['s_fa_eps_basic'])
        bps = get_field(ps, ['s_fa_bps'])
        ocfps = get_field(ps, ['s_fa_ocfps'])
        revenue = get_field(inc, ['revenue', 'operating_revenue', 'revenue_inc'])
        net_profit = get_field(inc, ['net_profit_incl_min_int_inc', 'net_profit_excl_min_int_inc'])
        operating_profit = get_field(inc, ['oper_profit'])
        operating_cost = get_field(inc, ['cost_of_goods_sold', 'total_operating_cost', 'total_expense'])

        grossprofit_margin = get_field(ps, ['sales_gross_profit'])
        if grossprofit_margin is None and revenue and operating_cost:
            r, c = float(revenue), float(operating_cost)
            if r > 0:
                grossprofit_margin = round((r - c) / r * 100, 2)

        roe = get_field(ps, ['du_return_on_equity', 'equity_roe', 'net_roe'])
        grossprofit_margin_ps = grossprofit_margin

        netprofit_margin = safe_divide(net_profit, revenue, pct=True)

        total_assets = get_field(bal, ['tot_assets'])
        total_liab = get_field(bal, ['tot_liab'])
        total_equity = get_field(bal, ['total_equity', 'tot_shrhldr_eqy_incl_min_int'])
        current_assets = get_field(bal, ['total_current_assets'])
        current_liab = get_field(bal, ['total_current_liability'])
        inventory = get_field(bal, ['inventories'])

        debt_to_assets = safe_divide(total_liab, total_assets, pct=True)
        current_ratio = safe_divide(current_assets, current_liab)
        quick_ratio = None
        if current_assets and inventory and current_liab:
            quick_ratio = safe_divide(float(current_assets) - float(inventory), current_liab)

        roa = safe_divide(net_profit, total_assets, pct=True)
        if roe is None and net_profit and total_equity:
            roe = safe_divide(net_profit, total_equity, pct=True)

        assets_turn = safe_divide(revenue, total_assets)
        operating_cashflow = get_field(cf, ['net_cash_flows_oper_act'])
        ocf_to_revenue = safe_divide(operating_cashflow, revenue, pct=True)
        ocf_to_profit = safe_divide(operating_cashflow, net_profit)

        record = {
            'end_date': period,
            'eps': eps,
            'bps': bps,
            'ocfps': ocfps,
            'roe': roe,
            'roa': roa,
            'grossprofit_margin': grossprofit_margin_ps,
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

    return records


def calc_netprofit_yoy(records):
    """
    用年报数据计算最新一期净利润同比增长率。
    取 end_date 以 1231 结尾的报告期，按 pct_change 计算同比。
    """
    if not records:
        return None
    annual = [r for r in records if str(r.get('end_date', '')).endswith('1231')]
    if len(annual) < 2:
        return None
    annual = sorted(annual, key=lambda x: x['end_date'])
    profits = [float(r['net_profit']) for r in annual if r.get('net_profit') is not None]
    if len(profits) < 2:
        return None
    s = pd.Series(profits)
    yoy = s.pct_change().iloc[-1] * 100
    return round(yoy, 2)


def get_stock_industry_map():
    """
    获取股票 -> 申万一级行业 映射。
    通过遍历申万一级行业板块，反向构建每只股票所属行业。
    """
    stock_to_industry = {}
    try:
        all_sectors = xtdata.get_sector_list()
        sw1_sectors = [s for s in all_sectors if str(s).upper().startswith('SW1') and '加权' not in str(s)]
        for sector in sw1_sectors:
            stocks = xtdata.get_stock_list_in_sector(sector)
            if stocks:
                for stk in stocks:
                    if '.' in str(stk):
                        stock_to_industry[stk] = sector
    except Exception as e:
        print(f"获取行业映射异常: {e}")
    return stock_to_industry


def get_stock_name(stock_code):
    """从 QMT 获取股票名称。"""
    try:
        detail = xtdata.get_instrument_detail(stock_code)
        if detail:
            return detail.get('InstrumentName', '') or ''
    except Exception:
        pass
    return ''


def download_one_stock(stock_code, industry_map=None):
    """下载单只股票的财务数据并提取最新一期指标（含 netprofit_yoy）。"""
    done_count = [0]

    def on_done(data):
        done_count[0] += 1

    for table_name in TABLE_LIST:
        xtdata.download_financial_data2(
            stock_list=[stock_code],
            table_list=[table_name],
            start_time=DATA_START,
            end_time=DATA_END,
            callback=on_done
        )

    for _ in range(60):
        if done_count[0] >= len(TABLE_LIST):
            break
        time.sleep(0.2)

    time.sleep(0.3)

    data = xtdata.get_financial_data(
        stock_list=[stock_code],
        table_list=TABLE_LIST,
        start_time=DATA_START,
        end_time=DATA_END,
        report_type='report_time'
    )

    if not data or stock_code not in data:
        return None

    records = extract_all_periods(data, stock_code)
    if not records:
        return None

    records = sorted(records, key=lambda x: x['end_date'])
    latest = records[-1].copy()
    latest['netprofit_yoy'] = calc_netprofit_yoy(records)
    latest['stock_code'] = stock_code
    latest['stock_name'] = get_stock_name(stock_code)
    latest['industry'] = (industry_map or {}).get(stock_code, '')
    return latest


def main():
    print("多因子选股 - 财务数据下载（QMT）")
    print(f"板块：{SECTOR}")
    print(f"日期：{DATA_START} ~ {DATA_END}")
    print("-" * 50)

    try:
        print("连接 QMT...")
        xtdata.connect()
        # 注意：download_sector_data() 在新版 QMT 中可能卡住，先直接尝试获取
        print("获取股票列表...")
        stock_list = xtdata.get_stock_list_in_sector(SECTOR)
        if not stock_list:
            sectors = xtdata.get_sector_list()
            print(f"错误：板块 '{SECTOR}' 返回空列表")
            if sectors:
                # 从板块列表中找包含"沪深"的
                hs = [s for s in sectors if '沪深' in str(s)][:15]
                print(f"含'沪深'的板块示例：{hs}")
            return None

        # 只保留股票代码（含 .SH / .SZ 的为主板/创业板等）
        stock_list = [c for c in stock_list if '.' in str(c)]
        total = len(stock_list)
        print(f"共 {total} 只股票待下载")

        print("获取行业映射...")
        industry_map = get_stock_industry_map()
        print(f"  已映射 {len(industry_map)} 只股票行业")

        pool = []
        failed = []
        start_time = time.time()

        def _download(code):
            """供线程池调用的包装，返回 (code, row 或 None)。"""
            try:
                row = download_one_stock(code, industry_map)
                return code, row
            except Exception:
                return code, None

        print(f"并行下载（{NUM_WORKERS} 线程）...")
        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = {executor.submit(_download, code): code for code in stock_list}
            done = 0
            for future in as_completed(futures):
                code, row = future.result()
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
        print(f"\n  完成，耗时 {elapsed:.1f} 秒")

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
