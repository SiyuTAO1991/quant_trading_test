# -*- coding: utf-8 -*-
"""
宏观经济数据采集 - 采集宏观经济指标存入MySQL

指标分组:
  通胀指标: CPI同比、PPI同比
  景气指标: PMI(制造业)
  流动性:   M2同比增速、社融规模增量
  利率:     LPR(1年/5年)、10年期国债收益率(中/美)

数据源(AkShare):
  - CPI: macro_china_cpi()
  - PPI: macro_china_ppi()
  - PMI: macro_china_pmi()
  - M2:  macro_china_supply_of_money()
  - 社融: macro_china_shrzgm()
  - LPR: macro_china_lpr()
  - 国债: bond_zh_us_rate()

运行: python 3-宏观数据采集.py
"""
import re
import sys
import os
import pandas as pd
import akshare as ak

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_config import get_connection, execute_query


def _parse_cn_date(series):
    """解析日期格式: '2026年01月份' / '202601' / '2026.01' -> datetime"""
    def _parse_one(s):
        if pd.isna(s):
            return pd.NaT
        s = str(s).strip()
        # 带分隔符: 2026年01月 / 2026.01
        m = re.match(r'(\d{4})\D+(\d{1,2})', s)
        if m:
            return pd.Timestamp(year=int(m.group(1)), month=int(m.group(2)), day=1)
        # 纯数字: 202601
        m = re.match(r'^(\d{4})(\d{2})$', s)
        if m:
            return pd.Timestamp(year=int(m.group(1)), month=int(m.group(2)), day=1)
        return pd.NaT
    return series.apply(_parse_one)


def _find_col(columns, keywords):
    """按关键词列表在列名中查找匹配列"""
    for kw in keywords:
        for col in columns:
            if kw in col:
                return col
    return None


def fetch_cpi():
    print("  采集CPI...")
    try:
        df = ak.macro_china_cpi()
        if df is None or len(df) == 0:
            return pd.DataFrame()
        date_col = df.columns[0]
        value_col = _find_col(df.columns, ['全国-同比增长', '同比增长', '同比'])
        if value_col is None:
            value_col = df.columns[2] if len(df.columns) > 2 else df.columns[1]
        result = pd.DataFrame({
            'date': _parse_cn_date(df[date_col]),
            'cpi_yoy': pd.to_numeric(df[value_col], errors='coerce')
        }).dropna()
        print(f"    CPI: {len(result)} 条")
        return result
    except Exception as e:
        print(f"    CPI 采集失败: {e}")
        return pd.DataFrame()


def fetch_ppi():
    print("  采集PPI...")
    try:
        df = ak.macro_china_ppi()
        if df is None or len(df) == 0:
            return pd.DataFrame()
        date_col = df.columns[0]
        value_col = _find_col(df.columns, ['当月同比增长', '同比增长', '同比'])
        if value_col is None:
            value_col = df.columns[2] if len(df.columns) > 2 else df.columns[1]
        result = pd.DataFrame({
            'date': _parse_cn_date(df[date_col]),
            'ppi_yoy': pd.to_numeric(df[value_col], errors='coerce')
        }).dropna()
        print(f"    PPI: {len(result)} 条")
        return result
    except Exception as e:
        print(f"    PPI 采集失败: {e}")
        return pd.DataFrame()


def fetch_pmi():
    print("  采集PMI...")
    try:
        df = ak.macro_china_pmi()
        if df is None or len(df) == 0:
            return pd.DataFrame()
        date_col = df.columns[0]
        value_col = _find_col(df.columns, ['制造业-指标', '制造业', 'PMI'])
        if value_col is None:
            value_col = df.columns[1]
        result = pd.DataFrame({
            'date': _parse_cn_date(df[date_col]),
            'pmi': pd.to_numeric(df[value_col], errors='coerce')
        }).dropna()
        print(f"    PMI: {len(result)} 条")
        return result
    except Exception as e:
        print(f"    PMI 采集失败: {e}")
        return pd.DataFrame()


def fetch_m2():
    print("  采集M2...")
    try:
        df = ak.macro_china_supply_of_money()
        if df is None or len(df) == 0:
            return pd.DataFrame()
        date_col = df.columns[0]
        value_col = _find_col(df.columns, ['M2）同比增长', 'M2)同比', 'M2同比'])
        if value_col is None:
            value_col = df.columns[2] if len(df.columns) > 2 else df.columns[1]
        result = pd.DataFrame({
            'date': _parse_cn_date(df[date_col]),
            'm2_yoy': pd.to_numeric(df[value_col], errors='coerce')
        }).dropna()
        print(f"    M2: {len(result)} 条")
        return result
    except Exception as e:
        print(f"    M2 采集失败: {e}")
        return pd.DataFrame()


def fetch_shrzgm():
    """采集社会融资规模增量(亿元)"""
    print("  采集社融...")
    try:
        df = ak.macro_china_shrzgm()
        if df is None or len(df) == 0:
            return pd.DataFrame()
        date_col = df.columns[0]  # 月份 (格式: 202512)
        total_col = df.columns[1]  # 社会融资规模增量
        result = pd.DataFrame({
            'date': _parse_cn_date(df[date_col]),
            'shrzgm': pd.to_numeric(df[total_col], errors='coerce')
        }).dropna()
        print(f"    社融: {len(result)} 条")
        return result
    except Exception as e:
        print(f"    社融 采集失败: {e}")
        return pd.DataFrame()


def fetch_lpr():
    """采集LPR利率(1年/5年)，月频取每月最新值"""
    print("  采集LPR...")
    try:
        df = ak.macro_china_lpr()
        if df is None or len(df) == 0:
            return pd.DataFrame()
        df['date'] = pd.to_datetime(df['TRADE_DATE'])
        df['lpr_1y'] = pd.to_numeric(df['LPR1Y'], errors='coerce')
        df['lpr_5y'] = pd.to_numeric(df['LPR5Y'], errors='coerce')
        result = df[['date', 'lpr_1y', 'lpr_5y']].dropna()
        print(f"    LPR: {len(result)} 条")
        return result
    except Exception as e:
        print(f"    LPR 采集失败: {e}")
        return pd.DataFrame()


def fetch_bond_yield():
    """采集中美10年期国债收益率(日频)，写入 trade_rate_daily"""
    print("  采集国债收益率...")
    try:
        # 近3年数据
        start = (pd.Timestamp.now() - pd.DateOffset(years=3)).strftime('%Y%m%d')
        df = ak.bond_zh_us_rate(start_date=start)
        if df is None or len(df) == 0:
            return 0

        cols = df.columns.tolist()
        date_col = cols[0]
        cn10_col = cols[3]   # 中国国债收益率10年
        us10_col = cols[9]   # 美国国债收益率10年

        conn = get_connection()
        cursor = conn.cursor()
        sql = """
            INSERT INTO trade_rate_daily (rate_date, cn_bond_10y, us_bond_10y, data_source)
            VALUES (%s, %s, %s, 'akshare')
            ON DUPLICATE KEY UPDATE
            cn_bond_10y=COALESCE(VALUES(cn_bond_10y), cn_bond_10y),
            us_bond_10y=COALESCE(VALUES(us_bond_10y), us_bond_10y)
        """
        count = 0
        for _, row in df.iterrows():
            d = row[date_col]
            if pd.isna(d):
                continue
            cn = float(row[cn10_col]) if pd.notna(row[cn10_col]) else None
            us = float(row[us10_col]) if pd.notna(row[us10_col]) else None
            if cn is None and us is None:
                continue
            cursor.execute(sql, (pd.Timestamp(d).strftime('%Y-%m-%d'), cn, us))
            count += 1

        conn.commit()
        cursor.close()
        conn.close()
        print(f"    国债收益率: {count} 条写入 trade_rate_daily")
        return count
    except Exception as e:
        print(f"    国债收益率 采集失败: {e}")
        return 0


# ==================== 月度数据合并写入 ====================

def merge_and_save(dfs):
    """合并月度宏观数据并写入MySQL"""
    merged = None
    for df in dfs:
        if df is None or len(df) == 0:
            continue
        df = df.copy()
        df['month'] = df['date'].dt.to_period('M').dt.to_timestamp('M')
        df = df.drop(columns=['date']).groupby('month').last().reset_index()
        if merged is None:
            merged = df
        else:
            merged = pd.merge(merged, df, on='month', how='outer')

    if merged is None or len(merged) == 0:
        print("  无数据可保存")
        return 0

    merged = merged.sort_values('month').reset_index(drop=True)

    # 只保留近10年
    cutoff = pd.Timestamp.now() - pd.DateOffset(years=10)
    merged = merged[merged['month'] >= cutoff]

    conn = get_connection()
    cursor = conn.cursor()

    sql = """
        INSERT INTO trade_macro_indicator
        (indicator_date, cpi_yoy, ppi_yoy, pmi, m2_yoy, shrzgm, lpr_1y, lpr_5y, data_source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        cpi_yoy=COALESCE(VALUES(cpi_yoy), cpi_yoy),
        ppi_yoy=COALESCE(VALUES(ppi_yoy), ppi_yoy),
        pmi=COALESCE(VALUES(pmi), pmi),
        m2_yoy=COALESCE(VALUES(m2_yoy), m2_yoy),
        shrzgm=COALESCE(VALUES(shrzgm), shrzgm),
        lpr_1y=COALESCE(VALUES(lpr_1y), lpr_1y),
        lpr_5y=COALESCE(VALUES(lpr_5y), lpr_5y)
    """

    def _val(row, col):
        v = row.get(col)
        return float(v) if pd.notna(v) else None

    count = 0
    for _, row in merged.iterrows():
        cursor.execute(sql, (
            row['month'].strftime('%Y-%m-%d'),
            _val(row, 'cpi_yoy'), _val(row, 'ppi_yoy'),
            _val(row, 'pmi'), _val(row, 'm2_yoy'),
            _val(row, 'shrzgm'), _val(row, 'lpr_1y'), _val(row, 'lpr_5y'),
            'akshare'
        ))
        count += 1

    conn.commit()
    cursor.close()
    conn.close()
    return count


def main():
    print("=" * 60)
    print("\u5b8f\u89c2\u7ecf\u6d4e\u6570\u636e\u91c7\u96c6 -> MySQL")
    print("=" * 60)

    # 月度指标
    print("\n[1/2] \u91c7\u96c6\u6708\u5ea6\u5b8f\u89c2\u6307\u6807...")
    df_cpi = fetch_cpi()
    df_ppi = fetch_ppi()
    df_pmi = fetch_pmi()
    df_m2 = fetch_m2()
    df_shrzgm = fetch_shrzgm()
    df_lpr = fetch_lpr()

    all_dfs = [df_cpi, df_ppi, df_pmi, df_m2, df_shrzgm, df_lpr]
    names = ['CPI', 'PPI', 'PMI', 'M2', '\u793e\u878d', 'LPR']
    ok = sum(1 for df in all_dfs if len(df) > 0)
    print(f"\n\u91c7\u96c6\u7ed3\u679c: {ok}/{len(all_dfs)} \u9879\u6210\u529f")

    print("\n\u5408\u5e76\u5e76\u5199\u5165MySQL...")
    count = merge_and_save(all_dfs)
    print(f"\u5199\u5165 {count} \u6761\u6708\u5ea6\u5b8f\u89c2\u6307\u6807")

    # 日频指标(国债收益率)
    print(f"\n[2/2] \u91c7\u96c6\u65e5\u9891\u5229\u7387\u6307\u6807...")
    fetch_bond_yield()

    # 概况
    summary = execute_query("""
        SELECT COUNT(*) as cnt,
               MIN(indicator_date) as min_date, MAX(indicator_date) as max_date
        FROM trade_macro_indicator
    """)
    if summary:
        r = summary[0]
        print(f"\ntrade_macro_indicator: {r['cnt']} \u671f ({r['min_date']} ~ {r['max_date']})")

    rate_summary = execute_query("""
        SELECT COUNT(*) as cnt,
               MIN(rate_date) as min_date, MAX(rate_date) as max_date
        FROM trade_rate_daily
    """)
    if rate_summary:
        r = rate_summary[0]
        print(f"trade_rate_daily: {r['cnt']} \u65e5 ({r['min_date']} ~ {r['max_date']})")

    print("\n" + "=" * 60)
    print("\u5b8f\u89c2\u6570\u636e\u91c7\u96c6\u5b8c\u6210!")
    print("=" * 60)


if __name__ == "__main__":
    main()
