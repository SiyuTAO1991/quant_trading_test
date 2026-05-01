# -*- coding: utf-8 -*-
"""
研报数据采集 - AkShare -> MySQL
全量A股研报采集

数据源：
  1. 同花顺 - 盈利预测一致预期 (ak.stock_profit_forecast_ths)  - 可用

采集范围：trade_stock_daily 中所有股票
跳过逻辑：7天内已采集的股票跳过

运行：python 5-研报数据采集.py
"""
import sys
import os
import time
import pandas as pd
import akshare as ak
import pymysql
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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
TEST_STOCKS = ['600519.SH', '600036.SH', '000001.SZ']
NUM_WORKERS = 4

_print_lock = threading.Lock()


def safe_print(msg):
    with _print_lock:
        print(msg)


# ============================================================
# 采集函数
# ============================================================

def fetch_profit_forecast(stock_code):
    """从同花顺获取盈利预测一致预期"""
    code_num = stock_code.split('.')[0]
    forecasts = {}
    for indicator in ['预测年报每股收益', '预测年报净利润']:
        try:
            df = ak.stock_profit_forecast_ths(symbol=code_num, indicator=indicator)
            if df is not None and len(df) > 0:
                df.columns = ['year', 'analyst_count', 'min_val', 'mean_val', 'max_val', 'industry_avg']
                forecasts[indicator] = df
        except Exception:
            pass
    return forecasts


def save_forecast_to_mysql(stock_code, forecasts):
    """将盈利预测一致预期写入MySQL"""
    if not forecasts:
        return 0

    eps_df = forecasts.get('预测年报每股收益')
    if eps_df is None or len(eps_df) == 0:
        return 0

    profit_df = forecasts.get('预测年报净利润')

    conn = get_connection()
    cursor = conn.cursor()

    sql = """
        INSERT INTO trade_report_consensus
        (stock_code, broker, report_date, rating, target_price,
         eps_forecast_current, eps_forecast_next, revenue_forecast, source_file)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        eps_forecast_current=VALUES(eps_forecast_current),
        eps_forecast_next=VALUES(eps_forecast_next),
        revenue_forecast=VALUES(revenue_forecast)
    """

    today = datetime.now().strftime('%Y-%m-%d')
    eps_current = float(eps_df.iloc[0]['mean_val']) if len(eps_df) > 0 else None
    eps_next = float(eps_df.iloc[1]['mean_val']) if len(eps_df) > 1 else None
    profit_current = None
    if profit_df is not None and len(profit_df) > 0:
        profit_current = float(profit_df.iloc[0]['mean_val'])

    analyst_count = int(eps_df.iloc[0]['analyst_count']) if len(eps_df) > 0 else 0

    cursor.execute(sql, (
        stock_code,
        "一致预期({}家)".format(analyst_count),
        today, None, None,
        eps_current, eps_next, profit_current,
        'ths_consensus'
    ))

    conn.commit()
    cursor.close()
    conn.close()
    return 1


# ============================================================
# 主流程
# ============================================================

def get_all_stocks():
    """获取全量股票列表"""
    rows = execute_query("SELECT DISTINCT stock_code FROM trade_stock_daily")
    return [r['stock_code'] for r in rows]


def get_recently_collected():
    """获取7天内已采集研报的股票"""
    rows = execute_query("""
        SELECT DISTINCT stock_code FROM trade_report_consensus
        WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    """)
    return {r['stock_code'] for r in rows}


def process_one_stock(stock_code):
    """采集并保存单只股票的研报数据"""
    forecast_count = 0

    try:
        forecasts = fetch_profit_forecast(stock_code)
        if forecasts:
            forecast_count = save_forecast_to_mysql(stock_code, forecasts)
        time.sleep(0.3)
    except Exception:
        pass

    return stock_code, 0, forecast_count


def main():
    print("=" * 60)
    print("研报数据采集 AkShare -> MySQL")
    print("=" * 60)

    if TEST_MODE:
        stock_list = TEST_STOCKS
        print("测试模式，采集 {} 只股票".format(len(stock_list)))
    else:
        stock_list = get_all_stocks()
        print("全量股票：{} 只".format(len(stock_list)))

        collected = get_recently_collected()
        if collected:
            stock_list = [c for c in stock_list if c not in collected]
            print("跳过近7天已采集：{} 只，待采集：{} 只".format(len(collected), len(stock_list)))

    if not stock_list:
        print("无股票需要采集")
        return

    total = len(stock_list)
    total_recommend = 0
    total_forecast = 0
    done = 0
    start_time = time.time()

    print("\n开始采集 ({} 线程)...".format(NUM_WORKERS))

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {
            executor.submit(process_one_stock, code): code
            for code in stock_list
        }

        for future in as_completed(futures):
            try:
                code, rec, fct = future.result()
                done += 1
                total_forecast += fct
            except Exception:
                done += 1

            if done % 10 == 0 or done == total:
                elapsed = time.time() - start_time
                speed = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / speed if speed > 0 else 0
                sys.stdout.write(
                    "\r  [{}/{}] {:.1f}% | {:.1f}/s | ETA {:.0f}s | 一致预期新增 {} 条    ".format(
                        done, total, done * 100 / total, speed, eta, total_forecast
                    )
                )
                sys.stdout.flush()

    print()

    elapsed = time.time() - start_time
    result = execute_query("SELECT COUNT(*) as cnt FROM trade_report_consensus")
    total_db = result[0]['cnt'] if result else 0

    print("\n" + "=" * 60)
    print("研报采集完成！耗时 {:.1f} 秒".format(elapsed))
    print("处理：{}/{} 只股票".format(done, total))
    print("一致预期新增：{} 条".format(total_forecast))
    print("trade_report_consensus 总计：{} 条".format(total_db))
    print("=" * 60)


if __name__ == '__main__':
    main()
