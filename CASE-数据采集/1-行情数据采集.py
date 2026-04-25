# -*- coding: utf-8 -*-
"""
行情数据采集 - 使用MiniQMT(xtquant)下载全量A股日线数据存入MySQL

功能：
  1. 连接MiniQMT数据服务
  2. 获取沪深A股全量股票列表（约5000只）
  3. 一次性批量查询DB中已有的最新日期，仅下载增量数据
  4. 多线程写入MySQL的trade_stock_daily表（ON DUPLICATE KEY UPDATE）

优化：
  - 不逐只查名称（太慢），直接用股票代码
  - 批量查询DB最新日期，跳过已是最新的股票
  - 移除不必要的sleep，提升吞吐量

模式：
  - TEST_MODE = True  -> 只采集1只股票(贵州茅台)，用于验证流程
  - TEST_MODE = False -> 采集沪深A股全量股票

运行：python 1-行情数据采集.py
环境：需安装QMT并配置好xtquant, pip install pymysql python-dotenv
"""
import sys
import os
import time
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from xtquant import xtdata

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
TEST_STOCK = '600519.SH'

SECTOR = '沪深A股'
NUM_WORKERS = 8
DATA_START = '20230101'


# ============================================================
# 数据库辅助
# ============================================================

def get_existing_latest_dates():
    """一次性查询所有股票在DB中的最新交易日，返回 {stock_code: 'YYYYMMDD'}"""
    rows = execute_query(
        "SELECT stock_code, MAX(trade_date) AS max_date FROM trade_stock_daily GROUP BY stock_code"
    )
    result = {}
    for r in rows:
        if r['max_date']:
            result[r['stock_code']] = r['max_date'].strftime('%Y%m%d')
    return result


# ============================================================
# 核心逻辑
# ============================================================

INSERT_SQL = """
    INSERT INTO trade_stock_daily
    (stock_code, trade_date, open_price, high_price, low_price, close_price, volume, amount, turnover_rate)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    open_price=VALUES(open_price), high_price=VALUES(high_price),
    low_price=VALUES(low_price), close_price=VALUES(close_price),
    volume=VALUES(volume), amount=VALUES(amount),
    turnover_rate=VALUES(turnover_rate)
"""


def _get_float_shares(stock_code):
    """获取流通股本（股），用于计算换手率"""
    try:
        detail = xtdata.get_instrument_detail(stock_code)
        if detail:
            neg = detail.get('NegotiableVolume') or detail.get('TotalVolume') or 0
            if neg > 0:
                return neg
    except Exception:
        pass
    return 0


def download_and_save(stock_code, start_date):
    """增量下载单只股票的日线数据并写入MySQL"""
    xtdata.download_history_data(stock_code, '1d', start_time=start_date)

    data = xtdata.get_market_data_ex(
        field_list=['open', 'high', 'low', 'close', 'volume', 'amount'],
        stock_list=[stock_code],
        period='1d',
        start_time=start_date,
        dividend_type='front',
    )

    if not data or stock_code not in data:
        return stock_code, 0

    df = data[stock_code]
    if df is None or len(df) == 0:
        return stock_code, 0

    float_shares = _get_float_shares(stock_code)

    rows = []
    for idx, row in df.iterrows():
        idx_str = str(idx)
        if len(idx_str) >= 8:
            trade_date = f"{idx_str[:4]}-{idx_str[4:6]}-{idx_str[6:8]}"
            vol = int(row['volume'])
            # xtdata volume 单位是手(1手=100股)，流通股本单位是股
            vol_shares = vol * 100
            turnover = round(vol_shares / float_shares * 100, 4) if float_shares > 0 and vol > 0 else None
            rows.append((
                stock_code, trade_date,
                float(row['open']), float(row['high']),
                float(row['low']), float(row['close']),
                vol, float(row['amount']),
                turnover,
            ))

    if rows:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.executemany(INSERT_SQL, rows)
        conn.commit()
        cursor.close()
        conn.close()

    return stock_code, len(rows)


# ============================================================
# 主流程
# ============================================================

def main():
    print("=" * 60)
    print("行情数据采集 (MiniQMT -> MySQL)")
    if TEST_MODE:
        print("[测试模式] 只采集贵州茅台")
    else:
        print(f"[全量模式] 采集{SECTOR}, {NUM_WORKERS}线程并行")
    print("=" * 60)

    print("\n连接QMT数据服务...")
    xtdata.connect()
    print("  连接成功")

    # 获取股票列表
    if TEST_MODE:
        all_codes = [TEST_STOCK]
        print(f"\n[测试模式] 只采集 {TEST_STOCK}")
    else:
        print(f"\n获取 {SECTOR} 股票列表...")
        all_codes = xtdata.get_stock_list_in_sector(SECTOR)
        all_codes = [c for c in all_codes if '.' in str(c)]
        print(f"  共 {len(all_codes)} 只股票")

    # 批量查询DB中已有的最新日期
    print("查询数据库已有数据...")
    existing = get_existing_latest_dates()
    # 今天的数据已有则跳过，否则尝试增量更新
    recent_cutoff = date.today().strftime('%Y%m%d')

    tasks = []
    skip_count = 0
    for code in all_codes:
        latest = existing.get(code)
        if latest and latest >= recent_cutoff:
            skip_count += 1
            continue
        start = latest if latest else DATA_START
        tasks.append((code, start))

    print(f"  需更新: {len(tasks)} 只, 跳过(今日已有数据): {skip_count} 只")

    if not tasks:
        print("\n全部已是最新，无需更新")
        _print_summary()
        return

    total = len(tasks)
    total_rows = 0
    success_count = 0
    fail_list = []
    start_time = time.time()

    if total <= 5:
        for i, (code, start) in enumerate(tasks, 1):
            print(f"\n[{i}/{total}] {code} (从 {start} 开始)")
            _, count = download_and_save(code, start)
            if count >= 0:
                print(f"  写入 {count} 条")
                success_count += 1
                total_rows += max(count, 0)
            else:
                print(f"  失败")
                fail_list.append(code)
    else:
        print(f"\n并行下载（{NUM_WORKERS} 线程）...")

        def _worker(args):
            code, start = args
            try:
                return download_and_save(code, start)
            except Exception:
                return code, -1

        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = {executor.submit(_worker, t): t[0] for t in tasks}
            done = 0
            for future in as_completed(futures):
                code, count = future.result()
                done += 1

                if count >= 0:
                    success_count += 1
                    total_rows += max(count, 0)
                else:
                    fail_list.append(code)

                elapsed = time.time() - start_time
                speed = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / speed if speed > 0 else 0
                sys.stdout.write(
                    f"\r  进度 {done}/{total} ({done*100/total:.1f}%) | "
                    f"{speed:.1f} 只/秒 | 剩余约 {eta:.0f}秒 | "
                    f"成功 {success_count} 失败 {len(fail_list)}    "
                )
                sys.stdout.flush()

        print()

    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print(f"采集完成! 耗时 {elapsed:.1f} 秒")
    print(f"  成功: {success_count}/{total} 只股票")
    print(f"  总写入: {total_rows:,} 条记录")

    if fail_list:
        print(f"  失败 {len(fail_list)} 只: {fail_list[:20]}{'...' if len(fail_list) > 20 else ''}")

    _print_summary()


def _print_summary():
    summary = execute_query("""
        SELECT COUNT(DISTINCT stock_code) as stock_cnt,
               COUNT(*) as row_cnt,
               MIN(trade_date) as min_date, MAX(trade_date) as max_date
        FROM trade_stock_daily
    """)
    if summary:
        row = summary[0]
        print(f"\n数据库 trade_stock_daily 概况:")
        print(f"  {row['stock_cnt']} 只股票, {row['row_cnt']:,} 条记录")
        print(f"  日期范围: {row['min_date']} ~ {row['max_date']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
