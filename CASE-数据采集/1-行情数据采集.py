# -*- coding: utf-8 -*-
"""
行情数据采集 - 使用Tushare Pro下载全量A股日线数据存入MySQL

功能：
  1. 连接Tushare Pro API
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
环境：pip install tushare pymysql python-dotenv
      需设置环境变量 TUSHARE_TOKEN
"""
import sys
import os
import time
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
import pandas as pd
import tushare as ts

# 加载.env环境变量
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_config import get_connection, execute_query

# 修复Windows终端编码问题
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# ============================================================
# 配置
# ============================================================
TEST_MODE = True
TEST_STOCK = '600519.SH'

SECTOR = '沪深A股'
NUM_WORKERS = 8
DATA_START = '20250101'


# ============================================================
# Tushare辅助
# ============================================================

def get_pro():
    """获取 Tushare Pro 实例（需环境变量 TUSHARE_TOKEN）"""
    token = os.environ.get("TUSHARE_TOKEN")
    if not token or not str(token).strip():
        raise RuntimeError("未设置环境变量 TUSHARE_TOKEN，请先设置后再运行")
    ts.set_token(str(token).strip())
    return ts.pro_api()


def get_stock_list():
    """获取沪深A股全量股票列表"""
    pro = get_pro()
    # 获取所有正常上市的股票
    df = pro.stock_basic(
        exchange='',
        list_status='L',
        fields='ts_code,symbol,name,area,industry,list_date'
    )
    # 转换为标准格式：600519.SH
    codes = []
    for _, row in df.iterrows():
        code = row['ts_code']
        codes.append(code)
    return codes


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


def download_and_save(stock_code, start_date):
    """增量下载单只股票的日线数据并写入MySQL"""
    # Tushare日期格式：YYYYMMDD
    end_date = date.today().strftime('%Y%m%d')
    
    try:
        # 使用 ts.pro_bar 获取前复权日线数据
        df = ts.pro_bar(
            ts_code=stock_code,
            start_date=start_date,
            end_date=end_date,
            adj='qfq',   # 前复权
            freq='D'      # 日线
        )

        if df is None or len(df) == 0:
            return stock_code, 0

        # 整理数据格式
        df = df.rename(columns={
            'trade_date': 'date',
            'vol': 'volume'
        })
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        df = df.sort_values('date').reset_index(drop=True)

        # 注：turnover_rate（换手率）需要 Tushare daily_basic 接口权限
        # 免费版无此权限，暂设为 None
        df['turnover_rate'] = None

        rows = []
        for _, row in df.iterrows():
            trade_date = row['date'].strftime('%Y-%m-%d')
            vol = int(row.get('volume', 0))  # Tushare volume单位是手
            amount = float(row.get('amount', 0)) if 'amount' in row else 0.0
            turnover = None  # 暂无换手率数据
            
            rows.append((
                stock_code, trade_date,
                float(row.get('open', 0)),
                float(row.get('high', 0)),
                float(row.get('low', 0)),
                float(row.get('close', 0)),
                vol,
                amount,
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
        
    except Exception as e:
        print(f"  下载 {stock_code} 失败: {e}")
        return stock_code, -1


# ============================================================
# 主流程
# ============================================================

def main():
    global pro
    
    print("=" * 60)
    print("行情数据采集 (Tushare Pro -> MySQL)")
    if TEST_MODE:
        print("[测试模式] 只采集贵州茅台")
    else:
        print(f"[全量模式] 采集{SECTOR}, {NUM_WORKERS}线程并行")
    print("=" * 60)

    print("\n初始化Tushare Pro...")
    pro = get_pro()
    print("  初始化成功")

    # 获取股票列表
    if TEST_MODE:
        all_codes = [TEST_STOCK]
        print(f"\n[测试模式] 只采集 {TEST_STOCK}")
    else:
        print(f"\n获取 {SECTOR} 股票列表...")
        all_codes = get_stock_list()
        print(f"  共 {len(all_codes)} 只股票")

    # 批量查询DB中已有的最新日期
    print("查询数据库已有数据...")
    existing = get_existing_latest_dates()
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

    print(f"  需要更新: {len(tasks)} 只, 跳过(今日已有数据): {skip_count} 只")

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
