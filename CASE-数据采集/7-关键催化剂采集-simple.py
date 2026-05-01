# -*- coding: utf-8 -*-
"""
关键催化剂事件采集 - 简化版本
从财经日历中提取重要事件，同时添加一些固定的重要事件
"""
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_config import get_connection, execute_query

if sys.platform == 'win32' and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# 手动配置的重要催化剂事件
MANUAL_EVENTS = [
    # 2026年重要事件
    {"date": "2026-05-01", "title": "五一劳动节假期", "country": "中国", "category": "policy", "importance": 2},
    {"date": "2026-06-01", "title": "A股6月月度交割", "country": "中国", "category": "market", "importance": 2},
    {"date": "2026-06-15", "title": "美联储FOMC利率决议", "country": "美国", "category": "interest_rate", "importance": 3},
    {"date": "2026-07-01", "title": "中国共产党成立105周年", "country": "中国", "category": "policy", "importance": 2},
    {"date": "2026-07-27", "title": "美联储FOMC利率决议", "country": "美国", "category": "interest_rate", "importance": 3},
    {"date": "2026-09-17", "title": "美联储FOMC利率决议", "country": "美国", "category": "interest_rate", "importance": 3},
    {"date": "2026-10-01", "title": "中华人民共和国成立77周年", "country": "中国", "category": "policy", "importance": 2},
    {"date": "2026-11-03", "title": "美国中期选举", "country": "美国", "category": "policy", "importance": 3},
    {"date": "2026-12-16", "title": "美联储FOMC利率决议", "country": "美国", "category": "interest_rate", "importance": 3},
    
    # 固定的年度重要事件
    {"date": "2026-03-05", "title": "全国两会（人大、政协）", "country": "中国", "category": "policy", "importance": 3},
    {"date": "2026-04-30", "title": "A股年报披露截止日", "country": "中国", "category": "market", "importance": 2},
    {"date": "2026-08-31", "title": "A股半年报披露截止日", "country": "中国", "category": "market", "importance": 2},
    {"date": "2026-10-31", "title": "A股三季报披露截止日", "country": "中国", "category": "market", "importance": 2},
]


def extract_important_events():
    """从财经日历中提取重要事件（importance >= 2）"""
    sql = """
        SELECT event_date, event_time, title, country, category, importance
        FROM trade_calendar_event
        WHERE importance >= 2
        AND event_date >= CURDATE()
        AND event_date <= DATE_ADD(CURDATE(), INTERVAL 180 DAY)
        ORDER BY event_date
    """
    return execute_query(sql)


def save_events(events):
    """写入数据库，source设置为'manual_catalyst'"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # 先查询已有的manual_catalyst事件用于去重
    existing = execute_query("""
        SELECT event_date, title FROM trade_calendar_event
        WHERE source = 'manual_catalyst'
    """)
    existing_set = set((row['event_date'], row['title']) for row in existing)
    
    sql = """
        INSERT INTO trade_calendar_event
        (event_date, event_time, title, country, category, importance, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        importance = GREATEST(importance, VALUES(importance))
    """
    
    count = 0
    for evt in events:
        key = (evt['date'], evt['title'])
        if key in existing_set:
            continue
            
        cursor.execute(sql, (
            evt['date'], None, evt['title'], evt['country'], 
            evt['category'], evt['importance'], 'manual_catalyst'
        ))
        count += 1
    
    conn.commit()
    cursor.close()
    conn.close()
    return count


def main():
    print("=" * 60)
    print("关键催化剂事件采集（简化版本）")
    print("=" * 60)
    
    # 从财经日历提取重要事件
    print("\n1. 从财经日历提取重要事件...")
    important_events = extract_important_events()
    print(f"   找到 {len(important_events)} 条重要性>=2的事件")
    
    # 转换为统一格式
    calendar_events = []
    for evt in important_events:
        calendar_events.append({
            "date": str(evt['event_date']),
            "title": evt['title'],
            "country": evt['country'],
            "category": evt['category'],
            "importance": evt['importance']
        })
    
    # 添加手动配置的事件
    print(f"\n2. 添加手动配置的催化剂事件...")
    print(f"   准备添加 {len(MANUAL_EVENTS)} 条手动配置事件")
    
    # 合并并去重
    all_events = calendar_events + MANUAL_EVENTS
    
    # 保存
    print(f"\n3. 保存到数据库...")
    count = save_events(all_events)
    print(f"   新增/更新 {count} 条催化剂事件")
    
    # 统计
    print("\n催化剂事件统计:")
    stats = execute_query("""
        SELECT source, COUNT(*) as cnt
        FROM trade_calendar_event
        WHERE importance >= 2
        AND event_date >= CURDATE()
        AND event_date <= DATE_ADD(CURDATE(), INTERVAL 180 DAY)
        GROUP BY source
    """)
    for r in stats:
        print(f"  {r['source']}: {r['cnt']} 条")
    
    # 显示前10条
    print("\n最近10条催化剂事件:")
    recent = execute_query("""
        SELECT * FROM trade_calendar_event
        WHERE importance >= 2
        AND event_date >= CURDATE()
        ORDER BY event_date
        LIMIT 10
    """)
    for r in recent:
        imp = '*' * r['importance']
        print(f"  [{imp}] {r['event_date']} {r['country']} {r['title']}")
    
    print("\n" + "=" * 60)
    print("催化剂采集完成!")
    print("=" * 60)


if __name__ == '__main__':
    main()
