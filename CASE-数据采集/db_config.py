# -*- coding: utf-8 -*-
"""
数据库连接配置
从 .env 读取配置
"""
import os
from pathlib import Path
import pymysql
from dotenv import dotenv_values

# 从 .env 读取
_env_path = Path(__file__).parent.parent / '.env'
_env = dotenv_values(_env_path)

DB_CONFIG = {
    'host': _env.get('WUCAI_SQL_HOST', 'localhost'),
    'user': _env.get('WUCAI_SQL_USERNAME', 'root'),
    'password': _env.get('WUCAI_SQL_PASSWORD', ''),
    'database': _env.get('WUCAI_SQL_DB', 'wucai_trade'),
    'port': int(_env.get('WUCAI_SQL_PORT', '3306')),
    'charset': 'utf8mb4'
}

# Kimi API
KIMI_API_KEY = _env.get('KIMI_API_KEY', '')
KIMI_BASE_URL = _env.get('KIMI_BASE_URL', 'https://api.moonshot.cn/v1')
KIMI_MODEL = _env.get('KIMI_MODEL', 'kimi-latest')

# DashScope/Qwen
DASHSCOPE_API_KEY = _env.get('DASHSCOPE_API_KEY', '')
QWEN_MODEL = _env.get('QWEN_MODEL', 'qwen-flash')


def get_connection():
    """获取数据库连接"""
    return pymysql.connect(**DB_CONFIG)


def execute_query(sql, params=None):
    """执行查询，返回字典列表"""
    conn = get_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(sql, params or ())
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result


def execute_update(sql, params=None):
    """执行单条更新/插入"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql, params or ())
    conn.commit()
    affected = cursor.rowcount
    cursor.close()
    conn.close()
    return affected


def execute_many(sql, data_list):
    """批量执行插入/更新"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.executemany(sql, data_list)
    conn.commit()
    affected = cursor.rowcount
    cursor.close()
    conn.close()
    return affected
