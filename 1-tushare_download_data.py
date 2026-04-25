# -*- coding: utf-8 -*-
"""
Tushare数据下载脚本
使用tushare下载寒武纪股票（688256.SH）的历史数据
保存到本地CSV文件，供后续策略分析使用

注意：运行此脚本需要安装tushare并配置好TUSHARE_TOKEN环境变量
"""
import os
import pandas as pd
import tushare as ts
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# 数据下载参数配置
STOCK_CODE = '002714.SZ'  # 牧原股份股票代码
STOCK_NAME = '牧原股份'
DATA_START = '20240101'   # 数据开始日期
DATA_END = '20251231'     # 数据结束日期


def download_stock_data():
    """
    下载股票历史数据并保存到CSV文件
    """
    print(f"开始下载股票数据")
    print(f"股票：{STOCK_NAME}({STOCK_CODE})")
    print(f"日期范围：{DATA_START} 至 {DATA_END}")
    print("-" * 60)
    
    try:
        # 步骤1：初始化tushare
        print("步骤1：初始化tushare...")
        token = os.getenv('TUSHARE_TOKEN')
        if not token:
            print("错误：未找到环境变量 TUSHARE_TOKEN")
            print("请检查.env文件是否存在并配置正确的TUSHARE_TOKEN")
            return None
        
        ts.set_token(token)
        pro = ts.pro_api()
        print("tushare初始化成功")
        
        # 步骤2：下载历史数据
        print("\n步骤2：下载历史数据...")
        # tushare的daily接口需要股票代码格式为：688256.SH
        # 日期格式：YYYYMMDD
        df = pro.daily(
            ts_code=STOCK_CODE,
            start_date=DATA_START,
            end_date=DATA_END
        )
        
        if df is None or df.empty:
            print("错误：无法获取历史数据")
            return None
        
        # 步骤3：数据预处理
        print(f"成功获取 {len(df)} 条历史数据")
        
        # 重命名列名，使其与原有格式一致
        df = df.rename(columns={
            'trade_date': 'date',
            'close': 'close',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'vol': 'volume'
        })
        
        # 选择需要的列
        columns_needed = ['date', 'close']
        if 'open' in df.columns:
            columns_needed.append('open')
        if 'high' in df.columns:
            columns_needed.append('high')
        if 'low' in df.columns:
            columns_needed.append('low')
        if 'volume' in df.columns:
            columns_needed.append('volume')
        
        df = df[columns_needed].copy()
        
        # 转换日期格式
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d', errors='coerce')
        
        # 过滤掉无效数据
        df = df.dropna(subset=['date', 'close'])
        df = df.sort_values('date').reset_index(drop=True)
        
        print(f"数据日期范围：{df['date'].iloc[0].strftime('%Y-%m-%d')} 至 {df['date'].iloc[-1].strftime('%Y-%m-%d')}")
        
        # 步骤4：保存到CSV文件
        print("\n步骤3：保存数据到CSV文件...")
        output_dir = os.path.join(os.getcwd(), 'data')
        os.makedirs(output_dir, exist_ok=True)
        
        # 保存完整数据
        output_file = os.path.join(output_dir, f'{STOCK_CODE.replace(".", "_")}_daily.csv')
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"数据已保存至：{output_file}")
        
        # 显示数据预览
        print("\n数据预览（前5行）：")
        print(df.head().to_string(index=False))
        print("\n数据预览（后5行）：")
        print(df.tail().to_string(index=False))
        
        # 显示数据统计
        print("\n数据统计信息：")
        print(f"  总记录数：{len(df)}")
        print(f"  收盘价范围：{df['close'].min():.2f} - {df['close'].max():.2f}")
        if 'volume' in df.columns:
            print(f"  成交量范围：{df['volume'].min():,.0f} - {df['volume'].max():,.0f}")
        
        return output_file
        
    except Exception as e:
        print(f"下载数据过程中发生错误：{e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    result = download_stock_data()
    
    if result:
        print("\n" + "=" * 60)
        print("数据下载完成!")
        print(f"数据文件：{result}")
        print("=" * 60)
    else:
        print("\n数据下载失败，请检查错误信息。")
