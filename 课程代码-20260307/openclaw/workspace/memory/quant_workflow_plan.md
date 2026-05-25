# 量化回测全流程规划

## 🎯 目标：从数据采集 → 指标计算 → 策略回测

### 1️⃣ 数据采集（MiniQMT）
- ✅ 前置条件：已安装 MiniQMT 客户端并登录，xtquant 已配置到 Python 环境
- 📥 获取股票 K 线数据（日线/分钟线）
  ```bash
  python3 get_kline.py <stock_code> [period] [count]
  ```
- 🗂️ 存储格式：CSV 或 MySQL（推荐 `trade_stock_daily` 表）

### 2️⃣ 指标计算（TA-Lib）
- ✅ 前置条件：Python 环境已安装 `talib`, `pandas`
- 🧠 计算常用指标：RSI、MACD、布林带、ATR 等
  ```bash
  python3 calc_indicators.py <indicator> <csv_path> [--period N]
  ```
- 📊 输出：每条 K 线新增指标列（如 RSI_14, MACD_DIF, BBANDS_upper）

### 3️⃣ 策略回测（Backtrader）
- ✅ 前置条件：MySQL 数据库有 `trade_stock_daily` 表，`.env` 配置好数据库连接
- 🧪 运行策略：双均线 / MACD / RSI / 布林带等
  ```bash
  python3 run_backtest.py <stock_code> [start_date] [end_date] [strategy]
  ```
- 📈 输出：绩效报告 + 交易记录 + 可视化图表

## 🔁 流程图（简化版）
```
[数据采集] → [指标计算] → [策略回测] → [结果分析]
```