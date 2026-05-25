---
name: backtrader
description: "Backtrader 量化回测。禁止 pip install、禁止检查环境。唯一正确做法：直接执行 exec，命令 python skills/backtrader/scripts/run_backtest.py <股票代码> <开始日期> <结束日期> [策略名]，workdir 为工作区根目录，返回脚本输出的 JSON。示例：python skills/backtrader/scripts/run_backtest.py 600519.SH 2025-01-01 2025-12-31 double_ma"
metadata:
  openclaw:
    emoji: "📉"
    os: ["win32", "linux"]
    requires:
      bins: ["python"]
      packages: ["backtrader", "pandas", "pymysql", "python-dotenv"]
    permissions:
      - "exec"
      - "file.read"
      - "file.write"
    timeout: 120
---

# Backtrader 回测技能

## 功能描述

使用 Backtrader 引擎对 A 股进行策略回测，支持双均线、MACD、RSI、布林带、动量等策略。

## 前置条件

1. MySQL 数据库已配置，表 `trade_stock_daily` 有 K 线数据
2. 数据库凭据已内置在脚本中，无需用户配置

## 执行流程（必须遵守）

1. 收到回测请求后，直接执行 exec，无需检查 pip、数据库连接、python 版本等
2. 命令：`python skills/backtrader/scripts/run_backtest.py <stock_code> [start_date] [end_date] [strategy]`
3. workdir 为工作区根目录
4. 将脚本输出的 JSON 直接作为结果返回给用户

禁止：安装依赖、检查环境、验证包、询问数据库密码。

## 使用方式

```bash
python skills/backtrader/scripts/run_backtest.py <stock_code> [start_date] [end_date] [strategy]
```

### 参数说明

| 参数 | 描述 | 示例 |
|------|------|------|
| stock_code | 股票代码 | 600519.SH |
| start_date | 开始日期 | 2024-01-01 |
| end_date | 结束日期 | 2025-12-31 |
| strategy | 策略名 | double_ma, macd, rsi, bbands, momentum |

### 策略说明

- `double_ma` - 双均线(10/30)金叉死叉
- `macd` - MACD 金叉死叉
- `rsi` - RSI 超买超卖(30/70)
- `bbands` - 布林带下轨买入上轨卖出
- `momentum` - 动量策略

## 触发示例

- "双均线策略回测 600519.SH（2025-01-01 至 2025-12-31）"
- "对贵州茅台跑双均线策略回测"
- "用 backtrader 回测 600519.SH 的 MACD 策略"
- "RSI 策略回测 000001.SZ 2024-01-01 到 2024-12-31"
