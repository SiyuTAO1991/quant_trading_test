---
name: talib
description: TA-Lib 技术指标库，计算 SMA/EMA、MACD、RSI、ATR、布林带等 158 种技术指标，用于量化策略的信号生成和选股。
metadata:
  openclaw:
    emoji: "📊"
    os: ["win32", "linux"]
    requires:
      bins: ["python3"]
      packages: ["talib", "numpy", "pandas"]
    permissions:
      - "exec"
      - "file.read"
      - "file.write"
    timeout: 30
---

# TA-Lib 技术指标技能

## 功能描述

使用 TA-Lib 计算技术分析指标，支持均线、动量、波动率、形态识别等 158 种指标。

## 使用方式

### 方式一：对 CSV 计算指标

CSV 需含 open/high/low/close 列：

```bash
python3 {baseDir}/scripts/calc_indicators.py <indicator> <csv_path> [--period N]
```

### 方式二：直接传入股票代码（需 MiniQMT）

若已配置 xtquant，可直接传入股票代码：

```bash
python3 {baseDir}/scripts/calc_indicators.py <indicator> --stock <stock_code> [--period N]
```

## 常用指标

| 指标 | 说明 | 参数示例 |
|------|------|----------|
| SMA | 简单移动平均 | period=10 |
| EMA | 指数移动平均 | period=12 |
| MACD | 指数平滑异同移动平均 | fast=12,slow=26,signal=9 |
| RSI | 相对强弱指标 | period=14 |
| ATR | 真实波幅均值 | period=14 |
| BBANDS | 布林带 | period=20 |

## 指标原理速查

- **SMA**: 简单算术平均
- **MACD**: DIF=EMA(12)-EMA(26), DEA=DIF的EMA(9), 金叉买入/死叉卖出
- **RSI**: RS=平均涨幅/平均跌幅, RSI>70 超买, RSI<30 超卖
- **ATR**: 真实波幅的 N 日平均，用于止损（止损=入场价-2*ATR）

## 触发示例

- "计算贵州茅台的 MACD 指标"
- "用 talib 算 600519 的 RSI"
- "给这份 K 线数据计算布林带"

