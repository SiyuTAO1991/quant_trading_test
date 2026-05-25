---
name: strategy-recommend
description: "根据 ADX 判断股票更适合趋势交易还是震荡交易。禁止 pip install、禁止检查环境。唯一正确做法：直接执行 exec，命令 python skills/strategy-recommend/scripts/recommend.py <股票代码> [--period 14] [--count 100]，workdir 为工作区根目录，返回脚本输出的 JSON。示例：python skills/strategy-recommend/scripts/recommend.py 600519.SH"
metadata:
  openclaw:
    emoji: "\u8c21"
    os: ["win32", "linux"]
    requires:
      bins: ["python"]
      packages: ["talib", "pandas", "numpy", "xtquant"]
    permissions:
      - "exec"
      - "file.read"
      - "file.write"
    timeout: 60
---

# 策略推荐技能

## 功能描述

根据 ADX 指标判断当前股票更适合**趋势交易**还是**震荡交易**，用于选策略参考。

- **ADX > 25**：趋势交易
- **15 < ADX <= 25**：灰色地带
- **ADX <= 15**：震荡交易

## 前置条件

1. 已安装 talib、pandas、numpy
2. 使用股票代码时需已配置 xtquant（MiniQMT），与 miniqmt-kline 一致

## 执行流程（必须遵守）

1. 收到策略推荐请求后，直接执行 exec，无需检查 pip、环境等
2. 命令：`python skills/strategy-recommend/scripts/recommend.py <stock_code> [--period 14] [--count 100]`
3. 或传入 CSV 路径：`python skills/strategy-recommend/scripts/recommend.py <csv_path> [--period 14]`
4. workdir 为工作区根目录
5. 将脚本输出的 JSON 直接作为结果返回给用户

禁止：安装依赖、检查环境、验证包。

## 使用方式

```bash
# 按股票代码（需 xtquant）
python skills/strategy-recommend/scripts/recommend.py <stock_code> [--period 14] [--count 100]

# 或按 CSV（需含 open/high/low/close 列）
python skills/strategy-recommend/scripts/recommend.py <csv_path> [--period 14]
```

### 参数说明

| 参数 | 类型 | 必选 | 描述 | 示例 |
|------|------|------|------|------|
| stock_code / csv_path | string | 是 | 股票代码或 CSV 文件路径 | 600519.SH, data/kline.csv |
| --period, -p | int | 否 | ADX 周期，默认 14 | 14 |
| --count, -n | int | 否 | K 线条数（仅股票代码时有效），默认 100 | 100 |

### ADX 分档规则

- ADX > 25：趋势明显，适合趋势策略（如均线、MACD 趋势跟踪）
- 15 < ADX <= 25：灰色地带，可结合其他指标
- ADX <= 15：震荡为主，适合震荡策略（如 RSI、布林带高抛低吸）

## 触发示例

- "600519 适合趋势还是震荡？"
- "根据 ADX 推荐一下贵州茅台的策略类型"
- "用 ADX 判断 000001.SZ 该用趋势还是震荡策略"
- "策略推荐 600519.SH"

## 输出格式

脚本输出 JSON，包含：

- `stock_code`: 股票代码（CSV 时为 source: csv）
- `adx_period`: ADX 周期
- `adx_current`: 当前 ADX 值
- `strategy_type`: trend | gray | range
- `strategy_label`: 趋势交易 | 灰色地带 | 震荡交易
- `bar_count`: 使用的 K 线数量
- `message`: 简短说明

## 参考

与 talib（ADX 计算）、miniqmt-kline（K 线数据）配合使用。
