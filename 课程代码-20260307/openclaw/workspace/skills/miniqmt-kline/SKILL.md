---
name: miniqmt-kline
description: "获取 A 股 K 线。禁止 pip install、禁止检查环境。唯一正确做法：直接执行 exec，命令 python skills/miniqmt-kline/scripts/get_kline.py <股票代码> 1d <条数>，workdir 为工作区根目录，返回脚本输出的 JSON。示例：python skills/miniqmt-kline/scripts/get_kline.py 600519.SH 1d 50"
metadata:
  openclaw:
    emoji: "📈"
    os: ["win32"]
    requires:
      bins: ["python"]
      packages: ["xtquant", "pandas"]
    permissions:
      - "exec"
      - "file.read"
      - "file.write"
    timeout: 60
---

# MiniQMT K 线数据获取技能

## 功能描述

连接 MiniQMT(迅投 QMT 精简版) 数据接口，获取指定股票的 K 线历史数据，支持多种周期（1 分钟、5 分钟、日线等）。

## 前置条件

1. 已安装 MiniQMT 客户端并登录
2. 已将 `xtquant` 包复制到 Python 环境（MiniQMT 安装目录下的 xtquant）
3. MiniQMT 客户端保持运行状态

## 执行流程（必须遵守）

1. 收到 K 线请求后，直接执行 exec，无需检查 pip、xtquant、python 版本等
2. 命令：`python skills/miniqmt-kline/scripts/get_kline.py <stock_code> [period] [count]`
3. workdir 为工作区根目录
4. 将脚本输出的 JSON 直接作为结果返回给用户

禁止：安装依赖、检查环境、验证包、调用 xtquant API。

## 使用方式

使用本技能的 get_kline.py 脚本，禁止直接调用 xtquant API（如 xtdata.get_price）。

```bash
python skills/miniqmt-kline/scripts/get_kline.py <stock_code> [period] [count]
```

### 参数说明

| 参数 | 类型 | 必选 | 描述 | 示例 |
|------|------|------|------|------|
| stock_code | string | 是 | 股票代码（带后缀） | 000001.SZ, 600519.SH |
| period | string | 否 | K 线周期，默认 1d | 1d(日线), 1m(1分钟), 5m(5分钟) |
| count | int | 否 | 获取条数，默认 100 | 100 |

### 周期说明

- `1d` - 日 K 线
- `1m` - 1 分钟 K 线
- `5m` - 5 分钟 K 线
- `15m` - 15 分钟 K 线
- `30m` - 30 分钟 K 线
- `1h` - 60 分钟 K 线

## 触发示例

- "获取贵州茅台 600519.SH 最近 50 天日线"
- "获取贵州茅台最近 100 天的日线数据"
- "查询 000001.SZ 的 5 分钟 K 线"
- "用 miniqmt 获取平安银行日线数据"

## 输出格式

脚本输出 JSON，包含：

- `stock_code`: 股票代码
- `period`: 周期
- `data_count`: 数据条数
- `data`: K 线列表，每条约含 `date`, `open`, `high`, `low`, `close`, `volume`, `amount`

