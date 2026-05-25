# -*- coding: utf-8 -*-
"""
chan.py 封装模块

将开源 chan.py 库的 CChan 接口封装成统一的数据结构，
方便教学脚本调用和可视化。

chan.py 位于: 9-缠论精华量化/chan.py
"""

import sys
import os
import pandas as pd
import numpy as np

CHAN_PY_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', 'chan.py'))


def _ensure_path():
    if CHAN_PY_PATH not in sys.path:
        sys.path.insert(0, CHAN_PY_PATH)


def _ts(t):
    """将 chan.py 的时间对象转为 Timestamp"""
    return pd.Timestamp(f'{t.year:04d}-{t.month:02d}-{t.day:02d}')


def run_chan(df, symbol='stock', config_dict=None):
    """
    对 DataFrame 运行 chan.py 缠论分析

    参数:
        df: DataFrame, 含 open/high/low/close/volume, DatetimeIndex
        symbol: 股票代码标识
        config_dict: CChanConfig 参数字典（可选）

    返回:
        dict: 包含 klc_list, fractals, bi_list, seg_list, zs_list, bsp_list 等
    """
    _ensure_path()
    from DataAPI import DfApi as DfApiModule
    from Common.CEnum import KL_TYPE
    from Chan import CChan
    from ChanConfig import CChanConfig

    DfApiModule._DF_CACHE[symbol] = df
    config = CChanConfig(config_dict or {})
    chan = CChan(
        code=symbol,
        data_src='custom:DfApi.DfApi',
        lv_list=[KL_TYPE.K_DAY],
        config=config,
    )
    kl = chan[0]

    # ---- 提取合并K线 ----
    klc_list = []
    for klc in kl.lst:
        t0 = klc.lst[0].time
        fx_type = 'unknown'
        if hasattr(klc, 'fx') and klc.fx is not None:
            fn = klc.fx.name if hasattr(klc.fx, 'name') else str(klc.fx)
            if 'TOP' in fn:
                fx_type = 'top'
            elif 'BOTTOM' in fn:
                fx_type = 'bottom'
        klc_list.append({
            'date': _ts(t0),
            'high': float(klc.high),
            'low': float(klc.low),
            'idx': klc.idx,
            'fx': fx_type,
            'raw_count': len(klc.lst),
        })

    # ---- 提取分型 ----
    fractals = [k for k in klc_list if k['fx'] in ('top', 'bottom')]

    # ---- 提取笔 ----
    bi_list = []
    for bi in kl.bi_list:
        is_up = bi.dir.name == 'UP'
        bklc = bi.begin_klc
        eklc = bi.end_klc
        bt = _ts(bklc.lst[0].time)
        et = _ts(eklc.lst[-1].time)
        if is_up:
            sp, ep = float(bklc.low), float(eklc.high)
        else:
            sp, ep = float(bklc.high), float(eklc.low)

        if is_up:
            peak_klu = eklc.get_high_peak_klu() if hasattr(eklc, 'get_high_peak_klu') else None
            trough_klu = bklc.get_low_peak_klu() if hasattr(bklc, 'get_low_peak_klu') else None
        else:
            peak_klu = bklc.get_high_peak_klu() if hasattr(bklc, 'get_high_peak_klu') else None
            trough_klu = eklc.get_low_peak_klu() if hasattr(eklc, 'get_low_peak_klu') else None
        start_raw = _ts(trough_klu.time) if (is_up and trough_klu) else bt
        end_raw = _ts(peak_klu.time) if (is_up and peak_klu) else et
        if not is_up:
            start_raw = _ts(peak_klu.time) if peak_klu else bt
            end_raw = _ts(trough_klu.time) if trough_klu else et

        bi_item = {
            'start_date': bt,
            'end_date': et,
            'start_raw_date': start_raw,
            'end_raw_date': end_raw,
            'start_price': sp,
            'end_price': ep,
            'direction': 'up' if is_up else 'down',
            'is_sure': bi.is_sure if hasattr(bi, 'is_sure') else True,
            'klc_count': bi.get_klc_cnt() if hasattr(bi, 'get_klc_cnt') else 0,
            'klu_count': bi.get_klu_cnt() if hasattr(bi, 'get_klu_cnt') else 0,
            'seg_idx': bi.seg_idx if hasattr(bi, 'seg_idx') else -1,
        }

        try:
            bi_item['macd_area'] = float(bi.Cal_MACD_area())
        except Exception:
            bi_item['macd_area'] = 0.0

        if bi.bsp:
            bsp_types = [str(t.value) if hasattr(t, 'value') else str(t) for t in bi.bsp.type]
            bi_item['bsp_type'] = ','.join(bsp_types)
            bi_item['bsp_is_buy'] = bi.bsp.is_buy
            bi_item['bsp_date'] = _ts(bi.bsp.klu.time) if hasattr(bi.bsp, 'klu') else et
        else:
            bi_item['bsp_type'] = None
            bi_item['bsp_is_buy'] = None
            bi_item['bsp_date'] = None

        bi_list.append(bi_item)

    # ---- 提取线段 ----
    seg_list = []
    for seg in kl.seg_list:
        bt = _ts(seg.start_bi.begin_klc.lst[0].time)
        et = _ts(seg.end_bi.end_klc.lst[-1].time)
        is_up = seg.dir.name == 'UP'

        seg_zs_list = []
        if hasattr(seg, 'zs_lst'):
            for zs in seg.zs_lst:
                seg_zs_list.append({
                    'ZG': float(zs.high),
                    'ZD': float(zs.low),
                })

        seg_list.append({
            'start_date': bt,
            'end_date': et,
            'direction': 'up' if is_up else 'down',
            'is_sure': seg.is_sure if hasattr(seg, 'is_sure') else True,
            'bi_count': seg.cal_bi_cnt() if hasattr(seg, 'cal_bi_cnt') else 0,
            'zs_list': seg_zs_list,
        })

    # ---- 提取中枢 ----
    zs_list = []
    for zs in kl.zs_list:
        begin_t = None
        end_t = None
        if hasattr(zs, 'begin_bi') and zs.begin_bi is not None:
            begin_t = _ts(zs.begin_bi.begin_klc.lst[0].time)
        elif hasattr(zs, 'begin') and hasattr(zs.begin, 'lst'):
            begin_t = _ts(zs.begin.lst[0].time)
        if hasattr(zs, 'end_bi') and zs.end_bi is not None:
            end_t = _ts(zs.end_bi.end_klc.lst[-1].time)
        elif hasattr(zs, 'end') and hasattr(zs.end, 'lst'):
            end_t = _ts(zs.end.lst[-1].time)
        zs_list.append({
            'ZG': float(zs.high),
            'ZD': float(zs.low),
            'center': float(zs.mid) if hasattr(zs, 'mid') else (float(zs.high) + float(zs.low)) / 2,
            'start_date': begin_t,
            'end_date': end_t,
            'peak_high': float(zs.peak_high) if hasattr(zs, 'peak_high') else float(zs.high),
            'peak_low': float(zs.peak_low) if hasattr(zs, 'peak_low') else float(zs.low),
        })

    # ---- 提取买卖点汇总 ----
    bsp_list = [b for b in bi_list if b['bsp_type'] is not None]

    DfApiModule._DF_CACHE.pop(symbol, None)

    return {
        'klc_list': klc_list,
        'fractals': fractals,
        'bi_list': bi_list,
        'seg_list': seg_list,
        'zs_list': zs_list,
        'bsp_list': bsp_list,
        'raw_kl': kl,
    }


def chan_to_signal_df(df, chan_data):
    """
    将 run_chan() 的结果转换为 ChanPandasData 兼容的 DataFrame

    新增列:
        chan_signal: 0=无信号, 1=一买, 2=二买, 3=三买, -3=三卖
        chan_zg:     当前所在中枢的ZG（向前填充）
        chan_zd:     当前所在中枢的ZD（向前填充）

    参数:
        df: 原始K线 DataFrame
        chan_data: run_chan() 的返回值

    返回:
        DataFrame, 含信号列, 可直接送入 ChanPandasData
    """
    result = df.copy()
    result['chan_signal'] = 0
    result['chan_zg'] = np.nan
    result['chan_zd'] = np.nan

    # 映射买卖点信号
    bsp_signal_map = {
        '1': (1, True), '2': (2, True), '2s': (2, True), '3': (3, True),
        '1s': (-1, False), '2_sell': (-2, False), '3_sell': (-3, False),
        '3a': (3, True),
    }

    for bi in chan_data['bsp_list']:
        bsp_type = bi['bsp_type']
        bsp_date = bi['bsp_date']
        if bsp_date is None or bsp_type is None:
            continue
        if bsp_date not in result.index:
            continue

        # bsp_type 可能是逗号分隔的多个类型，取优先级最高的
        best_signal = 0
        for t in bsp_type.split(','):
            t = t.strip()
            if t in bsp_signal_map:
                sig_val, is_buy = bsp_signal_map[t]
                if abs(sig_val) > abs(best_signal):
                    best_signal = sig_val

        if best_signal != 0:
            result.loc[bsp_date, 'chan_signal'] = best_signal

    # 填充中枢的 ZG/ZD
    for zs in chan_data['zs_list']:
        if zs['start_date'] is None or zs['end_date'] is None:
            continue
        mask = (result.index >= zs['start_date']) & (result.index <= zs['end_date'])
        result.loc[mask, 'chan_zg'] = result.loc[mask, 'chan_zg'].fillna(zs['ZG'])
        result.loc[mask, 'chan_zd'] = result.loc[mask, 'chan_zd'].fillna(zs['ZD'])

    # 在信号点也填充对应中枢的ZG/ZD
    for bi in chan_data['bsp_list']:
        bsp_date = bi['bsp_date']
        if bsp_date is None or bsp_date not in result.index:
            continue
        # 找到该信号对应的中枢
        for zs in reversed(chan_data['zs_list']):
            if zs['end_date'] and bsp_date >= zs['end_date']:
                if np.isnan(result.loc[bsp_date, 'chan_zg']):
                    result.loc[bsp_date, 'chan_zg'] = zs['ZG']
                if np.isnan(result.loc[bsp_date, 'chan_zd']):
                    result.loc[bsp_date, 'chan_zd'] = zs['ZD']
                break

    result['chan_zg'] = result['chan_zg'].ffill()
    result['chan_zd'] = result['chan_zd'].ffill()

    return result


def draw_chan_chart(ax, df, chan_data, show_bi=True, show_seg=True,
                   show_zs=True, show_bsp=True, show_fractals=False,
                   show_grid_levels=None):
    """
    在 ax 上绘制 chan.py 分析结果的蜡烛图

    参数:
        ax: matplotlib Axes
        df: 原始 DataFrame
        chan_data: run_chan() 返回的字典
        show_grid_levels: 可选, list of float, 在图上画网格水平线

    返回:
        date_to_x: dict, 日期到x位置的映射
    """
    import matplotlib.patches as patches

    # 简化版蜡烛图绘制（不依赖 chan_analyzer）
    d2x = _draw_candlestick(ax, df)

    def _lx(date_val):
        if date_val in d2x:
            return d2x[date_val]
        if hasattr(date_val, 'date'):
            return d2x.get(date_val.date())
        for k, v in d2x.items():
            if hasattr(k, 'date') and k.date() == date_val:
                return v
        return None

    # 分型
    if show_fractals and chan_data['fractals']:
        for f in chan_data['fractals']:
            x = _lx(f['date'])
            if x is None:
                continue
            if f['fx'] == 'top':
                ax.scatter(x, f['high'], marker='v', color='#e74c3c', s=50, zorder=5, alpha=0.6)
            else:
                ax.scatter(x, f['low'], marker='^', color='#2ecc71', s=50, zorder=5, alpha=0.6)

    # 笔
    if show_bi and chan_data['bi_list']:
        for bi in chan_data['bi_list']:
            c = '#e74c3c' if bi['direction'] == 'up' else '#27ae60'
            sx = _lx(bi['start_raw_date'])
            ex = _lx(bi['end_raw_date'])
            if sx is not None and ex is not None:
                ax.plot([sx, ex], [bi['start_price'], bi['end_price']],
                        color=c, linewidth=1.8, alpha=0.85, zorder=4)

    # 线段
    if show_seg and chan_data['seg_list']:
        seg_colors = ['#9b59b6', '#e67e22', '#1abc9c', '#e74c3c']
        for si, seg in enumerate(chan_data['seg_list']):
            seg_bis = [b for b in chan_data['bi_list']
                       if b['start_date'] >= seg['start_date']
                       and b['end_date'] <= seg['end_date']]
            if seg_bis:
                if seg['direction'] == 'up':
                    sp = min(b['start_price'] for b in seg_bis)
                    ep = max(b['end_price'] for b in seg_bis)
                else:
                    sp = max(b['start_price'] for b in seg_bis)
                    ep = min(b['end_price'] for b in seg_bis)
                sx = _lx(seg['start_date'])
                ex = _lx(seg['end_date'])
                if sx is not None and ex is not None:
                    sc = seg_colors[si % len(seg_colors)]
                    ax.plot([sx, ex], [sp, ep],
                            color=sc, linewidth=3.5, linestyle='--', alpha=0.7, zorder=3)

    # 中枢
    if show_zs and chan_data['zs_list']:
        for zs in chan_data['zs_list']:
            if zs['start_date'] and zs['end_date']:
                xl = _lx(zs['start_date'])
                xr = _lx(zs['end_date'])
                if xl is not None and xr is not None:
                    rect = patches.Rectangle(
                        (xl, zs['ZD']),
                        xr - xl,
                        zs['ZG'] - zs['ZD'],
                        linewidth=1.5, edgecolor='#3498db', facecolor='#3498db',
                        alpha=0.15, zorder=2,
                    )
                    ax.add_patch(rect)
                    ax.text(xl, zs['ZG'],
                            f" ZG={zs['ZG']:.1f}\n ZD={zs['ZD']:.1f}",
                            fontsize=7, color='#2c3e50', va='bottom')

    # 买卖点
    if show_bsp and chan_data['bsp_list']:
        bsp_names = {'1': '一买', '2': '二买', '2s': '二买S', '3': '三买',
                     '1s': '一卖', '2_sell': '二卖', '3_sell': '三卖', '3a': '三买'}
        bsp_colors = {'1': '#8e44ad', '2': '#e67e22', '2s': '#f39c12',
                      '3': '#e74c3c', '1s': '#2c3e50', '2_sell': '#27ae60',
                      '3_sell': '#16a085', '3a': '#e74c3c'}
        for bi in chan_data['bsp_list']:
            bsp_type = bi['bsp_type']
            is_buy = bi['bsp_is_buy']
            x = _lx(bi['bsp_date'])
            if x is None:
                continue
            marker = '^' if is_buy else 'v'
            first_type = bsp_type.split(',')[0].strip()
            color = bsp_colors.get(first_type, '#333333')
            name = bsp_names.get(first_type, bsp_type)
            price = bi['end_price']
            ax.scatter(x, price, marker=marker, color=color,
                       s=200, zorder=7, edgecolors='black', linewidths=1)
            offset_y = 10 if is_buy else -15
            ax.annotate(name, (x, price),
                        textcoords="offset points", xytext=(10, offset_y),
                        fontsize=9, fontweight='bold', color=color,
                        bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7))

    # 网格线（可选）
    if show_grid_levels:
        n = len(df)
        for level in show_grid_levels:
            ax.axhline(y=level, color='#f39c12', linewidth=0.8, linestyle=':', alpha=0.6, zorder=1)

    return d2x


def _draw_candlestick(ax, df, width_ratio=0.6):
    """在ax上绘制标准K线蜡烛图, 返回 date_to_x 映射"""
    n = len(df)
    d2x = {}
    for i, dt in enumerate(df.index):
        d2x[dt] = i
        if hasattr(dt, 'date'):
            d2x[dt.date()] = i

    opens = df['open'].values.astype(float)
    highs = df['high'].values.astype(float)
    lows = df['low'].values.astype(float)
    closes = df['close'].values.astype(float)
    price_range = highs.max() - lows.min()
    min_body = price_range * 0.002

    for i in range(n):
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        if c >= o:
            color = '#e74c3c'
            body_bottom, body_height = o, max(c - o, min_body)
        else:
            color = '#27ae60'
            body_bottom, body_height = c, max(o - c, min_body)
        ax.plot([i, i], [l, h], color=color, linewidth=0.8, zorder=1)
        ax.bar(i, body_height, bottom=body_bottom, width=width_ratio,
               color=color, edgecolor=color, linewidth=0.5, zorder=2)

    step = max(1, n // 12)
    tick_pos = list(range(0, n, step))
    if (n - 1) not in tick_pos:
        tick_pos.append(n - 1)
    total_days = (df.index[-1] - df.index[0]).days if n > 1 else 365
    if total_days <= 180:
        tick_lbl = [df.index[i].strftime('%m-%d') for i in tick_pos]
    else:
        tick_lbl = [df.index[i].strftime('%Y-%m') for i in tick_pos]
    ax.set_xticks(tick_pos)
    ax.set_xticklabels(tick_lbl, rotation=45, ha='right', fontsize=8)
    ax.set_xlim(-1, n)

    return d2x
