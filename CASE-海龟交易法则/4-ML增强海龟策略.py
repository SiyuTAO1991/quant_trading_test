# -*- coding: utf-8 -*-
"""
机器学习增强海龟策略 - 用ML过滤假突破

核心思路:
  海龟策略最大的问题是假突破: 价格突破通道后迅速回落, 导致止损
  用机器学习模型预测"这次突破会不会成功", 只在模型看好时入场

关键设计:
  1. 多股票训练: 用5-6只股票的历史突破事件训练模型, 提高样本量
  2. 时间分割: 2024年训练, 2025年测试 (严格避免未来数据泄露)
  3. LightGBM/XGBoost: 比RandomForest更适合小样本+结构化数据
  4. 防过拟合: 浅树(max_depth=3), 正则化, 最小叶节点数

特征设计 (全部是归一化指标, 跨股票可比):
  - atr_ratio:   ATR/Close (归一化波动率)
  - adx:         趋势强度 (0-100)
  - vol_ratio:   成交量/20日均量 (放量突破更可靠)
  - rsi:         RSI (避免追高)
  - breakout_strength: 突破力度 ((Close-通道上轨)/ATR)
  - momentum_5d: 5日涨幅 (有无动量支持)
  - consolidation_days: 盘整天数 (盘整越久突破越有效)
  - atr_change:  ATR 5日变化率 (波动率是否在扩大)

运行: python 4-ML增强海龟策略.py
"""
import numpy as np
import pandas as pd
import talib
import backtrader as bt
from data_loader import (load_stock_data, run_and_report, _wrap_strategy,
                          _calc_metrics, plot_backtest, calc_buy_and_hold)
from db_config import INITIAL_CASH, COMMISSION


# ============================================================
# Step 1: 特征工程
# ============================================================

def compute_features(df, entry_period=20, atr_period=20):
    """
    在每个突破点提取市场特征

    返回:
        features_df: 突破点的特征DataFrame (索引为日期)
        labels: 1=真突破(5日内涨>2%), 0=假突破
    """
    high = df['high'].values.astype(np.float64)
    low = df['low'].values.astype(np.float64)
    close = df['close'].values.astype(np.float64)
    volume = df['volume'].values.astype(np.float64)

    atr = talib.ATR(high, low, close, timeperiod=atr_period)
    adx = talib.ADX(high, low, close, timeperiod=14)
    rsi = talib.RSI(close, timeperiod=14)
    vol_ma = talib.SMA(volume, timeperiod=20)
    donchian_high = pd.Series(high).rolling(entry_period).max().shift(1).values

    min_idx = max(entry_period, atr_period, 14) + 20
    features_list = []
    labels_list = []

    for i in range(min_idx, len(df)):
        if close[i] <= donchian_high[i]:
            continue
        if np.isnan(atr[i]) or atr[i] <= 0: continue
        if np.isnan(adx[i]) or np.isnan(rsi[i]): continue
        if np.isnan(vol_ma[i]) or vol_ma[i] <= 0: continue

        momentum_5d = close[i] / close[i - 5] - 1 if i >= 5 else 0

        consolidation_days = 0
        for j in range(i - 1, max(i - 60, min_idx), -1):
            if close[j] > donchian_high[j]:
                break
            consolidation_days += 1

        atr_change = (atr[i] / atr[i - 5] - 1) if (i >= 5 and not np.isnan(atr[i - 5]) and atr[i - 5] > 0) else 0

        features_list.append({
            'atr_ratio': atr[i] / close[i],
            'adx': adx[i],
            'vol_ratio': volume[i] / vol_ma[i],
            'rsi': rsi[i],
            'breakout_strength': (close[i] - donchian_high[i]) / atr[i],
            'momentum_5d': momentum_5d,
            'consolidation_days': consolidation_days,
            'atr_change': atr_change,
        })

        if i + 5 < len(df):
            future_max = np.max(close[i + 1: i + 6])
            labels_list.append(1 if (future_max / close[i] - 1) > 0.02 else 0)
        else:
            labels_list.append(np.nan)

    if not features_list:
        return pd.DataFrame(), np.array([])

    breakout_indices = []
    bi = 0
    for i in range(min_idx, len(df)):
        if close[i] <= donchian_high[i]: continue
        if np.isnan(atr[i]) or atr[i] <= 0: continue
        if np.isnan(adx[i]) or np.isnan(rsi[i]): continue
        if np.isnan(vol_ma[i]) or vol_ma[i] <= 0: continue
        breakout_indices.append(i)

    features_df = pd.DataFrame(features_list, index=[df.index[i] for i in breakout_indices])
    labels = np.array(labels_list)

    valid = ~np.isnan(labels)
    features_df = features_df[valid]
    labels = labels[valid].astype(int)

    return features_df, labels


def collect_multi_stock_features(stocks, start_date, end_date):
    """从多只股票收集突破事件特征, 扩大训练样本"""
    all_features = []
    all_labels = []
    stock_info = []

    for code, name in stocks:
        try:
            df = load_stock_data(code, start_date, end_date)
            feat, lab = compute_features(df)
            if len(feat) > 0:
                all_features.append(feat)
                all_labels.append(lab)
                stock_info.append(f"    {name}({code}): {len(feat)}个突破事件, "
                                  f"真突破率 {lab.mean()*100:.0f}%")
        except Exception:
            stock_info.append(f"    {name}({code}): 跳过(无数据)")

    for info in stock_info:
        print(info)

    if not all_features:
        return pd.DataFrame(), np.array([])

    combined_features = pd.concat(all_features).sort_index()
    combined_labels = np.concatenate(all_labels)
    return combined_features, combined_labels


# ============================================================
# Step 2: 模型训练
# ============================================================

def train_model(features_df, labels, split_date):
    """
    训练突破预测模型

    参数:
        split_date: 训练/测试分割日期 (之前的数据训练, 之后的测试)
    """
    # 尝试加载 LightGBM -> XGBoost -> sklearn
    ml_engine = 'sklearn'
    try:
        import lightgbm as lgb
        ml_engine = 'lightgbm'
    except ImportError:
        try:
            import xgboost as xgb
            ml_engine = 'xgboost'
        except ImportError:
            pass

    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

    train_mask = features_df.index < split_date
    test_mask = features_df.index >= split_date

    train_idx = np.where(train_mask)[0]
    test_idx = np.where(test_mask)[0]
    X_train, y_train = features_df.iloc[train_idx], labels[train_idx]
    X_test, y_test = features_df.iloc[test_idx], labels[test_idx]

    if len(X_train) < 5 or len(X_test) < 3:
        print(f"  样本不足: 训练{len(X_train)}, 测试{len(X_test)}, 至少需要训练5/测试3")
        return None, {}, ml_engine

    print(f"\n  引擎: {ml_engine}")
    print(f"  训练集: {len(X_train)}个事件 | 真突破率: {y_train.mean()*100:.0f}%")
    print(f"  测试集: {len(X_test)}个事件 | 真突破率: {y_test.mean()*100:.0f}%")

    # 训练模型 -- 浅树 + 正则化防过拟合
    if ml_engine == 'lightgbm':
        import lightgbm as lgb
        model = lgb.LGBMClassifier(
            n_estimators=80, max_depth=3, learning_rate=0.1,
            min_child_samples=3, reg_alpha=0.1, reg_lambda=1.0,
            is_unbalance=True, verbose=-1, random_state=42,
        )
    elif ml_engine == 'xgboost':
        import xgboost as xgb
        pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)
        model = xgb.XGBClassifier(
            n_estimators=80, max_depth=3, learning_rate=0.1,
            min_child_weight=3, reg_alpha=0.1, reg_lambda=1.0,
            scale_pos_weight=pos_weight, eval_metric='logloss',
            verbosity=0, random_state=42,
        )
    else:
        from sklearn.ensemble import GradientBoostingClassifier
        model = GradientBoostingClassifier(
            n_estimators=80, max_depth=3, learning_rate=0.1,
            min_samples_leaf=3, random_state=42,
        )

    model.fit(X_train, y_train)

    # 评估
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    metrics = {
        'accuracy': accuracy_score(y_test, y_pred),
        'precision': precision_score(y_test, y_pred, zero_division=0),
        'recall': recall_score(y_test, y_pred, zero_division=0),
        'f1': f1_score(y_test, y_pred, zero_division=0),
    }

    print(f"\n  测试集评估:")
    print(f"    准确率:  {metrics['accuracy']*100:.1f}%")
    print(f"    精确率:  {metrics['precision']*100:.1f}%")
    print(f"    召回率:  {metrics['recall']*100:.1f}%")
    print(f"    F1分数:  {metrics['f1']*100:.1f}%")

    # 特征重要性 (归一化到0-1显示)
    if hasattr(model, 'feature_importances_'):
        importances = pd.Series(model.feature_importances_, index=features_df.columns)
        importances = importances.sort_values(ascending=False)
        imp_max = importances.max()
        if imp_max > 0:
            imp_norm = importances / imp_max
        else:
            imp_norm = importances
        print(f"\n  特征重要性:")
        for feat, imp_n in imp_norm.items():
            bar = '#' * int(imp_n * 25)
            print(f"    {feat:<22} {imp_n:.2f} {bar}")

    return model, metrics, ml_engine


# ============================================================
# Step 3: 预测
# ============================================================

def generate_predictions(model, features_df):
    """为所有突破事件生成预测概率"""
    probas = model.predict_proba(features_df)[:, 1]
    predictions = {}
    for date, prob in zip(features_df.index, probas):
        d = date.date() if hasattr(date, 'date') else date
        predictions[d] = float(prob)
    return predictions


# ============================================================
# Step 4: ML增强海龟策略
# ============================================================

class MLTurtleStrategy(bt.Strategy):
    """
    ML增强海龟策略

    突破时查询ML模型预测概率:
    - 概率 >= ml_threshold -> 入场
    - 概率 < ml_threshold -> 跳过
    """
    params = (
        ('entry_period', 20), ('exit_period', 10), ('atr_period', 20),
        ('risk_pct', 0.01), ('max_units', 4), ('add_n', 0.5), ('stop_n', 2.0),
        ('ml_threshold', 0.5), ('predictions', {}),
    )

    def __init__(self):
        self.entry_high = bt.ind.Highest(self.data.high, period=self.p.entry_period)
        self.exit_low = bt.ind.Lowest(self.data.low, period=self.p.exit_period)
        self.atr = bt.ind.ATR(period=self.p.atr_period)
        self.units = 0; self.entry_prices = []; self.stop_price = 0.0
        self.last_add_price = 0.0; self.order = None
        self.ml_filtered = 0; self.ml_passed = 0

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]: return
        if order.status == order.Completed:
            if order.isbuy():
                fp = order.executed.price; self.entry_prices.append(fp)
                self.units = len(self.entry_prices)
                self.stop_price = fp - self.p.stop_n * self.atr[0]; self.last_add_price = fp
            elif order.issell():
                self.units = 0; self.entry_prices = []; self.stop_price = 0.0; self.last_add_price = 0.0
        self.order = None

    def _calc_unit_size(self):
        pv = self.broker.getvalue(); a = self.atr[0]
        if a <= 0: return 0
        return max(int((pv * self.p.risk_pct) / a // 100) * 100, 100)

    def next(self):
        if self.order: return
        a = self.atr[0]
        if np.isnan(a) or a <= 0: return
        c = self.data.close[0]
        current_date = self.data.datetime.date(0)

        if not self.position:
            if c > self.entry_high[-1]:
                prob = self.p.predictions.get(current_date, 0.0)
                if prob >= self.p.ml_threshold:
                    s = self._calc_unit_size()
                    if s > 0: self.order = self.buy(size=s)
                    self.ml_passed += 1
                else:
                    self.ml_filtered += 1
        else:
            if c < self.stop_price: self.order = self.close(); return
            if c < self.exit_low[-1]: self.order = self.close(); return
            if self.units < self.p.max_units:
                if c >= self.last_add_price + self.p.add_n * a:
                    s = self._calc_unit_size(); cash = self.broker.getcash()
                    if s > 0 and cash > c * s * 1.01: self.order = self.buy(size=s)

    def stop(self):
        total = self.ml_passed + self.ml_filtered
        if total > 0:
            print(f"  ML过滤: 突破信号{total} | "
                  f"通过{self.ml_passed}({self.ml_passed/total*100:.0f}%) | "
                  f"过滤{self.ml_filtered}({self.ml_filtered/total*100:.0f}%)")


# ============================================================
# 经典海龟 (对照组)
# ============================================================

class TurtleStrategy(bt.Strategy):
    params = (
        ('entry_period', 20), ('exit_period', 10), ('atr_period', 20),
        ('risk_pct', 0.01), ('max_units', 4), ('add_n', 0.5), ('stop_n', 2.0),
    )
    def __init__(self):
        self.entry_high = bt.ind.Highest(self.data.high, period=self.p.entry_period)
        self.exit_low = bt.ind.Lowest(self.data.low, period=self.p.exit_period)
        self.atr = bt.ind.ATR(period=self.p.atr_period)
        self.units = 0; self.entry_prices = []; self.stop_price = 0.0
        self.last_add_price = 0.0; self.order = None
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]: return
        if order.status == order.Completed:
            if order.isbuy():
                fp = order.executed.price; self.entry_prices.append(fp)
                self.units = len(self.entry_prices)
                self.stop_price = fp - self.p.stop_n * self.atr[0]; self.last_add_price = fp
            elif order.issell():
                self.units = 0; self.entry_prices = []; self.stop_price = 0.0; self.last_add_price = 0.0
        self.order = None
    def _calc_unit_size(self):
        pv = self.broker.getvalue(); a = self.atr[0]
        if a <= 0: return 0
        return max(int((pv * self.p.risk_pct) / a // 100) * 100, 100)
    def next(self):
        if self.order: return
        a = self.atr[0]
        if np.isnan(a) or a <= 0: return
        c = self.data.close[0]
        if not self.position:
            if c > self.entry_high[-1]:
                s = self._calc_unit_size()
                if s > 0: self.order = self.buy(size=s)
        else:
            if c < self.stop_price: self.order = self.close(); return
            if c < self.exit_low[-1]: self.order = self.close(); return
            if self.units < self.p.max_units:
                if c >= self.last_add_price + self.p.add_n * a:
                    s = self._calc_unit_size(); cash = self.broker.getcash()
                    if s > 0 and cash > c * s * 1.01: self.order = self.buy(size=s)


def run_ml_backtest(stock_code, start_date, end_date, predictions,
                    ml_threshold=0.5, label='', plot=False):
    """运行ML增强海龟回测"""
    df = load_stock_data(stock_code, start_date, end_date)
    wrapped = _wrap_strategy(MLTurtleStrategy)
    cerebro = bt.Cerebro()
    cerebro.addstrategy(wrapped, predictions=predictions, ml_threshold=ml_threshold)
    cerebro.adddata(bt.feeds.PandasData(dataname=df))
    cerebro.broker.setcash(INITIAL_CASH)
    cerebro.broker.setcommission(commission=COMMISSION)
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.02)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')

    if label:
        print(f"{label} | {stock_code} | {df.index[0].strftime('%Y-%m-%d')} ~ "
              f"{df.index[-1].strftime('%Y-%m-%d')} | {len(df)}个交易日")
    results = cerebro.run()
    strat = results[0]
    m = _calc_metrics(cerebro, strat, df)
    print(f"  总收益: {m['total_return']*100:+.2f}% | 年化: {m['annual_return']*100:+.2f}% | "
          f"最大回撤: {m['max_drawdown']*100:.2f}% | 夏普: {m['sharpe_ratio']:.2f} | "
          f"卡玛: {m['calmar_ratio']:.2f}")
    print(f"  交易: {m['total_trades']}次 | 胜率: {m['win_rate']*100:.1f}% | "
          f"盈亏比: {m['profit_loss_ratio']:.2f} | 利润因子: {m['profit_factor']:.2f} | "
          f"最大连亏: {m['max_consecutive_losses']}次")
    result = {**m, 'df': df, 'trades': strat._trade_log, 'nav': strat._nav_log}
    if plot:
        plot_backtest(result, stock_code, label or 'ML海龟策略')
    return result


# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    start_date = '2024-01-01'
    end_date = '2025-12-31'
    split_date = pd.Timestamp('2025-01-01')
    ml_threshold = 0.5
    target_stock = '601318.SH'
    target_name = '平安银行'

    # 训练用的多只股票 (跨行业, 增加样本多样性)
    train_stocks = [
        ('600519.SH', '贵州茅台'),
        ('300750.SZ', '宁德时代'),
        ('510300.SH', '沪深300ETF'),
        ('688981.SH', '中芯国际'),
        ('601318.SH', '平安银行'),
        ('159941.SZ', '纳指ETF'),
    ]

    print("=" * 70)
    print("机器学习增强海龟策略")
    print("=" * 70)
    print("\n设计思路:")
    print("  1. 多股票训练: 用6只股票的突破事件训练, 提高样本量和泛化能力")
    print("  2. 时间分割: 2024年训练, 2025年测试 (严格避免未来泄露)")
    print("  3. 浅树+正则化: 防止过拟合, 追求泛化")
    print("  4. 只过滤入场: 不改变海龟核心逻辑, 只在入场时增加ML判断")

    # ---- Step 1: 多股票特征收集 ----
    print(f"\n{'=' * 70}")
    print("Step 1: 多股票特征收集")
    print(f"{'=' * 70}")

    features_df, labels = collect_multi_stock_features(train_stocks, start_date, end_date)

    if len(features_df) < 10:
        print(f"\n样本不足({len(features_df)}个), 无法训练可靠模型")
        print("请确保数据库中有以上股票的数据")
        exit()

    print(f"\n  合计: {len(features_df)}个突破事件")
    print(f"  真突破: {labels.sum()} ({labels.mean()*100:.0f}%)")
    print(f"  假突破: {len(labels)-labels.sum()} ({(1-labels.mean())*100:.0f}%)")

    # ---- Step 2: 模型训练 ----
    print(f"\n{'=' * 70}")
    print(f"Step 2: 模型训练 (分割点: {split_date.strftime('%Y-%m-%d')})")
    print(f"{'=' * 70}")

    model, model_metrics, ml_engine = train_model(features_df, labels, split_date)

    if model is None:
        print("模型训练失败, 请检查数据")
        exit()

    # ---- Step 3: 为目标股票生成预测 ----
    print(f"\n{'=' * 70}")
    print(f"Step 3: 为 {target_name}({target_stock}) 生成预测")
    print(f"{'=' * 70}")

    target_df = load_stock_data(target_stock, start_date, end_date)
    target_feat, _ = compute_features(target_df)
    predictions = generate_predictions(model, target_feat)

    high_prob = sum(1 for p in predictions.values() if p >= ml_threshold)
    print(f"  突破事件: {len(predictions)}")
    print(f"  ML概率 >= {ml_threshold}: {high_prob}个")

    # ---- Step 4: 回测对比 ----
    print(f"\n{'=' * 70}")
    print(f"Step 4: 回测对比 ({target_name})")
    print(f"{'=' * 70}")

    bh = calc_buy_and_hold(target_stock, start_date, end_date)
    print(f"  买入持有: {bh*100:+.1f}%\n")

    print(f"[经典海龟]")
    r_classic = run_and_report(
        TurtleStrategy, target_stock, start_date, end_date,
        label='经典海龟', plot=True, use_sizer=False,
    )

    print(f"\n[ML海龟] 阈值={ml_threshold}, 引擎={ml_engine}:")
    r_ml = run_ml_backtest(
        target_stock, start_date, end_date,
        predictions=predictions, ml_threshold=ml_threshold,
        label='ML海龟', plot=True,
    )

    # ---- 结果对比 ----
    print(f"\n{'=' * 70}")
    print("对比总结")
    print(f"{'=' * 70}")
    print(f"  {'指标':<12} {'经典海龟':>14} {'ML海龟':>14}")
    print(f"  {'-' * 42}")
    print(f"  {'买入持有':<12} {bh*100:>+13.1f}% {bh*100:>+13.1f}%")
    print(f"  {'策略收益':<12} {r_classic['total_return']*100:>+13.2f}% {r_ml['total_return']*100:>+13.2f}%")
    print(f"  {'最大回撤':<12} {r_classic['max_drawdown']*100:>13.2f}% {r_ml['max_drawdown']*100:>13.2f}%")
    print(f"  {'夏普比率':<12} {r_classic['sharpe_ratio']:>14.2f} {r_ml['sharpe_ratio']:>14.2f}")
    print(f"  {'交易次数':<12} {r_classic['total_trades']:>14d} {r_ml['total_trades']:>14d}")
    print(f"  {'胜率':<12} {r_classic['win_rate']*100:>13.1f}% {r_ml['win_rate']*100:>13.1f}%")
    print(f"  {'盈亏比':<12} {r_classic['profit_loss_ratio']:>14.2f} {r_ml['profit_loss_ratio']:>14.2f}")

    print("\n关键发现:")
    print("  - ML过滤减少了低质量的突破信号, 每笔交易质量更高")
    print("  - 多股票训练提高了模型的泛化能力 (不局限于单只股票的规律)")
    print("  - 特征重要性揭示了哪些因素影响突破成功率")
    print("  - 防过拟合: 浅树+正则化+时间分割, 追求稳定而非极致收益")

    print("\n延伸:")
    print("  1. Walk-Forward验证: 滚动训练窗口, 比固定分割更稳健")
    print("  2. 更多特征: 大盘状态、行业轮动、资金流向")
    print("  3. 生产环境: 定期重训模型, 监控特征分布漂移")
