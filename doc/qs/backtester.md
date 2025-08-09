# 回测框架 (`qs.backtester`) 使用说明 / 设计指南

> 更新: 新增 `Broker.rebalance_target_percents` 及多标的 `order_target_percent_sym` 等接口, 使仓位调仓方式更贴近 backtrader 风格 (batch rebalance + target percent)。

## 1. 组件概览
面向: 供大模型 / 开发者阅读以快速理解 `src/qs/backtester` 组件协同方式，并据此在 `src/qs/strategy` 新增策略、在 `scripts/` 下快速写测试脚本。

当前框架核心特征:
- 轻量、无事件撮合/撮合簿，基于顺序日线 Bar 循环。
- 支持多标的持仓 (Broker `positions` dict)；仍兼容单标的旧 API (`broker.buy()` 等)。
- 策略可提供 `mark_prices` 钩子为多个资产估值；否则默认仅对默认 symbol 以当日 `bar.close` 估值。
- 交易成本模型: 简化佣金 + 卖出印花税 + 固定最小佣金；固定百分比滑点。
- 结果: 逐 Bar 记录权益曲线；可通过 `qs.backtester.stats` 计算年化、最大回撤、波动率、夏普、胜率等。

目录组件速览:
| 模块 | 作用 | 关键类 / 函数 |
|------|------|--------------|
| `data.py` | 提供顺序遍历的 Bar 序列 | `Bar`, `DataFeed` |
| `broker.py` | 资金、持仓、成交记录、费用计算 | `Broker`, `Position`, `TradeRecord` |
| `engine.py` | 驱动回测循环，调用策略 & 更新估值 | `BacktestEngine`, `Strategy`(Protocol), `EquityPoint` |
| `stats.py` | 统计指标 | `compute_annual_returns`, `compute_max_drawdown`, `compute_risk_metrics` |
| `strategy/` | 用户策略实现 | 示例: `simple_strategy_2.py` |

---
## 2. Broker / 交易执行层
多标的支持要点:
- `positions: Dict[str, Position]`，`Position(size, avg_price)`。
- 单标模式下 `broker.symbol` 指定默认标的；仍可用 `broker.buy()/sell()` 等旧接口。
- 多标策略使用显式 `buy_sym(s, symbol, price, size)` / `sell_sym` / `buy_all_sym` / `sell_all_sym` / `order_target_percent_sym`。
- 估值: `broker.last_prices` 维护最新标的估值价；`total_equity()` = 现金 + Σ(size * mark_price)。未标记价格 fallback 为平均成本或调用时提供的 `fallback_price`。

执行流程 (示例买入):
1. 输入原始信号价 (通常当日开盘价)。
2. 滑点模型 `SlippageModel.adjust_price` 生成执行价。
3. 估算佣金：`CommissionInfo.buy_fees`，若卖出再加税。
4. 现金校验：若给定 size 资金不足则回退减小 size 直到可成交 (或放弃)。
5. 更新持仓均价 (加权) 与现金；记录 `TradeRecord`。

费用模型参数:
- `commission_rate` 默认 0.015%
- `tax_rate` (仅卖出) 默认 0.05%
- `min_commission` 单笔最低 5
- `slippage` 执行价±0.02%

`TradeRecord` 字段帮助审计：执行价、名义金额、费用、成交后现金/持仓/权益。

### 2.3 新增: 多标的目标权重再平衡
为接近 backtrader 的 `order_target_percent` 用法, 本框架在 `Broker` 中加入:

- `order_target_percent_sym(trade_date, symbol, price, target)` : 单标的目标权重调整 (基于当前组合总权益估算 size)。
- `rebalance_target_percents(trade_date, price_map, target_weights)` : 批量再平衡 (近似 backtrader 中在同一 bar 下多次下单最终形成目标组合)。
  - `price_map`: `{symbol: open_price}` 当前再平衡基准价 (建议用开盘价 / 下单执行价基准)。
  - `target_weights`: `{symbol: weight}` 目标权重 (0-1 之间, 未出现的 symbol 视为 0 → 若持仓会被清仓)。
  - 执行逻辑: 先卖出(含清仓) → 再买入, 降低资金占用冲突; size 向下取整, 允许残余现金。

示例:
```python
price_map = { '600028.SH': open_a, '00386.HK': open_h }
weights = { '600028.SH': 0.6, '00386.HK': 0.4 }
broker.rebalance_target_percents(trade_date, price_map, weights)
```

### 2.4 backtrader 对齐要点
| 目标 | backtrader | 本框架目前 | 说明 / 差异化计划 |
|------|------------|-----------|------------------|
| 下单粒度 | `order_target_percent` | `order_target_percent_sym` | 已实现单标的百分比调仓 |
| 批量再平衡 | 多次调用 + 同一 bar 统一撮合 | `rebalance_target_percents` | 新增一次性权重对齐 |
| 数据源 | `DataFeed` 多 data lines | 当前单序列 + strategy.mark_prices 辅助 | 计划: 引入 MultiDataFeed 支持多市场对齐 Bar |
| 委托生命周期 | Order 对象状态机 | 即时成交, 无 Order | 可选后续扩展: 加入挂单与滑点模型分离 |
| 账户属性 | `cash`, `value`, `position` | `cash`, `total_equity()`, positions dict | 已有多标的 positions, 增补统计接口可拓展 |
| 费用模型 | CommissionInfo & Slippage via broker params | CommissionInfo + SlippageModel | 结构相似, 支持定制化扩展 |
| 复权处理 | 内部按 data.adjusted | 需用户自行在数据预处理层处理 | 建议在数据层扩展复权价 feed |

后续若要进一步贴近 backtrader, 可增: (1) Order 对象, (2) Bar 多数据同步迭代器, (3) Analyzer 模块注册。

## 3. Strategy / 策略编写
策略需实现:
- `on_bar(bar, feed, broker)` —— 每个 Bar 调用，先于估值更新。内部可:
  - 读取 `feed.prev` 做基于“上一日信息”决策。
  - 通过 `broker` 下多标的 API 发单。
- 可选 `mark_prices(bar, feed, broker) -> Mapping[symbol, price]`:
  - 返回本 Bar 用于估值的价格字典 (例如 A/H 收盘价、或多资产不同来源的同步价)。
  - 若未提供或返回空，Engine 回退到默认 symbol 的 `bar.close`。

设计约定:
- `on_bar` 不直接修改 `feed` 内部索引。
- 策略应尽量使用上一日信息 (避免未来函数)。
- 若需要互斥持仓，先平另一侧再买入 (参见 `SimpleStrategy2`)。

### 3.3 A/H 溢价策略示例 (季度再平衡)
新策略需求: 每季度第一个交易日, 计算最近一个交易日 A/H 溢价 (来自 `ah_premium` 或现场计算), 选取溢价最高的 H 股若干支 + 溢价最低的 A 股若干支, 组合各占 50% 权重, 平均分配。

伪代码:
```python
if is_quarter_first_day(trade_date):
    df = load_latest_premium(trade_date_prev)  # 或 trade_date 当日若已可得
    top_h = select_top_h_symbols(df, k)
    bottom_a = select_bottom_a_symbols(df, k)
    # 目标权重: 50% 分配给 H 端, 50% 给 A 端
    w_each_h = 0.5 / len(top_h)
    w_each_a = 0.5 / len(bottom_a)
    targets = {**{s: w_each_h for s in top_h}, **{s: w_each_a for s in bottom_a}}
    broker.rebalance_target_percents(trade_date, price_map, targets)
```
注意点:
- 再平衡使用当日开盘价 (需准备 `price_map` ), 若缺开盘价可回退到昨收。
- 避免选股集合在极端情况 len=0 → 跳过。

## 4. DataFeed / 数据输入
`Bar` 字段:
- `trade_date` (YYYYMMDD)
- `open/high/low/close`
- `pct_chg` (可空, 策略可用上一日涨跌幅)

`DataFeed` 只维护内部索引 `_i`:
- `current` 返回当前位置 Bar。
- `prev` 返回上一 Bar (首个 Bar 时为 `None`)。
- `step()` 推进到下一条 (末尾返回 False)。
- `reset()` 重置遍历。

策略只读 feed，不直接修改。

## 5. Engine / 事件循环
初始化:
1. `feed.reset()`
2. 取首 Bar，调用一次 `mark_prices` (若有) 进行初始权益记录。
3. 记录第一点 `EquityPoint(trade_date, equity)`。

主循环 (直至 `feed.step()` 返回 False):
1. `bar = feed.current`
2. `strategy.on_bar(bar, feed, broker)` —— 可产生交易 (执行价基于当日开盘价 + 滑点)。
3. 收集 `mark_prices` (若提供)；否则默认当前 symbol `bar.close`。
4. `broker.update_marks(marks)`；计算并追加权益点。
5. 推进 feed。

权益曲线: `engine.run()` 返回 `List[EquityPoint]`。

## 6. Stats / 绩效指标
统计指标 | 计算方法
--------|----------------
年化收益率 | `compute_annual_returns(equity_curve)`
最大回撤 | `compute_max_drawdown(equity_curve)`
波动率 | `compute_risk_metrics(equity_curve, risk_free_rate=0.03)['volatility']`
夏普比率 | `compute_risk_metrics(equity_curve, risk_free_rate=0.03)['sharpe']`
胜率 | `compute_win_rate(trade_records)`

## 7. 扩展建议
- 增加分笔撮合 (成交量滑点模型)。
- 引入交易日历 / 盘中数据支持。
- 数据源多资产统一 (独立 DataFeed 多序列版)。
- 组合层风险控制 (最大仓位、行业暴露)。
- 资金曲线多维度统计 (Calmar, Sortino, Turnover)。
- 增加止损 / 止盈 / 再平衡工具函数。

---
## 8. 测试脚本模板
最小化脚本结构示例:
```python
from qs.backtester.data import Bar, DataFeed
from qs.backtester.broker import Broker
from qs.backtester.engine import BacktestEngine
from my_strategy import MyStrategy

bars = [...]  # List[Bar]
feed = DataFeed(bars)
broker = Broker(1_000_000, enable_trade_log=True)  # 无默认 symbol => 多标模式
strategy = MyStrategy(...)
engine = BacktestEngine(feed, broker, strategy)
curve = engine.run()
print('Final Equity', curve[-1].equity)
```

---
## 9. LLM 生成代码提示
- 始终先检查 `feed.prev is None` 以避免首 Bar 使用未来数据。
- 若需要全仓操作优先用 `buy_all_sym` / `sell_all_sym` 简化 sizing。
- 生成多标策略时记得实现 `mark_prices` 以保证权益准确。
- 复用统计函数时需传原始初始资金以便计算 CAGR / Sharpe。

---
若需在本文再加入更多范式（事件驱动、信号缓存、风险控制 API），可追加章节。
