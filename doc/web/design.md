# 量化策略网站设计方案

## 1. 文档目标

基于 [`doc/web/start.md`](/root/dev/quant_strategy_v2/doc/web/start.md) 的需求，结合当前工程已有的回测引擎、策略实现、SQLite 数据库和回测产物，设计一个可落地的 Web 方案。

本文同时用于约束首版实现，设计需与当前代码落地保持一致。

目标网站入口：

- `https://pangruitao.com/qs`

目标能力：

- 查看和管理当前工程里的策略
- 查看策略净值曲线，并和常用指数对比
- 查看核心绩效指标
- 查看策略当前应持仓标的及凯利建议仓位
- 在界面上勾选多个策略，生成组合并做凯利优化
- 尽量采用简单架构：`FastAPI + HTML + CSS + 少量 Vanilla JS`

---

## 2. 当前工程基线

### 2.1 已有能力

当前仓库已经具备以下基础：

- 行情与元数据统一存放在 `data/data.sqlite`
- 已有通用回测框架 `src/qs/backtester/`
- 已有多类正式策略 `src/qs/strategy/`
- 已有部分研究脚本会输出 `summary / equity_curve / trades / rebalance_history`
- 已有指数与汇率数据，可支持沪深 300、恒生、纳指等对比

当前可直接复用的核心模块：

- 回测运行：[`src/qs/backtester/runner.py`](/root/dev/quant_strategy_v2/src/qs/backtester/runner.py)
- CLI 入口：[`src/qs/backtester/cli.py`](/root/dev/quant_strategy_v2/src/qs/backtester/cli.py)
- 市场数据抽象：[`src/qs/backtester/market.py`](/root/dev/quant_strategy_v2/src/qs/backtester/market.py)
- 回测说明：[`doc/qs/backtester.md`](/root/dev/quant_strategy_v2/doc/qs/backtester.md)
- 数据库说明：[`doc/data/data.sqlite.md`](/root/dev/quant_strategy_v2/doc/data/data.sqlite.md)

### 2.2 当前策略现状

当前策略大致分三类：

- 框架化正式策略
  - `AHPremiumQuarterlyStrategy`
  - `LowPEQuarterlyStrategy`
  - `ETFEqualWeightAnnualStrategy`
  - `ETFMinPremiumWeeklyStrategy`
  - `IgnoredCrowdedAHMonthlyStrategy`
  - `SimpleStrategy`
  - `SimpleStrategy2`
- 研究/实验策略
  - `ignored_stock_strategy.py`
  - `scripts/ignored_buzz_ah_research.py`
- 工程化脚本产物
  - 例如 `data/backtests/ignored_buzz_ah_engine/engine_summary.json`

### 2.3 当前不足

虽然回测基础已经具备，但 Web 首版直接开发仍有 4 个缺口：

1. 缺少“策略注册中心”
2. 缺少统一的“标准快照结果格式”
3. 各策略 `rebalance_history` 结构不统一
4. 还没有 Web 层、应用层数据库和任务层

结论：

- Web 层不应该直接读取每个脚本自己的私有产物
- 必须先定义一层“策略标准化协议 + 结果快照标准”

---

## 3. 设计原则

### 3.1 总体原则

- 保留现有回测引擎，不重写交易和估值逻辑
- Web 层只消费标准化结果，不直接耦合到每个策略实现细节
- 行情数据库与应用数据库分离
- 页面以服务端渲染为主，图表数据通过 JSON 接口异步加载
- 组合优化先做“策略层凯利”，再聚合到证券层；证券层原生凯利作为二期增强

### 3.2 技术原则

- 不引入 React
- 不引入 ORM 作为首版必需依赖
- 尽量复用现有 `sqlite3`/仓库内 SQLite 工具风格
- 前端只使用：
  - `Jinja2`
  - `Vanilla JS`
  - 一个轻量图表库，推荐 `ECharts`

---

## 4. 范围定义

### 4.1 本期范围

- 策略列表与管理
- 策略详情页
- 回测结果展示
- 指数对比
- 当前持仓展示
- 单策略凯利建议仓位
- 多策略组合器
- 组合凯利优化
- 单用户登录
- 管理员权限控制
- SQLite 持久化
- nginx `/qs` 反向代理设计

### 4.2 暂不纳入

- 注册
- 多用户自助管理
- 完整 RBAC 权限体系
- 实盘下单
- 任务分布式调度
- 高频/分钟级别策略
- 复杂前端状态管理框架

---

## 5. 总体架构

## 5.1 架构分层

建议采用 5 层结构：

1. 展示层
   - FastAPI Pages
   - Jinja2 Templates
   - CSS + Vanilla JS + ECharts
2. API 层
   - 提供图表、组合计算、登录会话、任务触发、策略配置接口
3. 领域服务层
   - 策略注册
   - 回测快照生成
   - 指数对齐
   - 凯利优化
   - 组合聚合
   - 用户认证与权限判断
4. 持久化层
   - `data/data.sqlite`：市场数据，只读
   - `data/web.sqlite`：网站应用数据，可读写
5. 任务层
   - 手动刷新
   - 定时生成快照
   - 触发回测并导入结果

### 5.2 推荐架构图

```text
Browser
  -> FastAPI Pages (/qs/*)
  -> FastAPI APIs   (/qs/api/*)

FastAPI
  -> Session / Auth Service
  -> Strategy Registry Service
  -> Snapshot Service
  -> Benchmark Service
  -> Kelly / Composer Service
  -> Web SQLite Repository
  -> Market SQLite Reader

Jobs
  -> run strategy
  -> export standard snapshot
  -> ingest to web.sqlite
```

### 5.3 两个 SQLite 的职责

#### 市场数据库

- 文件：`data/data.sqlite`
- 用途：
  - A 股/港股/ETF/指数/汇率行情
  - 静态证券元数据
  - 回测时的底层数据源

#### 应用数据库

- 文件：`data/web.sqlite`
- 用途：
  - 策略注册信息
  - 用户账号信息
  - 页面配置
  - 回测运行记录
  - 标准化指标
  - 净值曲线缓存
  - 当前持仓快照
  - 组合结果缓存

这样做的原因：

- 避免 Web 写操作污染市场库
- 便于备份、迁移和应用级别重建

---

## 6. 领域模型设计

### 6.1 核心对象

#### StrategyDefinition

表示一个可被网站识别和运行的策略定义。

关键字段：

- `strategy_key`
- `display_name`
- `category`
- `module_path`
- `class_name`
- `feed_type`
- `default_params_json`
- `param_schema_json`
- `default_benchmarks_json`
- `supports_composer`
- `status`

#### StrategyRun

表示某个策略的一次具体运行。

关键字段：

- `run_id`
- `strategy_key`
- `run_tag`
- `params_json`
- `start_date`
- `end_date`
- `initial_cash`
- `status`
- `output_dir`
- `created_at`
- `completed_at`

版本约束：

- 每次运行都必须生成新的 `run_id`
- `run_tag` 必须唯一，建议包含时间戳或参数哈希
- 历史版本只保留最近 `7` 天
- 页面默认只读取“最近一次成功运行”

#### StrategySnapshot

表示供网站展示的标准化结果。

关键字段：

- `run_id`
- `as_of_date`
- `metrics_json`
- `final_equity`
- `cagr`
- `ann_return`
- `ann_vol`
- `sharpe`
- `max_drawdown`
- `drawdown_peak`
- `drawdown_trough`
- `trade_count`
- `rebalance_count`

#### HoldingSnapshot

表示某次运行截至某日的当前持仓。

关键字段：

- `run_id`
- `snapshot_date`
- `symbol`
- `symbol_name`
- `market`
- `price_cny`
- `quantity`
- `market_value`
- `raw_weight`
- `kelly_weight`
- `source_strategy_weight`

#### ComboRun

表示一次多策略组合计算结果。

关键字段：

- `combo_run_id`
- `selected_strategies_json`
- `optimizer_config_json`
- `metrics_json`
- `status`
- `created_at`

#### AppUser

表示网站内的可登录用户。

首版约束：

- 不开放注册
- 只保留一个预置账号：`pp`
- 角色固定为 `admin`
- 密码 `1994188` 只允许以 hash 形式存储

关键字段：

- `username`
- `password_hash`
- `role`
- `display_name`
- `is_active`
- `last_login_at`

---

## 7. SQLite 表设计

以下表建议放在 `data/web.sqlite`。

### 7.1 策略注册与配置

| 表名 | 说明 | 关键字段 |
|---|---|---|
| `strategy_definition` | 策略定义主表 | `strategy_key`, `display_name`, `module_path`, `class_name`, `param_schema_json` |
| `strategy_preset` | 预设参数模板 | `strategy_key`, `preset_name`, `params_json`, `is_default` |
| `strategy_benchmark` | 默认基准配置 | `strategy_key`, `benchmark_code`, `sort_order` |

### 7.2 运行与结果

| 表名 | 说明 | 关键字段 |
|---|---|---|
| `strategy_run` | 单次运行主表 | `run_id`, `strategy_key`, `params_json`, `status`, `output_dir` |
| `strategy_metric` | 结构化指标 | `run_id`, `metric_key`, `metric_value` |
| `strategy_equity_point` | 策略净值序列 | `run_id`, `trade_date`, `nav` |
| `strategy_benchmark_point` | 基准净值序列 | `run_id`, `benchmark_code`, `trade_date`, `nav` |
| `strategy_holding_snapshot` | 当前持仓快照 | `run_id`, `snapshot_date`, `symbol`, `raw_weight`, `kelly_weight` |
| `strategy_rebalance_event` | 调仓记录 | `run_id`, `rebalance_date`, `signal_date`, `payload_json` |

### 7.3 组合结果

| 表名 | 说明 | 关键字段 |
|---|---|---|
| `combo_run` | 组合计算主表 | `combo_run_id`, `selected_strategies_json`, `optimizer_config_json`, `status` |
| `combo_component_weight` | 策略层权重 | `combo_run_id`, `strategy_key`, `raw_weight`, `kelly_weight` |
| `combo_equity_point` | 组合净值序列 | `combo_run_id`, `trade_date`, `nav` |
| `combo_holding_snapshot` | 聚合后证券持仓 | `combo_run_id`, `symbol`, `raw_weight`, `kelly_weight` |

### 7.4 任务、系统状态与账号

| 表名 | 说明 | 关键字段 |
|---|---|---|
| `job_run` | 任务执行记录 | `job_id`, `job_type`, `target_key`, `status`, `started_at`, `ended_at` |
| `app_setting` | 站点配置 | `setting_key`, `setting_value` |
| `app_user` | 登录账号 | `username`, `password_hash`, `role`, `display_name`, `is_active` |

### 7.5 为什么指标单独存储

虽然 `metrics_json` 很方便，但单独建 `strategy_metric` 仍有价值：

- 便于列表页直接排序
- 便于筛选“最大回撤 < x”的策略
- 避免每次都反序列化 JSON

建议：

- 主表保留常用列
- 完整详情保留 `metrics_json`
- 排序/筛选依赖结构化字段

---

## 8. 策略标准化规范

### 8.1 目标

网站不直接依赖“某个策略脚本输出了哪些文件”，而是要求每个策略最终都能产出统一快照。

### 8.2 推荐规范

每个可上线策略必须提供以下能力：

1. 唯一 `strategy_key`
2. 可序列化参数定义
3. 可执行回测
4. 可导出标准化指标
5. 可导出当前持仓
6. 可导出调仓历史
7. 可指定默认基准

### 8.3 推荐协议

建议新增一层 Web 适配协议，而不是强行修改现有所有策略类本身。

建议协议字段：

- `key`
- `display_name`
- `description`
- `category`
- `supports_composer`
- `default_params()`
- `param_schema()`
- `run_snapshot(params, as_of_date) -> StandardSnapshot`

### 8.4 标准快照结构

建议标准快照至少包含：

```json
{
  "strategy_key": "ah_premium_quarterly",
  "run_tag": "ah_premium_q_default",
  "as_of_date": "20260312",
  "params": {},
  "metrics": {
    "cagr": 0.12,
    "ann_return": 0.15,
    "ann_vol": 0.19,
    "sharpe": 0.78,
    "max_drawdown": -0.21
  },
  "equity_curve": [],
  "benchmarks": [],
  "holdings": [],
  "rebalance_history": []
}
```

### 8.5 与当前工程的映射策略

#### 可直接接入的策略

这些策略已经是框架化策略，适合优先纳入：

- `AHPremiumQuarterlyStrategy`
- `LowPEQuarterlyStrategy`
- `ETFEqualWeightAnnualStrategy`
- `ETFMinPremiumWeeklyStrategy`
- `IgnoredCrowdedAHMonthlyStrategy`

#### 需要适配的策略

- `scripts/ignored_crowded_ah_engine_bt.py`
- `scripts/ignored_buzz_ah_research.py`

做法：

- 首版通过 adapter 包装现有脚本产物
- 二期逐步改造成统一策略类 + 统一 runner

### 8.6 调仓历史统一要求

当前不同策略的 `rebalance_history` 结构不同，因此必须统一成标准格式。

标准建议：

- `rebalance_date`
- `signal_date`
- `target_count`
- `targets_json`
- `decision_items_json`

其中 `decision_items_json` 可保留策略特有明细，但外层字段要固定。

---

## 9. 标准快照产物设计

### 9.1 文件落盘路径

建议统一落到：

```text
data/backtests/web_snapshots/{strategy_key}/{run_tag}/
```

### 9.2 标准文件集合

每次回测完成后产出：

- `manifest.json`
- `summary.json`
- `equity_curve.csv`
- `benchmarks.csv`
- `holdings.csv`
- `rebalance_history.json`

### 9.3 为什么保留文件

即使最终数据入库，仍建议保留文件：

- 方便人工排查
- 便于 notebook 或脚本继续复用
- 便于未来做离线静态导出

### 9.4 入库原则

- 文件是“原始快照”
- `web.sqlite` 是“查询加速层”

不建议页面直接读 CSV。

### 9.5 历史版本保留策略

策略快照需要保留历史版本，但只保留最近 `7` 天。

保留原则：

- 每次刷新都生成一个新的版本目录，不覆盖旧文件
- 每次刷新都插入新的 `strategy_run` 及其关联曲线/指标/持仓记录
- 页面默认展示最新成功版本
- 页面中的“运行历史”只展示最近 `7` 天内的版本

文件层规则：

- 标准快照目录保持按 `strategy_key/run_tag` 分目录存储
- 超过 `7` 天的快照目录由清理任务删除

数据库层规则：

- `strategy_run` 及其关联表保留最近 `7` 天数据
- 清理时按 `completed_at` 或 `created_at` 判断过期
- 过期运行删除时，需要级联删除：
  - `strategy_metric`
  - `strategy_equity_point`
  - `strategy_benchmark_point`
  - `strategy_holding_snapshot`
  - `strategy_rebalance_event`

实现建议：

- 每次刷新完成后执行一次该策略的历史清理
- 每日定时任务再做一次全局兜底清理

这样可以保证：

- 能查看短期历史版本，满足回溯和排查
- 不会让 SQLite 和快照目录无限增长
- 页面逻辑始终基于“最新版本优先”

---

## 10. 单策略凯利设计

### 10.1 业务目标

用户在策略详情页需要看到：

- 当前策略原始持仓权重
- 凯利建议总仓位
- 凯利折算后的标的仓位
- 剩余现金比例

### 10.2 建议实现方式

单策略凯利采用策略收益率序列本身估计，而不是对每只股票单独估计。

原因：

- 当前工程已经有策略净值曲线
- 样本粒度统一，易于计算
- 能与多策略组合层保持一致口径

### 10.3 计算口径

对单策略历史收益序列，计算：

- `mu`：平均收益
- `sigma^2`：收益方差

全凯利：

```text
f* = mu / sigma^2
```

首版建议不直接使用满凯利，而使用：

```text
deploy_ratio = clip(half_kelly, 0, max_gross_exposure)
half_kelly = 0.5 * f*
```

默认建议：

- `max_gross_exposure = 1.0`
- 若想支持激进模式，再开放到 `1.2` 或 `1.5`

### 10.4 页面展示逻辑

若某策略当前原始持仓为：

- A 40%
- B 35%
- C 25%

且 `deploy_ratio = 0.6`

则推荐仓位为：

- A 24%
- B 21%
- C 15%
- Cash 40%

### 10.5 风险控制

Kelly 很不稳定，建议首版固定加入：

- 半凯利
- 最大仓位上限
- 最小样本长度限制
- 波动过大时回退到保守仓位

---

## 11. 多策略组合器设计

### 11.1 推荐路线

多策略组合首版采用“策略层凯利 + 证券层聚合”。

即：

1. 先把每个策略视作一个 sleeve
2. 基于多个策略历史收益序列估计组合权重
3. 再把策略权重乘到各自当前持仓上
4. 最终得到证券层仓位占比

这是当前工程最稳妥的方案。

### 11.2 为什么不直接做证券层凯利

直接对所有股票做 Kelly 优化虽然更“纯”，但当前工程里有几个现实问题：

- 各策略信号频率不同
- 各策略持仓更新频率不同
- 没有统一的逐证券 alpha 预估层
- 证券层协方差矩阵更高维、更不稳定

因此建议：

- V1：策略层凯利
- V2：证券层原生 Kelly

### 11.3 首版组合优化公式

对选中的策略收益矩阵，估计：

- `mu`：策略预期收益向量
- `Sigma`：策略收益协方差矩阵

采用约束型 Kelly：

```text
maximize    mu^T w - 0.5 * w^T Sigma w
subject to  w_i >= 0
            sum(w_i) <= 1
            w_i <= max_strategy_weight
```

默认配置：

- 不做做空
- 不默认加杠杆
- 默认半凯利缩放

### 11.4 组合持仓聚合

若某组合中：

- 策略 A 凯利后权重 40%
- 策略 B 凯利后权重 60%

且：

- A 当前持有 `600000.SH` 50%、`000001.SZ` 50%
- B 当前持有 `000001.SZ` 30%、`00700.HK` 70%

则聚合后：

- `600000.SH = 0.4 * 0.5 = 20%`
- `000001.SZ = 0.4 * 0.5 + 0.6 * 0.3 = 38%`
- `00700.HK = 0.6 * 0.7 = 42%`

### 11.5 组合页面需要展示的内容

- 已选策略
- 每个策略的基础指标
- 每个策略的凯利前/后权重
- 聚合后的股票仓位
- 组合净值曲线
- 组合与指数对比
- 组合核心指标

### 11.6 二期增强

二期可考虑真正的证券层优化：

- 用各策略当前入选证券的未来预期收益估计
- 用统一频率历史收益构造证券协方差
- 在证券层直接求约束型 Kelly 权重

但这不建议作为首版依赖。

---

## 12. 登录与权限设计

### 12.1 认证边界

首版只做最小闭环：

- `GET /qs/login`：登录页
- `POST /qs/login`：提交用户名和密码
- `POST /qs/logout`：退出登录
- 使用服务端签名 session cookie 维持登录态

不做：

- 注册
- 找回密码
- 多管理员管理页面
- 第三方 OAuth

### 12.2 首版账号策略

首版仅保留一个预置用户：

- 用户名：`pp`
- 角色：`admin`
- 密码：`1994188`

安全约束：

- `data/web.sqlite` 中只保存 `password_hash`
- 不保存明文密码
- 登录校验基于 PBKDF2-SHA256
- cookie 仅保存最小会话信息，不保存密码 hash

### 12.3 权限矩阵

| 能力 | 未登录 | 非管理员 | 管理员 |
|---|---|---|---|
| 查看 Dashboard | 是 | 是 | 是 |
| 查看完整策略详情 | 是 | 是 | 是 |
| 使用组合器计算组合 | 是 | 是 | 是 |
| 看到“刷新快照”按钮 | 否 | 否 | 是 |
| 调用 `/qs/api/strategies/{strategy_key}/refresh` | 否 | 否 | 是 |

接口约束：

- 未登录调用刷新接口返回 `401`
- 已登录但非管理员返回 `403`

### 12.4 页面表现

- Sidebar 展示当前登录状态
- 未登录用户看到“管理员登录”入口
- 策略列表页和详情页仅管理员渲染刷新按钮
- 组合器页面继续保持公开可用

---

## 13. 页面设计

### 13.1 页面结构

建议站点包含 6 个页面：

1. 首页 Dashboard
2. 策略列表页
3. 策略详情页
4. 组合器页面
5. 运行记录/任务页面
6. 登录页

### 13.2 首页 `/qs/`

模块建议：

- 数据最新更新时间
- 可用策略数量
- 最近刷新成功/失败任务
- 策略排行榜
  - 年化收益 Top N
  - Sharpe Top N
  - 最大回撤最优 Top N
- 快速入口
  - 查看所有策略
  - 创建组合
  - 管理员登录 / 退出登录

### 13.3 策略列表页 `/qs/strategies`

每个策略卡片展示：

- 策略名称
- 类别
- 默认参数摘要
- 最近运行时间
- CAGR
- Sharpe
- Max Drawdown
- 当前持仓数
- 是否支持组合

支持：

- 按类别筛选
- 按状态筛选
- 按指标排序
- 管理员手动触发刷新

### 13.4 策略详情页 `/qs/strategies/{strategy_key}`

页面模块建议：

- 策略简介
- 参数配置
- 最近回测区间
- 指标卡片
- 策略净值 vs 基准曲线
- 年度收益表
- 当前持仓表
- 凯利建议表
- 最近调仓记录
- 运行历史

权限要求：

- 所有人都能查看完整策略信息
- 仅管理员看到“刷新快照”按钮

当前持仓表字段建议：

- `symbol`
- `name`
- `market`
- `raw_weight`
- `kelly_weight`
- `price_cny`
- `market_value`

### 13.5 组合器页面 `/qs/composer`

交互设计：

- 左侧策略列表，可勾选
- 右侧参数区
  - 回测窗口
  - Kelly 缩放
  - 单策略最大权重
  - 是否允许现金
- 点击“计算组合”

结果区展示：

- 组合指标卡片
- 组合曲线
- 策略层权重表
- 证券层聚合仓位表
- 指数对比

### 13.6 运行记录页 `/qs/runs`

用于管理和排查：

- 最近任务状态
- 输出目录
- 错误信息
- 运行参数
- 查看快照详情

### 13.7 登录页 `/qs/login`

页面模块建议：

- 用户名输入框
- 密码输入框
- 登录错误提示
- 当前权限说明

---

## 14. API 设计

页面以服务端渲染为主，但图表和组合计算建议走 JSON API。

### 14.1 页面接口

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/qs/` | 首页 |
| `GET` | `/qs/strategies` | 策略列表 |
| `GET` | `/qs/strategies/{strategy_key}` | 策略详情 |
| `GET` | `/qs/composer` | 组合器页面 |
| `GET` | `/qs/runs` | 任务页 |
| `GET` | `/qs/login` | 登录页 |
| `POST` | `/qs/login` | 提交登录 |
| `POST` | `/qs/logout` | 退出登录 |

### 14.2 数据接口

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/qs/api/strategies` | 获取策略列表 |
| `GET` | `/qs/api/strategies/{strategy_key}` | 获取策略基础信息 |
| `GET` | `/qs/api/strategies/{strategy_key}/latest` | 获取最新快照 |
| `GET` | `/qs/api/runs/{run_id}/equity` | 获取净值曲线 |
| `GET` | `/qs/api/runs/{run_id}/benchmarks` | 获取基准曲线 |
| `GET` | `/qs/api/runs/{run_id}/holdings` | 获取持仓 |
| `GET` | `/qs/api/runs/{run_id}/rebalances` | 获取调仓记录 |
| `POST` | `/qs/api/strategies/{strategy_key}/refresh` | 管理员触发刷新 |
| `POST` | `/qs/api/composer/evaluate` | 计算组合 |
| `POST` | `/qs/api/composer/save` | 保存组合模板 |

刷新接口权限约束：

- 未登录：`401`
- 非管理员：`403`
- 管理员：`200`

### 14.3 组合接口请求体建议

```json
{
  "strategies": [
    {"strategy_key": "ah_premium_quarterly"},
    {"strategy_key": "low_pe_quarterly"}
  ],
  "optimizer": {
    "kelly_scale": 0.5,
    "max_strategy_weight": 0.5,
    "allow_cash": true
  },
  "benchmarks": ["000300.SH", "HSI", "IXIC"]
}
```

---

## 15. 应用目录设计

建议新增：

```text
src/qs/web/
  app.py
  config.py
  routes/
    pages.py
    api.py
  services/
    auth_service.py
    strategy_registry.py
    snapshot_service.py
    benchmark_service.py
    kelly_service.py
    composer_service.py
  repo/
    web_db.py
  models/
    dto.py
  templates/
    base.html
    dashboard.html
    login.html
    strategies.html
    strategy_detail.html
    composer.html
    runs.html
  static/
    css/site.css
    js/charts.js

scripts/
  web_run.py
  web_refresh_snapshots.py
```

### 15.1 为什么单独建 `src/qs/web`

- 职责边界清晰
- 不污染现有 backtester
- 未来可独立部署/测试

---

## 16. 任务流设计

### 16.1 快照生成流程

建议统一流程：

1. 从 `strategy_definition` 读取策略配置
2. 触发统一 runner 或 adapter
3. 输出标准快照文件
4. 导入 `web.sqlite`
5. 更新 `strategy_run` 状态
6. 清理该策略超过 `7` 天的历史版本

### 16.2 刷新类型

建议支持 3 种：

- 全量刷新
- 单策略刷新
- 指定运行参数刷新

### 16.3 触发方式

首版建议：

- 管理员通过页面按钮手动触发
- crontab 定时刷新

不建议首版引入 Celery。

### 16.4 建议的定时任务

- 每日收盘后刷新全部策略快照
- 每周刷新 ETF/慢频策略
- 部署后手动补一次全量快照
- 每日执行一次历史版本清理，只保留最近 `7` 天

---

## 17. 基准对比设计

### 17.1 默认基准

根据当前数据库情况，首版建议支持：

- `000300.SH`：沪深 300
- `HSI`：恒生指数
- `IXIC`：纳斯达克综合指数

### 17.2 对齐规则

统一原则：

- 以策略净值序列日期为主轴
- 基准向前取最近可用交易日
- 所有曲线归一化到 `1.0`

### 17.3 数据来源

- A 股指数：`index_daily`
- 海外/港股指数：`index_global`

---

## 18. nginx 与部署设计

### 18.1 应用部署建议

推荐：

- `uvicorn` 单进程起步
- `systemd` 托管
- nginx 反向代理

### 18.2 FastAPI 路由前缀

应用需要支持部署在 `/qs` 子路径下，因此应按子路径部署设计。

建议：

- 页面和 API 都挂在 `/qs`
- 静态资源走 `/qs/static`

### 18.3 nginx 示例

```nginx
location /qs/ {
    proxy_pass http://127.0.0.1:8000/;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-Prefix /qs;
}
```

FastAPI 侧需要考虑：

- `root_path="/qs"`
- 静态资源 URL 不能写死为根路径

### 18.4 健康检查

建议提供：

- `/qs/healthz`

用于 nginx / systemd 检查服务状态。

---

## 19. 首版交付建议

### 19.1 V1 交付内容

建议首版只做以下闭环：

1. 策略注册中心
2. 标准快照格式
3. 单用户管理员登录
4. 管理员刷新权限控制
5. 单策略详情页
6. 指数对比
7. 当前持仓展示
8. 单策略半凯利建议
9. 多策略组合器
10. 策略层凯利组合
11. `/qs` nginx 部署

### 19.2 V1 明确不做

- 证券层原生 Kelly
- 注册
- 完整 RBAC 权限系统
- 历史多版本参数回放 UI
- 分布式任务队列

### 19.3 V2 再做

- 证券层凯利优化
- 更复杂的风险约束
- 策略比较页
- 参数回测实验页
- 缓存和分页优化

---

## 20. 风险与对策

### 20.1 Kelly 不稳定

风险：

- 样本短时结果剧烈波动
- 极端值会导致建议仓位过大

对策：

- 默认半凯利
- 强制上限
- 样本数不足时不给建议

### 20.2 SQLite 并发写入

风险：

- 页面读与任务写冲突

对策：

- 市场库只读
- 应用库集中写
- 刷新任务串行化

### 20.3 旧策略产物格式不统一

风险：

- Web 展示需要写很多特殊分支

对策：

- 必须先做标准快照层
- 旧策略只允许通过 adapter 接入

### 20.4 子路径部署问题

风险：

- `/qs` 下静态资源、跳转和 API 地址失效

对策：

- 设计阶段就按 `root_path=/qs` 处理

---

## 21. 最终建议

结合当前工程，最合理的路线不是直接“先写页面”，而是按下面顺序推进：

1. 建 `strategy_definition + standard snapshot` 规范
2. 先打通 2 到 3 个正式策略
3. 再做 FastAPI + Jinja2 页面
4. 组合器首版按“策略层凯利”落地
5. 最后逐步把旧研究脚本收敛到统一协议

这样可以最大化复用当前仓库已有能力，同时把 Web 复杂度控制在一个很薄的应用层里。
