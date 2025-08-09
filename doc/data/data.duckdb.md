# DuckDB 数据库总览与 LLM 使用指南 (`data/data.duckdb`)

本文件汇总所有已由同步脚本写入 DuckDB 的核心结构与语义，整合自:
- 基础列表同步: `tushare_sync_basic.py` (文档: `tushare_sync_basic.md`)
- 多市场日频同步: `tushare_sync_daily.py` (文档: `tushare_sync_daily.md`)

目标: 供量化研究 & 大模型 (LLM) 生成 SQL / 回答问题时快速掌握数据范围、字段含义、增量机制与安全使用注意事项。

---
## 1. 表清单与用途概览
| 表名 | 来源接口 | 主键 | 主要字段组 | 用途归类 |
|------|----------|------|------------|---------|
| stock_basic_a | stock_basic (doc_id=25) | ts_code | name/industry/area/list_date/is_hs | A 股证券静态元数据/代码池 |
| stock_basic_h | hk_basic (doc_id=191) | ts_code | name/fullname/trade_unit/list_status | 港股静态元数据/代码池 |
| fx_basic | fx_obasic (doc_id=178) | ts_code | name/classify/exchange/pip/pip_cost | 外汇静态元数据/代码池 |
| fx_daily | fx_daily (doc_id=179) | ts_code+trade_date | bid_*/ask_* | 汇率/跨市场折算 (代码来自 fx_basic 全量) |
| daily_a | daily (doc_id=27) | ts_code+trade_date | OHLC / pct_chg / vol / amount | A 股基础价量与收益 |
| adj_factor_a | adj_factor | ts_code+trade_date | adj_factor | A 股复权串接因子 |
| bak_daily_a | bak_daily (doc_id=255) | ts_code+trade_date | pct_change/turn_over/vol_ratio/float_mv 等 | A 股扩展行为 & 市值流动性因子 |
| daily_h | hk_daily (doc_id=192) | ts_code+trade_date | OHLC / pct_chg / vol / amount | 港股基础价量与收益 |
| adj_factor_h | hk_daily_adj | ts_code+trade_date | adj_factor | 港股复权串接因子 |
| index_daily | index_daily | ts_code+trade_date | open/high/low/close/pct_chg/vol/amount | 国内指数 (当前仅 000300.SH 沪深300) |
| index_global | index_global | ts_code+trade_date | open/close/high/low/pct_chg/swing/vol | 国际指数 (当前仅 HSI 恒生, IXIC 纳指) |
| sync_date | 本地控制 | table_name+ts_code | last_update_date | 每表每代码增量同步进度 |

所有日频表统一使用 `trade_date` (YYYYMMDD, TEXT)。

---
## 2. 字段与语义要点
### 2.1 基础股票表
- `stock_basic_a` 仅“追加新代码”不覆盖旧字段 → 行业/名称等可能滞后。
- `stock_basic_h` 港股字段命名与 A 股不同: `cn_spell` vs A 股的 `cnspell`。
- 字段典型使用: 过滤上市状态 (`list_status='L'`), 行业横截面分组, 建立 A/H 简易映射 (名称前缀匹配)。

### 2.2 外汇基础 (`fx_basic`)
| 字段 | 说明 |
|------|------|
| ts_code | 货币对代码 (USDCNH.FXCM 等) |
| name | 简称 |
| classify | 分类 (直盘/交叉等) |
| exchange | 交易渠道/报价来源 |
| min_unit / max_unit | 最小/最大交易单位 |
| pip | 点 (最小报价变动) |
| pip_cost | 点值 |
| traget_spread | 目标点差 (接口原拼写保留) |
| min_stop_distance | 最小止损距离 |
| trading_hours | 交易时段描述 |
| break_time | 休市时间 |

### 2.3 外汇日线 (`fx_daily`)
| 字段 | 说明 | 备注 |
|------|------|------|
| ts_code | 货币对 | 代码池来自 `fx_basic` (可 CLI 过滤) |
| trade_date | 日期 | - |
| bid_open/close/high/low | 买价四价 | - |
| ask_open/close/high/low | 卖价四价 | - |
| tick_qty | Ticks 数量 | 可能缺失 |

中间价近似: `(bid_close + ask_close)/2`。

### 2.4 国内指数 (`index_daily`)  当前: 000300.SH (沪深300)
| 字段 | 说明 |
|------|------|
| ts_code | 指数代码 |
| trade_date | 日期 |
| open/high/low/close | 当日 OHLC |
| pre_close | 昨收 |
| change | 涨跌额 |
| pct_chg | 涨跌幅(%) |
| vol | 成交量 (接口单位) |
| amount | 成交额 (接口单位) |

### 2.5 国际指数 (`index_global`) 当前: HSI (恒生), IXIC (纳斯达克综合)
| 字段 | 说明 |
|------|------|
| ts_code | 指数代码 |
| trade_date | 日期 |
| open/close/high/low | 当日价格 (字段顺序: open, close, high, low) |
| pre_close | 昨收 |
| change | 涨跌额 |
| pct_chg | 涨跌幅(%) |
| swing | 振幅(%) |
| vol | 成交量 (接口单位) |

### 2.6 日线行情 (`daily_a`, `daily_h`)
| 列 | 说明 | 注意 |
|----|------|------|
| open/high/low/close | 当日价格 | 需与复权因子结合校正 |
| pre_close | 昨日收盘 | 与 change/pct_chg 校验涨跌幅 |
| change | 涨跌额 | 浮点舍入差异正常 |
| pct_chg | 涨跌幅(%) | 与 `bak_daily_a.pct_change` 可能命名/口径不同 |
| vol | 成交量 (手/股) | 市场差异 (A=手, HK=接口定义) |
| amount | 成交额 | A 股千元单位; 港股 HKD (官方文档) |

### 2.7 复权因子 (`adj_factor_a`, `adj_factor_h`)
- 价格复权公式 (前复权): `close_fq = close * adj_factor / MAX(adj_factor) OVER (PARTITION BY ts_code)`。
- 因子通常非递减；若出现下降需排查数据回溯。

### 2.8 A 股扩展 (`bak_daily_a`)
部分重要列:
| 列 | 含义 | 说明 |
|----|------|------|
| pct_change | 涨跌幅(%) | 与 `daily_a.pct_chg` 比较可做口径差异分析 |
| turn_over | 换手率(%) | = 成交量 / 流通股本 (接口计算) |
| vol_ratio | 量比 | 短线交易活跃度因子 |
| float_share/total_share | (万股) | 推导市值或供特征归一化 |
| float_mv/total_mv | (万元) | 直接使用避免重复计算 |
| strength/activity/attack | 行为强度类指标 | 具体算法参见 TuShare 官方描述 |
| interval_3/interval_6 | 区间涨跌幅 | Rolling 派生字段 |

### 2.9 同步控制 (`sync_date`)
| 字段 | 语义 |
|------|------|
| table_name | 目标数据表名 |
| ts_code | 代码 |
| last_update_date | 最近一次“真实写入新交易日”或“补历史”完成的日期 |

刷新策略: 若当日数据尚未出现(接口空) → 不写入 sync_date → 允许晚到补抓。

---
## 3. 增量同步与幂等特性 (LLM 需要了解的运行语义)
1. 日频表主键唯一索引 `(ts_code, trade_date)` + `INSERT OR IGNORE` → 重跑不会重复。
2. `start_date = MAX(trade_date)+1`，无历史用全局 `START_DATE` (默认 20120101)。
3. sync_date 更新条件: 写入了新数据 或 正在补历史 (`start_date < today`)。
4. 当天未出数据的表保持“未完成”状态，可提示用户稍后再同步避免缺口。
5. 基础列表仅追加: 字段不会自动更正 (LLM 回答可提示“属性可能滞后”)。

---
## 4. 常见查询模板
| 目的 | SQL 模板 (示意) |
|------|------------------|
| 沪深300 最近 N 日 | `SELECT * FROM index_daily WHERE ts_code='000300.SH' ORDER BY trade_date DESC LIMIT N;` |
| 恒生 vs 纳指 涨跌幅 | 见每日同步文档 index_global 示例 |
| 指数与基准对齐 | 参考每日同步文档指数对齐查询 |
| 某股最近 N 日日线 | `SELECT * FROM daily_a WHERE ts_code='600000.SH' ORDER BY trade_date DESC LIMIT N;` |
| 汇率折算港股 | 联结 `daily_h` 与 `fx_daily` |
| 数据新鲜度 | `SELECT table_name, MAX(last_update_date) FROM sync_date GROUP BY table_name;` |

---
## 5. 数据质量快速自检 (可供自动化或 LLM 建议)
| 目标 | 示例 SQL |
|------|----------|
| 指数主键重复 | `SELECT ts_code,trade_date,COUNT(*) c FROM index_daily GROUP BY 1,2 HAVING c>1;` |
| 国际指数缺口 | `SELECT ts_code,trade_date FROM index_global EXCEPT SELECT ts_code,trade_date FROM index_global;` |
| FX 日期范围 | `SELECT ts_code,MIN(trade_date),MAX(trade_date),COUNT(*) FROM fx_daily GROUP BY ts_code;` |

---
## 6. LLM 生成/回答策略指引
(补充: 指数覆盖有限，当前仅 000300.SH, HSI, IXIC; 其它需配置后才可查询)

---
## 7. 维护与扩展建议
| 场景 | 操作 |
|------|------|
| 增加国内指数 | 在 `index_daily.ts_codes` 追加 ts_code |
| 增加国际指数 | 在 `index_global.ts_codes` 追加 ts_code |
| 改为动态指数基础表 | 新建 `index_basic` 后改 stock_table 模式 |
| 扩大 FX 范围过滤 | 使用日频脚本 `--fx-codes` 参数 |

---
其余章节如前版本一致。