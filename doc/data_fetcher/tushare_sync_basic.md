# A/H 股 + 外汇基础列表同步说明 (stock_basic_a / stock_basic_h / fx_basic)

本文档对应脚本 `src/data_fetcher/tushare_sync_basic.py`，描述其向 DuckDB (`data/data.duckdb`) 建立与增量维护的三张基础资产维度表：
- `stock_basic_a` : A 股基础信息（TuShare 接口 `stock_basic`, doc_id=25）
- `stock_basic_h` : 港股基础信息（TuShare 接口 `hk_basic`, doc_id=191）
- `fx_basic` : 外汇基础信息（TuShare 接口 `fx_obasic`, doc_id=178）

用途：
1. 作为日线行情、复权因子等后续数据同步脚本 (`tushare_sync_daily.py`) 的代码来源（A/H/FX）。
2. 为量化研究 & LLM 问答提供统一的基础资产元数据（名称、行业/分类、上市状态/交易单位等）。

---
## 1. 同步脚本核心逻辑
1. 脚本中 `MARKET_CONFIG` 定义各市场：接口名、字段列表、调用参数、目标表名。
2. 逐市场执行 `_fetch_table`：分页调用（`limit=3000`），直到返回行数 < limit 结束；每页失败重试 `MAX_RETRY=3`，间隔 `SLEEP=0.6s`（≈ 100 次/分钟频控）。
3. 首次落库：直接 `CREATE TABLE ... AS SELECT`。
4. 后续增量：仅比较 `ts_code`，新代码追加；既有代码不更新（“只追加，不改写”）。
5. 对外汇基础 `fx_basic` 同样遵循去重插入，作为 `fx_daily` 动态代码池。
6. 记录日志：每页拉取行数、表整体行数。

优势：实现简单、幂等、无需对比逐字段；适合股票 & 外汇基础列表新增频率较低场景。
局限：既有资产字段（行业、名称、点值等）变更不自动刷新。

---
## 2. 参数与常量
| 变量 | 含义 | 默认 |
|------|------|------|
| DUCKDB_PATH | DuckDB 数据库文件路径 | data/data.duckdb |
| LIMIT | 每次分页拉取条数 | 3000 |
| MAX_RETRY | 每页最大重试次数 | 3 |
| SLEEP | 正常分页间隔 (秒) | 0.6 |

---
## 3. 表结构与字段语义
### 3.1 表：stock_basic_a (A 股)
来源：TuShare `stock_basic` (doc_id=25)。

| 字段 | 说明 | 可能变化 | 示例 |
|------|------|----------|------|
| ts_code | 统一代码 (带后缀) | 否 | 600000.SH |
| symbol | 数字代码（无后缀） | 否 | 600000 |
| name | 证券简称 | 是 | 浦发银行 |
| area | 地域 | 低频 | 上海 |
| industry | 行业分类 | 是 | 银行 |
| cnspell | 简称拼音首字母 | 可能 | PFYX |
| market | 市场类别（主板/创业/科创等） | 低频 | 主板 |
| list_date | 上市日期 YYYYMMDD | 否 | 19991110 |
| act_name | 实际控制人名称 | 是 | 上海国资委 |
| act_ent_type | 实控人企业性质 | 是 | 国有控股 |
| fullname | 公司全称 | 是 | 上海浦东发展银行股份有限公司 |
| enname | 英文全称 | 是 | Shanghai Pudong Development Bank Co.,Ltd |
| exchange | 交易所代码 (SSE/SZSE) | 否 | SSE |
| curr_type | 交易货币 | 低频 | CNY |
| list_status | 上市状态 L=上市 P=暂停 D=退市 | 是 | L |
| delist_date | 退市日期 (若退市) | 是 | (空/日期) |
| is_hs | 是否沪/深港通标的 N/S/H | 是 | H |

主键策略：逻辑上以 `ts_code` 唯一；脚本未显式创建主键约束（DuckDB 自动推断列）。

### 3.2 表：stock_basic_h (港股)
来源：TuShare `hk_basic` (doc_id=191)。

| 字段 | 说明 | 可能变化 | 示例 |
|------|------|----------|------|
| ts_code | 港股统一代码 (带 .HK) | 否 | 00700.HK |
| name | 简称 | 是 | 腾讯控股 |
| fullname | 中文全称 | 是 | 腾讯控股有限公司 |
| enname | 英文全称 | 是 | Tencent Holdings Ltd. |
| cn_spell | 中文简称拼音首字母 | 可能 | TXKG |
| market | 市场（主板/创业等） | 低频 | 主板 |
| list_status | 上市状态 L/P/D | 是 | L |
| list_date | 上市日期 | 否 | 20040616 |
| delist_date | 退市日期 | 是 | (空) |
| trade_unit | 每手股数 | 可能 | 100 |
| isin | ISIN 国际证券代码 | 低频 | KYG875721634 |
| curr_type | 交易货币 | 低频 | HKD |

主键策略：逻辑上 `ts_code` 唯一；同样未显式建唯一约束。

字段差异：A 股使用 `cnspell`，港股为 `cn_spell`；下游查询 / LLM 生成 SQL 需区分。

### 3.3 表：fx_basic (外汇基础)
来源：TuShare `fx_obasic` (doc_id=178)。示例调用：
```python
pro.fx_obasic(**{"exchange": "", "classify": "", "ts_code": "", "limit": "", "offset": ""}, fields=[
    "ts_code","name","classify","exchange","min_unit","max_unit","pip","pip_cost","traget_spread","min_stop_distance","trading_hours","break_time"
])
```

| 字段 | 说明 | 备注 |
|------|------|------|
| ts_code | 货币对代码 | 例如 USDCNH.FXCM |
| name | 简称 | - |
| classify | 分类 | 官方文档：直盘/交叉等 |
| exchange | 交易渠道/报价来源 | - |
| min_unit | 最小交易单位 | 风险/合约控制参考 |
| max_unit | 最大交易单位 | - |
| pip | 点 | 最小报价变动单位 |
| pip_cost | 点值 | 估算盈亏/滑点成本 |
| traget_spread | 目标点差(原文拼写保留) | 可能有拼写错误 (target) |
| min_stop_distance | 最小止损距离 | - |
| trading_hours | 交易时段 | 文档字符串描述 |
| break_time | 休市时间 | - |

主键策略：逻辑上以 `ts_code` 唯一。

---
## 4. 维护策略与建议
| 需求 | 操作方式 | 说明 |
|------|----------|------|
| 捕获字段最新变更（全量刷新） | `DROP TABLE fx_basic;` 然后重跑脚本 | 同适用于 stock_basic_* |
| 仅新增代码 | 直接运行脚本 | 自动识别新 `ts_code` 插入 |
| 支持字段变更自动更新 | 改写 `_upsert` 为 MERGE 或全量替换 | - |
| 扩展新市场/资产类别 | 在 `MARKET_CONFIG` 中新增配置 | 填写 api_name/fields/params/table |

---
## 5. 示例查询
列出外汇基础列表规模：
```sql
SELECT COUNT(*) FROM fx_basic;
```

随机查看 10 个外汇：
```sql
SELECT * FROM fx_basic ORDER BY RANDOM() LIMIT 10;
```

`fx_daily` 可用代码覆盖情况：
```sql
SELECT b.ts_code, COUNT(d.trade_date) AS days
FROM fx_basic b LEFT JOIN fx_daily d USING(ts_code)
GROUP BY 1 ORDER BY days DESC LIMIT 20;
```

---
## 6. 面向 LLM 的使用提示 (新增 FX)
- 外汇代码池：请通过 `fx_basic` 获取完整货币对列表，不再假设只有 USDCNH / USDHKD。
- 若需要新增尚未出现的货币对，请先确认 TuShare 接口返回后重跑本脚本。
- `traget_spread` 为接口原始字段拼写（可能为 target_spread），保持一致以避免字段不存在错误。

---
## 7. 数据质量自检 (FX 补充)
| 目标 | SQL |
|------|-----|
| FX 主键唯一 | `SELECT ts_code,COUNT(*) c FROM fx_basic GROUP BY 1 HAVING c>1;` |
| 缺少关键字段 | `SELECT * FROM fx_basic WHERE name IS NULL LIMIT 20;` |

---
其余 A/H 股相关章节及查询示例与原版本保持不变；如需扩展请继续追加。