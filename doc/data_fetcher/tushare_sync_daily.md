# TuShare 日频增量同步（A/H/ETF/FX/指数）`tushare_sync_daily.py`

说明：本文件为 `doc/data/tushare_sync_daily.md` 的同内容副本，方便在 `doc/data_fetcher/` 目录下查阅。

对应脚本：`src/data_fetcher/tushare_sync_daily.py`  
目标数据库：`data/data.sqlite`  
增量控制表：`sync_date`（按 `table_name + ts_code` 记录“当日是否已处理”）

该脚本按“表 + ts_code”粒度独立增量同步（互不影响），覆盖：
- `daily_a`：A 股日线（TuShare `daily`）
- `adj_factor_a`：A 股复权因子（TuShare `adj_factor`）
- `bak_daily_a`：A 股特色扩展行情（TuShare `bak_daily`）
- `daily_h`：港股日线（TuShare `hk_daily`）
- `adj_factor_h`：港股复权因子（TuShare `hk_daily_adj`）
- `etf_daily`：ETF 日线行情（TuShare `fund_daily`，代码来源 `etf_basic`）
- `adj_factor_etf`：ETF 复权因子（TuShare `fund_adj`，代码来源 `etf_basic`，含 `discount_rate` 溢价率字段）
- `index_daily_etf`：ETF 对应指数日线（TuShare `index_daily`，代码来源 `etf_basic.index_code`，目标表内 `ts_code=指数代码`）
- `fx_daily`：外汇日线（TuShare `fx_daily`，代码来源 `fx_basic`）
- `index_daily`：国内指数日线（TuShare `index_daily`，当前仅 `000300.SH`）
- `index_global`：国际/全球指数日线（TuShare `index_global`，当前仅 `HSI`、`IXIC`）

---

## 0. 推荐运行方式

脚本依赖 `src/` 可导入，推荐直接使用项目入口：
- `bash scripts/fetch_data.sh`
- 或 `python scripts/fetch_data.py`

如只跑“日频增量”（不跑基础表/不 vacuum）：
- `python -c "import _bootstrap; from data_fetcher.tushare_sync_daily import sync; sync()"`

注意：日频脚本默认依赖基础表 `stock_basic_a / stock_basic_h / etf_basic / fx_basic` 作为代码池；若缺少会报错并提示先运行基础同步脚本。

---

## 1. 增量策略（与脚本实现一致）

### 1.1 代码池来源
由 `TABLE_CONFIG` 决定：
- A 股相关：从 `stock_basic_a` 读取 `ts_code`
- 港股相关：从 `stock_basic_h` 读取 `ts_code`
- ETF：从 `etf_basic` 读取 `ts_code`
- ETF 对应指数：从 `etf_basic` 读取 `index_code`，去重后作为 `ts_code` 拉取 `index_daily`
- 外汇：从 `fx_basic` 读取 `ts_code`（可通过 CLI `--fx-codes` 过滤）
- 指数：当前使用显式 `ts_codes` 列表（见 `TABLE_CONFIG['index_daily'/'index_global']`）

### 1.2 起止日期计算（逐 ts_code）
对每个表、每个 `ts_code`：
1) 读取目标表内该标的的最新日期：`latest = MAX(trade_date)`  
2) `start_date = latest + 1`；若该标的无数据则使用全局 `START_DATE`  
3) `end_date = today`（运行当日）

`START_DATE` 的来源：`data_fetcher.settings.get_start_date("20120101")`  
可通过 `.env` 设置 `start_date=YYYYMMDD`（也兼容 `START_DATE`）。

### 1.3 “当日去重跑”控制：`sync_date`
表结构（脚本自动创建）：
```sql
CREATE TABLE IF NOT EXISTS sync_date (
  table_name       VARCHAR,
  ts_code          VARCHAR,
  last_update_date VARCHAR,
  PRIMARY KEY (table_name, ts_code)
);
```

控制规则（逐表、逐标的）：
- 若 `sync_date.last_update_date == today`：跳过（当日已处理）
- 若 `start_date > today`：直接将 `sync_date` 标记为 `today` 并跳过
- 若 `start_date < today`：
  - 即使接口返回空，也会将 `sync_date` 标记为 `today`，避免同日重复跑历史区间
- 若 `start_date == today`：
  - 当天接口返回空时不标记 `sync_date`，允许当日后续重跑继续尝试补抓“今日数据”

---

## 2. 拉取与落库机制

### 2.1 接口请求、分页与失败重试
统一由 `_fetch_api_with_paging()` 完成：
- 分页：使用 `offset + limit` 翻页（每表可在 `TABLE_CONFIG` 里覆盖 `limit`）
- 重试：每页最多 `MAX_RETRY=3`
- 节流：全局默认 `DEFAULT_SLEEP=0.01`，失败等待默认 `DEFAULT_SLEEP_ON_FAIL=2`（每表可覆盖 `sleep / sleep_on_fail`）

兼容兜底：如遇到 `AttributeError` 且包含 `is_unique`（部分环境 pandas/TuShare 组合触发），会切换为“按自然日 trade_date”逐日抓取并拼接结果。

### 2.2 幂等写入（INSERT OR IGNORE）
写入由 `_upsert()` 完成：
- 首次写入会按返回数据列创建目标表（`df.head(0).to_sql(...)`）
- 对包含 `ts_code + trade_date` 的表，会创建唯一索引（并在建索引前做去重兜底）：
  - `"{table}_uq"` on (`ts_code`, `trade_date`)
- 实际插入使用 `INSERT OR IGNORE`（由 `qs.sqlite_utils.insert_df_ignore()` 实现），重复行自动忽略

### 2.3 事务与批量提交
- 每张表同步时显式 `BEGIN`，每处理 `BATCH_SIZE=100` 个标的提交一次，最后 `COMMIT`。

---

## 3. CLI 用法（脚本内置）

脚本支持选择同步哪些表，以及外汇代码过滤（见 `main()` / `_parse_args()`）：
- `-t/--table`：指定目标表（可多选）；缺省=全部
  - 可选值：`daily_a adj_factor_a bak_daily_a daily_h adj_factor_h etf_daily adj_factor_etf index_daily_etf fx_daily index_daily index_global`
- `--fx-codes`：过滤外汇代码（源自 `fx_basic`）
  - `all`：使用 `fx_basic` 全量（默认行为）
  - `none`：不拉取 `fx_daily`
  - 或显式列出若干代码：如 `USDCNH.FXCM EURUSD.FXCM`

示例（需要保证 `src/` 可导入）：
```bash
PYTHONPATH=src python -m data_fetcher.tushare_sync_daily -t daily_a adj_factor_a
PYTHONPATH=src python -m data_fetcher.tushare_sync_daily -t fx_daily --fx-codes USDCNH.FXCM
PYTHONPATH=src python -m data_fetcher.tushare_sync_daily --fx-codes none
```

---

## 4. 表配置速查（来自 `TABLE_CONFIG`）

| 本地表 | TuShare 接口 | 代码池来源 |
|---|---|---|
| `daily_a` | `daily` | `stock_basic_a` |
| `adj_factor_a` | `adj_factor` | `stock_basic_a` |
| `bak_daily_a` | `bak_daily` | `stock_basic_a` |
| `daily_h` | `hk_daily` | `stock_basic_h` |
| `adj_factor_h` | `hk_daily_adj` | `stock_basic_h` |
| `etf_daily` | `fund_daily` | `etf_basic` |
| `adj_factor_etf` | `fund_adj` | `etf_basic` |
| `index_daily_etf` | `index_daily` | `etf_basic.index_code`（去重后作为 `ts_code`） |
| `fx_daily` | `fx_daily` | `fx_basic` |
| `index_daily` | `index_daily` | 显式 `ts_codes`（当前 `000300.SH`） |
| `index_global` | `index_global` | 显式 `ts_codes`（当前 `HSI`、`IXIC`） |

如需扩展指数覆盖：
- 在 `src/data_fetcher/tushare_sync_daily.py` 的 `TABLE_CONFIG['index_daily'/'index_global']['ts_codes']` 中追加即可。

---

## 5. SQL 自检示例

检查增量是否在推进（以沪深300为例）：
```sql
SELECT MIN(trade_date) AS min_d, MAX(trade_date) AS max_d, COUNT(*) AS n
FROM index_daily
WHERE ts_code='000300.SH';
```

查看当日哪些标的已“跑过一次”（不等价于一定写入了数据）：
```sql
SELECT table_name, COUNT(*) AS n
FROM sync_date
WHERE last_update_date=strftime('%Y%m%d', 'now')
GROUP BY 1
ORDER BY 2 DESC;
```
